use super::process::{self, ProcInfo};
use crate::model::{
    AgentSession, ChatMessage, ChatRole, ChildProcess, FileAccess, FileOp, LaunchSurface,
    RateLimitInfo, SessionStatus, ToolCall, MAX_CHAT_MESSAGES, MAX_FILE_ACCESSES,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

mod native;

/// Codex discovery stays local to this collector. One worker performs native
/// queries and compatibility scans; JSONL parsing remains on the collection path.
pub struct CodexCollector {
    pub last_rate_limit: Option<RateLimitInfo>,
    scanner: DiscoveryScanner,
}

#[derive(Clone, Copy)]
enum CodexProcessContext {
    Exclusive { pid: u32, is_exec: bool },
    Unknown,
}

impl CodexProcessContext {
    fn pid(self) -> Option<u32> {
        match self {
            Self::Exclusive { pid, .. } => Some(pid),
            Self::Unknown => None,
        }
    }
}

#[derive(Clone, Default)]
struct Discovery {
    roots: Vec<PathBuf>,
    native: HashMap<PathBuf, native::Snapshot>,
    queried_hosts: HashSet<u32>,
    fds: HashMap<u32, Vec<PathBuf>>,
    recent: HashMap<PathBuf, Vec<PathBuf>>,
}

impl Discovery {
    fn apply_native(
        &mut self,
        root: &Path,
        result: std::io::Result<native::Snapshot>,
        hosts: &[u32],
    ) -> bool {
        match result {
            Ok(mut snapshot) => {
                if let Some(old) = self.native.get(root) {
                    for thread in &mut snapshot.threads {
                        if thread.status == native::Status::Unknown && thread.cwd.is_empty() {
                            if let Some(prior) = old.threads.iter().find(|old| old.id == thread.id)
                            {
                                *thread = prior.clone();
                                thread.status = native::Status::Unknown;
                            }
                        }
                    }
                }
                if let Some(pid) = snapshot.host_pid {
                    self.queried_hosts.insert(pid);
                }
                self.native.insert(root.to_path_buf(), snapshot);
                true
            }
            Err(_) => {
                if let Some(old) = self.native.get_mut(root) {
                    if old.host_pid.is_some_and(|pid| hosts.contains(&pid)) {
                        for thread in &mut old.threads {
                            thread.status = native::Status::Unknown;
                        }
                    } else {
                        self.native.remove(root);
                    }
                }
                false
            }
        }
    }
}

struct ScanRequest {
    pids: Vec<u32>,
    hosts: Vec<u32>,
    slow_tick: bool,
}

struct DiscoveryScanner {
    cached: Discovery,
    tx: Sender<ScanRequest>,
    rx: Receiver<Discovery>,
    in_flight: bool,
    child_pid: Arc<AtomicU32>,
}

impl DiscoveryScanner {
    fn new(roots: Vec<PathBuf>) -> Self {
        let (tx, requests) = mpsc::channel::<ScanRequest>();
        let (results, rx) = mpsc::channel();
        let cached = Discovery {
            roots,
            ..Discovery::default()
        };
        let mut state = cached.clone();
        let child_pid = Arc::new(AtomicU32::new(0));
        #[cfg(not(windows))]
        let scan_child = child_pid.clone();
        std::thread::spawn(move || {
            let mut previous_pids = Vec::new();
            let mut last_recent = None;
            while let Ok(request) = requests.recv() {
                if request.slow_tick || previous_pids != request.pids {
                    #[cfg(not(windows))]
                    if let Some(fds) = super::mcp::map_pid_to_rollouts_with_timeout_and_pid_slot(
                        &request.pids,
                        Duration::from_secs(2),
                        Some(scan_child.clone()),
                    ) {
                        state.fds = fds;
                    }
                    // Windows filesystem guesses are not open-file evidence.
                    previous_pids.clone_from(&request.pids);
                }
                state.fds.retain(|pid, _| request.pids.contains(pid));
                for path in state.fds.values().flatten() {
                    if let Some(root) = rollout_root(path) {
                        if !state.roots.contains(&root) {
                            state.roots.push(root);
                        }
                    }
                }
                state.queried_hosts.clear();
                let mut unavailable = HashSet::new();
                for root in state.roots.clone() {
                    let result = native::query(&root);
                    if !state.apply_native(&root, result, &request.hosts) {
                        unavailable.insert(root);
                    }
                }
                let legacy_host = request
                    .hosts
                    .iter()
                    .any(|pid| !state.queried_hosts.contains(pid));
                let scan_recent = legacy_host || (cfg!(windows) && !request.pids.is_empty());
                state
                    .recent
                    .retain(|root, _| scan_recent && unavailable.contains(root));
                if scan_recent
                    && last_recent.is_none_or(|at: Instant| at.elapsed() >= Duration::from_secs(60))
                {
                    for root in &unavailable {
                        state
                            .recent
                            .insert(root.clone(), recent_rollouts(&root.join("sessions")));
                    }
                    last_recent = Some(Instant::now());
                }
                if results.send(state.clone()).is_err() {
                    break;
                }
            }
        });
        Self {
            cached,
            tx,
            rx,
            in_flight: false,
            child_pid,
        }
    }

    fn update(&mut self, request: ScanRequest) {
        if let Ok(result) = self.rx.try_recv() {
            self.cached = result;
            self.in_flight = false;
        }
        if !self.in_flight && self.tx.send(request).is_ok() {
            self.in_flight = true;
        }
    }

    fn wait(&mut self, timeout: Duration) {
        if self.in_flight {
            if let Ok(result) = self.rx.recv_timeout(timeout) {
                self.cached = result;
                self.in_flight = false;
            }
        }
    }
}

impl Drop for DiscoveryScanner {
    fn drop(&mut self) {
        super::mcp::kill_rollout_scan_child(self.child_pid.swap(0, Ordering::SeqCst));
    }
}

fn canonical_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn rollout_root(path: &Path) -> Option<PathBuf> {
    let sessions = path.ancestors().nth(4)?;
    (sessions.file_name()? == "sessions")
        .then(|| canonical_path(sessions.parent().unwrap_or(sessions)))
}

fn is_recent(path: &Path) -> bool {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .is_ok_and(|mtime| {
            std::time::SystemTime::now()
                .duration_since(mtime)
                .unwrap_or_default()
                .as_secs()
                < 1800
        })
}

/// This scan only discovers uncertain compatibility candidates. Runtime-backed
/// sessions never pass through an mtime filter. Do not follow directory symlinks.
fn recent_rollouts(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            files.extend(recent_rollouts(&path));
        } else if kind.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
            && is_recent(&path)
        {
            files.push(canonical_path(&path));
        }
    }
    files
}

impl CodexCollector {
    pub fn new() -> Self {
        let mut roots = vec![dirs::home_dir().unwrap_or_default().join(".codex")];
        if let Some(root) = std::env::var_os("CODEX_HOME").filter(|root| !root.is_empty()) {
            roots.push(PathBuf::from(root));
        }
        let mut roots: Vec<_> = roots.iter().map(|root| canonical_path(root)).collect();
        roots.sort();
        roots.dedup();
        Self {
            last_rate_limit: None,
            scanner: DiscoveryScanner::new(roots),
        }
    }

    fn collect_sessions(&mut self, shared: &super::SharedProcessData) -> Vec<AgentSession> {
        let mut pids: Vec<_> =
            Self::find_codex_pids_from_shared(&shared.process_info, &shared.mcp_server_pids)
                .into_iter()
                .map(|(pid, _)| pid)
                .collect();
        let hosts = Self::find_codex_desktop_pids_from_shared(
            &shared.process_info,
            &shared.mcp_server_pids,
        );
        pids.extend(&hosts);
        pids.sort_unstable();
        self.scanner.update(ScanRequest {
            pids,
            hosts,
            slow_tick: shared.slow_tick,
        });
        self.sessions_from_discovery(shared)
    }

    fn sessions_from_discovery(&mut self, shared: &super::SharedProcessData) -> Vec<AgentSession> {
        let discovery = &self.scanner.cached;
        let suppressed: HashSet<_> = shared
            .mcp_owned_rollouts
            .iter()
            .map(|path| canonical_path(path))
            .collect();
        let mut paths = HashMap::new();
        for (pid, files) in &discovery.fds {
            let Some(proc) = shared.process_info.get(pid) else {
                continue;
            };
            if shared.mcp_server_pids.contains(pid)
                || discovery.queried_hosts.contains(pid)
                || !process::cmd_has_binary(&proc.command, "codex")
            {
                continue;
            }
            let files: HashSet<_> = files.iter().map(|path| canonical_path(path)).collect();
            let exclusive = files.len() == 1
                && !proc.command.contains(" app-server")
                && !proc.command.contains(" mcp-server");
            for path in files {
                if suppressed.contains(&path) || (!exclusive && !is_recent(&path)) {
                    continue;
                }
                let context = if exclusive {
                    CodexProcessContext::Exclusive {
                        pid: *pid,
                        is_exec: proc.command.contains(" exec"),
                    }
                } else {
                    CodexProcessContext::Unknown
                };
                // Two exclusive processes holding one file are ambiguous as well.
                paths
                    .entry(path)
                    .and_modify(|existing: &mut CodexProcessContext| {
                        if existing.pid() != context.pid() {
                            *existing = CodexProcessContext::Unknown;
                        }
                    })
                    .or_insert(context);
            }
        }
        let mut recent = HashSet::new();
        for path in discovery.recent.values().flatten() {
            let path = canonical_path(path);
            if !suppressed.contains(&path) && is_recent(&path) && !paths.contains_key(&path) {
                recent.insert(path.clone());
                paths.insert(path, CodexProcessContext::Unknown);
            }
        }
        let mut native_paths = HashSet::new();
        for snapshot in discovery.native.values() {
            for thread in &snapshot.threads {
                if thread.is_guardian() || thread.status == native::Status::NotLoaded {
                    continue;
                }
                if let Some(path) = &thread.path {
                    let path = canonical_path(path);
                    if !suppressed.contains(&path) {
                        native_paths.insert(path.clone());
                        paths.entry(path).or_insert(CodexProcessContext::Unknown);
                    }
                }
            }
        }
        // One parse per canonical path, irrespective of discovery source.
        let parsed: HashMap<_, _> = paths
            .keys()
            .map(|path| (path.clone(), parse_codex_jsonl(path)))
            .collect();
        let mut sessions = HashMap::new();
        let mut latest_limit: Option<RateLimitInfo> = None;
        for (path, context) in &paths {
            let Some(result) = parsed[path].as_ref() else {
                continue;
            };
            if native_paths.contains(path) && context.pid().is_none() {
                continue;
            }
            if recent.contains(path) && !native_paths.contains(path) && !result.is_codex_desktop() {
                continue;
            }
            let root = rollout_root(path)
                .unwrap_or_else(|| discovery.roots.first().cloned().unwrap_or_default());
            let (session, limit) = Self::build_session(*context, result.clone(), &root, shared);
            remember_limit(&mut latest_limit, limit);
            sessions.insert((root, session.session_id.clone()), session);
        }
        for (root, snapshot) in &discovery.native {
            for thread in &snapshot.threads {
                if thread.is_guardian() || thread.status == native::Status::NotLoaded {
                    continue;
                }
                let path = thread.path.as_ref().map(|path| canonical_path(path));
                if path.as_ref().is_some_and(|path| suppressed.contains(path)) {
                    continue;
                }
                let mut result = path
                    .as_ref()
                    .and_then(|path| parsed.get(path))
                    .and_then(|parsed| parsed.as_ref())
                    .filter(|parsed| parsed.session_id == thread.id)
                    .cloned()
                    .unwrap_or_default();
                result.session_id.clone_from(&thread.id);
                if !thread.cwd.is_empty() {
                    result.cwd = super::sanitize_terminal_text(&thread.cwd);
                }
                if thread.created_at > 0 {
                    result.started_at = thread.created_at.saturating_mul(1000);
                }
                if let Some(model) = &thread.model {
                    result.model = super::sanitize_terminal_text(model);
                }
                if let Some(effort) = &thread.reasoning_effort {
                    result.effort = super::sanitize_terminal_text(effort);
                }
                if !thread.cli_version.is_empty() {
                    result.version = super::sanitize_terminal_text(&thread.cli_version);
                }
                if result.initial_prompt.is_empty() {
                    result.initial_prompt =
                        super::sanitize_terminal_text(&super::redact_secrets(&thread.preview));
                }
                let context = path
                    .as_ref()
                    .and_then(|path| paths.get(path))
                    .copied()
                    .unwrap_or(CodexProcessContext::Unknown);
                let current_task = result.current_task.clone();
                let (mut session, limit) = Self::build_session(context, result, root, shared);
                apply_native_status(&mut session, &thread.status);
                if session.status == SessionStatus::Executing && !current_task.is_empty() {
                    session.current_tasks = vec![current_task];
                }
                remember_limit(&mut latest_limit, limit);
                sessions.insert((root.clone(), thread.id.clone()), session);
            }
        }
        if let Some(limit) = &latest_limit {
            if self
                .last_rate_limit
                .as_ref()
                .is_none_or(|old| old.updated_at != limit.updated_at)
            {
                super::rate_limit::write_codex_cache(limit);
            }
        }
        self.last_rate_limit = latest_limit;
        let mut sessions: Vec<_> = sessions
            .into_values()
            .filter(|session| session.status != SessionStatus::Done)
            .collect();
        sessions.sort_by(|a, b| {
            b.started_at
                .cmp(&a.started_at)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });
        sessions
    }

    #[cfg(test)]
    fn load_session_with_rate_limit(
        &self,
        process_ctx: CodexProcessContext,
        jsonl_path: &Path,
        process_info: &HashMap<u32, ProcInfo>,
        children_map: &HashMap<u32, Vec<u32>>,
        ports: &HashMap<u32, Vec<u16>>,
    ) -> Option<(AgentSession, Option<RateLimitInfo>)> {
        let shared = super::SharedProcessData {
            process_info: process_info
                .iter()
                .map(|(&pid, info)| {
                    (
                        pid,
                        ProcInfo {
                            pid: info.pid,
                            ppid: info.ppid,
                            rss_kb: info.rss_kb,
                            cpu_pct: info.cpu_pct,
                            command: info.command.clone(),
                        },
                    )
                })
                .collect(),
            children_map: children_map.clone(),
            ports: ports.clone(),
            slow_tick: false,
            mcp_server_pids: HashSet::new(),
            mcp_owned_rollouts: HashSet::new(),
            mcp_suppress: true,
        };
        Some(Self::build_session(
            process_ctx,
            parse_codex_jsonl(jsonl_path)?,
            self.scanner
                .cached
                .roots
                .first()
                .map(PathBuf::as_path)
                .unwrap_or(Path::new(".")),
            &shared,
        ))
    }

    fn build_session(
        process_ctx: CodexProcessContext,
        result: CodexJSONLResult,
        root: &Path,
        shared: &super::SharedProcessData,
    ) -> (AgentSession, Option<RateLimitInfo>) {
        let process_info = &shared.process_info;
        let children_map = &shared.children_map;
        let ports = &shared.ports;
        let pid = process_ctx.pid();
        let is_exec = matches!(
            process_ctx,
            CodexProcessContext::Exclusive { is_exec: true, .. }
        );
        let proc = pid.and_then(|p| process_info.get(&p));
        let mem_mb = proc.map(|p| p.rss_kb / 1024).unwrap_or(0);
        let display_pid = pid.unwrap_or(0);

        let project_name = process::last_path_segment(&result.cwd)
            .unwrap_or("?")
            .to_string();

        // Status detection
        // Note: Codex interactive sessions emit task_complete after every turn,
        // so task_complete alone does NOT mean the session is finished when PID is alive.
        // However, for exec (one-shot) sessions, task_complete means truly done.
        let pid_alive = proc.is_some();
        // Mirrors Claude: trust the trailing-event-is-user signal alone.
        // Codex tool outputs flow through response_item, not user_message,
        // so model_generating only flips on real prompts.
        let status = if pid.is_none() {
            SessionStatus::Unknown
        } else if !pid_alive || (is_exec && result.task_complete) {
            SessionStatus::Done
        } else {
            let has_active_child = pid.is_some_and(|p| {
                process::has_active_descendant(p, children_map, process_info, 5.0)
            });
            if result.task_complete || result.waiting_for_user {
                SessionStatus::Waiting
            } else if result.pending_since_ms > 0 {
                SessionStatus::Executing
            } else if result.model_generating {
                SessionStatus::Thinking
            } else if has_active_child {
                SessionStatus::Executing
            } else {
                SessionStatus::Waiting
            }
        };

        // Current task from last tool use
        // For exec (one-shot) sessions, task_complete means truly finished.
        // For interactive sessions, task_complete fires after every turn — ignore it.
        let current_tasks = if matches!(status, SessionStatus::Unknown) {
            vec!["unknown".to_string()]
        } else if matches!(status, SessionStatus::Done) {
            vec!["finished".to_string()]
        } else if matches!(status, SessionStatus::Waiting) {
            vec!["waiting for input".to_string()]
        } else if !result.current_task.is_empty() {
            vec![result.current_task]
        } else {
            vec!["thinking...".to_string()]
        };

        // Context window percentage from token usage
        let context_percent = if result.context_window > 0 && result.last_context_tokens > 0 {
            (result.last_context_tokens as f64 / result.context_window as f64) * 100.0
        } else {
            0.0
        };

        // Children: collect all descendants recursively (not just direct children)
        // so we catch grandchild processes that listen on ports.
        let mut children = Vec::new();
        if let Some(p) = pid {
            let mut stack: Vec<u32> = children_map.get(&p).cloned().unwrap_or_default();
            let mut visited = std::collections::HashSet::new();
            while let Some(cpid) = stack.pop() {
                if !visited.insert(cpid) {
                    continue;
                }
                if let Some(cproc) = process_info.get(&cpid) {
                    let port = ports.get(&cpid).and_then(|v| v.first().copied());
                    children.push(ChildProcess {
                        pid: cpid,
                        command: cproc.command.clone(),
                        mem_kb: cproc.rss_kb,
                        port,
                    });
                }
                if let Some(grandchildren) = children_map.get(&cpid) {
                    stack.extend(grandchildren);
                }
            }
        }

        // Git stats: populated by MultiCollector on slow ticks
        let (git_added, git_modified) = (0, 0);
        let rate_limit = result.rate_limit.clone();

        (
            AgentSession {
                agent_cli: "codex",
                launch_surface: LaunchSurface::Cli,
                pid: display_pid,
                session_id: result.session_id,
                cwd: result.cwd,
                project_name,
                started_at: result.started_at,
                status,
                model: result.model,
                effort: result.effort,
                context_percent,
                total_input_tokens: result.total_input,
                total_output_tokens: result.total_output,
                total_cache_read: result.total_cache_read,
                total_cache_create: 0, // Codex doesn't report cache write
                turn_count: result.turn_count,
                current_tasks,
                mem_mb,
                version: result.version,
                git_branch: result.git_branch,
                git_added,
                git_modified,
                token_history: result.token_history,
                context_history: vec![],
                compaction_count: 0,
                context_window: result.context_window,
                subagents: vec![],
                mem_file_count: 0,
                mem_line_count: 0,
                children,
                initial_prompt: result.initial_prompt,
                first_assistant_text: String::new(),
                chat_messages: result.chat_messages,
                tool_calls: result.tool_calls,
                pending_since_ms: result.pending_since_ms,
                thinking_since_ms: result.thinking_since_ms,
                file_accesses: result.file_accesses,
                config_root: super::abbrev_path(root),
            },
            rate_limit,
        )
    }

    /// Find PIDs of running codex processes from shared process data (no extra ps call).
    /// Returns (pid, is_exec) tuples — `is_exec` is true for one-shot `codex exec` runs.
    /// PIDs in `mcp_server_pids` are skipped so `codex mcp-server` processes
    /// are reported via the MCP servers panel instead.
    fn find_codex_pids_from_shared(
        process_info: &HashMap<u32, ProcInfo>,
        mcp_server_pids: &HashSet<u32>,
    ) -> Vec<(u32, bool)> {
        let mut pids = Vec::new();
        for (pid, info) in process_info {
            if mcp_server_pids.contains(pid) {
                continue;
            }
            let cmd = &info.command;
            let is_exec = cmd.contains(" exec");
            let is_codex = process::cmd_has_binary(cmd, "codex");
            if is_codex && !cmd.contains(" app-server") && !cmd.contains("grep") {
                pids.push((*pid, is_exec));
            }
        }

        // Windows npm/Git shims can create a chain like:
        // sh.exe -> node.exe ...\codex.js -> codex.exe.
        // Once the real codex child exists, keep that child and drop wrapper
        // ancestors; otherwise Windows rollout fallback maps each candidate PID
        // to a different recent JSONL file and historical sessions look live.
        let candidates = pids.clone();
        pids.retain(|(pid, _)| {
            process::cmd_first_token_has_binary(
                process_info
                    .get(pid)
                    .map(|info| info.command.as_str())
                    .unwrap_or_default(),
                "codex",
            ) || !candidates.iter().any(|(other_pid, _)| {
                *other_pid != *pid && process::is_descendant_of(*other_pid, *pid, process_info)
            })
        });

        pids
    }

    /// Find Codex Desktop app-server host PIDs. Desktop is kept separate from
    /// CLI discovery because a single app-server PID can hold many rollout fds.
    pub(crate) fn find_codex_desktop_pids_from_shared(
        process_info: &HashMap<u32, ProcInfo>,
        mcp_server_pids: &HashSet<u32>,
    ) -> Vec<u32> {
        let mut pids = Vec::new();
        for (pid, info) in process_info {
            if mcp_server_pids.contains(pid) {
                continue;
            }
            let cmd = &info.command;
            if process::cmd_has_binary(cmd, "codex")
                && cmd.contains(" app-server")
                && !cmd.contains("grep")
            {
                pids.push(*pid);
            }
        }
        pids.sort_unstable();
        pids
    }
}

impl Default for CodexCollector {
    fn default() -> Self {
        Self::new()
    }
}

fn remember_limit(latest: &mut Option<RateLimitInfo>, limit: Option<RateLimitInfo>) {
    if let Some(limit) = limit {
        if latest
            .as_ref()
            .is_none_or(|old| limit.updated_at > old.updated_at)
        {
            *latest = Some(limit);
        }
    }
}

fn apply_native_status(session: &mut AgentSession, status: &native::Status) {
    use native::Status;
    let (state, task) = match status {
        Status::Idle => (SessionStatus::Waiting, Some("waiting for input")),
        Status::Active { flags } if flags.iter().any(|flag| flag == "waitingOnApproval") => {
            (SessionStatus::Waiting, Some("waiting for approval"))
        }
        Status::Active { flags } if flags.iter().any(|flag| flag == "waitingOnUserInput") => {
            (SessionStatus::Waiting, Some("waiting for user input"))
        }
        Status::Active { .. } if session.pending_since_ms > 0 => (SessionStatus::Executing, None),
        Status::Active { .. } => (SessionStatus::Thinking, Some("thinking...")),
        Status::SystemError => (SessionStatus::Error, Some("codex system error")),
        // Missing metadata/status cannot override a verified exclusive process.
        Status::Unknown if session.pid != 0 => return,
        Status::Unknown => (
            SessionStatus::Unknown,
            Some("Codex runtime status unavailable"),
        ),
        Status::NotLoaded => return,
    };
    session.status = state;
    if let Some(task) = task {
        session.current_tasks = vec![task.to_string()];
    }
    if session.status != SessionStatus::Executing {
        session.pending_since_ms = 0;
    }
    if session.status != SessionStatus::Thinking {
        session.thinking_since_ms = 0;
    }
}

impl super::AgentCollector for CodexCollector {
    fn collect(&mut self, shared: &super::SharedProcessData) -> Vec<AgentSession> {
        self.collect_sessions(shared)
    }

    fn live_rate_limit(&self) -> Option<RateLimitInfo> {
        self.last_rate_limit
            .clone()
            .or_else(super::rate_limit::read_codex_cache)
    }

    fn wait_for_discovery(&mut self, timeout: Duration) {
        self.scanner.wait(timeout);
    }
}

/// Parsed result from a Codex rollout JSONL file.
#[derive(Clone)]
struct CodexJSONLResult {
    session_id: String,
    cwd: String,
    originator: String,
    started_at: u64,
    model: String,
    /// Reasoning effort setting from turn_context: "minimal" | "low" | "medium" | "high".
    /// Tracks the most recent value — users can change `/effort` mid-session.
    effort: String,
    version: String,
    git_branch: String,
    context_window: u64,
    turn_count: u32,
    current_task: String,
    task_complete: bool,
    /// True while a Codex turn is active, from the user prompt until
    /// `task_complete`. Intermediate progress messages and tool calls do not
    /// end the turn.
    model_generating: bool,
    last_activity: std::time::SystemTime,
    initial_prompt: String,
    chat_messages: Vec<ChatMessage>,
    /// Input tokens excluding cached input, matching AgentSession's additive
    /// token accounting where cache reads are stored separately.
    total_input: u64,
    total_output: u64,
    total_cache_read: u64,
    last_context_tokens: u64,
    token_history: Vec<u64>,
    /// Rate limit info from the latest token_count event.
    rate_limit: Option<RateLimitInfo>,
    /// Timeline of standard and custom tool calls extracted from response items.
    tool_calls: Vec<ToolCall>,
    /// Earliest start timestamp among currently open tool calls.
    pending_since_ms: u64,
    /// True when an open tool call is explicitly waiting for the user.
    waiting_for_user: bool,
    /// Timestamp when the current model-thinking segment began.
    thinking_since_ms: u64,
    /// Files touched, from the item_completed schema (Codex ≥ ~0.149).
    /// The old schema never carried file information.
    file_accesses: Vec<FileAccess>,
}

impl Default for CodexJSONLResult {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            cwd: String::new(),
            originator: String::new(),
            started_at: 0,
            model: String::from("-"),
            effort: String::new(),
            version: String::new(),
            git_branch: String::new(),
            context_window: 0,
            turn_count: 0,
            current_task: String::new(),
            task_complete: false,
            model_generating: false,
            last_activity: std::time::UNIX_EPOCH,
            initial_prompt: String::new(),
            chat_messages: Vec::new(),
            total_input: 0,
            total_output: 0,
            total_cache_read: 0,
            last_context_tokens: 0,
            token_history: Vec::new(),
            rate_limit: None,
            tool_calls: Vec::new(),
            pending_since_ms: 0,
            waiting_for_user: false,
            thinking_since_ms: 0,
            file_accesses: Vec::new(),
        }
    }
}

impl CodexJSONLResult {
    fn is_codex_desktop(&self) -> bool {
        // The Desktop discovery path also handles CLI sessions (including
        // those launched in herdr) whose rollouts are held by an app-server.
        matches!(
            self.originator.as_str(),
            "Codex Desktop" | "codex_work_desktop" | "codex_vscode" | "codex-tui"
        )
    }
}

fn event_timestamp_ms(val: &Value) -> Option<u64> {
    val["timestamp"]
        .as_str()
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .and_then(|dt| u64::try_from(dt.timestamp_millis()).ok())
}

fn value_to_tool_arg(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }
    if let Some(items) = value.as_array() {
        let parts: Vec<&str> = items.iter().filter_map(|item| item.as_str()).collect();
        if parts.is_empty() {
            return None;
        }
        if parts.len() >= 3 && parts[0] == "bash" && parts[1] == "-lc" {
            return Some(parts[2].to_string());
        }
        return Some(parts.join(" "));
    }
    if value.is_number() || value.is_boolean() {
        return Some(value.to_string());
    }
    None
}

fn sanitize_tool_arg(arg: &str) -> String {
    let redacted = super::redact_secrets(arg);
    redacted.chars().take(120).collect()
}

/// Joined text of an item's `content` array. The schema is not consistent
/// about casing ("Text" on AgentMessage, "text" on UserMessage), so any
/// object with a string `text` field counts.
fn concat_item_text(content: &Value) -> String {
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| p["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// One completed item from the new rollout schema (Codex ≥ ~0.149).
///
/// UserMessage/AgentMessage carry the chat, CommandExecution and Extension
/// are the tool timeline, and FileChange is the only place file writes appear
/// (the old schema never reported files at all).
fn handle_item_completed(payload: &Value, ts: u64, result: &mut CodexJSONLResult) {
    let item = &payload["item"];
    match item["type"].as_str() {
        Some("UserMessage") => {
            result.task_complete = false;
            result.model_generating = true;
            result.thinking_since_ms = ts;
            let text = concat_item_text(&item["content"]);
            if !text.is_empty() {
                if result.initial_prompt.is_empty() {
                    result.initial_prompt = clean_chat_text(&text, 120);
                }
                push_chat_message(
                    &mut result.chat_messages,
                    ChatRole::User,
                    clean_chat_text(&text, 500),
                );
            }
        }
        Some("AgentMessage") => {
            result.turn_count += 1;
            // Progress messages do not end a turn. New rollouts can signal
            // completion with final_answer even without a task_complete event.
            if item["phase"].as_str() == Some("final_answer") {
                result.task_complete = true;
                result.model_generating = false;
                result.thinking_since_ms = 0;
            }
            let text = concat_item_text(&item["content"]);
            push_chat_message(
                &mut result.chat_messages,
                ChatRole::Assistant,
                clean_chat_text(&text, 500),
            );
        }
        Some("CommandExecution") => {
            if result.model_generating {
                result.thinking_since_ms = ts;
            }
            let started = item["started_at_ms"]
                .as_u64()
                .or_else(|| payload["started_at_ms"].as_u64())
                .unwrap_or(ts);
            let completed = item["completed_at_ms"]
                .as_u64()
                .or_else(|| payload["completed_at_ms"].as_u64())
                .unwrap_or(started);
            // Raw argv and parsed commands can contain scripts, file bodies,
            // or credentials. Display only a known operation and a path.
            let parsed = &item["parsed_cmd"][0];
            let name = match parsed["type"].as_str() {
                Some("read") => "read",
                Some("write") => "write",
                Some("search") => "search",
                _ => "exec",
            };
            let arg = parsed["path"]
                .as_str()
                .map(clean_item_path)
                .unwrap_or_default();
            if result.tool_calls.len() < 500 {
                result.tool_calls.push(ToolCall {
                    name: name.to_string(),
                    arg,
                    duration_ms: completed.saturating_sub(started),
                });
            }
            if let Some(entries) = item["parsed_cmd"].as_array() {
                for pc in entries {
                    let Some(path) = pc["path"].as_str() else {
                        continue;
                    };
                    let op = match pc["type"].as_str() {
                        Some("write") => FileOp::Write,
                        _ => FileOp::Read,
                    };
                    push_file_access(result, path, op);
                }
            }
        }
        Some("FileChange") => {
            if result.model_generating {
                result.thinking_since_ms = ts;
            }
            if let Some(changes) = item["changes"].as_object() {
                for (path, change) in changes {
                    let op = match change["type"].as_str() {
                        Some("add") => FileOp::Write,
                        _ => FileOp::Edit,
                    };
                    push_file_access(result, path, op);
                    if result.tool_calls.len() < 500 {
                        let short = process::last_path_segment(path).unwrap_or(path);
                        result.tool_calls.push(ToolCall {
                            name: "edit".to_string(),
                            arg: clean_item_path(short),
                            duration_ms: 0,
                        });
                    }
                }
            }
        }
        Some("Extension") => {
            if result.model_generating {
                result.thinking_since_ms = ts;
            }
            // Extension queries are opaque content, like custom tool inputs.
            if result.tool_calls.len() < 500 {
                result.tool_calls.push(ToolCall {
                    name: "extension".to_string(),
                    arg: String::new(),
                    duration_ms: 0,
                });
            }
        }
        _ => {}
    }
}

fn clean_item_path(path: &str) -> String {
    let safe = super::sanitize_terminal_text(path);
    super::redact_secrets(&safe).chars().take(512).collect()
}

fn push_file_access(result: &mut CodexJSONLResult, path: &str, op: FileOp) {
    result.file_accesses.push(FileAccess {
        path: clean_item_path(path),
        operation: op,
        turn_index: result.turn_count,
    });
    let len = result.file_accesses.len();
    if len > MAX_FILE_ACCESSES {
        result.file_accesses.drain(..len - MAX_FILE_ACCESSES);
    }
}

fn push_chat_message(messages: &mut Vec<ChatMessage>, role: ChatRole, text: String) {
    if text.is_empty() {
        return;
    }
    messages.push(ChatMessage { role, text });
    let len = messages.len();
    if len > MAX_CHAT_MESSAGES {
        messages.drain(..len - MAX_CHAT_MESSAGES);
    }
}

fn clean_chat_text(raw: &str, max: usize) -> String {
    let cleaned = raw
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with("```"))
        .collect::<Vec<_>>()
        .join(" ");
    let terminal_safe = super::sanitize_terminal_text(&cleaned);
    let redacted = super::redact_secrets(&terminal_safe);
    redacted.chars().take(max).collect()
}

fn parse_codex_tool_arg(arguments: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(arguments) else {
        return String::new();
    };

    for key in ["file_path", "path"] {
        if let Some(raw) = value[key].as_str() {
            let short = process::last_path_segment(raw).unwrap_or(raw);
            return sanitize_tool_arg(short);
        }
    }

    for key in ["cmd", "command", "chars", "target", "session_id"] {
        if let Some(raw) = value_to_tool_arg(&value[key]) {
            return sanitize_tool_arg(&raw);
        }
    }

    if let Some(obj) = value.as_object() {
        for val in obj.values() {
            if let Some(raw) = value_to_tool_arg(val) {
                return sanitize_tool_arg(&raw);
            }
        }
    }

    String::new()
}

fn parse_codex_tool_session_id(arguments: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(arguments).ok()?;
    let raw = &value["session_id"];
    if let Some(s) = raw.as_str() {
        return Some(s.to_string());
    }
    raw.as_u64().map(|n| n.to_string())
}

fn running_process_session_id(output: &str) -> Option<String> {
    let marker = "Process running with session ID ";
    let after = output
        .lines()
        .find_map(|line| line.trim_start().strip_prefix(marker))?;
    let id = after.split_whitespace().next()?;
    if id.is_empty() {
        None
    } else {
        Some(
            id.trim_matches(|c: char| !c.is_ascii_alphanumeric())
                .to_string(),
        )
    }
}

fn output_reports_process_exit(output: &str) -> bool {
    output
        .lines()
        .any(|line| line.trim_start().starts_with("Process exited"))
}

fn tool_waits_for_user(name: &str) -> bool {
    matches!(name, "request_user_input" | "AskUserQuestion")
}

fn close_codex_tool_call(
    call_id: &str,
    end_ms: u64,
    tool_calls: &mut [ToolCall],
    call_indices: &HashMap<String, usize>,
    call_starts: &mut HashMap<String, u64>,
    pending_tasks: &mut Vec<(String, String)>,
) {
    if let Some(start_ms) = call_starts.remove(call_id) {
        if let Some(idx) = call_indices.get(call_id).copied() {
            if let Some(tool_call) = tool_calls.get_mut(idx) {
                tool_call.duration_ms = end_ms.saturating_sub(start_ms);
            }
        }
    }
    pending_tasks.retain(|(id, _)| id != call_id);
}

/// Parse a Codex rollout-*.jsonl file.
///
/// Event types:
/// - session_meta: session ID, cwd, version, git
/// - event_msg.task_started: context window size
/// - event_msg.token_count: rate limits (handled at app level)
/// - event_msg.user_message: user prompt
/// - event_msg.agent_message: turn count
/// - event_msg.task_complete: session done
/// - response_item (function_call/custom_tool_call): current tool use
/// - turn_context: model, effort
fn parse_codex_jsonl(path: &Path) -> Option<CodexJSONLResult> {
    let file = fs::File::open(path).ok()?;
    let mut reader = BufReader::new(file);

    let mut result = CodexJSONLResult::default();
    let mut call_indices: HashMap<String, usize> = HashMap::new();
    let mut call_starts: HashMap<String, u64> = HashMap::new();
    let mut call_names: HashMap<String, String> = HashMap::new();
    let mut write_stdin_targets: HashMap<String, String> = HashMap::new();
    let mut running_exec_by_session: HashMap<String, String> = HashMap::new();
    let mut pending_tasks: Vec<(String, String)> = Vec::new();

    // Match Claude transcript cap: a malformed/hostile line beyond this size
    // aborts the scan to prevent OOM. take(MAX+1) physically bounds the read.
    const MAX_LINE_BYTES: usize = 10 * 1024 * 1024;
    let mut line_buf = String::new();
    loop {
        line_buf.clear();
        match reader
            .by_ref()
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_line(&mut line_buf)
        {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        // Cap hit without a newline — skip this file's remainder.
        if line_buf.len() > MAX_LINE_BYTES && !line_buf.ends_with('\n') {
            break;
        }
        let line = line_buf.trim();
        if line.is_empty() {
            continue;
        }

        let val: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue, // partial line at EOF or malformed
        };

        // Update last_activity from timestamp
        if let Some(ts_str) = val["timestamp"].as_str() {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                let sys_time = std::time::UNIX_EPOCH
                    + std::time::Duration::from_millis(dt.timestamp_millis() as u64);
                if sys_time > result.last_activity {
                    result.last_activity = sys_time;
                }
            }
        }

        match val["type"].as_str() {
            Some("session_meta") => {
                let payload = &val["payload"];
                // Guardian rollouts are internal approval reviews, not user sessions.
                if payload["source"]["subagent"]["other"].as_str() == Some("guardian") {
                    return None;
                }
                if let Some(id) = payload["id"].as_str() {
                    result.session_id = id.to_string();
                }
                if let Some(cwd) = payload["cwd"].as_str() {
                    result.cwd = cwd.to_string();
                }
                if let Some(originator) = payload["originator"].as_str() {
                    result.originator = originator.to_string();
                }
                if let Some(ver) = payload["cli_version"].as_str() {
                    result.version = ver.to_string();
                }
                // started_at from timestamp
                if let Some(ts) = payload["timestamp"].as_str() {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
                        result.started_at = dt.timestamp_millis() as u64;
                    }
                }
                // Git branch
                if let Some(branch) = payload["git"]["branch"].as_str() {
                    result.git_branch = branch.to_string();
                }
            }

            Some("event_msg") => {
                let payload = &val["payload"];
                match payload["type"].as_str() {
                    Some("task_started") => {
                        result.task_complete = false;
                        result.model_generating = true;
                        result.thinking_since_ms = event_timestamp_ms(&val).unwrap_or(0);
                        if let Some(cw) = payload["model_context_window"].as_u64() {
                            result.context_window = cw;
                        }
                    }
                    // Newer Codex versions replaced the
                    // user_message/agent_message/function_call vocabulary
                    // with item_completed wrappers.
                    Some("item_completed") => {
                        let ts = event_timestamp_ms(&val).unwrap_or(0);
                        handle_item_completed(payload, ts, &mut result);
                    }
                    Some("turn_aborted") => {
                        result.task_complete = true;
                        result.model_generating = false;
                        result.thinking_since_ms = 0;
                    }
                    Some("user_message") => {
                        result.task_complete = false;
                        result.model_generating = true;
                        result.thinking_since_ms = event_timestamp_ms(&val).unwrap_or(0);
                        if let Some(msg) = payload["message"].as_str() {
                            if result.initial_prompt.is_empty() {
                                let truncated: String = msg.chars().take(120).collect();
                                result.initial_prompt = super::redact_secrets(&truncated);
                            }
                            push_chat_message(
                                &mut result.chat_messages,
                                ChatRole::User,
                                clean_chat_text(msg, 500),
                            );
                        }
                    }
                    Some("token_count") => {
                        let info = &payload["info"];
                        // Codex input_tokens already includes cached_input_tokens.
                        // Store only the non-cached input portion so
                        // AgentSession::total_tokens() does not double-count cache.
                        let total = &info["total_token_usage"];
                        if total.is_object() {
                            let inp = total["input_tokens"].as_u64().unwrap_or(0);
                            let out = total["output_tokens"].as_u64().unwrap_or(0);
                            let cache = total["cached_input_tokens"]
                                .as_u64()
                                .or_else(|| total["cache_read_input_tokens"].as_u64())
                                .unwrap_or(0);
                            result.total_input = inp.saturating_sub(cache);
                            result.total_output = out;
                            result.total_cache_read = cache;
                        }
                        // Use last_token_usage input as the current context window.
                        // cached_input_tokens is a subset of input_tokens, not extra
                        // context after compaction.
                        let last = &info["last_token_usage"];
                        if last.is_object() {
                            let inp = last["input_tokens"].as_u64().unwrap_or(0);
                            let out = last["output_tokens"].as_u64().unwrap_or(0);
                            result.last_context_tokens = inp;
                            if result.token_history.len() < 10_000 {
                                result.token_history.push(inp + out);
                            }
                        }
                        // Context window may also appear inside info
                        if let Some(cw) = info["model_context_window"].as_u64() {
                            result.context_window = cw;
                        }
                        // Rate limits: assign to short/long slots based on window_minutes.
                        // Plus plans: primary=5h(300min), secondary=7d(10080min).
                        // Free plans: primary can be a longer window, such as 30d(43200min).
                        let rl = &payload["rate_limits"];
                        if rl.is_object() && is_account_level_codex_rate_limit(rl) {
                            let event_secs = val["timestamp"]
                                .as_str()
                                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                                .map(|dt| dt.timestamp() as u64);
                            let mut info = RateLimitInfo {
                                source: "codex".to_string(),
                                updated_at: event_secs,
                                ..Default::default()
                            };
                            for slot in &["primary", "secondary"] {
                                let w = &rl[slot];
                                if !w.is_object() {
                                    continue;
                                }
                                let mins = w["window_minutes"].as_u64().unwrap_or(0);
                                let pct = w["used_percent"].as_f64();
                                let resets = w["resets_at"].as_u64();
                                if mins <= 300 {
                                    info.five_hour_pct = pct;
                                    info.five_hour_resets_at = resets;
                                    info.five_hour_window_minutes = Some(mins);
                                } else {
                                    info.seven_day_pct = pct;
                                    info.seven_day_resets_at = resets;
                                    info.seven_day_window_minutes = Some(mins);
                                }
                            }
                            result.rate_limit = Some(info);
                        }
                    }
                    Some("agent_message") => {
                        result.turn_count += 1;
                        if let Some(msg) = payload["message"].as_str() {
                            push_chat_message(
                                &mut result.chat_messages,
                                ChatRole::Assistant,
                                clean_chat_text(msg, 500),
                            );
                        }
                    }
                    Some("task_complete") => {
                        result.task_complete = true;
                        result.model_generating = false;
                        result.thinking_since_ms = 0;
                    }
                    Some(event_type) if event_type.ends_with("_end") => {
                        if let Some(call_id) = payload["call_id"].as_str() {
                            let end_ms = event_timestamp_ms(&val).unwrap_or(0);
                            close_codex_tool_call(
                                call_id,
                                end_ms,
                                &mut result.tool_calls,
                                &call_indices,
                                &mut call_starts,
                                &mut pending_tasks,
                            );
                            if result.model_generating && pending_tasks.is_empty() {
                                result.thinking_since_ms = end_ms;
                            }
                        }
                    }
                    _ => {}
                }
            }

            Some("response_item") => {
                let payload = &val["payload"];
                let item_type = payload["type"].as_str();
                // Newer Codex versions route code-mode tools through
                // custom_tool_call; both forms have the same lifecycle.
                if matches!(item_type, Some("function_call" | "custom_tool_call")) {
                    if let Some(name) = payload["name"].as_str() {
                        let arg = if item_type == Some("custom_tool_call") {
                            // Custom inputs are opaque scripts or patch bodies,
                            // which can contain private file contents. Show only
                            // the tool name; token-prefix redaction is insufficient.
                            String::new()
                        } else {
                            payload["arguments"]
                                .as_str()
                                .map(parse_codex_tool_arg)
                                .unwrap_or_default()
                        };

                        let task = if arg.is_empty() {
                            name.to_string()
                        } else {
                            format!("{} {}", name, arg)
                        };

                        result.thinking_since_ms = 0;

                        if let Some(call_id) = payload["call_id"].as_str() {
                            let start_ms = event_timestamp_ms(&val).unwrap_or(0);
                            call_names.insert(call_id.to_string(), name.to_string());
                            if name == "write_stdin" {
                                if let Some(session_id) = payload["arguments"]
                                    .as_str()
                                    .and_then(parse_codex_tool_session_id)
                                {
                                    write_stdin_targets.insert(call_id.to_string(), session_id);
                                }
                            }
                            call_starts.insert(call_id.to_string(), start_ms);
                            pending_tasks.retain(|(id, _)| id != call_id);
                            pending_tasks.push((call_id.to_string(), task));
                            if result.tool_calls.len() < 500 {
                                let idx = result.tool_calls.len();
                                result.tool_calls.push(ToolCall {
                                    name: name.to_string(),
                                    arg,
                                    duration_ms: 0,
                                });
                                call_indices.insert(call_id.to_string(), idx);
                            }
                        }
                    }
                } else if matches!(
                    item_type,
                    Some("function_call_output" | "custom_tool_call_output")
                ) {
                    if let Some(call_id) = payload["call_id"].as_str() {
                        let end_ms = event_timestamp_ms(&val).unwrap_or(0);
                        let output = payload["output"].as_str().unwrap_or_default();
                        match (item_type, call_names.get(call_id).map(String::as_str)) {
                            (Some("custom_tool_call_output"), _) => {
                                close_codex_tool_call(
                                    call_id,
                                    end_ms,
                                    &mut result.tool_calls,
                                    &call_indices,
                                    &mut call_starts,
                                    &mut pending_tasks,
                                );
                            }
                            (_, Some("exec_command")) => {
                                if let Some(session_id) = running_process_session_id(output) {
                                    running_exec_by_session.insert(session_id, call_id.to_string());
                                } else {
                                    close_codex_tool_call(
                                        call_id,
                                        end_ms,
                                        &mut result.tool_calls,
                                        &call_indices,
                                        &mut call_starts,
                                        &mut pending_tasks,
                                    );
                                }
                            }
                            (_, Some("write_stdin")) => {
                                close_codex_tool_call(
                                    call_id,
                                    end_ms,
                                    &mut result.tool_calls,
                                    &call_indices,
                                    &mut call_starts,
                                    &mut pending_tasks,
                                );
                                if output_reports_process_exit(output) {
                                    if let Some(exec_call_id) =
                                        write_stdin_targets.get(call_id).and_then(|session_id| {
                                            running_exec_by_session.remove(session_id)
                                        })
                                    {
                                        close_codex_tool_call(
                                            &exec_call_id,
                                            end_ms,
                                            &mut result.tool_calls,
                                            &call_indices,
                                            &mut call_starts,
                                            &mut pending_tasks,
                                        );
                                    }
                                }
                            }
                            _ => {
                                close_codex_tool_call(
                                    call_id,
                                    end_ms,
                                    &mut result.tool_calls,
                                    &call_indices,
                                    &mut call_starts,
                                    &mut pending_tasks,
                                );
                            }
                        }
                        if result.model_generating && pending_tasks.is_empty() {
                            result.thinking_since_ms = end_ms;
                        }
                    }
                }
            }

            Some("turn_context") => {
                let payload = &val["payload"];
                if let Some(m) = payload["model"].as_str() {
                    result.model = m.to_string();
                }
                // Effort may change mid-session via /effort — always take the latest.
                if let Some(e) = payload["effort"].as_str() {
                    result.effort = e.to_string();
                }
                if let Some(cw) = payload["model_context_window"].as_u64() {
                    result.context_window = cw;
                }
            }

            _ => {}
        }
    }

    if result.session_id.is_empty() {
        return None;
    }

    result.current_task = pending_tasks
        .last()
        .map(|(_, task)| task.clone())
        .unwrap_or_default();
    result.pending_since_ms = call_starts.values().copied().min().unwrap_or(0);
    result.waiting_for_user = call_starts.keys().any(|call_id| {
        call_names
            .get(call_id)
            .is_some_and(|name| tool_waits_for_user(name))
    });
    if !result.model_generating {
        result.thinking_since_ms = 0;
    }

    Some(result)
}

fn is_account_level_codex_rate_limit(rate_limits: &Value) -> bool {
    matches!(rate_limits["limit_id"].as_str(), Some("codex") | None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::{Duration, SystemTime};

    const SESSION_META: &str = r#"{"type":"session_meta","timestamp":"2026-03-28T15:00:00Z","payload":{"id":"sess-123","cwd":"/home/user/project","cli_version":"0.1.5","timestamp":"2026-03-28T15:00:00Z","git":{"branch":"feature/x"}}}"#;
    const DESKTOP_SESSION_META: &str = r#"{"type":"session_meta","timestamp":"2026-03-28T15:00:00Z","payload":{"id":"desktop-123","cwd":"/home/user/project","originator":"Codex Desktop","cli_version":"0.131.0-alpha.9","timestamp":"2026-03-28T15:00:00Z","git":{"branch":"feature/x"}}}"#;
    const DAEMON_CLI_SESSION_META: &str = r#"{"type":"session_meta","timestamp":"2026-09-30T09:59:14Z","payload":{"id":"cli-123","cwd":"/home/user/project","originator":"codex-tui","source":"vscode","cli_version":"0.159.2","timestamp":"2026-09-30T09:59:14Z"}}"#;

    fn write_lines(file: &mut tempfile::NamedTempFile, lines: &[&str]) {
        for line in lines {
            writeln!(file, "{}", line).unwrap();
        }
        file.flush().unwrap();
    }

    fn proc_info(pid: u32, ppid: u32, command: &str) -> ProcInfo {
        ProcInfo {
            pid,
            ppid,
            rss_kb: 0,
            cpu_pct: 0.0,
            command: command.to_string(),
        }
    }

    fn owned_process(pid: u32) -> CodexProcessContext {
        CodexProcessContext::Exclusive {
            pid,
            is_exec: false,
        }
    }

    fn write_jsonl(path: &Path, lines: &[&str]) {
        let mut file = File::create(path).unwrap();
        for line in lines {
            writeln!(file, "{}", line).unwrap();
        }
        file.flush().unwrap();
    }

    fn set_modified(path: &Path, when: SystemTime) {
        // Open with write access: on Windows, setting timestamps through a
        // read-only handle fails with PermissionDenied.
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_modified(when).unwrap();
    }

    fn shared(processes: Vec<ProcInfo>) -> super::super::SharedProcessData {
        let process_info: HashMap<_, _> =
            processes.into_iter().map(|info| (info.pid, info)).collect();
        super::super::SharedProcessData {
            children_map: process::get_children_map(&process_info),
            process_info,
            ports: HashMap::new(),
            slow_tick: false,
            mcp_server_pids: HashSet::new(),
            mcp_owned_rollouts: HashSet::new(),
            mcp_suppress: true,
        }
    }

    fn native_thread(id: &str, path: Option<PathBuf>, status: native::Status) -> native::Thread {
        native::Thread {
            id: id.to_string(),
            path,
            status,
            cwd: "/home/user/project".into(),
            created_at: 100,
            ..native::Thread::default()
        }
    }

    fn collector_with(discovery: Discovery) -> CodexCollector {
        let mut scanner = DiscoveryScanner::new(discovery.roots.clone());
        scanner.cached = discovery;
        CodexCollector {
            scanner,
            last_rate_limit: None,
        }
    }

    #[test]
    fn old_loaded_idle_thread_overrides_stale_tool_without_duplication() {
        let dir = tempfile::tempdir().unwrap();
        let root = canonical_path(dir.path());
        let day = root.join("sessions/2020/01/01");
        fs::create_dir_all(&day).unwrap();
        let path = day.join("rollout-old.jsonl");
        write_jsonl(
            &path,
            &[
                SESSION_META,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cargo build\"}","call_id":"c1"}}"#,
            ],
        );
        set_modified(&path, SystemTime::now() - Duration::from_secs(7200));
        let mut discovery = Discovery {
            roots: vec![root.clone()],
            ..Discovery::default()
        };
        discovery.fds.insert(42, vec![path.clone()]);
        discovery.recent.insert(root.clone(), vec![path.clone()]);
        discovery.native.insert(
            root,
            native::Snapshot {
                host_pid: Some(90),
                threads: vec![native_thread("sess-123", Some(path), native::Status::Idle)],
            },
        );
        let mut collector = collector_with(discovery);
        let sessions = collector.sessions_from_discovery(&shared(vec![proc_info(42, 1, "codex")]));
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "sess-123");
        assert_eq!(sessions[0].pid, 42);
        assert_eq!(sessions[0].status, SessionStatus::Waiting);
        assert_eq!(sessions[0].pending_since_ms, 0);
        assert_eq!(sessions[0].current_tasks, ["waiting for input"]);
        collector
            .scanner
            .cached
            .native
            .values_mut()
            .next()
            .unwrap()
            .threads[0]
            .status = native::Status::Active { flags: vec![] };
        let sessions = collector.sessions_from_discovery(&shared(vec![proc_info(42, 1, "codex")]));
        assert_eq!(sessions[0].status, SessionStatus::Executing);
        assert!(sessions[0].current_tasks[0].contains("cargo build"));
        collector
            .scanner
            .cached
            .native
            .values_mut()
            .next()
            .unwrap()
            .threads[0]
            .status = native::Status::Active {
            flags: vec!["waitingOnUserInput".into()],
        };
        let sessions = collector.sessions_from_discovery(&shared(vec![proc_info(42, 1, "codex")]));
        assert_eq!(sessions[0].status, SessionStatus::Waiting);
        assert_eq!(sessions[0].current_tasks, ["waiting for user input"]);
        assert_eq!(sessions[0].pending_since_ms, 0);
    }

    #[test]
    fn native_threads_without_rollouts_use_thread_id_and_keep_shared_hosts_safe() {
        let root = PathBuf::from("/codex-home");
        let threads: Vec<_> = [
            (
                "parent",
                serde_json::json!({"type":"active","activeFlags":[]}),
            ),
            (
                "child",
                serde_json::json!({"type":"active","activeFlags":["waitingOnApproval"]}),
            ),
            ("fork", serde_json::json!({"type":"systemError"})),
        ]
        .into_iter()
        .map(|(id, status)| {
            serde_json::from_value(serde_json::json!({
                "id": id, "sessionId":"shared-tree", "cwd":"/same-cwd", "ephemeral":true,
                "status":status, "preview":"prompt sk-proj-secret", "model":"gpt-5"
            }))
            .unwrap()
        })
        .collect();
        let mut discovery = Discovery {
            roots: vec![root.clone()],
            ..Discovery::default()
        };
        discovery.native.insert(
            root,
            native::Snapshot {
                host_pid: Some(90),
                threads,
            },
        );
        let mut collector = collector_with(discovery);
        let mut process = proc_info(90, 1, "codex app-server");
        process.rss_kb = 999_999;
        let sessions = collector
            .sessions_from_discovery(&shared(vec![process, proc_info(91, 90, "cargo build")]));
        assert_eq!(sessions.len(), 3);
        for session in &sessions {
            assert_eq!(session.pid, 0);
            assert_eq!(session.mem_mb, 0);
            assert!(session.children.is_empty());
            assert!(!session.initial_prompt.contains("sk-proj-secret"));
            assert_eq!(session.model, "gpt-5");
        }
        assert_eq!(
            sessions
                .iter()
                .find(|s| s.session_id == "parent")
                .unwrap()
                .status,
            SessionStatus::Thinking
        );
        let child = sessions.iter().find(|s| s.session_id == "child").unwrap();
        assert_eq!(child.status, SessionStatus::Waiting);
        assert_eq!(child.current_tasks, ["waiting for approval"]);
        assert_eq!(
            sessions
                .iter()
                .find(|s| s.session_id == "fork")
                .unwrap()
                .status,
            SessionStatus::Error
        );
    }

    #[test]
    fn native_error_lifecycle_preserves_uncertainty_then_removes_unloaded_threads() {
        let root = PathBuf::from("/home/codex");
        let mut discovery = Discovery::default();
        let snapshot = native::Snapshot {
            host_pid: Some(90),
            threads: vec![native_thread("a", None, native::Status::Idle)],
        };
        assert!(discovery.apply_native(&root, Ok(snapshot.clone()), &[90]));
        assert!(!discovery.apply_native(&root, Err(std::io::Error::other("timeout")), &[90]));
        assert_eq!(
            discovery.native[&root].threads[0].status,
            native::Status::Unknown
        );
        let partial_read = native::Snapshot {
            host_pid: Some(90),
            threads: vec![native::Thread {
                id: "a".into(),
                ..native::Thread::default()
            }],
        };
        assert!(discovery.apply_native(&root, Ok(partial_read), &[90]));
        assert_eq!(discovery.native[&root].threads[0].cwd, "/home/user/project");
        assert!(discovery.apply_native(
            &root,
            Ok(native::Snapshot {
                host_pid: Some(90),
                threads: vec![]
            }),
            &[90]
        ));
        assert!(discovery.native[&root].threads.is_empty());
        discovery.apply_native(&root, Ok(snapshot), &[90]);
        discovery.apply_native(&root, Err(std::io::Error::other("gone")), &[]);
        assert!(!discovery.native.contains_key(&root));
    }

    #[test]
    fn weak_recent_candidates_keep_cutoff_exclusions_and_unknown_status() {
        let dir = tempfile::tempdir().unwrap();
        let root = canonical_path(dir.path());
        let day = root.join("sessions/2020/01/01");
        fs::create_dir_all(&day).unwrap();
        let live = day.join("rollout-live.jsonl");
        let stale = day.join("rollout-stale.jsonl");
        let mcp = day.join("rollout-mcp.jsonl");
        let guardian = day.join("rollout-guardian.jsonl");
        let unsupported = day.join("rollout-unsupported.jsonl");
        for path in [&live, &stale, &mcp] {
            write_jsonl(path, &[DAEMON_CLI_SESSION_META]);
        }
        write_jsonl(&unsupported, &[SESSION_META]);
        let mut meta: Value = serde_json::from_str(DESKTOP_SESSION_META).unwrap();
        meta["payload"]["source"] = serde_json::json!({"subagent":{"other":"guardian"}});
        write_jsonl(&guardian, &[&meta.to_string()]);
        set_modified(&stale, SystemTime::now() - Duration::from_secs(3600));
        let candidates = recent_rollouts(&root.join("sessions"));
        assert!(candidates.contains(&live));
        assert!(!candidates.contains(&stale));
        let mut discovery = Discovery {
            roots: vec![root.clone()],
            ..Discovery::default()
        };
        discovery.recent.insert(root, candidates);
        let mut collector = collector_with(discovery);
        let mut shared = shared(vec![proc_info(90, 1, "codex app-server")]);
        shared.mcp_owned_rollouts.insert(mcp);
        let sessions = collector.sessions_from_discovery(&shared);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, SessionStatus::Unknown);
        assert_eq!(sessions[0].pid, 0);
    }

    #[test]
    fn queried_host_fds_cannot_recreate_historical_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-done.jsonl");
        write_jsonl(&path, &[DAEMON_CLI_SESSION_META]);
        let mut discovery = Discovery {
            roots: vec![dir.path().to_path_buf()],
            ..Discovery::default()
        };
        discovery.fds.insert(90, vec![path]);
        discovery.queried_hosts.insert(90);
        let mut collector = collector_with(discovery);
        assert!(collector
            .sessions_from_discovery(&shared(vec![proc_info(90, 1, "codex app-server")]))
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn canonical_paths_discover_custom_roots_and_skip_symlink_scans() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("custom");
        let day = root.join("sessions/2020/01/01");
        fs::create_dir_all(&day).unwrap();
        let file = day.join("rollout-a.jsonl");
        write_jsonl(&file, &[SESSION_META]);
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&root, &alias).unwrap();
        assert_eq!(canonical_path(&root), canonical_path(&alias));
        assert_eq!(
            rollout_root(&alias.join("sessions/2020/01/01/rollout-a.jsonl")),
            Some(canonical_path(&root))
        );
        std::os::unix::fs::symlink(&root, root.join("sessions/loop")).unwrap();
        assert_eq!(recent_rollouts(&root.join("sessions")).len(), 1);
    }

    #[cfg(windows)]
    #[test]
    fn find_codex_pids_windows_keeps_real_child_over_wrappers() {
        let mut process_info = HashMap::new();
        process_info.insert(
            10,
            proc_info(
                10,
                1,
                r#""C:\Program Files\Git\usr\bin\sh.exe" /c/Users/GK/AppData/Roaming/npm/codex -m gpt-5.5"#,
            ),
        );
        process_info.insert(
            20,
            proc_info(
                20,
                10,
                r#""C:\Program Files\nodejs\node.exe" C:\Users\GK\AppData\Roaming\npm\node_modules\@openai\codex\bin\codex.js -m gpt-5.5"#,
            ),
        );
        process_info.insert(
            30,
            proc_info(
                30,
                20,
                r#"C:\Users\GK\AppData\Roaming\npm\node_modules\@openai\codex\node_modules\@openai\codex-win32-x64\vendor\x86_64-pc-windows-msvc\codex\codex.exe -m gpt-5.5"#,
            ),
        );

        let pids = CodexCollector::find_codex_pids_from_shared(
            &process_info,
            &std::collections::HashSet::new(),
        );

        assert_eq!(pids, vec![(30, false)]);
    }

    #[test]
    fn find_codex_pids_excludes_app_server() {
        let mut process_info = HashMap::new();
        process_info.insert(10, proc_info(10, 1, "codex --resume abc"));
        process_info.insert(
            20,
            proc_info(
                20,
                1,
                "/Applications/Codex.app/Contents/Resources/codex app-server --analytics-default-enabled",
            ),
        );

        let pids = CodexCollector::find_codex_pids_from_shared(&process_info, &HashSet::new());

        assert_eq!(pids, vec![(10, false)]);
    }

    #[test]
    fn find_codex_pids_keeps_cli_with_app_server_in_path() {
        let mut process_info = HashMap::new();
        process_info.insert(
            10,
            proc_info(10, 1, "codex --cd /home/user/app-server --resume abc"),
        );

        let pids = CodexCollector::find_codex_pids_from_shared(&process_info, &HashSet::new());

        assert_eq!(pids, vec![(10, false)]);
    }

    #[test]
    fn find_codex_desktop_pids_detects_app_servers() {
        let mut process_info = HashMap::new();
        process_info.insert(
            10,
            proc_info(
                10,
                1,
                "/Applications/Codex.app/Contents/Resources/codex app-server --analytics-default-enabled",
            ),
        );
        process_info.insert(20, proc_info(20, 1, "codex app-server --listen stdio://"));

        let pids =
            CodexCollector::find_codex_desktop_pids_from_shared(&process_info, &HashSet::new());

        assert_eq!(pids, vec![10, 20]);
    }

    #[test]
    fn find_codex_desktop_pids_ignores_mcp_and_non_codex() {
        let mut process_info = HashMap::new();
        process_info.insert(10, proc_info(10, 1, "codex mcp-server"));
        process_info.insert(20, proc_info(20, 1, "node app-server"));
        process_info.insert(30, proc_info(30, 1, "grep codex app-server"));
        process_info.insert(40, proc_info(40, 1, "codex app-server --listen stdio://"));
        let mut mcp = HashSet::new();
        mcp.insert(10);

        let pids = CodexCollector::find_codex_desktop_pids_from_shared(&process_info, &mcp);

        assert_eq!(pids, vec![40]);
    }

    #[test]
    fn desktop_rollout_filter_requires_originator() {
        for originator in [
            "Codex Desktop",
            "codex_work_desktop",
            "codex_vscode",
            "codex-tui",
        ] {
            let mut desktop = tempfile::NamedTempFile::new().unwrap();
            let metadata = DESKTOP_SESSION_META.replace("Codex Desktop", originator);
            write_lines(&mut desktop, &[&metadata]);
            assert!(parse_codex_jsonl(desktop.path())
                .unwrap()
                .is_codex_desktop());
        }
        let mut cli = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut cli, &[SESSION_META]);
        assert!(!parse_codex_jsonl(cli.path()).unwrap().is_codex_desktop());
        let mut unsupported = tempfile::NamedTempFile::new().unwrap();
        let metadata = DESKTOP_SESSION_META.replace("Codex Desktop", "unsupported-client");
        write_lines(&mut unsupported, &[&metadata]);
        assert!(!parse_codex_jsonl(unsupported.path())
            .unwrap()
            .is_codex_desktop());
    }

    #[test]
    fn guardian_rollouts_are_not_sessions() {
        let mut metadata: Value = serde_json::from_str(DESKTOP_SESSION_META).unwrap();
        metadata["payload"]["source"] = serde_json::json!({"subagent": {"other": "guardian"}});
        let mut guardian = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut guardian, &[&metadata.to_string()]);

        assert!(parse_codex_jsonl(guardian.path()).is_none());

        metadata["payload"]["source"] = serde_json::json!({"subagent": {"other": "explore"}});
        let mut other_subagent = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut other_subagent, &[&metadata.to_string()]);
        assert!(parse_codex_jsonl(other_subagent.path()).is_some());
    }

    #[test]
    fn test_parse_codex_session_meta() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut file, &[SESSION_META]);
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.session_id, "sess-123");
        assert_eq!(result.cwd, "/home/user/project");
        assert_eq!(result.version, "0.1.5");
        assert_eq!(result.git_branch, "feature/x");
    }

    #[test]
    fn test_parse_codex_token_count() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":500,"output_tokens":200,"cached_input_tokens":100,"total_tokens":700},"last_token_usage":{"input_tokens":50,"output_tokens":20,"cached_input_tokens":10,"total_tokens":70},"model_context_window":128000}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.total_input, 400);
        assert_eq!(result.total_output, 200);
        assert_eq!(result.total_cache_read, 100);
        assert_eq!(result.last_context_tokens, 50);
        assert_eq!(result.context_window, 128000);
        assert_eq!(result.token_history.len(), 1);
        assert_eq!(result.token_history[0], 70);
    }

    #[test]
    fn test_parse_codex_context_does_not_double_count_cached_input() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":58140501,"cached_input_tokens":55267712,"output_tokens":114278,"total_tokens":58254779},"last_token_usage":{"input_tokens":151839,"cached_input_tokens":146816,"output_tokens":621,"total_tokens":152460},"model_context_window":258400}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.last_context_tokens, 151_839);
        assert_eq!(result.context_window, 258_400);
        assert!(result.last_context_tokens < result.context_window);
    }

    #[test]
    fn test_parse_codex_rate_limits() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1,"output_tokens":1},"last_token_usage":{"input_tokens":1,"output_tokens":1}},"rate_limits":{"limit_id":"codex","primary":{"used_percent":9.0,"window_minutes":300,"resets_at":1774686045},"secondary":{"used_percent":14.0,"window_minutes":10080,"resets_at":1775186466},"plan_type":"plus"}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        let rl = result.rate_limit.expect("rate_limit should be Some");
        assert_eq!(rl.five_hour_pct, Some(9.0));
        assert_eq!(rl.five_hour_window_minutes, Some(300));
        assert_eq!(rl.seven_day_pct, Some(14.0));
        assert_eq!(rl.seven_day_window_minutes, Some(10_080));
    }

    #[test]
    fn test_parse_codex_free_rate_limit_uses_thirty_day_window() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-06-17T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1,"output_tokens":1},"last_token_usage":{"input_tokens":1,"output_tokens":1}},"rate_limits":{"limit_id":"codex","primary":{"used_percent":48.0,"window_minutes":43200,"resets_at":1780000000},"secondary":null,"plan_type":"free"}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        let rl = result.rate_limit.expect("rate_limit should be Some");
        assert_eq!(rl.five_hour_pct, None);
        assert_eq!(rl.seven_day_pct, Some(48.0));
        assert_eq!(rl.seven_day_window_minutes, Some(43_200));
    }

    #[test]
    fn test_parse_codex_rate_limits_ignores_model_specific_limits() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1,"output_tokens":1},"last_token_usage":{"input_tokens":1,"output_tokens":1}},"rate_limits":{"limit_id":"codex","primary":{"used_percent":25.0,"window_minutes":300,"resets_at":1774686045},"secondary":{"used_percent":4.0,"window_minutes":10080,"resets_at":1775186466}}}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:01Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1,"output_tokens":1},"last_token_usage":{"input_tokens":1,"output_tokens":1}},"rate_limits":{"limit_id":"codex_bengalfox","limit_name":"GPT-5.3-Codex-Spark","primary":{"used_percent":0.0,"window_minutes":300,"resets_at":1774686045},"secondary":{"used_percent":0.0,"window_minutes":10080,"resets_at":1775186466}}}}"#,
            ],
        );

        let result = parse_codex_jsonl(file.path()).unwrap();
        let rl = result.rate_limit.expect("account rate_limit should remain");
        assert_eq!(rl.five_hour_pct, Some(25.0));
        assert_eq!(rl.seven_day_pct, Some(4.0));
        assert_eq!(rl.seven_day_window_minutes, Some(10_080));
    }

    #[test]
    fn test_parse_codex_cache_read_fallback_field_name() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                // Uses cache_read_input_tokens instead of cached_input_tokens
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"output_tokens":50,"cache_read_input_tokens":30},"last_token_usage":{"input_tokens":20,"output_tokens":10,"cache_read_input_tokens":5},"model_context_window":200000}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.total_cache_read, 30);
        assert_eq!(result.last_context_tokens, 20);
    }

    #[test]
    fn test_parse_codex_skips_malformed_lines() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"NOT VALID JSON AT ALL"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"agent_message"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        // Bad line skipped, agent_message still counted
        assert_eq!(result.turn_count, 1);
    }

    #[test]
    fn test_parse_codex_model_generating_after_user_message() {
        // Latest event is a user_message → the model has not replied yet.
        // Combined with recent rollout mtime this drives the Thinking
        // status branch in CodexCollector::collect_sessions.
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"agent_message","message":"hi"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:00Z","payload":{"type":"user_message","message":"do a thing"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert!(
            result.model_generating,
            "trailing user_message must mark model as generating"
        );
    }

    #[test]
    fn test_parse_codex_task_started_without_user_message_is_thinking() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"task_started"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:05Z","payload":{"type":"agent_message","message":"Inspecting the repository."}}"#,
            ],
        );

        let result = parse_codex_jsonl(file.path()).unwrap();
        assert!(
            result.model_generating,
            "task_started must open an active turn even without user_message"
        );
        assert_eq!(result.thinking_since_ms, 1_774_710_060_000);
    }

    #[test]
    fn test_codex_task_started_tool_output_returns_to_thinking_without_user_message() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"task_started"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"custom_tool_call","name":"exec","input":"const r = await tools.exec_command({cmd: \"git status\"});","call_id":"call_custom_1"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:09Z","payload":{"type":"custom_tool_call_output","call_id":"call_custom_1","output":"Script completed"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex"));

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Thinking);
        assert_eq!(session.thinking_since_ms, 1_774_710_069_000);
    }

    #[test]
    fn test_parse_codex_progress_message_keeps_model_generating() {
        // Codex can emit progress messages before continuing to reason or
        // invoking a tool. Only task_complete closes the active turn.
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"do a thing"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:00Z","payload":{"type":"agent_message","message":"done"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert!(
            result.model_generating,
            "an intermediate agent_message must not close the thinking window"
        );
    }

    #[test]
    fn test_parse_codex_model_generating_cleared_by_task_complete() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"do a thing"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:00Z","payload":{"type":"agent_message","message":"done"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:01Z","payload":{"type":"task_complete"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert!(
            !result.model_generating,
            "task_complete must close the thinking window"
        );
        assert_eq!(result.thinking_since_ms, 0);
    }

    #[test]
    fn test_parse_codex_chat_tail_from_user_and_agent_messages() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"check \u0007auth\u202E sk-proj-secret"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:00Z","payload":{"type":"agent_message","message":"Auth guard\u0008 is the failing path."}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.chat_messages.len(), 2);
        assert_eq!(result.chat_messages[0].role, ChatRole::User);
        assert_eq!(result.chat_messages[0].text, "check auth [REDACTED]");
        assert_eq!(result.chat_messages[1].role, ChatRole::Assistant);
        assert_eq!(
            result.chat_messages[1].text,
            "Auth guard is the failing path."
        );
    }

    #[test]
    fn test_parse_codex_turn_context_effort() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"turn_context","timestamp":"2026-03-28T15:01:00Z","payload":{"cwd":"/home/user/project","model":"gpt-5-codex","effort":"low","summary":"auto"}}"#,
                // Later turn_context overrides — /effort can change mid-session
                r#"{"type":"turn_context","timestamp":"2026-03-28T15:02:00Z","payload":{"cwd":"/home/user/project","model":"gpt-5-codex","effort":"high","summary":"auto"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.model, "gpt-5-codex");
        assert_eq!(result.effort, "high");
    }

    #[test]
    fn test_parse_codex_missing_effort_is_empty() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                // turn_context without effort field
                r#"{"type":"turn_context","timestamp":"2026-03-28T15:01:00Z","payload":{"cwd":"/home/user/project","model":"gpt-5-codex"}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.effort, "");
    }

    #[test]
    fn test_codex_pending_function_call_marks_session_executing_and_timeline() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"run tests"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:05Z","payload":{"type":"agent_message","message":"I'll run them."}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cargo test\"}","call_id":"call_1"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(
            42,
            ProcInfo {
                pid: 42,
                ppid: 1,
                rss_kb: 1024,
                cpu_pct: 0.0,
                command: "codex".to_string(),
            },
        );

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Executing);
        assert_eq!(
            session.current_tasks,
            vec!["exec_command cargo test".to_string()]
        );
        assert_eq!(session.tool_calls.len(), 1);
        assert_eq!(session.tool_calls[0].name, "exec_command");
        assert_eq!(session.tool_calls[0].arg, "cargo test");
        assert_eq!(session.tool_calls[0].duration_ms, 0);
        assert!(session.pending_since_ms > 0);
        assert_eq!(session.thinking_since_ms, 0);
    }

    #[test]
    fn test_codex_pending_custom_tool_call_marks_session_executing_and_timeline() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"inspect the repository"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:05Z","payload":{"type":"agent_message","message":"I'll inspect it first."}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"custom_tool_call","name":"exec","input":"const r = await tools.exec_command({cmd: \"git status\"});","call_id":"call_custom_1"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex"));

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Executing);
        assert_eq!(session.tool_calls.len(), 1);
        assert_eq!(session.tool_calls[0].name, "exec");
        assert!(session.pending_since_ms > 0);
        assert_eq!(session.thinking_since_ms, 0);
    }

    #[test]
    fn test_codex_custom_tool_payloads_stay_out_of_display_data() {
        for (name, input) in [
            (
                "apply_patch",
                "*** Begin Patch\n*** Add File: .env\n+PASSWORD=private-test-value\n*** End Patch",
            ),
            (
                "exec",
                "await tools.exec_command({cmd: 'printf private-test-value > .env'});",
            ),
            ("future_tool", "private-test-value"),
        ] {
            let mut file = tempfile::NamedTempFile::new().unwrap();
            let call = serde_json::json!({
                "type": "response_item",
                "timestamp": "2026-03-28T15:01:06Z",
                "payload": {
                    "type": "custom_tool_call",
                    "name": name,
                    "input": input,
                    "call_id": "private_call"
                }
            });
            write_lines(
                &mut file,
                &[
                    SESSION_META,
                    r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"task_started"}}"#,
                    &call.to_string(),
                ],
            );

            let collector = CodexCollector::new();
            let mut process_info = HashMap::new();
            process_info.insert(42, proc_info(42, 1, "codex"));
            let (session, _) = collector
                .load_session_with_rate_limit(
                    owned_process(42),
                    file.path(),
                    &process_info,
                    &HashMap::new(),
                    &HashMap::new(),
                )
                .unwrap();

            // These fields feed the TUI, --once, and JSON snapshots.
            assert_eq!(session.current_tasks, vec![name.to_string()]);
            assert_eq!(session.tool_calls.len(), 1);
            assert_eq!(session.tool_calls[0].name, name);
            assert!(session.tool_calls[0].arg.is_empty());
            assert_eq!(session.status, SessionStatus::Executing);
            assert_eq!(session.pending_since_ms, 1_774_710_066_000);

            write_lines(
                &mut file,
                &[
                    r#"{"type":"response_item","timestamp":"2026-03-28T15:01:09Z","payload":{"type":"custom_tool_call_output","call_id":"private_call","output":"private-test-value"}}"#,
                ],
            );
            let result = parse_codex_jsonl(file.path()).unwrap();
            assert!(result.current_task.is_empty());
            assert!(result.tool_calls[0].arg.is_empty());
            assert_eq!(result.tool_calls[0].duration_ms, 3_000);
            assert_eq!(result.pending_since_ms, 0);
            assert!(result.model_generating);
        }
    }

    #[test]
    fn test_codex_request_user_input_marks_session_waiting() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"change the comparison key"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"function_call","name":"request_user_input","arguments":"{\"question\":\"Which key?\"}","call_id":"call_question_1"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex"));
        process_info.insert(
            43,
            ProcInfo {
                pid: 43,
                ppid: 42,
                rss_kb: 1024,
                cpu_pct: 20.0,
                command: "codex-code-mode-host".to_string(),
            },
        );
        let children_map = process::get_children_map(&process_info);

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &children_map,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Waiting);
        assert_eq!(session.current_tasks, vec!["waiting for input".to_string()]);
        assert_eq!(session.pending_since_ms, 1_774_710_066_000);
        assert_eq!(session.thinking_since_ms, 0);
    }

    #[test]
    fn test_codex_completed_custom_tool_returns_to_thinking_until_task_complete() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"inspect the repository"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:05Z","payload":{"type":"agent_message","message":"I'll inspect it first."}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"custom_tool_call","name":"exec","input":"const r = await tools.exec_command({cmd: \"git status\"});","call_id":"call_custom_1"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:09Z","payload":{"type":"custom_tool_call_output","call_id":"call_custom_1","output":[{"type":"input_text","text":"Script completed"}]}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex"));
        process_info.insert(
            43,
            ProcInfo {
                pid: 43,
                ppid: 42,
                rss_kb: 1024,
                cpu_pct: 20.0,
                command: "codex-code-mode-host".to_string(),
            },
        );
        let children_map = process::get_children_map(&process_info);

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &children_map,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Thinking);
        assert_eq!(session.current_tasks, vec!["thinking...".to_string()]);
        assert_eq!(session.tool_calls.len(), 1);
        assert_eq!(session.tool_calls[0].duration_ms, 3_000);
        assert_eq!(session.pending_since_ms, 0);
        assert_eq!(session.thinking_since_ms, 1_774_710_069_000);
    }

    #[test]
    fn test_codex_completed_turn_stays_waiting_with_active_child() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"user_message","message":"start a server"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:00Z","payload":{"type":"agent_message","message":"The server is running."}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:01Z","payload":{"type":"task_complete"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex"));
        process_info.insert(
            43,
            ProcInfo {
                pid: 43,
                ppid: 42,
                rss_kb: 1024,
                cpu_pct: 20.0,
                command: "long-running-server".to_string(),
            },
        );
        let children_map = process::get_children_map(&process_info);

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &children_map,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Waiting);
        assert_eq!(session.current_tasks, vec!["waiting for input".to_string()]);
    }

    #[test]
    fn test_codex_completed_exec_labels_current_task_finished() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:00Z","payload":{"type":"task_started"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:02:01Z","payload":{"type":"task_complete"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(42, proc_info(42, 1, "codex exec"));
        let process_ctx = CodexProcessContext::Exclusive {
            pid: 42,
            is_exec: true,
        };

        let (session, _) = collector
            .load_session_with_rate_limit(
                process_ctx,
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Done);
        assert_eq!(session.current_tasks, vec!["finished".to_string()]);
    }

    #[test]
    fn test_codex_exec_command_end_closes_task_and_records_duration() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cargo test\"}","call_id":"call_1"}}"#,
                r#"{"type":"event_msg","timestamp":"2026-03-28T15:01:09Z","payload":{"type":"exec_command_end","call_id":"call_1"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(
            42,
            ProcInfo {
                pid: 42,
                ppid: 1,
                rss_kb: 1024,
                cpu_pct: 0.0,
                command: "codex".to_string(),
            },
        );

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Waiting);
        assert_eq!(session.current_tasks, vec!["waiting for input".to_string()]);
        assert_eq!(session.tool_calls.len(), 1);
        assert_eq!(session.tool_calls[0].duration_ms, 3_000);
        assert_eq!(session.pending_since_ms, 0);
    }

    #[test]
    fn test_codex_exec_command_output_closes_task_without_end_event() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cargo test\"}","call_id":"call_1"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:09Z","payload":{"type":"function_call_output","call_id":"call_1","output":"Chunk ID: abc\nWall time: 0.1000 seconds\nProcess exited with code 0\nOutput:\nok"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(
            42,
            ProcInfo {
                pid: 42,
                ppid: 1,
                rss_kb: 1024,
                cpu_pct: 0.0,
                command: "codex".to_string(),
            },
        );

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Waiting);
        assert_eq!(session.current_tasks, vec!["waiting for input".to_string()]);
        assert_eq!(session.tool_calls.len(), 1);
        assert_eq!(session.tool_calls[0].duration_ms, 3_000);
        assert_eq!(session.pending_since_ms, 0);
    }

    #[test]
    fn test_codex_running_exec_closes_when_write_stdin_reports_exit() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:06Z","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cargo test\"}","call_id":"call_1"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:07Z","payload":{"type":"function_call_output","call_id":"call_1","output":"Chunk ID: abc\nWall time: 1.0000 seconds\nProcess running with session ID 12345\nOutput:\ncompiling"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:08Z","payload":{"type":"function_call","name":"write_stdin","arguments":"{\"session_id\":12345,\"chars\":\"\"}","call_id":"call_2"}}"#,
                r#"{"type":"response_item","timestamp":"2026-03-28T15:01:12Z","payload":{"type":"function_call_output","call_id":"call_2","output":"Chunk ID: abc\nWall time: 0.0000 seconds\nProcess exited with code 0\nOutput:\nok"}}"#,
            ],
        );

        let collector = CodexCollector::new();
        let mut process_info = HashMap::new();
        process_info.insert(
            42,
            ProcInfo {
                pid: 42,
                ppid: 1,
                rss_kb: 1024,
                cpu_pct: 0.0,
                command: "codex".to_string(),
            },
        );

        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &process_info,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(session.status, SessionStatus::Waiting);
        assert_eq!(session.current_tasks, vec!["waiting for input".to_string()]);
        assert_eq!(session.tool_calls.len(), 2);
        assert_eq!(session.tool_calls[0].name, "exec_command");
        assert_eq!(session.tool_calls[0].duration_ms, 6_000);
        assert_eq!(session.tool_calls[1].name, "write_stdin");
        assert_eq!(session.tool_calls[1].duration_ms, 4_000);
        assert_eq!(session.pending_since_ms, 0);
    }

    #[test]
    fn test_parse_codex_empty_returns_none() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(parse_codex_jsonl(file.path()).is_none());
    }

    #[test]
    fn test_item_completed_chat_and_turns() {
        // Codex ≥ ~0.149: chat arrives as item_completed wrappers. Note the
        // casing drift — "text" on UserMessage content, "Text" on AgentMessage.
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-08-21T05:45:27Z","payload":{"type":"item_completed","item":{"type":"UserMessage","id":"u1","content":[{"type":"text","text":"please fix the build"}]}}}"#,
                r#"{"type":"event_msg","timestamp":"2026-08-21T05:45:54Z","payload":{"type":"item_completed","item":{"type":"AgentMessage","id":"a1","content":[{"type":"Text","text":"done"}],"phase":"final_answer"}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.chat_messages.len(), 2);
        assert_eq!(result.chat_messages[0].text, "please fix the build");
        assert_eq!(result.chat_messages[1].text, "done");
        assert_eq!(result.turn_count, 1);
        assert!(!result.model_generating, "AgentMessage ends the turn");
        assert_eq!(result.initial_prompt, "please fix the build");
    }

    #[test]
    fn test_item_completed_trailing_user_message_marks_generating() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-08-21T05:45:27Z","payload":{"type":"item_completed","item":{"type":"UserMessage","id":"u1","content":[{"type":"text","text":"go"}]}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert!(
            result.model_generating,
            "unanswered UserMessage means the model is working"
        );
    }

    #[test]
    fn test_item_completed_command_execution_tools_and_files() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-08-21T05:46:11Z","payload":{"type":"item_completed","item":{"type":"CommandExecution","id":"exec-1","command":["/bin/zsh","-lc","sed -n '1,240p' README.md"],"parsed_cmd":[{"type":"read","cmd":"sed -n '1,240p' README.md","name":"README.md","path":"/work/README.md"}],"started_at_ms":1787291167583,"completed_at_ms":1787291168442}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.tool_calls.len(), 1);
        assert_eq!(result.tool_calls[0].name, "read");
        assert_eq!(result.tool_calls[0].duration_ms, 859);
        assert_eq!(result.file_accesses.len(), 1);
        assert_eq!(result.file_accesses[0].path, "/work/README.md");
        assert!(matches!(result.file_accesses[0].operation, FileOp::Read));
    }

    #[test]
    fn test_item_completed_file_change_records_accesses() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-08-21T06:10:59Z","payload":{"type":"item_completed","item":{"type":"FileChange","id":"exec-2","changes":{"/work/new.md":{"type":"add"},"/work/old.rs":{"type":"update"}}}}}"#,
            ],
        );
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.file_accesses.len(), 2);
        let write = result
            .file_accesses
            .iter()
            .find(|f| f.path == "/work/new.md")
            .unwrap();
        let edit = result
            .file_accesses
            .iter()
            .find(|f| f.path == "/work/old.rs")
            .unwrap();
        assert!(matches!(write.operation, FileOp::Write));
        assert!(matches!(edit.operation, FileOp::Edit));
        assert_eq!(
            result.tool_calls.len(),
            2,
            "each change also lands on the tool timeline"
        );
    }

    #[test]
    fn item_completed_preserves_active_turn_until_final_answer() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(
            &mut file,
            &[
                SESSION_META,
                r#"{"type":"event_msg","timestamp":"2026-09-14T00:00:00Z","payload":{"type":"task_started"}}"#,
            ],
        );
        let collector = CodexCollector::new();
        let processes = HashMap::from([(42, proc_info(42, 1, "codex"))]);
        for item in [
            serde_json::json!({"type":"AgentMessage","phase":"commentary","content":[{"type":"Text","text":"Working"}]}),
            serde_json::json!({"type":"CommandExecution","command":["echo","done"]}),
            serde_json::json!({"type":"FileChange","changes":{"/tmp/report":{"type":"add"}}}),
            serde_json::json!({"type":"Extension","kind":"search","query":"private-query"}),
        ] {
            let event = serde_json::json!({"type":"event_msg","timestamp":"2026-09-14T00:00:01Z","payload":{"type":"item_completed","item":item}});
            write_lines(&mut file, &[&event.to_string()]);
            let (session, _) = collector
                .load_session_with_rate_limit(
                    owned_process(42),
                    file.path(),
                    &processes,
                    &HashMap::new(),
                    &HashMap::new(),
                )
                .unwrap();
            assert_eq!(session.status, SessionStatus::Thinking);
        }
        write_lines(
            &mut file,
            &[
                r#"{"type":"event_msg","timestamp":"2026-09-14T00:00:02Z","payload":{"type":"item_completed","item":{"type":"AgentMessage","phase":"final_answer","content":[{"type":"Text","text":"Done"}]}}}"#,
            ],
        );
        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &processes,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(session.status, SessionStatus::Waiting);
        // A new user item must clear the previous turn's completion flag.
        write_lines(
            &mut file,
            &[
                r#"{"type":"event_msg","timestamp":"2026-09-14T00:00:03Z","payload":{"type":"item_completed","item":{"type":"UserMessage","content":[{"type":"text","text":"Continue"}]}}}"#,
            ],
        );
        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &processes,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(session.status, SessionStatus::Thinking);
        write_lines(
            &mut file,
            &[
                r#"{"type":"event_msg","timestamp":"2026-09-14T00:00:04Z","payload":{"type":"turn_aborted"}}"#,
            ],
        );
        let (session, _) = collector
            .load_session_with_rate_limit(
                owned_process(42),
                file.path(),
                &processes,
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(session.status, SessionStatus::Waiting);
    }

    #[test]
    fn item_completed_omits_opaque_content_and_sanitizes_metadata() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut file, &[SESSION_META]);
        for item in [
            serde_json::json!({"type":"CommandExecution","command":["sh","-c","echo PRIVATE_BODY > .env"],"parsed_cmd":[{"type":"read","cmd":"PRIVATE_BODY","path":"/tmp/\u{1b}\u{202e}safe.txt"}]}),
            serde_json::json!({"type":"CommandExecution","command":["echo PRIVATE_BODY"],"parsed_cmd":[{"type":"PRIVATE_BODY"}]}),
            serde_json::json!({"type":"Extension","kind":"PRIVATE_BODY","query":"PRIVATE_BODY"}),
            serde_json::json!({"type":"FileChange","changes":{"/tmp/\u{1b}\u{202e}safe.txt":{"type":"add","diff":"PRIVATE_BODY"}}}),
        ] {
            let event = serde_json::json!({"type":"event_msg","payload":{"type":"item_completed","item":item,"started_at_ms":100,"completed_at_ms":130}});
            write_lines(&mut file, &[&event.to_string()]);
        }
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.tool_calls.len(), 4);
        assert_eq!(result.tool_calls[0].arg, "/tmp/safe.txt");
        assert_eq!(result.tool_calls[0].duration_ms, 30);
        assert_eq!(result.tool_calls[1].name, "exec");
        assert!(result.tool_calls[1].arg.is_empty());
        assert_eq!(result.tool_calls[2].name, "extension");
        assert!(result.tool_calls[2].arg.is_empty());
        assert_eq!(result.tool_calls[3].arg, "safe.txt");
        for tool in &result.tool_calls {
            assert!(!tool.name.contains("PRIVATE_BODY"));
            assert!(!tool.arg.contains("PRIVATE_BODY"));
        }
        assert!(result
            .file_accesses
            .iter()
            .all(|f| f.path == "/tmp/safe.txt"));
        assert_eq!(clean_item_path("/tmp/ghp_synthetic"), "/tmp/[REDACTED]");
        assert_eq!(clean_item_path(&"x".repeat(1024)).len(), 512);
    }

    #[test]
    fn item_completed_tolerates_missing_fields_and_bounds_history() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write_lines(&mut file, &[SESSION_META]);
        for _ in 0..(MAX_FILE_ACCESSES + 10) {
            write_lines(
                &mut file,
                &[
                    r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"FileChange","changes":{"/tmp/file":{"type":"add"}}}}}"#,
                ],
            );
        }
        for item in [
            serde_json::Value::Null,
            serde_json::json!({"type":"CommandExecution"}),
            serde_json::json!({"type":"UnknownFutureItem"}),
        ] {
            let event = serde_json::json!({"type":"event_msg","payload":{"type":"item_completed","item":item}});
            write_lines(&mut file, &[&event.to_string()]);
        }
        let result = parse_codex_jsonl(file.path()).unwrap();
        assert_eq!(result.file_accesses.len(), MAX_FILE_ACCESSES);
        assert!(result.tool_calls.len() <= 500);
        // Once the timeline is full, later items must not grow it.
        assert_eq!(result.tool_calls.len(), 500);
    }
}
