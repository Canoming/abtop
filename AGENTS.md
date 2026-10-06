# abtop

AI agent monitor for your terminal. Like btop++, but for AI coding agents.

Supports Claude Code (CLI, desktop app, and IDE extension processes), Codex CLI and desktop app-server sessions, and OpenCode sessions.

## Architecture

```
src/
├── main.rs                 # Thin binary entry: abtop::run()
├── lib.rs                  # Public modules, CLI modes, terminal/event loop, input handling
├── app.rs                  # App state, collection ticks, UI actions, summary jobs/cache
├── config.rs               # Config parsing/persistence, panel visibility, profile roots
├── theme.rs                # Built-in color palettes and gradients
├── locale.rs               # English/Simplified Chinese UI strings
├── host_info.rs            # Host CPU/memory/load sampling and agent aggregates
├── snapshot.rs             # Serializable Snapshot DTOs and App::to_snapshot()
├── demo.rs                 # Demo data for --demo
├── setup.rs                # StatusLine hook installation (abtop --setup)
├── ui/
│   ├── mod.rs              # Responsive layout, shared drawing helpers, click targets
│   ├── context.rs          # Token-rate graph and per-session context meters
│   ├── quota.rs            # Claude/Codex remaining quota and reset countdowns
│   ├── tokens.rs           # Selected session token breakdown and history
│   ├── projects.rs         # Project branches and Git file counts
│   ├── ports.rs            # Session ports, conflicts, orphan ports
│   ├── sessions.rs         # Session table/tree, chat detail, timeline, file audit
│   ├── mcp.rs              # Codex MCP-server processes and rollout activity
│   ├── header.rs           # Version, clock, host/agent metrics
│   ├── footer.rs           # Shortcuts and transient status
│   ├── config.rs           # Theme/panel configuration overlay
│   ├── help.rs             # Shortcut help overlay
│   └── view_menu.rs        # View-action overlay
├── collector/
│   ├── mod.rs              # AgentCollector trait, shared process data, caches/orphans
│   ├── claude.rs           # Claude Code: session discovery, transcript parsing
│   ├── codex.rs            # One worker, native/fallback merging, rollout parsing
│   ├── codex/native.rs     # Read-only local app-server metadata/status protocol
│   ├── opencode.rs         # Process/cwd matching and read-only SQLite queries
│   ├── mcp.rs              # codex mcp-server detection and rollout ownership
│   ├── process.rs          # Platform process/port backends, process trees, Git stats
│   └── rate_limit.rs       # Claude hook files and persistent Codex quota cache
├── jump/
│   ├── mod.rs              # Herdr-first routing and TerminalJumper registry
│   ├── herdr.rs            # Codex session identity/cwd to Herdr pane
│   ├── cmux.rs             # Workspace routing
│   ├── tmux.rs             # Pane routing
│   └── iterm2.rs           # macOS tty-based AppleScript routing
└── model/
    ├── mod.rs              # Re-exports
    └── session.rs          # Sessions/status, quota, children/subagents, chat/tools/files
```

Data flows from local files/process metadata through `MultiCollector` into
`App`, then into `ui::draw` or `App::to_snapshot`. The binary and library share
the same collection layer; there is no bundled HTTP server. `App` is not `Send`:
keep it on its owning thread and pass serialized snapshots to other threads.
`tick_no_summaries()` skips LLM summary jobs, but collection can still invoke
local commands such as `ps`, `lsof`, `git`, and `sqlite3`.

## Layout

```text
Header: version, available host metrics, agent aggregates, clock/counts
┌─ ¹context ───────────────────────────────────────────────────────────┐
│ Token-rate graph (200 points) | Per-session context/window/compaction│
└─────────────────────────────────────────────────────────────────────┘
┌─ ²quota ───┐┌─ ³tokens ──┐┌─ ⁴projects ┐┌─ ⁵ports ──┐┌─ ⁷mcp ────┐
│ Claude /   ││ Selected  ││ Branch /  ││ Owned /   ││ PID/profile│
│ Codex left ││ session   ││ Git counts││ orphans   ││ Rollouts   │
│ + resets   ││ breakdown ││           ││ conflicts ││ Activity   │
└────────────┘└───────────┘└───────────┘└───────────┘└────────────┘
┌─ ⁶sessions ─────────────────────────────────────────────────────────┐
│ Responsive session table, two rows/session; optional subagent tree   │
│ ─────────────────────────────────────────────────────────────────── │
│ Selected session: summary, chat tail, tools/children/subagents       │
│ Optional tool timeline and file-access audit                        │
└─────────────────────────────────────────────────────────────────────┘
Footer: shortcuts or transient status
```

All seven panels default to visible and can be toggled/persisted independently.

Wide layout (width >= 100 columns):
1. Reserve one row each for **header** and **footer**.
2. Reserve up to 6 rows for enabled **mid-tier** panels (quota, tokens, projects, ports, MCP), with an ideal height of 8. Split their widths equally.
3. Allocate **sessions** the remaining budget, with a minimum of 5 rows when space permits and an ideal height of `max(8, 2 * session_count + 7)`. The session area can absorb leftover space.
4. Show **context** when sessions have their ideal height and at least 5 surplus rows remain; its target height is `clamp(session_count + 4, 5, 10)`. If sessions are disabled, context can use the surplus directly.

Compact layout (60–99 columns, minimum height 18): **Work** contains sessions/projects,
**Usage** contains context/quota/tokens, and **System** contains ports/MCP.
Only the active tab renders; its enabled panels split the body vertically.
`+`/`=` maximizes the active section and `-` restores the split. Terminals below
60×18 show a size warning. At valid sizes, header/footer remain visible.

Panel descriptions:

- **¹context**: Left = token rate braille sparkline (200-point history). Right = per-session context % bars with yellow/red warning.
- **²quota**: Claude + Codex remaining-quota gauges side-by-side, with source-reported window durations and reset countdowns. Defaults are 5h/7d; Codex can report a longer window such as 30d. Quota is intentionally limited to Claude and Codex; do not add an OpenCode row unless OpenCode exposes a reliable account-level provider rate-limit source.
- **³tokens**: Total token breakdown (in/out/cache) + per-turn sparkline for selected session.
- **⁴projects**: Per-project git branch + added/modified file counts.
- **⁵ports**: Agent-spawned open ports + orphan ports (from dead sessions). Conflict detection.
- **⁶sessions**: Session list table (or subagent tree) + selected session detail, separated by a divider. Detail defaults to bounded chat messages; timeline and file audit are optional views. Columns adapt to available width.
- **⁷mcp**: Detected `codex mcp-server` PIDs, parent CLI/profile, active/total rollout counts, and latest activity age. This is not a general MCP-service inventory.

## Data Sources

Monitoring uses local filesystem/process state without API keys or service auth.
On macOS, process enumeration uses `ps`, Claude open-path discovery uses
`proc_pidinfo` with `lsof` fallback, and ports/Codex open files use `lsof`.
Linux reads `/proc` directly; Windows uses `sysinfo`, `netstat -ano`, and
filesystem-based discovery fallbacks. OpenCode uses `sqlite3 -readonly -json`.
Monitoring does not modify agent transcripts or the OpenCode DB; abtop does
write its own config/cache files. Summary generation, setup, self-update,
terminal jumps, and explicit process termination are separate actions.

### 1. Claude Code session discovery: process + config-root mapping

Discovery strategy:
1. Find running `claude` processes in shared platform process data
2. Map PID → open files/directories via platform-specific discovery
3. Infer Claude config roots from open paths that contain `sessions/` and `projects/`
4. Read `{config-root}/sessions/{PID}.json`, falling back to scanning session files for the matching embedded PID
5. Parse `{config-root}/projects/{encoded-path}/{sessionId}.jsonl`

Fallback config roots are still scanned: `~/.claude`, direct home profile roots matching `~/.claude-*` when they contain both `sessions/` and `projects/`, `claude_config_dirs` from `<config-dir>/abtop/config.toml` (`dirs::config_dir()`), abtop's own `CLAUDE_CONFIG_DIR`, and on Linux any `CLAUDE_CONFIG_DIR` read from `/proc/{pid}/environ`.

Session file format:
```json
{ "pid": 7336, "sessionId": "2f029acc-...", "cwd": "/Users/graykode/abtop", "startedAt": 1774715116826, "kind": "interactive", "entrypoint": "cli" }
```

- ~170 bytes. Created on start, deleted on exit.
- Verify PID alive with shared process data containing a `claude` binary.
- Skip sessions whose PID descends from abtop's own `claude --print` summary children without hiding user-spawned non-interactive sessions.

### 2. Claude Code transcript: `{config-root}/projects/{encoded-path}/{sessionId}.jsonl`
Path encoding: `/Users/foo/bar` → `-Users-foo-bar`

Key line types:

**`assistant`** (tokens, model, tools):
```json
{
  "type": "assistant",
  "timestamp": "2026-03-28T15:25:55.123Z",
  "message": {
    "model": "claude-opus-4-6",
    "stop_reason": "end_turn",
    "usage": {
      "input_tokens": 2,
      "output_tokens": 5,
      "cache_read_input_tokens": 11313,
      "cache_creation_input_tokens": 4350
    },
    "content": [
      { "type": "text", "text": "..." },
      { "type": "tool_use", "name": "Edit", "input": { "file_path": "src/main.rs", ... } }
    ]
  }
}
```

**`user`** (prompts, version):
```json
{ "type": "user", "timestamp": "...", "version": "2.1.86", "gitBranch": "main", "message": { "role": "user", "content": "..." } }
```

**`last-prompt`** (session tail marker):
```json
{ "type": "last-prompt", "lastPrompt": "...", "sessionId": "..." }
```

- **Size: 1KB–18MB**. Append-only, new line per message.
- **Reading strategy**: On first discovery, scan full file to build cumulative token totals. Then watch file size — on growth, read only new bytes appended since last read (track file offset). This gives both lifetime totals and real-time updates without re-reading.
- **Partial line handling**: leave the offset before incomplete invalid JSON and retry those bytes on the next read. Valid JSON without a trailing newline is accepted. Complete malformed lines are skipped; reads cap individual lines at 10 MiB.
- **File rotation**: if the file shrinks or its identity changes, reset and re-scan.

### 3. Codex sessions: local app-server + rollout JSONL

Discovery strategy:
1. One Codex-owned worker queries existing Unix sockets on macOS/Linux under default `~/.codex`, `CODEX_HOME`, and roots inferred from live processes' open rollouts. Canonicalize roots/paths. No daemon startup, thread resume, or subscriptions.
2. Initialize the local WebSocket-over-Unix connection, paginate `thread/loaded/list`, and request each thread via `thread/read` with `includeTurns: false`. The Unix peer PID identifies the queried instance, never an individual session's kill target. Two-second socket I/O/RPC timeouts; transport failure skips remaining reads on that connection.
3. Merge by `(canonical config root, thread.id)`. Store thread ID in `AgentSession.session_id`; the API's session-tree `sessionId`, PID, and cwd are not deduplication keys. Loaded subagent/fork threads are independent rows, including threads with no rollout yet or ephemeral history.
4. Parse selected rollout paths once per tick for tokens, chat, tools, and context using `session_meta`, `turn_context`, `event_msg`, `response_item`, and newer `item_completed` wrappers. Codex still re-parses from the beginning rather than using Claude's incremental offsets. Runtime metadata/status takes precedence; missing metrics use the existing empty defaults.
5. CLI/open-file fallback preserves exclusive ownership only when one verified CLI process holds one rollout. Multiple owners/files and shared app-server/MCP processes remain Unknown/PID 0; shared children, memory, and ports are never attached to every thread. Successfully queried host FDs cannot recreate unloaded historical rows.
6. For roots with unavailable native queries and an unqueried app-server host, recursively scan rollout date directories at most once per minute, without following symlinks. Keep files less than 30 minutes old as Unknown candidates, using the existing interactive-originator filter. Windows uses recent-file Unknown candidates without heuristic PID assignment. The age filter never gates a loaded native thread or exclusive live CLI rollout.
7. Guardian review threads are excluded; MCP-owned rollouts are suppressed by default and shown in the MCP panel. Remove the old recently-finished file scan: Done rows are already filtered from live results.
8. Failed/incomplete native queries retain prior metadata as Unknown only while the recorded server PID is still an app-server in fresh process data. A complete successful list replaces that instance's snapshot. A missing thread on one instance says nothing about independent CLI/app-server instances.

`--once`/`--json` wait up to ten seconds for initial discovery, then collect again.
Library users may call `App::wait_for_discovery(timeout)` between their first two
collection ticks. Interactive polling consumes results without waiting.

Rate limits extracted from `token_count` events:
```json
{
  "rate_limits": {
    "limit_id": "codex",
    "primary": { "used_percent": 9.0, "window_minutes": 300, "resets_at": 1774686045 },
    "secondary": { "used_percent": 14.0, "window_minutes": 10080, "resets_at": 1775186466 },
    "plan_type": "plus"
  }
}
```

Only account-level limits (`limit_id` absent or `codex`) are accepted; model-specific
limits are ignored. Window durations are preserved. The latest values are written
to `<cache-dir>/abtop/codex-rate-limits.json` and used when no live session reports
limits. Both Claude and Codex readers retain old values; the quota UI dims a
source after 10 minutes and hides its reset countdowns.

### 4. OpenCode sessions: `~/.local/share/opencode/opencode.db`

- Discover running `opencode` processes via shared process data.
- Read recent sessions from OpenCode's SQLite DB through `sqlite3 -readonly -json` on slow ticks and reuse cached rows between queries. Windows also probes `%LOCALAPPDATA%/opencode` and `%APPDATA%/opencode`.
- Match live PIDs to DB sessions by process cwd. OpenCode does not expose a PID/session mapping, so when multiple DB rows share one cwd, only live PIDs should be assigned and older rows should not be shown as live duplicates.
- OpenCode contributes session/token/project/port data, but not quota data. Quota remains Claude + Codex only.

### 5. Subagents: `~/.claude/projects/{path}/{sessionId}/subagents/`

- `agent-{hash}.jsonl` — same JSONL format as main transcript
- `agent-{hash}.meta.json` — `{ "agentType": "general-purpose", "description": "..." }`

### 6. Process tree and ports

On macOS, the equivalent commands are:
```bash
ps -eo pid,ppid,rss,%cpu,command    # All processes
lsof -i -P -n -sTCP:LISTEN         # Open ports
```

- Linux uses `/proc` process/socket data; Windows uses `sysinfo` and `netstat -ano`.
- Build parent→children map from ppid
- Map listening PID → parent agent PID → session

### 7. Git status per project
```bash
git -C {cwd} status --porcelain     # added/modified file counts
```

### 8. Memory status

- Path: `~/.claude/projects/{encoded-path}/memory/`
- Count files in directory + lines in `MEMORY.md`

### 9. Rate limit (Claude Code)

NOT in transcript JSONL. Collected via StatusLine mechanism.

`abtop --setup` creates `abtop-statusline.sh` and registers it in `settings.json`
under an existing `CLAUDE_CONFIG_DIR`, or `~/.claude` otherwise. The Bash hook
uses `python3` to write `abtop-rate-limits.json` under its runtime
`CLAUDE_CONFIG_DIR` (default `~/.claude`).

File format read by abtop:
```json
{
  "source": "claude",
  "five_hour": { "used_percentage": 35.0, "resets_at": 1774715000 },
  "seven_day": { "used_percentage": 12.0, "resets_at": 1775320000 },
  "updated_at": 1774714400
}
```

- Keeps stale data; after 10 minutes the quota UI dims the source and suppresses reset countdowns.
- Requires Claude Code to supply `rate_limits` in its StatusLine input; availability depends on the account/provider.
- Account-level metric, shared across all sessions.
- Show "—" when not configured or data unavailable.

### 10. MCP and host metrics

- `collector/mcp.rs` detects `codex mcp-server`, its parent CLI/profile, and open rollout files. A rollout counts as active only when its mtime is less than 30 minutes old; an open fd alone is not activity.
- `host_info.rs` samples CPU/memory/one-minute load from `/proc` on Linux and CPU/memory via `sysinfo` on Windows (load is 0). macOS currently returns no host metrics; agent/session aggregates still render.

Claude's `stats-cache.json` and `history.jsonl` are not live-monitor inputs; do not use stale daily aggregates as a substitute for transcript collection.

## Session Status Detection

```
◉ Thinking    = model generation inferred from transcript/events
● Executing   = pending tool, active descendant, or working Claude subagent
◌ Waiting     = completed turn, user/permission wait, or no active signal
? Unknown     = unconfirmed ownership or unavailable Codex runtime status
✗ Error       = Codex reports systemError
⏳ RateLimited = non-Codex Waiting + same-agent quota usage > 90%
✓ Done        = collector reports finished/dead session; filtered from live results
```

Claude uses pending tool calls, active descendants (> 5% CPU), working subagents,
and real user prompts awaiting a reply. It does not gate Thinking on transcript
mtime: a long streamed response may not update the file until it completes.
Codex prefers native status: idle and approval/user-input flags map to Waiting,
systemError to Error, and active turns to Executing when a recorded tool is
pending, otherwise Thinking. Native waiting/error/unknown states clear stale
execution timers. Without native status, exclusive CLI ownership uses the
existing turn/tool/generation inference; completed interactive turns are Waiting,
whereas completed `codex exec` sessions are Done. OpenCode uses DB updates within 30 seconds or
CPU activity (agent > 1%, descendants > 5%) to infer Thinking, otherwise Waiting.
There is no `Working` variant; `Error` is available for native Codex system errors.

Dead Claude/OpenCode sessions are omitted by their collectors. Codex may emit
Done internally, but `MultiCollector` removes Done from the final session list;
abtop does not delete agent session files.

**PID reuse risk**: discovery checks expected agent commands in fresh shared process data. Destructive actions additionally re-check `ps -p {pid} -o command=`. Don't trust PID alone.

Current task (2nd line under each session):

- Executing → tool name + bounded argument preview (e.g. `Edit src/main.rs`)
- Thinking → "thinking..." when no tool preview is present
- Waiting → "waiting for input"
- Unknown → "unknown"

**Known limitations**:

- Native Codex loaded/idle/active/error/wait flags describe the connected instance; Thinking versus Executing still depends on recorded tool events.
- Independent or older app-server instances without reachable sockets need heuristic fallback; coverage is not guaranteed. Recently finished files may still appear as Unknown candidates.
- Non-Codex RateLimited is inferred from quota saturation rather than an explicit provider wait event.
- Shared app-server open files alone do not prove a particular thread is loaded or running.
- Polling reflects the most recent observation and can be briefly stale.

## Session Summary Generation

Each session gets a one-line summary title generated via `claude --print`:

- Spawned as background process with 10s timeout
- Uses up to 200 characters each from the initial prompt and first assistant text. Rejects generic/empty/overlong output; falls back to sanitized prompt text (up to 80 characters). Display fallback can also use first assistant text.
- Cached to `<cache-dir>/abtop/summaries.json` via `dirs::cache_dir()` (persists across runs; typically `~/.cache` on Linux and `~/Library/Caches` on macOS).
- Max 3 concurrent summary jobs, max 2 attempts per session. `--once` allows up to 30 seconds for jobs/retries; `--json` and `tick_no_summaries()` do not start summary jobs.

## Context Window Calculation

- **Claude window size**: default 200,000; use 1,000,000 if the transcript/configured model contains `[1m]` or the maximum observed context exceeds 200,000. This is a heuristic, not a complete model catalog.
- **Claude current usage**: last assistant usage's `input_tokens + cache_read_input_tokens`. If cache read is zero and cache creation is positive, use `input_tokens + cache_creation_input_tokens` for fresh-cache context. Never sum both cache fields, which can double-count compaction turns (#54).
- **Codex**: read `model_context_window` from events and use `last_token_usage.input_tokens` directly; cached input is already included.
- **OpenCode**: no context-window percentage is provided.
- **Percentage**: current_usage / window_size * 100
- **Display**: theme gradients reflect usage; the context panel adds `!` at 75%+ and a ⚠ icon at 90%+.
- **Claude compaction**: infer from a context drop greater than 30% combined with a large cache-read drop; retain history/counts for the UI.

## Orphan Port Detection

Tracks child processes that have open ports. When a parent session dies but the child process remains alive and listening:

- Added to `orphan_ports` list automatically
- Displayed in ports panel under "ORPHAN PORTS" section
- Can be terminated via `X` (Shift+X) with safety checks: fresh port scan + exact PID command verification before invoking `kill` with its default signal (SIGTERM on Unix).
- Requires history across ticks; a fresh one-shot snapshot has no previously tracked orphans.

## Key Bindings

| Key | Action |
|-----|--------|
| `↑`/`↓` or `k`/`j` | Select session in list |
| `Enter` | Jump to session pane (Herdr Codex / cmux / tmux / iTerm2) |
| `x` twice within 2s | Kill selected session (SIGKILL) after PID verification; Unknown/Done and shared Codex app-server hosts are protected |
| `X` | Terminate verified orphan port processes |
| `t` | Cycle and persist theme |
| `1`–`7` | Toggle context, quota, tokens, projects, ports, sessions, MCP |
| `T` | Toggle subagent tree view |
| `l` / `L` | Toggle tool timeline |
| `f` / `F` | Toggle file audit |
| `/` | Enter session filter; Enter ends editing, Esc clears |
| `c` | Open/close config overlay; Esc/q also close it |
| `v` | Open/close view menu; Esc also closes it |
| `?` | Open help; any key dismisses it |
| `←`/`→`, `Tab`/`Shift+Tab` | Previous/next compact tab |
| `w` / `u` / `s` | Select compact Work / Usage / System tab |
| `+` / `=` / `-` | Maximize/restore compact section |
| `M` | Toggle suppression of MCP-owned sessions |
| `q` | Quit |
| `r` | Force refresh |

## Tech Stack

- **Rust** (2021 edition; minimum Rust 1.88)
- **ratatui 0.29**: layout, tables, paragraphs, borders and frame rendering; custom bars/gradients/Braille graphs live in `ui/mod.rs`.
- **crossterm 0.28**: terminal backend, raw/alternate-screen setup, keyboard/mouse events. Mouse capture is opt-in (`--mouse`).
- **serde** + **serde_json** for JSON/JSONL parsing
- **chrono** for timestamp formatting
- **dirs** for platform home/config/cache directories; **unicode-width** for display widths; **tempfile** for the updater download.
- Platform dependencies: **proc_pidinfo** on Apple targets, **libc** and **tungstenite 0.28** on Unix, **sysinfo** on Windows. SQLite access uses the external `sqlite3` CLI.
- **Scheduling** (polling, not a filesystem watcher):
  - Input polling/rendering: 500ms idle interval; input can trigger earlier redraws.
  - Session collection + process tree + host sampling: target 2s; ticks are deferred while handling input. Claude tails incrementally, Codex re-parses, OpenCode reuses DB cache.
  - Ports + Git + OpenCode DB: every 5 ticks (~10s); port PID-set changes and new Git cwd entries trigger earlier work.
  - Quota: first tick, then every 6 ticks (~12s) with the current counter; if no data is available, retry every tick.
  - Codex: one background discovery task at a time; native queries each collection cycle, FDs on slow ticks/PID changes (2s macOS timeout), recent compatibility files at most once a minute. Native RPC I/O timeout is 2s.

## Library / JSON Snapshot

The supported library surface includes `app`, `snapshot`, `config`, `demo`,
`host_info`, and `model`. Internal collector/UI helpers may change without a
major version bump. `App::tick_no_summaries()` refreshes monitored data without
LLM summary jobs; `App::to_snapshot(interval_ms)` is a pure read that creates an
owned JSON-serializable DTO. There is no HTTP listener in this crate.

Snapshots include host/agent aggregates, sessions, rate limits, orphan ports,
MCP servers, and bounded per-session token/tool/chat/subagent detail; full
transcripts and the file-access audit are omitted. `token_rate` is the per-tick
delta of input + output + cache creation, excluding cache reads; divide by
`interval_ms / 1000` for a per-second rate. Status variants serialize as
CamelCase (`Thinking`, `Executing`, etc.); chat roles are `user`/`assistant`.

## Commit Convention

```
<type>: <description>
```
Types: `feat`, `fix`, `refactor`, `docs`, `chore`

## Documentation Maintenance

Treat current source as the authority and update both this guide and README.md
when changing modules, CLI flags/key handling, panel layout, collection/status
heuristics, snapshot fields, or privacy behavior. Check `Cargo.toml` for dependency
versions, `lib.rs` for CLI/input dispatch, `ui/mod.rs` for sizing/layout, and
the relevant collector for discovery/status rules. Distinguish per-platform
behavior and heuristics from guarantees; do not describe comments or intended
behavior as implemented when the code differs.

## Commands

```bash
cargo build                    # Build
cargo run                      # Run TUI
cargo run -- --once            # Print snapshot and exit
cargo run -- --json            # Print a JSON snapshot without summary jobs
cargo run -- --demo            # Run TUI with synthetic data
cargo run -- --demo --json     # Print a synthetic JSON snapshot
cargo run -- --mouse           # Enable click/scroll navigation
cargo run -- --theme dracula   # Choose a built-in theme
cargo run -- --version         # Print package version
cargo run -- --update          # Download/run the GitHub shell installer (requires curl/sh)
cargo run -- --setup           # Install StatusLine hook for rate limit collection
cargo run -- --exit-on-jump    # Quit after Enter-jumping to a session terminal (for popup overlays)
cargo test                     # Tests
cargo clippy                   # Lint
```

## Release Process

1. Pick the target semver version and update both `Cargo.toml` and `Cargo.lock`.
2. Verify the package locally:
   ```bash
   cargo test
   cargo clippy -- -D warnings
   cargo build --release
   cargo publish --dry-run
   ```
3. Commit and merge or push the version bump to `main`:
   ```bash
   git add Cargo.toml Cargo.lock
   git commit -m "chore: bump version to X.Y.Z"
   git push origin main
   ```
4. From a clean, up-to-date `main`, create and push an annotated release tag:
   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```
5. Watch the tag-triggered workflows:
   ```bash
   gh run list --workflow Release --limit 5
   gh run list --workflow "Publish to crates.io" --limit 5
   ```
6. `release.yml` builds platform binaries, creates the GitHub Release, and updates the Homebrew formula.
7. `publish.yml` runs `cargo publish` to crates.io automatically.

**Do NOT run `cargo publish` or `gh release create` manually** — the CI workflows handle both.
**Do NOT push the tag before the version bump is on `main`.**
**Do NOT reuse a release tag after a failed publish; bump to a new patch version instead.**

## Current Scope Limits

- Additional agent backends such as Gemini or Cursor (Claude IDE-extension discovery is supported)
- Cost estimation
- Built-in HTTP server or remote/SSH monitoring (external consumers can use snapshots)
- Notifications/alerts

## Terminal Jump (`Enter`)

`Enter` first tries Herdr pane routing for unfinished Codex sessions, then
focuses the terminal running the selected session's agent process.
`jump/herdr.rs` queries `herdr agent list` only on Enter and prefers an exact
`agent_session` identity. Without identity, canonical cwd matching requires
one Codex pane and one unfinished abtop Codex session in that directory.
Ambiguity and focus failures surface as `herdr: <msg>` in the status line.
Each Herdr command has a two-second timeout. Missing/unavailable Herdr or no
matching pane falls through to PID routing; PID `0` never reaches PID adapters.
Herdr selects only its pane, without raising the outer terminal window.

The logic lives in `src/jump/` as a registry of `TerminalJumper` adapters
(one file per backend). `jumpers()` is the single ordered source of truth;
`resolve()` walks it and the first applicable adapter wins.

Each adapter returns a three-way `JumpAttempt`:

- `NotApplicable` — not this backend's terminal; try the next adapter.
- `Jumped` — focused successfully; stop.
- `Failed(msg)` — this backend owns the process but the focus command errored;
  stop and surface `"<backend>: <msg>"` in the status line.

Order (most specific first), mutually exclusive by controlling tty:

1. **cmux** (`jump/cmux.rs`) — reads `CMUX_WORKSPACE_ID` (a UUID cmux exports
   into every surface, inherited by the agent) from the process environment via
   `ps eww`, then `cmux select-workspace --workspace <uuid>`.
2. **tmux** (`jump/tmux.rs`) — only when abtop itself runs inside tmux (`$TMUX`).
   Maps PID → pane via `tmux list-panes -a -F '#{pane_pid} #{session_name}:#{window_index}.#{pane_index}'`
   + process-tree descent, then `switch-client` / `select-window` / `select-pane`.
   PID in no pane → `NotApplicable` (lets another backend try).
3. **iTerm2** (`jump/iterm2.rs`) — resolves the PID's controlling tty (`ps -o tty=`),
   then AppleScript selects the session whose `tty` matches and brings its
   window/app to the front. First call triggers a one-time macOS Automation
   permission prompt; until granted, `osascript` exits non-zero → `Failed`.

Parsing/registry logic is unit-tested in `jump/mod.rs`; the thin `ps`/`osascript`/
`tmux` I/O wrappers are verified manually.

## Privacy

abtop reads transcripts, prompts, tool inputs, and memory files. These may contain secrets.

- **Tool display**: collectors keep bounded tool argument previews (paths/command prefixes/patterns), not full edit/write contents. Known secret prefixes are redacted and terminal control/bidi characters are sanitized where transcript display data is built; this is best-effort, not a guarantee that all secrets are removed.
- **TUI/`--once` summaries**: generated titles or sanitized prompt/assistant-text fallbacks can reveal conversation context. The selected TUI detail also shows up to 12 redacted chat messages by default; timeline/file-audit views expose local activity metadata.
- **JSON snapshots**: include summaries, chat tails, cwd/config roots, tool previews and child commands. Treat them as private data; external consumers must provide their own access controls.
- **Network**: collection itself makes no provider API calls. Summary jobs send bounded prompt/assistant context through `claude --print`, which may call the provider. Explicit `--update` downloads and runs the GitHub installer via `curl`/`sh`.
- **Local writes/actions**: own config/cache persistence and explicit setup/kill/jump/update operations mean the entire application is not strictly read-only.

## Gotchas

- **Transcript size**: Claude scans fully once, then tracks offset/identity and retries incomplete bytes. Codex currently re-parses each selected rollout from the beginning; avoid assuming all collectors have an incremental cache.
- **Session file deletion**: files disappear when Claude exits. Handle `NotFound` between scan and read.
- **stats-cache.json is stale**: only updated on `/stats` command. Don't use for live data.
- **Context window**: Claude's 200K/1M inference can misclassify new models; Codex relies on reported window metadata. Neither is an authoritative live provider API.
- **Rate limit is account-level**: shared across all sessions. Don't show per-session.
- **Path encoding**: `/Users/foo/bar` → `-Users-foo-bar`. Used for transcript directory names.
- **Path encoding collision**: `-Users-foo-bar-baz` could be `/Users/foo/bar-baz` or `/Users/foo-bar/baz`. Use session JSON's `cwd` as source of truth.
- **lsof can be slow**: macOS port scans are cached on slow ticks; desktop rollout ownership uses a background scanner. Some CLI/MCP discovery still performs local commands during collection.
- **Child process tree**: `pgrep -P` only gets direct children. Build full tree from `ps -eo ppid`.
- **Port detection race**: a port can close between lsof and display. Show stale data gracefully.
- **Subagent directory may not exist**: only created when Agent tool is used. Check existence before scanning.
- **Undocumented internals**: all data sources are Claude Code/Codex implementation details, not stable APIs. Schema may change without notice. Defensive parsing with `serde(default)` everywhere.
- **Terminal size**: minimum 60x18; below 100 columns use compact tabs. Wide layout reserves mid-tier space before allocating sessions and shows context only with surplus.
- **PID reuse in port cache**: invalidate cached ports when the set of tracked PIDs changes.
- **Rate limit staleness**: old values remain available; quota sources dim after 10 minutes and reset countdowns disappear. RateLimited promotion still uses retained percentages, so it is a heuristic.
- **`/clear` + multi-PID same cwd**: after `/clear`, Claude Code mints a new `sessionId` + `.jsonl` without rewriting `sessions/{PID}.json`. abtop overrides the stale sid by picking the newest transcript in the project dir, but this heuristic can't disambiguate ownership when two live `claude` PIDs share a cwd — so the override is disabled in that case and both sessions keep their original sid until exit. Use separate worktrees if live tracking is needed on both simultaneously.
