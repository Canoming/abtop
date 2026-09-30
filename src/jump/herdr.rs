//! Herdr pane routing for Codex sessions hosted by a background app-server.

use super::JumpAttempt;
use serde::Deserialize;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
struct AgentListResponse {
    result: AgentList,
}

#[derive(Deserialize)]
struct AgentList {
    agents: Vec<HerdrAgent>,
}

#[derive(Deserialize)]
struct HerdrAgent {
    agent: String,
    pane_id: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    agent_session: Option<String>,
}

pub(super) fn try_jump(session_id: &str, cwd: &str, unfinished_cwds: &[&str]) -> JumpAttempt {
    try_jump_with(session_id, cwd, unfinished_cwds, |args| {
        command_output(Command::new("herdr").args(args), COMMAND_TIMEOUT)
    })
}

fn try_jump_with(
    session_id: &str,
    cwd: &str,
    unfinished_cwds: &[&str],
    mut run: impl FnMut(&[&str]) -> io::Result<Output>,
) -> JumpAttempt {
    let output = match run(&["agent", "list"]) {
        Ok(output) if output.status.success() => output,
        // Herdr may be absent or have no running server. Let terminal routing try.
        _ => return JumpAttempt::NotApplicable,
    };
    let Ok(response) = serde_json::from_slice::<AgentListResponse>(&output.stdout) else {
        return JumpAttempt::NotApplicable;
    };
    let pane = match matching_pane(&response.result.agents, session_id, cwd, unfinished_cwds) {
        Ok(Some(pane)) => pane,
        Ok(None) => return JumpAttempt::NotApplicable,
        Err(msg) => return JumpAttempt::Failed(msg),
    };
    match run(&["agent", "focus", pane]) {
        Ok(output) if output.status.success() => JumpAttempt::Jumped,
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr);
            JumpAttempt::Failed(format!(
                "focus {pane} exited {}: {}",
                output.status,
                detail.trim()
            ))
        }
        Err(error) => JumpAttempt::Failed(format!("focus {pane} failed ({error})")),
    }
}

fn canonical_cwd(cwd: &str) -> Option<PathBuf> {
    if cwd.is_empty() {
        return None;
    }
    Path::new(cwd).canonicalize().ok()
}

fn matching_pane<'a>(
    agents: &'a [HerdrAgent],
    session_id: &str,
    cwd: &str,
    unfinished_cwds: &[&str],
) -> Result<Option<&'a str>, String> {
    let eligible: Vec<_> = agents
        .iter()
        .filter(|a| a.agent == "codex" && !a.pane_id.trim().is_empty())
        .collect();
    if !session_id.is_empty() {
        let exact: Vec<_> = eligible
            .iter()
            .filter(|a| a.agent_session.as_deref() == Some(session_id))
            .collect();
        match exact.as_slice() {
            [agent] => return Ok(Some(&agent.pane_id)),
            [] => {}
            _ => return Err("ambiguous Codex session identity; cannot select a pane".into()),
        }
    }

    let Some(cwd) = canonical_cwd(cwd) else {
        return Ok(None);
    };
    let candidates: Vec<_> = eligible
        .iter()
        .filter(|a| a.cwd.as_deref().and_then(canonical_cwd).as_ref() == Some(&cwd))
        .collect();
    let agent = match candidates.as_slice() {
        [] => return Ok(None),
        [agent] => agent,
        _ => {
            return Err("ambiguous Codex panes in this directory; session identity required".into())
        }
    };
    // An explicit different identity rules this pane out, even if its cwd matches.
    if agent
        .agent_session
        .as_deref()
        .is_some_and(|id| !id.is_empty())
    {
        return Ok(None);
    }
    let session_count = unfinished_cwds
        .iter()
        .filter(|dir| canonical_cwd(dir).as_ref() == Some(&cwd))
        .count();
    if session_count != 1 {
        return Err(
            "ambiguous unfinished Codex sessions in this directory; session identity required"
                .into(),
        );
    }
    Ok(Some(&agent.pane_id))
}

/// Files avoid pipe backpressure while polling, and let timeout cleanup reap
/// the child without blocking on a reader thread or inherited pipe handles.
fn command_output(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    let stdout = tempfile::NamedTempFile::new()?;
    let stderr = tempfile::NamedTempFile::new()?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.reopen()?))
        .stderr(Stdio::from(stderr.reopen()?))
        .spawn()?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < timeout => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(result.err().unwrap_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("command timed out after {} ms", timeout.as_millis()),
                    )
                }));
            }
        }
    };
    Ok(Output {
        status,
        stdout: std::fs::read(stdout.path())?,
        stderr: std::fs::read(stderr.path())?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(pane: &str, cwd: &str, session: Option<&str>) -> HerdrAgent {
        HerdrAgent {
            agent: "codex".into(),
            pane_id: pane.into(),
            cwd: Some(cwd.into()),
            agent_session: session.map(String::from),
        }
    }

    fn output(success: bool, stdout: &[u8], stderr: &[u8]) -> Output {
        #[cfg(unix)]
        let status = {
            use std::os::unix::process::ExitStatusExt;
            std::process::ExitStatus::from_raw(if success { 0 } else { 256 })
        };
        #[cfg(windows)]
        let status = {
            use std::os::windows::process::ExitStatusExt;
            std::process::ExitStatus::from_raw(if success { 0 } else { 1 })
        };
        Output {
            status,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    fn listing(cwd: &str) -> Output {
        output(
            true,
            serde_json::json!({
                "id": "cli:agent:list",
                "result": {"agents": [{
                    "agent": "codex", "pane_id": "w6:p1", "cwd": cwd,
                    // Herdr's done status still means a live agent ready for input.
                    "agent_status": "done"
                }]}
            })
            .to_string()
            .as_bytes(),
            b"",
        )
    }

    #[test]
    fn unique_directory_selects_codex_pane() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let mut claude = agent("w2:p1", cwd, None);
        claude.agent = "claude".into();
        let agents = [claude, agent("w6:p1", cwd, None)];
        assert_eq!(matching_pane(&agents, "s1", cwd, &[cwd]), Ok(Some("w6:p1")));
    }

    #[test]
    fn exact_identity_wins_over_directory_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let agents = [
            agent("w1:p1", cwd, Some("s1")),
            agent("w2:p1", cwd, Some("s2")),
        ];
        for (sid, pane) in [("s1", "w1:p1"), ("s2", "w2:p1")] {
            assert_eq!(
                matching_pane(&agents, sid, cwd, &[cwd, cwd]),
                Ok(Some(pane))
            );
        }
    }

    #[test]
    fn exact_identity_does_not_require_a_readable_directory() {
        let agents = [agent("w6:p1", "", Some("s1"))];
        assert_eq!(matching_pane(&agents, "s1", "", &[]), Ok(Some("w6:p1")));
    }

    #[test]
    fn duplicate_exact_identities_are_ambiguous() {
        let agents = [
            agent("w1:p1", "", Some("s1")),
            agent("w2:p1", "", Some("s1")),
        ];
        assert!(matching_pane(&agents, "s1", "", &[])
            .unwrap_err()
            .contains("ambiguous"));
    }

    #[test]
    fn multiple_panes_or_unfinished_sessions_require_identity() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let agents = [agent("w1:p1", cwd, None), agent("w2:p1", cwd, None)];
        assert!(matching_pane(&agents, "s1", cwd, &[cwd])
            .unwrap_err()
            .contains("panes"));
        assert!(matching_pane(&agents[..1], "s1", cwd, &[cwd, cwd])
            .unwrap_err()
            .contains("unfinished"));
    }

    #[test]
    fn conflicting_identity_never_uses_directory_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        assert_eq!(
            matching_pane(&[agent("w1:p1", cwd, Some("other"))], "s1", cwd, &[cwd]),
            Ok(None)
        );
    }

    #[test]
    fn no_matching_directory_or_invalid_pane_is_not_applicable() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        assert_eq!(
            matching_pane(&[agent("w1:p1", "", None)], "s1", cwd, &[cwd]),
            Ok(None)
        );
        assert_eq!(
            matching_pane(&[agent("", cwd, Some("s1"))], "s1", cwd, &[cwd]),
            Ok(None)
        );
        assert_eq!(
            matching_pane(&[agent("w1:p1", "", None)], "", "", &[""]),
            Ok(None)
        );
    }

    #[test]
    fn empty_identity_uses_directory_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        assert_eq!(
            matching_pane(&[agent("w1:p1", cwd, Some(""))], "", cwd, &[cwd]),
            Ok(Some("w1:p1"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn directory_fallback_canonicalizes_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let alias = dir.path().join("alias");
        std::fs::create_dir(&project).unwrap();
        std::os::unix::fs::symlink(&project, &alias).unwrap();
        let cwd = project.to_str().unwrap();
        let alias = alias.to_str().unwrap();
        assert_eq!(
            matching_pane(&[agent("w1:p1", alias, None)], "s1", cwd, &[alias]),
            Ok(Some("w1:p1"))
        );
    }

    #[test]
    fn successful_discovery_focuses_resolved_pane() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let mut calls = Vec::new();
        let attempt = try_jump_with("s1", cwd, &[cwd], |args| {
            calls.push(args.iter().map(|s| s.to_string()).collect::<Vec<_>>());
            if args == ["agent", "list"] {
                Ok(listing(cwd))
            } else {
                Ok(output(true, b"{}", b""))
            }
        });
        assert_eq!(attempt, JumpAttempt::Jumped);
        assert_eq!(
            calls,
            [vec!["agent", "list"], vec!["agent", "focus", "w6:p1"]]
        );
    }

    #[test]
    fn discovery_with_session_ids_focuses_exact_pane_in_shared_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let mut calls = 0;
        let result = try_jump_with("s2", cwd, &[cwd, cwd], |args| {
            calls += 1;
            if args == ["agent", "list"] {
                let response = serde_json::json!({"result": {"agents": [
                    {"agent": "codex", "pane_id": "w1:p1", "cwd": cwd, "agent_session": "s1"},
                    {"agent": "codex", "pane_id": "w2:p1", "cwd": cwd, "agent_session": "s2"}
                ]}});
                Ok(output(true, response.to_string().as_bytes(), b""))
            } else {
                assert_eq!(args, ["agent", "focus", "w2:p1"]);
                Ok(output(true, b"{}", b""))
            }
        });
        assert_eq!(result, JumpAttempt::Jumped);
        assert_eq!(calls, 2);
    }

    #[test]
    fn missing_herdr_and_discovery_failures_allow_terminal_fallback() {
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::TimedOut,
        ] {
            assert_eq!(
                try_jump_with("s1", "", &[], |args| {
                    assert_eq!(args, ["agent", "list"]);
                    Err(io::Error::new(kind, "unavailable"))
                }),
                JumpAttempt::NotApplicable
            );
        }
        assert_eq!(
            try_jump_with("s1", "", &[], |_| Ok(output(
                false,
                b"",
                b"server unavailable"
            ))),
            JumpAttempt::NotApplicable
        );
    }

    #[test]
    fn malformed_or_empty_responses_never_focus() {
        for json in [
            "not json",
            "{}",
            r#"{"result":{"agents":null}}"#,
            r#"{"result":{"agents":[{"agent":"codex"}]}}"#,
            r#"{"result":{"agents":[]}}"#,
        ] {
            assert_eq!(
                try_jump_with("s1", "", &[], |args| {
                    assert_eq!(args, ["agent", "list"]);
                    Ok(output(true, json.as_bytes(), b""))
                }),
                JumpAttempt::NotApplicable
            );
        }
    }

    #[test]
    fn ambiguous_discovery_reports_failure_without_focusing() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let result = try_jump_with("s1", cwd, &[cwd, cwd], |args| {
            assert_eq!(args, ["agent", "list"]);
            Ok(listing(cwd))
        });
        assert!(matches!(result, JumpAttempt::Failed(msg) if msg.contains("ambiguous")));
    }

    #[test]
    fn focus_exit_failure_includes_command_detail() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        let result = try_jump_with("s1", cwd, &[cwd], |args| {
            Ok(if args == ["agent", "list"] {
                listing(cwd)
            } else {
                output(false, b"", b"pane no longer exists")
            })
        });
        assert!(
            matches!(result, JumpAttempt::Failed(msg) if msg.contains("w6:p1") && msg.contains("pane no longer exists"))
        );
    }

    #[test]
    fn focus_spawn_failure_and_timeout_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().to_str().unwrap();
        for kind in [io::ErrorKind::NotFound, io::ErrorKind::TimedOut] {
            let result = try_jump_with("s1", cwd, &[cwd], |args| {
                if args == ["agent", "list"] {
                    Ok(listing(cwd))
                } else {
                    Err(io::Error::new(kind, "focus unavailable"))
                }
            });
            assert!(
                matches!(result, JumpAttempt::Failed(msg) if msg.contains("focus w6:p1 failed"))
            );
        }
    }

    #[test]
    fn command_runner_reports_missing_binary() {
        let dir = tempfile::tempdir().unwrap();
        let error = command_output(
            &mut Command::new(dir.path().join("absent-herdr")),
            COMMAND_TIMEOUT,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[cfg(unix)]
    #[test]
    fn command_runner_times_out_and_reaps_child() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("pid");
        let started = Instant::now();
        let error = command_output(
            Command::new("sh")
                .args(["-c", "echo $$ > \"$1\"; exec sleep 5", "test"])
                .arg(&pid_file),
            Duration::from_millis(100),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
        let pid = std::fs::read_to_string(pid_file).unwrap();
        assert!(!Command::new("kill")
            .args(["-0", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
    }

    #[cfg(unix)]
    #[test]
    fn command_runner_handles_output_larger_than_pipe_capacity() {
        let result = command_output(
            Command::new("sh").args(["-c", "head -c 131072 /dev/zero; echo diagnostic >&2"]),
            COMMAND_TIMEOUT,
        )
        .unwrap();
        assert!(result.status.success());
        assert_eq!(result.stdout.len(), 131072);
        assert_eq!(result.stderr, b"diagnostic\n");
    }
}
