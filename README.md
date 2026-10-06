# abtop

**Like [btop](https://github.com/aristocratos/btop), but for your AI coding agents.**

See Claude Code, Codex CLI/desktop, and OpenCode sessions at a glance — token usage, context window %, rate limits, child processes, open ports, and more.
Sessions are discovered from local process/file state across macOS, Linux, and Windows. Claude Code discovery supports multiple active profile roots and CLI, desktop-app, and IDE-extension processes.

![demo](https://raw.githubusercontent.com/graykode/abtop/main/assets/demo.gif)

## Why

- Running 3+ agents across projects? See them all in one screen.
- Hitting rate limits? Watch your quota in real-time.
- Agent spawned a server and forgot to kill it? Orphan port detection.
- Context window filling up? Per-session % bars with warnings.

Monitoring reads local agent data without API keys or service auth. abtop saves
its own configuration/caches and offers explicit process termination and setup
actions. Session titles use `claude --print`, which may call your provider; use
`--json` or `App::tick_no_summaries()` to collect without title-generation jobs.

## Install

### macOS / Linux

> [!IMPORTANT]
> On Linux, ensure `sqlite3` is installed to enable monitoring for OpenCode sessions.

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/graykode/abtop/releases/latest/download/abtop-installer.sh | sh
```

### Cargo

```bash
cargo install abtop
```

### Windows

Native support — no WSL required. Uses `sysinfo` for process info and host CPU/MEM metrics, and `netstat -ano` for listening ports. Windows has no load average, so LOAD is reported as 0. OpenCode session discovery additionally requires the `sqlite3` CLI (`winget install SQLite.SQLite`); without it abtop prints a one-time warning to stderr.

```powershell
powershell -c "irm https://github.com/graykode/abtop/releases/latest/download/abtop-installer.ps1 | iex"
```

Or `cargo install abtop` from any terminal with Git in PATH. Claude Code config is resolved automatically from `%USERPROFILE%\.claude`.

### Other

Pre-built binaries for all platforms are available on the [GitHub Releases](https://github.com/graykode/abtop/releases) page.

## Usage

```bash
abtop                    # Launch TUI
abtop --once             # Print snapshot and exit
abtop --json             # Print one JSON snapshot and exit (for scripts/tools)
abtop --setup            # Install rate limit collection hook
abtop --theme dracula    # Launch with a specific theme
abtop --mouse            # Enable mouse click/scroll navigation
abtop --demo             # Launch with synthetic sessions
abtop --demo --json      # Print synthetic JSON without reading live sessions
abtop --exit-on-jump     # Quit after a successful Enter jump
abtop --version          # Print version
abtop --update           # Run the GitHub shell installer (requires curl/sh)
```

Recommended terminal size: **120x40** or larger. Minimum **60x18**. At widths
below 100 columns, the UI switches to **Work** (sessions/projects), **Usage**
(context/quota/tokens), and **System** (ports/MCP) tabs. Enabled sections share
the active tab vertically; `+`/`=` maximizes a section and `-` restores the split.
The wide layout reserves space for the mid row and sessions, then shows context
when enough height remains. All seven panels can be toggled independently.
Mouse capture is off by default so terminal drag selection and copy keep working. Launch with `--mouse` if you prefer click targets and wheel navigation.

### Terminal Jump

Press `Enter` to focus the terminal running the selected agent. PID routing tries
cmux and tmux, then iTerm2 on macOS. Unsupported terminals produce no jump.

For Codex sessions in Herdr, abtop first tries `herdr agent list` and focuses the matching pane. It prefers an exact session ID; when Herdr does not expose one, the working directory must contain exactly one Herdr Codex pane and one unfinished abtop Codex session. Ambiguous matches and focus failures appear in the status line. Herdr commands run only on Enter, with a two-second timeout each. Pane selection does not raise Herdr's outer terminal window.

```bash
tmux new -s work
# pane 0: abtop
# pane 1: claude (project A)
# pane 2: claude (project B)
# → Enter on a session in abtop jumps to its pane
```

## Supported Agents

| Feature           | Claude Code | Codex CLI | OpenCode |
| ----------------- | :---------: | :-------: | :------: |
| Session Discovery |     ✅      |    ✅     |    ✅    |
| Token Tracking    |     ✅      |    ✅     |    ✅    |
| Context Window %  |     ✅      |    ✅     |    ❌    |
| Status Detection  |     ✅      |    ✅     |    ✅    |
| Current Task      |     ✅      |    ✅     |    ❌    |
| Rate Limit        |     ✅      |    ✅     |    ❌    |
| Git Status        |     ✅      |    ✅     |    ✅    |
| Children / Ports  |     ✅      |    ✅     |    ✅    |
| Subagents         |     ✅      |    ❌     |    ❌    |
| Memory Status     |     ✅      |    ❌     |    ❌    |

OpenCode support reads the local SQLite database at `~/.local/share/opencode/opencode.db` (also the default location on Windows; `%LOCALAPPDATA%\opencode` and `%APPDATA%\opencode` are probed as fallbacks) and requires `sqlite3` in `PATH` (on Windows: `winget install SQLite.SQLite`).

Codex desktop/app-server sessions are discovered from recent rollout files,
with background open-file scans to confirm ownership. Sessions with unconfirmed
ownership show **Unknown** and PID 0; shared app-server memory/children are not
attributed to individual sessions. Guardian review rollouts are excluded.
The MCP panel currently detects `codex mcp-server` processes, rather than all
MCP services; their rollouts are suppressed from the session list by default
(`M` toggles this).

Session states are **Thinking**, **Executing**, **Waiting**, **Unknown**, and
**RateLimited**. Status is inferred from transcript/tool events, process activity,
or OpenCode DB updates. Waiting sessions are marked RateLimited when a retained
same-agent account quota exceeds 90%; this does not prove a provider is blocking
the session. Finished (`Done`) rows are filtered from the live list.

Quota is account-level and limited to Claude/Codex. Gauges show **remaining**
quota; window labels follow reported durations (normally 5h/7d, sometimes 30d).
Values older than 10 minutes remain visible with a dimmed source name and no
reset countdown. Claude data comes from the StatusLine hook; Codex values are
cached under the platform cache directory for use without a live session.

## Themes

12 built-in themes, including 4 colorblind-friendly options (`high-contrast`, `protanopia`, `deuteranopia`, `tritanopia`). Press `t` to cycle at runtime, or launch with `--theme <name>`. Your choice is saved to the abtop configuration file (see below).

| btop (default) | dracula | catppuccin |
|:-:|:-:|:-:|
| ![btop](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/btop.png) | ![dracula](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/dracula.png) | ![catppuccin](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/catppuccin.png) |

| tokyo-night | gruvbox | nord |
|:-:|:-:|:-:|
| ![tokyo-night](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/tokyo-night.png) | ![gruvbox](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/gruvbox.png) | ![nord](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/nord.png) |

Colorblind-friendly themes:

| high-contrast | protanopia |
|:-:|:-:|
| ![high-contrast](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/high-contrast.png) | ![protanopia](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/protanopia.png) |

| deuteranopia | tritanopia |
|:-:|:-:|
| ![deuteranopia](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/deuteranopia.png) | ![tritanopia](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/tritanopia.png) |

Light themes (`light` — Solarized cream, `white` — GitHub-style pure white) for bright terminals:

| light | white |
|:-:|:-:|
| ![light](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/light.png) | ![white](https://raw.githubusercontent.com/graykode/abtop/main/assets/themes/white.png) |

## Configuration

Configuration is stored at `<config-dir>/abtop/config.toml`, resolved by
`dirs::config_dir()`: typically `~/.config` on Linux, `~/Library/Application Support`
on macOS, and `%APPDATA%` on Windows. Linux respects `XDG_CONFIG_HOME`.
The file supports:

```toml
theme = "btop"
# Hide specific agent CLIs from the TUI (case-insensitive).
# Useful if you only use one agent and want a cleaner view.
hidden_agents = ["codex"]
# Additional Claude Code profile roots to scan.
# abtop also auto-discovers ~/.claude and ~/.claude-* roots that contain
# both sessions/ and projects/.
claude_config_dirs = ["~/.claude-personal", "~/.claude-work-team"]
# UI language. Omit or leave empty to auto-detect from LANG.
language = "zh"
# All seven panels default to true. Toggles are saved here at runtime.
show_context = true
show_quota = true
show_tokens = true
show_projects = true
show_ports = true
show_sessions = true
show_mcp = true
```

`abtop --setup` installs a Bash/Python StatusLine hook under an existing
`CLAUDE_CONFIG_DIR`, or `~/.claude` otherwise. The hook requires `bash` and
`python3`; monitoring itself does not require Python.

### Supported Languages

| Code | Language            |
| ---- | ------------------- |
| `en` | English (default)   |
| `zh` | Simplified Chinese  |

When `language` is unset, abtop auto-detects from `LANG` — any value starting with `zh` switches to Simplified Chinese, otherwise English.

## Key Bindings

| Key                | Action                               |
| ------------------ | ------------------------------------ |
| `↑`/`↓` or `k`/`j` | Select session                       |
| `Enter`            | Jump to session terminal             |
| `x` twice within 2s | Kill selected session after PID verification (SIGKILL) |
| `X`                | Terminate verified orphan port processes (default Unix SIGTERM) |
| `t`                | Cycle theme                          |
| `1`–`7`            | Toggle context, quota, tokens, projects, ports, sessions, MCP |
| `T`                | Toggle subagent tree view            |
| `l` / `L`          | Toggle tool timeline                 |
| `f` / `F`          | Toggle file audit                    |
| `/`                | Edit session filter; Enter finishes editing |
| `Esc`              | Clear filter or close config/view overlay |
| `c`                | Open/close config overlay            |
| `v`                | Open/close view menu                 |
| `?`                | Open help; any key dismisses it      |
| `←`/`→`, `Tab`/`Shift+Tab` | Previous/next compact tab     |
| `w` / `u` / `s`    | Select Work / Usage / System compact tab |
| `+` / `=` / `-`    | Maximize/restore compact section     |
| `M`                | Toggle suppression of MCP-owned sessions |
| `q`                | Quit                                 |
| `r`                | Force refresh                        |

Unknown/Done sessions cannot be killed, and PID verification refuses shared
Codex app-server hosts. Orphan termination rechecks listening ports and requires
an exact command match. Orphan detection needs history across collection ticks,
so a fresh one-shot snapshot has no previously tracked orphan ports.

## Architecture

abtop is a Rust 2021 binary and library crate (minimum Rust 1.88). The TUI uses
**Ratatui 0.29** for layout/widgets/rendering and **Crossterm 0.28** for its
terminal backend and keyboard/mouse events. Gradient meters and Braille graphs
are custom rendering helpers; there is no additional TUI component framework.

```text
Local transcripts / SQLite / process state
                  ↓
            MultiCollector
                  ↓
                 App
                  ↓
       Ratatui TUI / JSON Snapshot
```

| Module | Responsibility |
| ------ | -------------- |
| `src/main.rs` / `src/lib.rs` | Thin binary entry; CLI modes, terminal setup, event loop, key/mouse dispatch |
| `src/app.rs` | Shared monitor state, collection ticks, selection/filter/view actions, summary jobs/cache |
| `src/collector/` | `AgentCollector` interface and `MultiCollector`; Claude/Codex/OpenCode adapters, shared process/port/Git data, quota cache, MCP and orphan detection |
| `src/model/` | Session/status, quota, child/subagent, tool, chat and file-access types |
| `src/ui/` | Responsive layout and seven panels: context, quota, tokens, projects, ports, sessions, MCP; header/footer and config/help/view overlays |
| `src/jump/` | Herdr-first routing and cmux/tmux/iTerm2 terminal adapters |
| `src/snapshot.rs` | Owned, serializable views for JSON/library consumers |
| `src/host_info.rs` | Host CPU/memory/load sampling and agent aggregates |
| `src/config.rs`, `src/theme.rs`, `src/locale.rs` | Config persistence, color palettes and English/Chinese UI text |
| `src/setup.rs`, `src/demo.rs` | Claude StatusLine installation and synthetic demo data |

The event loop polls input/redraws at a 500ms idle interval and targets collection
every 2 seconds, deferring collection while handling input. Ports/Git/OpenCode
DB refresh every 5 ticks (~10 seconds), with earlier port refreshes on PID-set
changes. Quota refreshes initially, then every 6 ticks (~12 seconds), or every
tick when no data is available. Claude parsing is incremental; Codex currently
re-parses selected rollouts. Slow desktop rollout discovery and summary jobs
run in background threads.

Linux reads `/proc` for process/port and host metrics. macOS uses `ps`,
`proc_pidinfo`/`lsof` for process/open-file discovery; host CPU/memory/load metrics
are currently unavailable there. Windows uses `sysinfo` and `netstat -ano`.
See [AGENTS.md](./AGENTS.md) for implementation details and maintenance guidance.

## Library / JSON snapshot

abtop is also a library crate, so local tools can reuse its data-collection
layer in-process and serialize the same state the TUI renders. The library
avoids shelling out to a second abtop process; collection itself still reads
local state and can launch commands such as `git`, `lsof`, and `sqlite3`.

```bash
abtop --json    # one-shot JSON snapshot for scripts
```

For long-running consumers, build an `App`, refresh it with
`App::tick_no_summaries()` (which never spawns `claude --print`, so it doesn't
touch your Claude quota), and call `App::to_snapshot(interval_ms)` to get a
JSON-serializable `Snapshot`:

```rust,no_run
use abtop::app::App;
use abtop::{config, theme::Theme};

let cfg = config::load_config();
let mut app = App::new_with_config_and_claude_dirs(
    Theme::default(), &cfg.hidden_agents, cfg.panels, &cfg.claude_config_dirs,
);
app.tick_no_summaries();
let json = serde_json::to_string(&app.to_snapshot(2_000)).unwrap();
```

`App` is not `Send` (it owns the collectors), so keep it on one thread and pass
the serialized JSON elsewhere. `to_snapshot()` is a pure read and does not
refresh data or start subprocesses. The crate does not include an HTTP server.
`token_rate` is the per-tick delta of input + output + cache creation (excluding
cache reads); divide by `interval_ms / 1000` for tokens per second. Snapshots
contain bounded token/tool/chat/subagent detail, but omit full transcripts and
the file-access audit. Status names serialize as `Thinking`, `Executing`, etc.,
and chat roles as `user`/`assistant`.
[abtop-web-ui](https://github.com/XKHoshizora/abtop-web-ui)
is a reference consumer: a local-first web dashboard built on exactly this API.

## Privacy

Collection reads local files and process/open-file metadata without provider
API calls. Tool displays retain bounded argument previews (paths, command
prefixes or patterns), rather than full edit/write contents. Known secret
prefixes and unsafe terminal characters are removed from transcript display
data on a best-effort basis.

The selected TUI detail shows up to 12 redacted chat messages by default;
timeline/file-audit views expose local activity metadata. TUI and `--once`
session titles can fall back to sanitized prompt or assistant text. Session
summaries send up to 200 characters each of the initial prompt and first
assistant text through `claude --print`, which may make a provider API call.
`--json` skips these jobs. Explicit `--update` downloads and runs a GitHub
installer via `curl`/`sh`; config/cache persistence and explicit setup/kill/jump
actions also mean the application as a whole is not strictly read-only.

The JSON snapshot includes richer local dashboard data, including `summary`, `chat_messages`, working directories, config roots, tool-call previews, child process commands, token counts, and port metadata. Chat text is bounded and redacted by the collectors, but it is still derived from local transcripts and may contain sensitive project context. Treat JSON snapshots as local/private data and avoid writing them to shared logs or exposing them on a network without your own access controls.

## Acknowledgements

Huge thanks to [@tbouquet](https://github.com/tbouquet) for driving much of abtop's recent shape — themes, config overlay and panel toggles, session filtering, subagent tree view, the context window gauge with compaction detection, plus a steady stream of fixes and security hardening along the way.

## License

MIT
