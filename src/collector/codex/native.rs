//! Read-only metadata/status queries to an existing Codex app-server.
//! No daemon startup, thread loading, subscriptions, or turn requests.

use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Thread {
    pub id: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub cli_version: String,
    #[serde(default)]
    pub preview: String,
    #[serde(default)]
    pub source: Value,
    #[serde(default)]
    pub thread_source: Option<String>,
    #[serde(default)]
    pub status: Status,
}

impl Thread {
    pub fn is_guardian(&self) -> bool {
        self.thread_source.as_deref() == Some("guardian_review")
            || self.source["subAgent"]["other"].as_str() == Some("guardian")
            || self.source["subagent"]["other"].as_str() == Some("guardian")
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum Status {
    NotLoaded,
    Idle,
    SystemError,
    Active {
        #[serde(default, rename = "activeFlags")]
        flags: Vec<String>,
    },
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Default)]
pub(super) struct Snapshot {
    /// Only identifies the server instance; never a per-session kill target.
    pub host_pid: Option<u32>,
    pub threads: Vec<Thread>,
}

#[cfg(unix)]
mod transport {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;
    use std::io;
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::time::{Duration, Instant};
    use tungstenite::{client, Message, WebSocket};

    const TIMEOUT: Duration = Duration::from_secs(2);

    pub fn query(root: &Path) -> io::Result<Snapshot> {
        query_socket(
            &root.join("app-server-control/app-server-control.sock"),
            TIMEOUT,
        )
    }

    fn query_socket(path: &Path, timeout: Duration) -> io::Result<Snapshot> {
        let stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let host_pid = peer_pid(&stream);
        let (socket, _) = client("ws://localhost/", stream).map_err(io::Error::other)?;
        let mut client = Client {
            socket,
            next_id: 0,
            timeout,
        };
        client.request(
            "initialize",
            json!({
                "clientInfo": {"name": "abtop", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi": false}
            }),
        )?;
        client
            .socket
            .send(Message::Text(
                json!({"method": "initialized"}).to_string().into(),
            ))
            .map_err(io::Error::other)?;

        // Finish pagination before accepting removals. A failed page invalidates
        // the snapshot instead of making previously loaded threads disappear.
        let mut ids = HashSet::new();
        let mut cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Page {
                data: Vec<String>,
                next_cursor: Option<String>,
            }
            let page: Page = serde_json::from_value(client.request(
                "thread/loaded/list",
                json!({"cursor": cursor, "limit": 100}),
            )?)
            .map_err(io::Error::other)?;
            ids.extend(page.data);
            cursor = page.next_cursor;
            match &cursor {
                None => break,
                Some(cursor) if cursors.insert(cursor.clone()) => {}
                Some(_) => return Err(io::Error::other("repeated Codex cursor")),
            }
        }
        let mut ids: Vec<_> = ids.into_iter().collect();
        ids.sort();
        let mut threads = Vec::new();
        let mut transport_failed = false;
        for id in ids {
            let read = if transport_failed {
                Err(io::Error::other("Codex connection unavailable"))
            } else {
                client.request(
                    "thread/read",
                    json!({"threadId": id, "includeTurns": false}),
                )
            };
            if read
                .as_ref()
                .is_err_and(|err| err.kind() != io::ErrorKind::InvalidInput)
            {
                // Do not spend another timeout on every remaining loaded thread.
                transport_failed = true;
            }
            let thread = read
                .and_then(|result| {
                    let thread: Thread = serde_json::from_value(result["thread"].clone())
                        .map_err(io::Error::other)?;
                    if thread.id != id {
                        return Err(io::Error::other("Codex thread id mismatch"));
                    }
                    Ok(thread)
                })
                .unwrap_or_else(|_| Thread {
                    id,
                    ..Thread::default()
                });
            threads.push(thread);
        }
        let _ = client.socket.close(None);
        Ok(Snapshot { host_pid, threads })
    }

    struct Client {
        socket: WebSocket<UnixStream>,
        next_id: u64,
        timeout: Duration,
    }

    impl Client {
        fn request(&mut self, method: &str, params: Value) -> io::Result<Value> {
            self.next_id += 1;
            let id = self.next_id;
            let deadline = Instant::now() + self.timeout;
            self.socket
                .send(Message::Text(
                    json!({"id": id, "method": method, "params": params})
                        .to_string()
                        .into(),
                ))
                .map_err(io::Error::other)?;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Codex RPC timed out",
                    ));
                }
                self.socket.get_mut().set_read_timeout(Some(remaining))?;
                if let Message::Text(text) = self.socket.read().map_err(io::Error::other)? {
                    let response: Value = serde_json::from_str(&text).map_err(io::Error::other)?;
                    // Global status notifications can arrive between replies.
                    if response.get("method").is_some() {
                        continue;
                    }
                    if response["id"].as_u64() != Some(id) {
                        continue;
                    }
                    if let Some(error) = response.get("error") {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            error.to_string(),
                        ));
                    }
                    return response
                        .get("result")
                        .cloned()
                        .ok_or_else(|| io::Error::other("missing Codex RPC result"));
                }
            }
        }
    }

    fn peer_pid(stream: &UnixStream) -> Option<u32> {
        #[cfg(target_os = "linux")]
        {
            let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of_val(&cred) as libc::socklen_t;
            // SAFETY: cred/len point to writable buffers of the expected size.
            let result = unsafe {
                libc::getsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_PEERCRED,
                    (&mut cred as *mut libc::ucred).cast(),
                    &mut len,
                )
            };
            (result == 0).then_some(cred.pid as u32)
        }
        #[cfg(target_vendor = "apple")]
        {
            let mut pid: libc::pid_t = 0;
            let mut len = std::mem::size_of_val(&pid) as libc::socklen_t;
            // SAFETY: pid/len point to writable buffers of the expected size.
            let result = unsafe {
                libc::getsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_LOCAL,
                    libc::LOCAL_PEERPID,
                    (&mut pid as *mut libc::pid_t).cast(),
                    &mut len,
                )
            };
            (result == 0 && pid > 0).then_some(pid as u32)
        }
        #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
        {
            let _ = stream;
            None
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::net::UnixListener;

        #[test]
        fn paginated_read_only_queries_handle_notifications_and_read_errors() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("socket");
            let listener = UnixListener::bind(&path).unwrap();
            let server = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut ws = tungstenite::accept(stream).unwrap();
                let mut methods = Vec::new();
                for _ in 0..6 {
                    let Message::Text(text) = ws.read().unwrap() else {
                        panic!("expected request")
                    };
                    let req: Value = serde_json::from_str(&text).unwrap();
                    let method = req["method"].as_str().unwrap();
                    methods.push(method.to_string());
                    if method == "initialized" {
                        continue;
                    }
                    let result = match method {
                        "initialize" => json!({"userAgent":"test"}),
                        "thread/loaded/list" if req["params"]["cursor"].is_null() => {
                            json!({"data":["a"],"nextCursor":"page2"})
                        }
                        "thread/loaded/list" => json!({"data":["a","b"],"nextCursor":null}),
                        "thread/read" if req["params"]["threadId"] == "a" => {
                            assert_eq!(req["params"]["includeTurns"], false);
                            json!({"thread":{"id":"a","sessionId":"shared-tree","cwd":"/project","status":{"type":"idle"}}})
                        }
                        "thread/read" => {
                            ws.send(Message::Text(
                                json!({"id":req["id"],"error":{"code":-1,"message":"gone"}})
                                    .to_string()
                                    .into(),
                            ))
                            .unwrap();
                            continue;
                        }
                        _ => panic!("unexpected mutation: {method}"),
                    };
                    ws.send(Message::Text(
                        json!({"method":"thread/status/changed","params":{}})
                            .to_string()
                            .into(),
                    ))
                    .unwrap();
                    ws.send(Message::Text(
                        json!({"id":req["id"],"method":"unrelated/server/request","params":{}})
                            .to_string()
                            .into(),
                    ))
                    .unwrap();
                    ws.send(Message::Text(
                        json!({"id":req["id"],"result":result}).to_string().into(),
                    ))
                    .unwrap();
                }
                methods
            });
            let snapshot = query_socket(&path, Duration::from_secs(1)).unwrap();
            assert!(snapshot.host_pid.is_some());
            assert_eq!(snapshot.threads.len(), 2);
            assert_eq!(snapshot.threads[0].status, Status::Idle);
            assert_eq!(snapshot.threads[1].status, Status::Unknown);
            assert_eq!(
                server.join().unwrap(),
                [
                    "initialize",
                    "initialized",
                    "thread/loaded/list",
                    "thread/loaded/list",
                    "thread/read",
                    "thread/read"
                ]
            );
        }

        #[test]
        fn disconnected_or_incomplete_pagination_is_not_an_empty_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("socket");
            let listener = UnixListener::bind(&path).unwrap();
            let server = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut ws = tungstenite::accept(stream).unwrap();
                let Message::Text(text) = ws.read().unwrap() else {
                    panic!()
                };
                let req: Value = serde_json::from_str(&text).unwrap();
                ws.send(Message::Text(
                    json!({"id":req["id"],"result":{}}).to_string().into(),
                ))
                .unwrap();
                let _ = ws.read().unwrap(); // initialized
                let Message::Text(text) = ws.read().unwrap() else {
                    panic!()
                };
                let req: Value = serde_json::from_str(&text).unwrap();
                ws.send(Message::Text(
                    json!({"id":req["id"],"result":{"data":["a"],"nextCursor":"next"}})
                        .to_string()
                        .into(),
                ))
                .unwrap();
            });
            assert!(query_socket(&path, Duration::from_millis(100)).is_err());
            server.join().unwrap();
        }

        #[test]
        fn rpc_timeout_is_bounded_even_with_notifications() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("socket");
            let listener = UnixListener::bind(&path).unwrap();
            let server = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut ws = tungstenite::accept(stream).unwrap();
                let _ = ws.read().unwrap();
                for _ in 0..10 {
                    if ws
                        .send(Message::Text(
                            json!({"method":"notification"}).to_string().into(),
                        ))
                        .is_err()
                    {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            });
            let started = Instant::now();
            assert!(query_socket(&path, Duration::from_millis(50)).is_err());
            assert!(started.elapsed() < Duration::from_secs(1));
            server.join().unwrap();
        }
    }
}

#[cfg(unix)]
pub(super) use transport::query;

#[cfg(not(unix))]
pub(super) fn query(_root: &std::path::Path) -> std::io::Result<Snapshot> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Codex Unix socket unavailable",
    ))
}
