//! Embedded Prometheus HTTP exporter (zero dependencies, std only).
//!
//! `serve_metrics` runs a blocking `TcpListener` on a background daemon
//! thread: `GET /metrics` renders [`engine::Database::prometheus_text`],
//! `GET /health` (and `GET /`) answers liveness. The accept loop polls the
//! shared shutdown flags with a short accept timeout so SIGINT/SIGTERM and
//! COM_SHUTDOWN drain it cleanly alongside the wire listeners.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use engine::Database;

/// First line of the request head decides the route.
pub fn route(request_head: &str) -> &'static str {
    let line = request_head.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    if parts.next() != Some("GET") {
        return "not_found";
    }
    match parts.next().unwrap_or_default() {
        "/metrics" | "/metrics/" => "metrics",
        "/" | "/health" | "/health/" | "/healthz" => "health",
        _ => "not_found",
    }
}

/// Pure responder (unit-testable without sockets): status code + body.
pub fn handle_request(db: &Database, request_head: &str) -> (u16, String) {
    match route(request_head) {
        "metrics" => (200, db.prometheus_text()),
        "health" => (200, "ok\n".to_string()),
        _ => (404, "not found\n".to_string()),
    }
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    }
}

fn serve_one(db: &Database, mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = [0u8; 8192];
    let head = match stream.read(&mut buf) {
        Ok(0) => return,
        Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
        Err(_) => return,
    };
    let (code, body) = handle_request(db, &head);
    let header = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reason(code),
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

/// Blocking scrape loop; returns when either shutdown flag is set.
/// Nonblocking accept with a short sleep keeps shutdown latency ~100ms
/// like the wire listeners.
pub fn serve_metrics(
    db: Arc<Database>,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    global: &'static AtomicBool,
) {
    let _ = listener.set_nonblocking(true);
    loop {
        if shutdown.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nonblocking(false);
                serve_one(&db, stream);
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                if shutdown.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
}

/// Bind the metrics listener, trying `port..port+8` like the PG listener.
/// Returns `None` (with a stderr note) when everything is busy — metrics
/// stay disabled rather than failing the whole server.
pub fn bind_metrics(port: u16) -> Option<(TcpListener, u16)> {
    for p in port..port.saturating_add(9) {
        match TcpListener::bind(("0.0.0.0", p)) {
            Ok(l) => return Some((l, p)),
            Err(_) => continue,
        }
    }
    eprintln!("metrics: ports {port}-{} all busy, prometheus exporter disabled", port.saturating_add(8));
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db(name: &str) -> Arc<Database> {
        let dir = std::env::temp_dir().join(format!("hdbmetrics_{}_{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).expect("open");
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 2)").unwrap();
        Arc::new(db)
    }

    #[test]
    fn routes_classify() {
        assert_eq!(route("GET /metrics HTTP/1.1\r\n"), "metrics");
        assert_eq!(route("GET /health HTTP/1.1\r\n"), "health");
        assert_eq!(route("GET / HTTP/1.0\r\n"), "health");
        assert_eq!(route("GET /nope HTTP/1.1\r\n"), "not_found");
        assert_eq!(route("POST /metrics HTTP/1.1\r\n"), "not_found");
        assert_eq!(route(""), "not_found");
    }

    #[test]
    fn handle_request_shapes() {
        let db = test_db("shapes");
        let (code, body) = handle_request(&db, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(code, 200);
        assert!(body.contains("queries_total"));
        assert!(body.contains("# HELP"));
        let (code, body) = handle_request(&db, "GET /health HTTP/1.1\r\n\r\n");
        assert_eq!(code, 200);
        assert_eq!(body, "ok\n");
        let (code, _) = handle_request(&db, "GET /favicon.ico HTTP/1.1\r\n\r\n");
        assert_eq!(code, 404);
    }

    #[test]
    fn http_loopback_roundtrip() {
        let db = test_db("loopback");
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = std::thread::spawn({
            let db = db.clone();
            let shutdown = shutdown.clone();
            move || {
                let _ = listener.set_nonblocking(true);
                // Accept exactly two connections, then exit via shutdown.
                let mut served = 0;
                while served < 2 {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            serve_one(&db, stream);
                            served += 1;
                        }
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
                shutdown.store(true, Ordering::Relaxed);
            }
        });
        for path in ["/metrics", "/health"] {
            let mut sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
            sock.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
                .unwrap();
            let mut resp = Vec::new();
            sock.read_to_end(&mut resp).unwrap();
            let text = String::from_utf8_lossy(&resp).into_owned();
            assert!(
                text.starts_with("HTTP/1.1 200 OK\r\n"),
                "bad status for {path}: {text:?}"
            );
            assert!(
                text.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"),
                "missing content type for {path}"
            );
            if path == "/metrics" {
                assert!(text.contains("queries_total"), "no metrics in body");
            } else {
                assert!(text.ends_with("ok\n"), "bad health body");
            }
        }
        handle.join().unwrap();
        assert!(shutdown.load(Ordering::Relaxed));
    }
}
