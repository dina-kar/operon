//! A tiny HTTP/1.1 server for tests: one handler, real sockets.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub struct Req {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: String,
}

impl Req {
    pub fn form(&self) -> HashMap<String, String> {
        url::form_urlencoded::parse(self.body.as_bytes())
            .into_owned()
            .collect()
    }
}

pub struct Res {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// Written one part at a time, `gap` apart (a close-delimited body).
    pub parts: Vec<String>,
    pub gap: Duration,
}

impl Res {
    pub fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            parts: vec![body.into()],
            gap: Duration::ZERO,
        }
    }

    pub fn redirect(to: &str) -> Self {
        Self {
            status: 302,
            headers: vec![("location".into(), to.into())],
            parts: vec![],
            gap: Duration::ZERO,
        }
    }
}

pub type Handler = Arc<dyn Fn(&Req) -> Res + Send + Sync>;

pub struct Server {
    pub addr: SocketAddr,
    pub requests: Arc<Mutex<Vec<Req>>>,
}

impl Server {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

pub async fn serve(handler: impl Fn(&Req) -> Res + Send + Sync + 'static) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let handler: Handler = Arc::new(handler);
    let log = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let header_end = loop {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                let mut lines = head.lines();
                let mut first = lines.next().unwrap_or("").split_whitespace();
                let method = first.next().unwrap_or("").to_owned();
                let target = first.next().unwrap_or("/").to_owned();
                let (path, query) = target
                    .split_once('?')
                    .map_or((target.clone(), String::new()), |(p, q)| {
                        (p.into(), q.into())
                    });
                let headers: HashMap<String, String> = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
                    .collect();
                let length: usize = headers
                    .get("content-length")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(0);
                while buf.len() < header_end + length {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8_lossy(&buf[header_end..]).to_string();
                let req = Req {
                    method,
                    path,
                    query,
                    headers,
                    body,
                };
                log.lock().unwrap().push(req.clone());
                let res = handler(&req);
                let mut out = format!("HTTP/1.1 {} X\r\nconnection: close\r\n", res.status);
                for (k, v) in &res.headers {
                    out.push_str(&format!("{k}: {v}\r\n"));
                }
                out.push_str("\r\n");
                let _ = socket.write_all(out.as_bytes()).await;
                for part in res.parts {
                    let _ = socket.write_all(part.as_bytes()).await;
                    let _ = socket.flush().await;
                    if !res.gap.is_zero() {
                        tokio::time::sleep(res.gap).await;
                    }
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    Server { addr, requests }
}
