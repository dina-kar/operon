//! The slice of the Postgres frontend/backend protocol a safekeeper needs:
//! the startup handshake (refusing SSL and GSS encryption), simple queries,
//! small result sets, errors, and CopyBoth streams. walproposer and the
//! pageserver connect with libpq; nothing else is spoken.
//!
//! [`client`] is the frontend side, for tests and the benchmark tools.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;

const PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;
/// The largest message accepted: an AppendRequest is at most 128 KiB plus
/// its header; anything much larger is not a safekeeper client.
const MAX_MESSAGE: usize = 1 << 20;

/// Postgres epoch (2000-01-01) in Unix seconds.
pub const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

fn io(e: std::io::Error) -> Error {
    Error::Io(e.to_string())
}

/// What the startup packet said.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Startup {
    pub params: HashMap<String, String>,
}

impl Startup {
    /// `options='-c k=v k2=v2'` (walproposer) or `options=k=v` pairs.
    pub fn options(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if let Some(opts) = self.params.get("options") {
            for w in opts.split_whitespace() {
                if w == "-c" {
                    continue;
                }
                let w = w.strip_prefix("-c").unwrap_or(w);
                if let Some((k, v)) = w.split_once('=') {
                    out.insert(k.to_string(), v.to_string());
                }
            }
        }
        out
    }
}

/// Read the startup packet, answering `N` to SSL and GSS encryption
/// requests. `None` for a cancel request (the caller closes).
pub async fn read_startup<R, W>(rd: &mut R, wr: &mut W) -> Result<Option<Startup>, Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let len = rd.read_u32().await.map_err(io)? as usize;
        if !(8..=10_000).contains(&len) {
            return Err(Error::Protocol(format!("startup packet of {len} bytes")));
        }
        let code = rd.read_u32().await.map_err(io)?;
        let mut body = vec![0u8; len - 8];
        rd.read_exact(&mut body).await.map_err(io)?;
        match code {
            SSL_REQUEST | GSSENC_REQUEST => {
                wr.write_all(b"N").await.map_err(io)?;
                wr.flush().await.map_err(io)?;
            }
            CANCEL_REQUEST => return Ok(None),
            PROTOCOL_V3 => {
                let mut params = HashMap::new();
                let mut parts = body.split(|b| *b == 0);
                while let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                    if k.is_empty() {
                        break;
                    }
                    params.insert(
                        String::from_utf8_lossy(k).into_owned(),
                        String::from_utf8_lossy(v).into_owned(),
                    );
                }
                return Ok(Some(Startup { params }));
            }
            other => {
                return Err(Error::Protocol(format!("unsupported protocol {other:#x}")));
            }
        }
    }
}

/// Read one tagged message: `(tag, body)`. `None` at a clean EOF.
pub async fn read_message<R: AsyncRead + Unpin>(rd: &mut R) -> Result<Option<(u8, Bytes)>, Error> {
    let tag = match rd.read_u8().await {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let len = rd.read_u32().await.map_err(io)? as usize;
    if !(4..=MAX_MESSAGE).contains(&len) {
        return Err(Error::Protocol(format!(
            "message {:?} of {len} bytes",
            tag as char
        )));
    }
    let mut body = vec![0u8; len - 4];
    rd.read_exact(&mut body).await.map_err(io)?;
    Ok(Some((tag, Bytes::from(body))))
}

/// Append a tagged message to `buf`.
pub fn put_message(buf: &mut BytesMut, tag: u8, body: &[u8]) {
    buf.put_u8(tag);
    buf.put_u32(body.len() as u32 + 4);
    buf.put_slice(body);
}

/// `AuthenticationOk`, a few parameters, and `ReadyForQuery`.
pub fn put_login_ok(buf: &mut BytesMut) {
    put_message(buf, b'R', &0u32.to_be_bytes());
    for (k, v) in [
        ("server_version", "16.9"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO"),
        ("integer_datetimes", "on"),
    ] {
        let mut body = BytesMut::new();
        put_cstr(&mut body, k);
        put_cstr(&mut body, v);
        put_message(buf, b'S', &body);
    }
    put_ready(buf);
}

pub fn put_ready(buf: &mut BytesMut) {
    put_message(buf, b'Z', b"I");
}

fn put_cstr(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// A `RowDescription` of text (or int4, when named in `int4`) columns.
pub fn put_row_description(buf: &mut BytesMut, cols: &[&str], int4: &[&str]) {
    let mut body = BytesMut::new();
    body.put_u16(cols.len() as u16);
    for c in cols {
        put_cstr(&mut body, c);
        body.put_u32(0); // table oid
        body.put_u16(0); // attnum
        if int4.contains(c) {
            body.put_u32(23);
            body.put_i16(4);
        } else {
            body.put_u32(25);
            body.put_i16(-1);
        }
        body.put_i32(-1); // typmod
        body.put_u16(0); // text format
    }
    put_message(buf, b'T', &body);
}

pub fn put_data_row(buf: &mut BytesMut, values: &[Option<&str>]) {
    let mut body = BytesMut::new();
    body.put_u16(values.len() as u16);
    for v in values {
        match v {
            Some(v) => {
                body.put_i32(v.len() as i32);
                body.put_slice(v.as_bytes());
            }
            None => body.put_i32(-1),
        }
    }
    put_message(buf, b'D', &body);
}

pub fn put_command_complete(buf: &mut BytesMut, tag: &str) {
    let mut body = BytesMut::new();
    put_cstr(&mut body, tag);
    put_message(buf, b'C', &body);
}

/// An `ErrorResponse` with severity, SQLSTATE and message.
pub fn put_error(buf: &mut BytesMut, sqlstate: &str, message: &str) {
    let mut body = BytesMut::new();
    for (f, v) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        body.put_u8(f);
        put_cstr(&mut body, v);
    }
    body.put_u8(0);
    put_message(buf, b'E', &body);
}

/// `CopyBothResponse`: binary format, no columns.
pub fn put_copy_both(buf: &mut BytesMut) {
    put_message(buf, b'W', &[0, 0, 0]);
}

pub fn put_copy_data(buf: &mut BytesMut, data: &[u8]) {
    put_message(buf, b'd', data);
}

/// Microseconds since the Postgres epoch, now.
pub fn pg_now_us() -> i64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default();
    unix - PG_EPOCH_UNIX_SECS * 1_000_000
}

/// The frontend side: enough of libpq for tests and the benchmark tools.
pub mod client {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    use super::{PROTOCOL_V3, io, put_message, read_message};
    use crate::Error;

    /// Send a startup packet and wait for `ReadyForQuery`.
    pub async fn startup<S: AsyncRead + AsyncWrite + Unpin>(
        s: &mut S,
        params: &[(&str, &str)],
    ) -> Result<(), Error> {
        let mut body = BytesMut::new();
        body.put_u32(PROTOCOL_V3);
        for (k, v) in params.iter().filter(|(k, _)| *k != "password") {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let mut pkt = BytesMut::new();
        pkt.put_u32(body.len() as u32 + 4);
        pkt.put_slice(&body);
        s.write_all(&pkt).await.map_err(io)?;
        s.flush().await.map_err(io)?;
        loop {
            match read_message(s).await? {
                Some((b'Z', _)) => return Ok(()),
                // AuthenticationCleartextPassword: answer with the `password`
                // parameter, as libpq does.
                Some((b'R', body)) if body.as_ref() == 3u32.to_be_bytes() => {
                    let pw = params
                        .iter()
                        .find(|(k, _)| *k == "password")
                        .map(|(_, v)| *v)
                        .unwrap_or_default();
                    let mut m = BytesMut::from(pw.as_bytes());
                    m.put_u8(0);
                    let mut buf = BytesMut::new();
                    put_message(&mut buf, b'p', &m);
                    s.write_all(&buf).await.map_err(io)?;
                    s.flush().await.map_err(io)?;
                }
                Some((b'E', body)) => return Err(Error::Protocol(error_text(&body))),
                Some(_) => {}
                None => return Err(Error::Io("closed during startup".into())),
            }
        }
    }

    /// The message of an `ErrorResponse` body.
    pub fn error_text(body: &[u8]) -> String {
        body.split(|b| *b == 0)
            .find_map(|f| f.strip_prefix(b"M"))
            .map(|m| String::from_utf8_lossy(m).into_owned())
            .unwrap_or_else(|| "error".into())
    }

    /// Send a simple query.
    pub async fn query<S: AsyncWrite + Unpin>(s: &mut S, q: &str) -> Result<(), Error> {
        let mut body = BytesMut::from(q.as_bytes());
        body.put_u8(0);
        let mut buf = BytesMut::new();
        put_message(&mut buf, b'Q', &body);
        s.write_all(&buf).await.map_err(io)?;
        s.flush().await.map_err(io)
    }

    /// Run a simple query and collect its text rows.
    pub async fn query_rows<S: AsyncRead + AsyncWrite + Unpin>(
        s: &mut S,
        q: &str,
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        query(s, q).await?;
        let mut rows = Vec::new();
        let mut err = None;
        loop {
            match read_message(s).await? {
                Some((b'D', mut body)) => {
                    let n = body.get_u16();
                    let mut row = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        let len = body.get_i32();
                        if len < 0 {
                            row.push(None);
                        } else {
                            let v = body.split_to(len as usize);
                            row.push(Some(String::from_utf8_lossy(&v).into_owned()));
                        }
                    }
                    rows.push(row);
                }
                Some((b'E', body)) => err = Some(error_text(&body)),
                Some((b'Z', _)) => break,
                Some(_) => {}
                None => return Err(Error::Io("closed during query".into())),
            }
        }
        match err {
            Some(e) => Err(Error::Protocol(e)),
            None => Ok(rows),
        }
    }

    /// Wait for `CopyBothResponse` after a streaming command.
    pub async fn expect_copy_both<S: AsyncRead + Unpin>(s: &mut S) -> Result<(), Error> {
        loop {
            match read_message(s).await? {
                Some((b'W', _)) => return Ok(()),
                Some((b'E', body)) => return Err(Error::Protocol(error_text(&body))),
                Some(_) => {}
                None => return Err(Error::Io("closed before CopyBoth".into())),
            }
        }
    }

    pub async fn send_copy_data<S: AsyncWrite + Unpin>(
        s: &mut S,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        put_message(&mut buf, b'd', data);
        s.write_all(&buf).await.map_err(io)?;
        s.flush().await.map_err(io)
    }

    /// The next CopyData payload; `None` when the stream ends.
    pub async fn recv_copy_data<S: AsyncRead + Unpin>(s: &mut S) -> Result<Option<Bytes>, Error> {
        loop {
            match read_message(s).await? {
                Some((b'd', body)) => return Ok(Some(body)),
                Some((b'E', body)) => return Err(Error::Protocol(error_text(&body))),
                Some((b'c', _)) | None => return Ok(None),
                Some(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup_refuses_ssl_then_reads_params() {
        let (mut c, s) = tokio::io::duplex(4096);
        let (mut srd, mut swr) = tokio::io::split(s);
        let server = tokio::spawn(async move { read_startup(&mut srd, &mut swr).await });
        // SSLRequest, then the real startup packet.
        c.write_all(&8u32.to_be_bytes()).await.unwrap();
        c.write_all(&SSL_REQUEST.to_be_bytes()).await.unwrap();
        let mut n = [0u8; 1];
        c.read_exact(&mut n).await.unwrap();
        assert_eq!(&n, b"N");
        let mut body = BytesMut::new();
        body.put_u32(PROTOCOL_V3);
        for (k, v) in [
            ("dbname", "replication"),
            ("options", "-c timeline_id=aa tenant_id=bb"),
        ] {
            put_cstr(&mut body, k);
            put_cstr(&mut body, v);
        }
        body.put_u8(0);
        c.write_all(&(body.len() as u32 + 4).to_be_bytes())
            .await
            .unwrap();
        c.write_all(&body).await.unwrap();
        let st = server.await.unwrap().unwrap().unwrap();
        let opts = st.options();
        assert_eq!(opts.get("timeline_id").map(String::as_str), Some("aa"));
        assert_eq!(opts.get("tenant_id").map(String::as_str), Some("bb"));
    }
}
