//! The safekeeper wire protocol, version 3: the CopyData payloads exchanged
//! after `START_WAL_PUSH (proto_version '3')`, and the replication commands.
//!
//! Byte layouts follow walproposer's `PAMessageSerialize` / `AsyncReadMessage`
//! (`pgxn/neon/walproposer.c`) and Neon's `ProposerAcceptorMessage::parse` /
//! `AcceptorProposerMessage::serialize` (`safekeeper/src/safekeeper.rs`).
//! All integers are big-endian (network order). Version 2 (C structs sent as
//! is, little-endian) is not supported: walproposer defaults to 3
//! (`neon.safekeeper_proto_version`).
//!
//! Both directions are encoded and decoded, so that tests and the benchmark
//! client can play the proposer.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Error;
use crate::types::{
    Configuration, HotStandbyFeedback, Id, Lsn, NodeId, PageserverFeedback, SafekeeperId, Term,
    TermHistory, TermLsn, TimelineId,
};

/// The only protocol version served.
pub const PROTO_VERSION: u32 = 3;

/// walproposer's `MAX_SEND_SIZE`: the most WAL one `AppendRequest` carries.
pub const MAX_SEND_SIZE: usize = 8192 * 16;

/// `ProposerGreeting` (`'g'`, proposer → acceptor).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposerGreeting {
    pub tenant_id: Id,
    pub timeline_id: Id,
    pub mconf: Configuration,
    pub pg_version: u32,
    pub system_id: u64,
    pub wal_seg_size: u32,
}

impl ProposerGreeting {
    pub fn timeline(&self) -> TimelineId {
        TimelineId::new(self.tenant_id, self.timeline_id)
    }
}

/// `VoteRequest` (`'v'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoteRequest {
    pub generation: u32,
    pub term: Term,
}

/// `ProposerElected` (`'e'`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposerElected {
    pub generation: u32,
    pub term: Term,
    pub start_streaming_at: Lsn,
    pub term_history: TermHistory,
}

/// The header of an `AppendRequest` (`'a'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendRequestHeader {
    pub generation: u32,
    pub term: Term,
    pub begin_lsn: Lsn,
    pub end_lsn: Lsn,
    /// The proposer's quorum commit LSN.
    pub commit_lsn: Lsn,
    /// The lowest LSN the proposer may still need (`peer_horizon_lsn`).
    pub truncate_lsn: Lsn,
}

/// An `AppendRequest`: the header, then `end_lsn - begin_lsn` bytes of WAL.
/// Empty WAL is a heartbeat carrying `commit_lsn`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendRequest {
    pub h: AppendRequestHeader,
    pub wal: Bytes,
}

/// Proposer → acceptor messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposerMessage {
    Greeting(ProposerGreeting),
    VoteRequest(VoteRequest),
    Elected(ProposerElected),
    Append(AppendRequest),
}

/// `AcceptorGreeting` (`'g'`, acceptor → proposer).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptorGreeting {
    pub node_id: NodeId,
    pub mconf: Configuration,
    pub term: Term,
}

/// `VoteResponse` (`'v'`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoteResponse {
    pub generation: u32,
    pub term: Term,
    pub vote_given: bool,
    pub flush_lsn: Lsn,
    pub truncate_lsn: Lsn,
    pub term_history: TermHistory,
}

/// `AppendResponse` (`'a'`). walproposer counts `flush_lsn` towards its
/// commit quorum, so it must only be sent once the WAL is durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResponse {
    pub generation: u32,
    pub term: Term,
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub hs_feedback: HotStandbyFeedback,
    pub pageserver_feedback: Option<PageserverFeedback>,
}

impl AppendResponse {
    /// The refusal sent to a deposed proposer: only the higher term.
    pub fn term_only(generation: u32, term: Term) -> Self {
        Self {
            generation,
            term,
            flush_lsn: Lsn::INVALID,
            commit_lsn: Lsn::INVALID,
            hs_feedback: HotStandbyFeedback::default(),
            pageserver_feedback: None,
        }
    }
}

/// Acceptor → proposer messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptorMessage {
    Greeting(AcceptorGreeting),
    VoteResponse(VoteResponse),
    AppendResponse(AppendResponse),
}

// ---- reading -------------------------------------------------------------

fn short(what: &str) -> Error {
    Error::Protocol(format!("message truncated while reading {what}"))
}

fn get_u8(buf: &mut Bytes, what: &str) -> Result<u8, Error> {
    if buf.remaining() < 1 {
        return Err(short(what));
    }
    Ok(buf.get_u8())
}

fn get_u16(buf: &mut Bytes, what: &str) -> Result<u16, Error> {
    if buf.remaining() < 2 {
        return Err(short(what));
    }
    Ok(buf.get_u16())
}

fn get_u32(buf: &mut Bytes, what: &str) -> Result<u32, Error> {
    if buf.remaining() < 4 {
        return Err(short(what));
    }
    Ok(buf.get_u32())
}

fn get_u64(buf: &mut Bytes, what: &str) -> Result<u64, Error> {
    if buf.remaining() < 8 {
        return Err(short(what));
    }
    Ok(buf.get_u64())
}

fn get_i32(buf: &mut Bytes, what: &str) -> Result<i32, Error> {
    if buf.remaining() < 4 {
        return Err(short(what));
    }
    Ok(buf.get_i32())
}

fn get_i64(buf: &mut Bytes, what: &str) -> Result<i64, Error> {
    if buf.remaining() < 8 {
        return Err(short(what));
    }
    Ok(buf.get_i64())
}

fn get_lsn(buf: &mut Bytes, what: &str) -> Result<Lsn, Error> {
    get_u64(buf, what).map(Lsn)
}

fn get_cstr(buf: &mut Bytes, what: &str) -> Result<String, Error> {
    let pos = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| Error::Protocol(format!("missing NUL after {what}")))?;
    let s = buf.split_to(pos);
    buf.advance(1);
    String::from_utf8(s.to_vec()).map_err(|_| Error::Protocol(format!("{what} is not UTF-8")))
}

fn get_members(buf: &mut Bytes, what: &str) -> Result<Vec<SafekeeperId>, Error> {
    let n = get_u32(buf, what)?;
    let mut out = Vec::with_capacity(n.min(64) as usize);
    for _ in 0..n {
        let id = get_u64(buf, "member node_id")?;
        let host = get_cstr(buf, "member host")?;
        let pg_port = get_u16(buf, "member port")?;
        out.push(SafekeeperId { id, host, pg_port });
    }
    Ok(out)
}

fn get_mconf(buf: &mut Bytes) -> Result<Configuration, Error> {
    let generation = get_u32(buf, "generation")?;
    let members = get_members(buf, "members")?;
    if generation != 0 && members.is_empty() {
        return Err(Error::Protocol(format!(
            "generation {generation} with an empty member set"
        )));
    }
    let new_members = get_members(buf, "new_members")?;
    Ok(Configuration {
        generation,
        members,
        new_members: (!new_members.is_empty()).then_some(new_members),
    })
}

fn get_term_history(buf: &mut Bytes) -> Result<TermHistory, Error> {
    let n = get_u32(buf, "term history length")?;
    let mut out = Vec::with_capacity(n.min(1024) as usize);
    for _ in 0..n {
        let term = get_u64(buf, "term history term")?;
        let lsn = get_lsn(buf, "term history lsn")?;
        out.push(TermLsn { term, lsn });
    }
    Ok(TermHistory(out))
}

fn get_id(buf: &mut Bytes, what: &str) -> Result<Id, Error> {
    get_cstr(buf, what)?.parse()
}

fn expect_end(buf: &Bytes, what: &str) -> Result<(), Error> {
    if buf.has_remaining() {
        return Err(Error::Protocol(format!(
            "{} trailing bytes after {what}",
            buf.remaining()
        )));
    }
    Ok(())
}

impl ProposerMessage {
    /// Decode one CopyData payload from walproposer.
    pub fn parse(mut buf: Bytes) -> Result<Self, Error> {
        let tag = get_u8(&mut buf, "tag")?;
        match tag {
            b'g' => {
                let tenant_id = get_id(&mut buf, "tenant_id")?;
                let timeline_id = get_id(&mut buf, "timeline_id")?;
                let mconf = get_mconf(&mut buf)?;
                let pg_version = get_u32(&mut buf, "pg_version")?;
                let system_id = get_u64(&mut buf, "system_id")?;
                let wal_seg_size = get_u32(&mut buf, "wal_seg_size")?;
                Ok(Self::Greeting(ProposerGreeting {
                    tenant_id,
                    timeline_id,
                    mconf,
                    pg_version,
                    system_id,
                    wal_seg_size,
                }))
            }
            b'v' => {
                let generation = get_u32(&mut buf, "generation")?;
                let term = get_u64(&mut buf, "term")?;
                Ok(Self::VoteRequest(VoteRequest { generation, term }))
            }
            b'e' => {
                let generation = get_u32(&mut buf, "generation")?;
                let term = get_u64(&mut buf, "term")?;
                let start_streaming_at = get_lsn(&mut buf, "start_streaming_at")?;
                let term_history = get_term_history(&mut buf)?;
                Ok(Self::Elected(ProposerElected {
                    generation,
                    term,
                    start_streaming_at,
                    term_history,
                }))
            }
            b'a' => {
                let h = AppendRequestHeader {
                    generation: get_u32(&mut buf, "generation")?,
                    term: get_u64(&mut buf, "term")?,
                    begin_lsn: get_lsn(&mut buf, "begin_lsn")?,
                    end_lsn: get_lsn(&mut buf, "end_lsn")?,
                    commit_lsn: get_lsn(&mut buf, "commit_lsn")?,
                    truncate_lsn: get_lsn(&mut buf, "truncate_lsn")?,
                };
                let len = h.end_lsn.0.checked_sub(h.begin_lsn.0).ok_or_else(|| {
                    Error::Protocol(format!(
                        "AppendRequest begin_lsn {} > end_lsn {}",
                        h.begin_lsn, h.end_lsn
                    ))
                })?;
                let len = usize::try_from(len)
                    .ok()
                    .filter(|l| *l <= MAX_SEND_SIZE)
                    .ok_or_else(|| {
                        Error::Protocol(format!("AppendRequest of {len} bytes > MAX_SEND_SIZE"))
                    })?;
                if buf.remaining() < len {
                    return Err(Error::Protocol(format!(
                        "AppendRequest carries {} WAL bytes, header says {len}",
                        buf.remaining()
                    )));
                }
                let wal = buf.split_to(len);
                Ok(Self::Append(AppendRequest { h, wal }))
            }
            other => Err(Error::Protocol(format!(
                "unknown proposer message tag {:?}",
                other as char
            ))),
        }
    }

    /// Encode as walproposer does (for tests and the benchmark client).
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Self::Greeting(m) => {
                buf.put_u8(b'g');
                put_cstr(buf, &m.tenant_id.to_string());
                put_cstr(buf, &m.timeline_id.to_string());
                put_mconf(buf, &m.mconf);
                buf.put_u32(m.pg_version);
                buf.put_u64(m.system_id);
                buf.put_u32(m.wal_seg_size);
            }
            Self::VoteRequest(m) => {
                buf.put_u8(b'v');
                buf.put_u32(m.generation);
                buf.put_u64(m.term);
            }
            Self::Elected(m) => {
                buf.put_u8(b'e');
                buf.put_u32(m.generation);
                buf.put_u64(m.term);
                buf.put_u64(m.start_streaming_at.0);
                put_term_history(buf, &m.term_history);
            }
            Self::Append(m) => {
                buf.put_u8(b'a');
                buf.put_u32(m.h.generation);
                buf.put_u64(m.h.term);
                buf.put_u64(m.h.begin_lsn.0);
                buf.put_u64(m.h.end_lsn.0);
                buf.put_u64(m.h.commit_lsn.0);
                buf.put_u64(m.h.truncate_lsn.0);
                buf.put_slice(&m.wal);
            }
        }
    }
}

// ---- writing -------------------------------------------------------------

fn put_cstr(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

fn put_members(buf: &mut BytesMut, members: &[SafekeeperId]) {
    buf.put_u32(members.len() as u32);
    for m in members {
        buf.put_u64(m.id);
        put_cstr(buf, &m.host);
        buf.put_u16(m.pg_port);
    }
}

fn put_mconf(buf: &mut BytesMut, mconf: &Configuration) {
    buf.put_u32(mconf.generation);
    put_members(buf, &mconf.members);
    put_members(buf, mconf.new_members.as_deref().unwrap_or(&[]));
}

fn put_term_history(buf: &mut BytesMut, th: &TermHistory) {
    buf.put_u32(th.0.len() as u32);
    for e in &th.0 {
        buf.put_u64(e.term);
        buf.put_u64(e.lsn.0);
    }
}

impl PageserverFeedback {
    /// Neon's key/value encoding (`libs/utils/src/pageserver_feedback.rs`).
    pub fn serialize(&self, buf: &mut BytesMut) {
        let count_at = buf.len();
        buf.put_u8(0);
        let mut n = 0u8;
        let mut kv8 = |buf: &mut BytesMut, key: &[u8], v: u64| {
            buf.put_slice(key);
            buf.put_i32(8);
            buf.put_u64(v);
            n += 1;
        };
        kv8(buf, b"current_timeline_size\0", self.current_timeline_size);
        kv8(buf, b"ps_writelsn\0", self.last_received_lsn.0);
        kv8(buf, b"ps_flushlsn\0", self.disk_consistent_lsn.0);
        kv8(buf, b"ps_applylsn\0", self.remote_consistent_lsn.0);
        kv8(buf, b"ps_replytime\0", self.replytime_us as u64);
        if self.shard_number > 0 {
            buf.put_slice(b"shard_number\0");
            buf.put_i32(4);
            buf.put_u32(self.shard_number);
            n += 1;
        }
        if self.corruption_detected {
            buf.put_slice(b"corruption_detected\0");
            buf.put_i32(1);
            buf.put_u8(1);
            n += 1;
        }
        buf[count_at] = n;
    }

    /// Decode the key/value encoding; unknown keys are skipped by length.
    pub fn parse(mut buf: Bytes) -> Result<Self, Error> {
        let mut fb = PageserverFeedback::default();
        let n = get_u8(&mut buf, "feedback key count")?;
        for _ in 0..n {
            let key = get_cstr(&mut buf, "feedback key")?;
            let len = get_i32(&mut buf, "feedback value length")?;
            let len = usize::try_from(len)
                .map_err(|_| Error::Protocol(format!("negative length for {key}")))?;
            if buf.remaining() < len {
                return Err(short(&key));
            }
            let mut v = buf.split_to(len);
            match (key.as_str(), len) {
                ("current_timeline_size", 8) => fb.current_timeline_size = v.get_u64(),
                ("ps_writelsn", 8) => fb.last_received_lsn = Lsn(v.get_u64()),
                ("ps_flushlsn", 8) => fb.disk_consistent_lsn = Lsn(v.get_u64()),
                ("ps_applylsn", 8) => fb.remote_consistent_lsn = Lsn(v.get_u64()),
                ("ps_replytime", 8) => fb.replytime_us = v.get_i64(),
                ("shard_number", 4) => fb.shard_number = v.get_u32(),
                ("corruption_detected", 1) => fb.corruption_detected = v.get_u8() != 0,
                _ => {}
            }
        }
        Ok(fb)
    }
}

impl AcceptorMessage {
    /// Encode one CopyData payload for walproposer.
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Self::Greeting(m) => {
                buf.put_u8(b'g');
                buf.put_u64(m.node_id);
                put_mconf(buf, &m.mconf);
                buf.put_u64(m.term);
            }
            Self::VoteResponse(m) => {
                buf.put_u8(b'v');
                buf.put_u32(m.generation);
                buf.put_u64(m.term);
                buf.put_u8(u8::from(m.vote_given));
                buf.put_u64(m.flush_lsn.0);
                buf.put_u64(m.truncate_lsn.0);
                put_term_history(buf, &m.term_history);
            }
            Self::AppendResponse(m) => {
                buf.put_u8(b'a');
                buf.put_u32(m.generation);
                buf.put_u64(m.term);
                buf.put_u64(m.flush_lsn.0);
                buf.put_u64(m.commit_lsn.0);
                buf.put_i64(m.hs_feedback.ts);
                buf.put_u64(m.hs_feedback.xmin);
                buf.put_u64(m.hs_feedback.catalog_xmin);
                // walproposer decodes feedback only if bytes remain.
                if let Some(fb) = &m.pageserver_feedback {
                    fb.serialize(buf);
                }
            }
        }
    }

    /// Decode as walproposer does (for tests and the benchmark client).
    pub fn parse(mut buf: Bytes) -> Result<Self, Error> {
        let tag = get_u8(&mut buf, "tag")?;
        let msg = match tag {
            b'g' => Self::Greeting(AcceptorGreeting {
                node_id: get_u64(&mut buf, "node_id")?,
                mconf: get_mconf(&mut buf)?,
                term: get_u64(&mut buf, "term")?,
            }),
            b'v' => Self::VoteResponse(VoteResponse {
                generation: get_u32(&mut buf, "generation")?,
                term: get_u64(&mut buf, "term")?,
                vote_given: get_u8(&mut buf, "vote_given")? != 0,
                flush_lsn: get_lsn(&mut buf, "flush_lsn")?,
                truncate_lsn: get_lsn(&mut buf, "truncate_lsn")?,
                term_history: get_term_history(&mut buf)?,
            }),
            b'a' => {
                let generation = get_u32(&mut buf, "generation")?;
                let term = get_u64(&mut buf, "term")?;
                let flush_lsn = get_lsn(&mut buf, "flush_lsn")?;
                let commit_lsn = get_lsn(&mut buf, "commit_lsn")?;
                let hs_feedback = HotStandbyFeedback {
                    ts: get_i64(&mut buf, "hs ts")?,
                    xmin: get_u64(&mut buf, "hs xmin")?,
                    catalog_xmin: get_u64(&mut buf, "hs catalog_xmin")?,
                };
                let pageserver_feedback = if buf.has_remaining() {
                    Some(PageserverFeedback::parse(buf.split_to(buf.remaining()))?)
                } else {
                    None
                };
                Self::AppendResponse(AppendResponse {
                    generation,
                    term,
                    flush_lsn,
                    commit_lsn,
                    hs_feedback,
                    pageserver_feedback,
                })
            }
            other => {
                return Err(Error::Protocol(format!(
                    "unknown acceptor message tag {:?}",
                    other as char
                )));
            }
        };
        expect_end(&buf, "acceptor message")?;
        Ok(msg)
    }
}

// ---- replication commands ------------------------------------------------

/// A simple-query command on a safekeeper connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// `START_WAL_PUSH [(proto_version 'N', allow_timeline_creation 'b')]`.
    StartWalPush {
        proto_version: u32,
        allow_timeline_creation: bool,
    },
    /// `START_REPLICATION [SLOT s] [PHYSICAL] X/Y [(term='N')]`.
    StartReplication {
        start_lsn: Lsn,
        term: Option<Term>,
    },
    IdentifySystem,
    TimelineStatus,
    /// `SET datestyle TO …`, which some clients send first; answered `SELECT 1`.
    SetDatestyle,
}

impl Command {
    /// Parse a query string as Neon's `handler.rs` `parse_cmd` does.
    pub fn parse(query: &str) -> Result<Self, Error> {
        let q = query.trim().trim_end_matches(';').trim();
        if q.to_ascii_lowercase().starts_with("set datestyle to ") {
            return Ok(Self::SetDatestyle);
        }
        if let Some(rest) = q.strip_prefix("START_WAL_PUSH") {
            // Neon's default without options is version 2; walproposer
            // always sends the option.
            let mut proto_version = 2;
            let mut allow_timeline_creation = true;
            let rest = rest.trim();
            if !rest.is_empty() {
                let inner = rest
                    .strip_prefix('(')
                    .and_then(|r| r.strip_suffix(')'))
                    .ok_or_else(|| Error::Protocol(format!("cannot parse {query:?}")))?;
                for kv in inner.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    let mut it = kv.split_whitespace();
                    let (Some(k), Some(v)) = (it.next(), it.next()) else {
                        return Err(Error::Protocol(format!("cannot parse option {kv:?}")));
                    };
                    let v = v.trim_matches('\'');
                    match k {
                        "proto_version" => {
                            proto_version = v
                                .parse()
                                .map_err(|_| Error::Protocol(format!("bad proto_version {v:?}")))?;
                        }
                        "allow_timeline_creation" => {
                            allow_timeline_creation = v.parse().map_err(|_| {
                                Error::Protocol(format!("bad allow_timeline_creation {v:?}"))
                            })?;
                        }
                        _ => {}
                    }
                }
            }
            return Ok(Self::StartWalPush {
                proto_version,
                allow_timeline_creation,
            });
        }
        if let Some(rest) = q.strip_prefix("START_REPLICATION") {
            let mut words = rest.split_whitespace().peekable();
            if words.peek() == Some(&"SLOT") {
                words.next();
                words.next();
            }
            if words.peek() == Some(&"PHYSICAL") {
                words.next();
            }
            let start_lsn: Lsn = words
                .next()
                .ok_or_else(|| Error::Protocol(format!("no start LSN in {query:?}")))?
                .parse()?;
            let tail: String = words.collect::<Vec<_>>().join(" ");
            let term = if tail.is_empty() {
                None
            } else {
                let t = tail
                    .strip_prefix("(term='")
                    .and_then(|r| r.strip_suffix("')"))
                    .ok_or_else(|| Error::Protocol(format!("cannot parse {query:?}")))?;
                Some(
                    t.parse()
                        .map_err(|_| Error::Protocol(format!("bad term {t:?}")))?,
                )
            };
            return Ok(Self::StartReplication { start_lsn, term });
        }
        if q.starts_with("IDENTIFY_SYSTEM") {
            return Ok(Self::IdentifySystem);
        }
        if q.starts_with("TIMELINE_STATUS") {
            return Ok(Self::TimelineStatus);
        }
        Err(Error::Protocol(format!("unsupported command {query:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> Id {
        "cf0480929707ee75372337efaa5ecf96".parse().unwrap()
    }

    fn timeline() -> Id {
        "112ded66422aa5e953e5440fa5427ac4".parse().unwrap()
    }

    fn round_trip_p(m: ProposerMessage) {
        let mut buf = BytesMut::new();
        m.serialize(&mut buf);
        assert_eq!(ProposerMessage::parse(buf.freeze()).unwrap(), m);
    }

    fn round_trip_a(m: AcceptorMessage) {
        let mut buf = BytesMut::new();
        m.serialize(&mut buf);
        assert_eq!(AcceptorMessage::parse(buf.freeze()).unwrap(), m);
    }

    /// The greeting walproposer sends with `neon.safekeepers` and no
    /// generation: an empty configuration.
    #[test]
    fn greeting_bytes_match_walproposer() {
        let mut want = BytesMut::new();
        want.put_u8(b'g');
        want.put_slice(b"cf0480929707ee75372337efaa5ecf96\0");
        want.put_slice(b"112ded66422aa5e953e5440fa5427ac4\0");
        want.put_u32(0); // generation
        want.put_u32(0); // members
        want.put_u32(0); // new members
        want.put_u32(160_009);
        want.put_u64(0x1234_5678_9abc_def0);
        want.put_u32(16 << 20);
        let m = ProposerMessage::parse(want.clone().freeze()).unwrap();
        assert_eq!(
            m,
            ProposerMessage::Greeting(ProposerGreeting {
                tenant_id: tenant(),
                timeline_id: timeline(),
                mconf: Configuration::default(),
                pg_version: 160_009,
                system_id: 0x1234_5678_9abc_def0,
                wal_seg_size: 16 << 20,
            })
        );
        let mut got = BytesMut::new();
        m.serialize(&mut got);
        assert_eq!(got, want);
    }

    #[test]
    fn proposer_messages_round_trip() {
        round_trip_p(ProposerMessage::Greeting(ProposerGreeting {
            tenant_id: tenant(),
            timeline_id: timeline(),
            mconf: Configuration {
                generation: 3,
                members: vec![SafekeeperId {
                    id: 1,
                    host: "wal".into(),
                    pg_port: 5454,
                }],
                new_members: Some(vec![SafekeeperId {
                    id: 2,
                    host: "w2".into(),
                    pg_port: 1,
                }]),
            },
            pg_version: 170_005,
            system_id: 7,
            wal_seg_size: 16 << 20,
        }));
        round_trip_p(ProposerMessage::VoteRequest(VoteRequest {
            generation: 0,
            term: 9,
        }));
        round_trip_p(ProposerMessage::Elected(ProposerElected {
            generation: 0,
            term: 9,
            start_streaming_at: Lsn(0x16B_5A00),
            term_history: TermHistory(vec![
                (1, Lsn(0x149_6F10)).into(),
                (9, Lsn(0x16B_5A00)).into(),
            ]),
        }));
        round_trip_p(ProposerMessage::Append(AppendRequest {
            h: AppendRequestHeader {
                generation: 0,
                term: 9,
                begin_lsn: Lsn(100),
                end_lsn: Lsn(105),
                commit_lsn: Lsn(90),
                truncate_lsn: Lsn(80),
            },
            wal: Bytes::from_static(b"hello"),
        }));
    }

    #[test]
    fn append_request_rejects_short_and_oversized_wal() {
        let mut buf = BytesMut::new();
        ProposerMessage::Append(AppendRequest {
            h: AppendRequestHeader {
                generation: 0,
                term: 1,
                begin_lsn: Lsn(0),
                end_lsn: Lsn(10),
                commit_lsn: Lsn(0),
                truncate_lsn: Lsn(0),
            },
            wal: Bytes::from_static(b"12345"),
        })
        .serialize(&mut buf);
        assert!(ProposerMessage::parse(buf.freeze()).is_err());

        let mut buf = BytesMut::new();
        buf.put_u8(b'a');
        buf.put_u32(0);
        buf.put_u64(1);
        buf.put_u64(0);
        buf.put_u64(MAX_SEND_SIZE as u64 + 1);
        buf.put_u64(0);
        buf.put_u64(0);
        assert!(ProposerMessage::parse(buf.freeze()).is_err());
    }

    #[test]
    fn acceptor_messages_round_trip() {
        round_trip_a(AcceptorMessage::Greeting(AcceptorGreeting {
            node_id: 1,
            mconf: Configuration::default(),
            term: 4,
        }));
        round_trip_a(AcceptorMessage::VoteResponse(VoteResponse {
            generation: 0,
            term: 5,
            vote_given: true,
            flush_lsn: Lsn(77),
            truncate_lsn: Lsn(70),
            term_history: TermHistory(vec![(4, Lsn(10)).into()]),
        }));
        round_trip_a(AcceptorMessage::AppendResponse(AppendResponse::term_only(
            0, 8,
        )));
        round_trip_a(AcceptorMessage::AppendResponse(AppendResponse {
            generation: 0,
            term: 5,
            flush_lsn: Lsn(99),
            commit_lsn: Lsn(98),
            hs_feedback: HotStandbyFeedback {
                ts: -1,
                xmin: 3,
                catalog_xmin: 4,
            },
            pageserver_feedback: Some(PageserverFeedback {
                current_timeline_size: 1 << 20,
                last_received_lsn: Lsn(97),
                disk_consistent_lsn: Lsn(96),
                remote_consistent_lsn: Lsn(95),
                replytime_us: 812_345_678,
                shard_number: 2,
                corruption_detected: false,
            }),
        }));
    }

    #[test]
    fn append_response_layout() {
        let mut buf = BytesMut::new();
        AcceptorMessage::AppendResponse(AppendResponse::term_only(0, 8)).serialize(&mut buf);
        // tag, generation, term, flush, commit, hs.ts, hs.xmin, hs.catalog_xmin
        assert_eq!(buf.len(), 1 + 4 + 8 * 6);
        assert_eq!(buf[0], b'a');
        assert_eq!(&buf[5..13], &8u64.to_be_bytes());
    }

    #[test]
    fn commands() {
        assert_eq!(
            Command::parse("START_WAL_PUSH (proto_version '3', allow_timeline_creation 'false')")
                .unwrap(),
            Command::StartWalPush {
                proto_version: 3,
                allow_timeline_creation: false
            }
        );
        assert_eq!(
            Command::parse("START_WAL_PUSH").unwrap(),
            Command::StartWalPush {
                proto_version: 2,
                allow_timeline_creation: true
            }
        );
        assert_eq!(
            Command::parse("START_REPLICATION PHYSICAL 0/16B5A00").unwrap(),
            Command::StartReplication {
                start_lsn: Lsn(0x16B_5A00),
                term: None
            }
        );
        assert_eq!(
            Command::parse("START_REPLICATION SLOT s PHYSICAL 1/0 (term='7')").unwrap(),
            Command::StartReplication {
                start_lsn: Lsn(1 << 32),
                term: Some(7)
            }
        );
        assert_eq!(
            Command::parse("IDENTIFY_SYSTEM").unwrap(),
            Command::IdentifySystem
        );
        assert_eq!(
            Command::parse("TIMELINE_STATUS").unwrap(),
            Command::TimelineStatus
        );
        assert_eq!(
            Command::parse("set datestyle to ISO").unwrap(),
            Command::SetDatestyle
        );
        assert!(Command::parse("SELECT 1").is_err());
    }
}
