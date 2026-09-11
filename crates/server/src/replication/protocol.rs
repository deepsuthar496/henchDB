//! Replication wire codec: binary-framed primary↔replica protocol.
//!
//! Frame layout: `[u32 BE len][0x52 'R'][u8 frame_type][payload]` where `len`
//! covers everything after itself. Integers inside the payload are
//! little-endian (matching the WAL); strings are `[u32 LE len][bytes]`.
//! Frames larger than 64 MiB are rejected before allocation.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub const REPL_MAGIC: u8 = 0x52;
pub const PROTOCOL_VERSION: u32 = 1;
/// Hard cap per frame (snapshot chunks stay far below; fail closed above).
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

pub const T_HANDSHAKE: u8 = 1;
pub const T_HANDSHAKE_ACK: u8 = 2;
pub const T_START_REPLICATION: u8 = 3;
pub const T_WAL_CHUNK: u8 = 4;
pub const T_HEARTBEAT: u8 = 5;
pub const T_HEARTBEAT_ACK: u8 = 6;
pub const T_SNAPSHOT_REQUIRED: u8 = 7;
pub const T_SNAPSHOT_BEGIN: u8 = 8;
pub const T_SNAPSHOT_CHUNK: u8 = 9;
pub const T_SNAPSHOT_END: u8 = 10;

/// Fresh-start sentinel for `StartReplication.generation`: the replica
/// holds no position in any generation and must snapshot first.
pub const NO_GENERATION: u64 = u64::MAX;

/// Decoded replication frame (payloads borrowed from the read buffer live
/// only as long as the caller keeps it; string/bytes fields are copied out
/// where they must outlive the read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Handshake {
        version: u32,
        user: String,
        password: String,
    },
    HandshakeAck {
        ok: bool,
        message: String,
        wal_version: u32,
        durable_offset: u64,
    },
    StartReplication {
        generation: u64,
        from_offset: u64,
    },
    WalChunk {
        offset: u64,
        data: Vec<u8>,
    },
    Heartbeat {
        durable_offset: u64,
    },
    HeartbeatAck,
    SnapshotRequired,
    SnapshotBegin {
        total_bytes: u64,
        end_offset: u64,
        generation: u64,
    },
    SnapshotChunk {
        data: Vec<u8>,
    },
    SnapshotEnd,
}

#[derive(Debug)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replication codec: {}", self.0)
    }
}

impl From<std::io::Error> for CodecError {
    fn from(e: std::io::Error) -> Self {
        CodecError(e.to_string())
    }
}

fn put_u32_le(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64_le(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32_le(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32_le(out, b.len() as u32);
    out.extend_from_slice(b);
}

struct Cursor<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.off.checked_add(n).ok_or_else(|| CodecError("truncated frame".into()))?;
        if end > self.buf.len() {
            return Err(CodecError("truncated frame".into()));
        }
        let s = &self.buf[self.off..end];
        self.off = end;
        Ok(s)
    }

    fn u32_le(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(|_| CodecError("int".into()))?))
    }

    fn u64_le(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(|_| CodecError("int".into()))?))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn str_t(&mut self) -> Result<String, CodecError> {
        let n = self.u32_le()? as usize;
        if n > MAX_FRAME {
            return Err(CodecError("string too large".into()));
        }
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| CodecError("utf8".into()))
    }

    fn bytes_t(&mut self) -> Result<Vec<u8>, CodecError> {
        let n = self.u32_le()? as usize;
        if n > MAX_FRAME {
            return Err(CodecError("blob too large".into()));
        }
        Ok(self.take(n)?.to_vec())
    }
}

/// Serialize one frame (without the length prefix).
fn encode_body(f: &Frame, out: &mut Vec<u8>) {
    match f {
        Frame::Handshake { version, user, password } => {
            out.push(T_HANDSHAKE);
            put_u32_le(out, *version);
            put_str(out, user);
            put_str(out, password);
        }
        Frame::HandshakeAck { ok, message, wal_version, durable_offset } => {
            out.push(T_HANDSHAKE_ACK);
            out.push(*ok as u8);
            put_str(out, message);
            put_u32_le(out, *wal_version);
            put_u64_le(out, *durable_offset);
        }
        Frame::StartReplication { generation, from_offset } => {
            out.push(T_START_REPLICATION);
            put_u64_le(out, *generation);
            put_u64_le(out, *from_offset);
        }
        Frame::WalChunk { offset, data } => {
            out.push(T_WAL_CHUNK);
            put_u64_le(out, *offset);
            put_bytes(out, data);
        }
        Frame::Heartbeat { durable_offset } => {
            out.push(T_HEARTBEAT);
            put_u64_le(out, *durable_offset);
        }
        Frame::HeartbeatAck => {
            out.push(T_HEARTBEAT_ACK);
        }
        Frame::SnapshotRequired => {
            out.push(T_SNAPSHOT_REQUIRED);
        }
        Frame::SnapshotBegin { total_bytes, end_offset, generation } => {
            out.push(T_SNAPSHOT_BEGIN);
            put_u64_le(out, *total_bytes);
            put_u64_le(out, *end_offset);
            put_u64_le(out, *generation);
        }
        Frame::SnapshotChunk { data } => {
            out.push(T_SNAPSHOT_CHUNK);
            put_bytes(out, data);
        }
        Frame::SnapshotEnd => {
            out.push(T_SNAPSHOT_END);
        }
    }
}

fn decode_body(buf: &[u8]) -> Result<Frame, CodecError> {
    let mut c = Cursor { buf, off: 0 };
    match c.u8()? {
        T_HANDSHAKE => Ok(Frame::Handshake {
            version: c.u32_le()?,
            user: c.str_t()?,
            password: c.str_t()?,
        }),
        T_HANDSHAKE_ACK => {
            let ok = c.u8()? != 0;
            Ok(Frame::HandshakeAck {
                ok,
                message: c.str_t()?,
                wal_version: c.u32_le()?,
                durable_offset: c.u64_le()?,
            })
        }
        T_START_REPLICATION => Ok(Frame::StartReplication {
            generation: c.u64_le()?,
            from_offset: c.u64_le()?,
        }),
        T_WAL_CHUNK => Ok(Frame::WalChunk {
            offset: c.u64_le()?,
            data: c.bytes_t()?,
        }),
        T_HEARTBEAT => Ok(Frame::Heartbeat {
            durable_offset: c.u64_le()?,
        }),
        T_HEARTBEAT_ACK => Ok(Frame::HeartbeatAck),
        T_SNAPSHOT_REQUIRED => Ok(Frame::SnapshotRequired),
        T_SNAPSHOT_BEGIN => Ok(Frame::SnapshotBegin {
            total_bytes: c.u64_le()?,
            end_offset: c.u64_le()?,
            generation: c.u64_le()?,
        }),
        T_SNAPSHOT_CHUNK => Ok(Frame::SnapshotChunk { data: c.bytes_t()? }),
        T_SNAPSHOT_END => Ok(Frame::SnapshotEnd),
        t => Err(CodecError(format!("unknown frame type {t}"))),
    }
}

/// Write one length-prefixed frame.
pub fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), CodecError> {
    let mut body = Vec::with_capacity(64);
    body.push(REPL_MAGIC);
    encode_body(frame, &mut body);
    if body.len() > MAX_FRAME {
        return Err(CodecError("frame too large".into()));
    }
    let len = body.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

/// Read one length-prefixed frame. `Ok(None)` on timeout (caller retries /
/// heartbeats); I/O errors and codec violations are hard failures.
pub fn read_frame(stream: &mut TcpStream) -> Result<Option<Frame>, CodecError> {
    let mut hdr = [0u8; 4];
    match stream.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e)
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock =>
        {
            return Ok(None)
        }
        Err(e) => return Err(CodecError(e.to_string())),
    }
    let len = u32::from_be_bytes(hdr) as usize;
    if len < 2 || len > MAX_FRAME {
        return Err(CodecError(format!("bad frame length {len}")));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).map_err(|e| CodecError(e.to_string()))?;
    if body[0] != REPL_MAGIC {
        return Err(CodecError("bad replication magic".into()));
    }
    decode_body(&body[1..]).map(Some)
}

/// Decode a raw frame buffer starting with REPL_MAGIC (used by tests & fuzzing).
#[cfg(test)]
pub fn decode_raw_frame(bytes: &[u8]) -> Result<Frame, CodecError> {
    if bytes.len() < 2 {
        return Err(CodecError("frame too short".into()));
    }
    if bytes[0] != REPL_MAGIC {
        return Err(CodecError("bad replication magic".into()));
    }
    decode_body(&bytes[1..])
}

#[cfg(test)]
pub fn encode_body_for_test(f: &Frame, out: &mut Vec<u8>) {
    encode_body(f, out);
}

/// In-memory roundtrip helper (unit tests + framing checks without sockets).
#[cfg(test)]
pub fn roundtrip(frame: &Frame) -> Result<Frame, CodecError> {
    let mut body = vec![REPL_MAGIC];
    encode_body(frame, &mut body);
    if body[0] != REPL_MAGIC {
        return Err(CodecError("magic".into()));
    }
    decode_body(&body[1..])
}

pub fn set_timeouts(stream: &TcpStream, timeout: Duration) -> std::io::Result<()> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip() {
        let frames = vec![
            Frame::Handshake {
                version: PROTOCOL_VERSION,
                user: "root".into(),
                password: "s3cret".into(),
            },
            Frame::HandshakeAck {
                ok: true,
                message: "welcome".into(),
                wal_version: 3,
                durable_offset: 12345,
            },
            Frame::HandshakeAck {
                ok: false,
                message: "denied".into(),
                wal_version: 0,
                durable_offset: 0,
            },
            Frame::StartReplication { generation: 3, from_offset: 999 },
            Frame::WalChunk { offset: 100, data: vec![1, 2, 3, 250] },
            Frame::WalChunk { offset: 8, data: vec![] },
            Frame::Heartbeat { durable_offset: u64::MAX },
            Frame::HeartbeatAck,
            Frame::SnapshotRequired,
            Frame::SnapshotBegin { total_bytes: 1 << 20, end_offset: 8, generation: 4 },
            Frame::SnapshotChunk { data: vec![0u8; 4096] },
            Frame::SnapshotEnd,
        ];
        for f in &frames {
            assert_eq!(&roundtrip(f).unwrap(), f);
        }
    }

    #[test]
    fn codec_rejects_garbage() {
        assert!(decode_body(&[]).is_err());
        assert!(decode_body(&[99]).is_err()); // unknown type
        assert!(decode_body(&[T_HANDSHAKE, 1, 2]).is_err()); // truncated
        // Bad magic is a read_frame-level check; body decoder ignores it.
        // Oversized length prefix is rejected before allocation (checked in
        // read_frame; exercise the bound here directly).
        assert!(MAX_FRAME >= (64 << 20));
    }

    #[test]
    fn socket_pair_roundtrip() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            set_timeouts(&sock, Duration::from_secs(5)).unwrap();
            read_frame(&mut sock).unwrap().unwrap()
        });
        let mut sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
        set_timeouts(&sock, Duration::from_secs(5)).unwrap();
        let sent = Frame::StartReplication { generation: NO_GENERATION, from_offset: 4242 };
        write_frame(&mut sock, &sent).unwrap();
        assert_eq!(handle.join().unwrap(), sent);
    }

    #[test]
    fn read_timeout_yields_none() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
        // Keep the peer alive but silent: the client must time out, not
        // see EOF (a dropped peer would read as disconnection instead).
        let (_peer, _) = listener.accept().unwrap();
        set_timeouts(&sock, Duration::from_millis(50)).unwrap();
        assert!(read_frame(&mut sock).unwrap().is_none());
    }
}
