//! HandshakeV10 packet construction and HandshakeResponse41 parsing.

use std::sync::atomic::{AtomicU64, Ordering};

use super::constants::*;
use super::packet::read_nul_str;

/// Per-connection scramble source: pid + nanosecond time + atomic counter
/// through xorshift. The scramble is public (sent in clear) — it only needs
/// uniqueness per connection to defeat replay, not secrecy.
static SCRAMBLE_COUNTER: AtomicU64 = AtomicU64::new(0x9E3779B97F4A7C15);

pub fn fresh_scramble() -> [u8; 20] {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(0x12345678);
    let mut x = (std::process::id() as u64)
        .wrapping_mul(0x1000193)
        ^ t
        ^ SCRAMBLE_COUNTER.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
    let mut out = [0u8; 20];
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = (x >> 11) as u8;
        if *b == 0 {
            *b = 0x5A; // NUL bytes would truncate C-string handling in clients
        }
    }
    out
}

pub fn handshake_payload(connection_id: u32, scramble: &[u8; 20], plugin: &str) -> Vec<u8> {
    let caps = SERVER_CAPS;
    let mut p = Vec::with_capacity(80 + plugin.len());
    p.push(10); // protocol version
    p.extend_from_slice(server_version().as_bytes());
    p.push(0);
    p.extend_from_slice(&connection_id.to_le_bytes());
    p.extend_from_slice(&scramble[..8]);
    p.push(0); // filler
    p.extend_from_slice(&((caps & 0xFFFF) as u16).to_le_bytes());
    p.push(CHARSET_UTF8MB4);
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes());
    p.push(21); // auth data len (20 + NUL)
    p.extend_from_slice(&[0u8; 10]); // reserved
    p.extend_from_slice(&scramble[8..]);
    p.push(0); // NUL terminator for part2
    p.extend_from_slice(plugin.as_bytes());
    p.push(0);
    p
}

pub struct HandshakeResponse {
    pub caps: u32,
    pub username: String,
    /// Client's auth proof bytes (empty for passwordless login).
    pub auth: Vec<u8>,
    /// Auth plugin the client answered with (may differ from the offer).
    pub plugin: String,
    /// Requested database (`USE` equivalent; routing is a backlog item).
    #[allow(dead_code)]
    pub db: Option<String>,
}

/// Capability bit for length-encoded auth data (never advertised by us, but
/// parsed when a client sets it).
const CAP_AUTH_LENENC_DATA: u32 = 0x0020_0000;

pub fn parse_handshake_response(buf: &[u8]) -> Option<HandshakeResponse> {
    if buf.len() < 32 {
        return None;
    }
    let caps = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    // max_packet(4) + charset(1) + 23 filler = 28 bytes after caps.
    let mut pos = 4 + 4 + 1 + 23;
    let username = read_nul_str(buf, &mut pos).unwrap_or_default();
    // Auth proof encoding depends on negotiated flags.
    let auth = if caps & CAP_AUTH_LENENC_DATA != 0 {
        super::packet::read_lenenc_bytes(buf, &mut pos).unwrap_or_default()
    } else if caps & CAP_SECURE_CONNECTION != 0 {
        if pos >= buf.len() {
            return None;
        }
        let n = buf[pos] as usize;
        pos += 1;
        if pos + n > buf.len() {
            return None;
        }
        let v = buf[pos..pos + n].to_vec();
        pos += n;
        v
    } else {
        match read_nul_str(buf, &mut pos) {
            Some(s) => s.into_bytes(),
            None => Vec::new(),
        }
    };
    let db = if caps & CAP_CONNECT_WITH_DB != 0 {
        read_nul_str(buf, &mut pos)
    } else {
        None
    };
    let plugin = if caps & CAP_PLUGIN_AUTH != 0 {
        read_nul_str(buf, &mut pos).unwrap_or_default()
    } else {
        String::new()
    };
    Some(HandshakeResponse { caps, username, auth, plugin, db })
}
