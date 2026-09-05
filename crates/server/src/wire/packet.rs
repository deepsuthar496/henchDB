//! Packet framing, length-encoded primitive codecs, and core payload builders.

use std::io::{BufReader, Read, Write};

use super::constants::{MAX_PACKET, STATUS_AUTOCOMMIT};

// ---------------------------------------------------------------------------
// Length-encoded integers / strings
// ---------------------------------------------------------------------------

pub fn enc_lenenc_int(out: &mut Vec<u8>, v: u64) {
    if v < 251 {
        out.push(v as u8);
    } else if v < 0x1_0000 {
        out.push(0xFC);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v < 0x1_00_0000 {
        out.push(0xFD);
        let b = (v as u32).to_le_bytes();
        out.extend_from_slice(&b[..3]);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[allow(dead_code)]
pub fn dec_lenenc_int(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos >= buf.len() {
        return None;
    }
    let first = buf[*pos];
    *pos += 1;
    match first {
        v if v < 251 => Some(v as u64),
        251 => None, // NULL marker
        0xFC => {
            if *pos + 2 > buf.len() {
                return None;
            }
            let v = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]) as u64;
            *pos += 2;
            Some(v)
        }
        0xFD => {
            if *pos + 3 > buf.len() {
                return None;
            }
            let v = u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], 0]) as u64;
            *pos += 3;
            Some(v)
        }
        0xFE => {
            if *pos + 8 > buf.len() {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[*pos..*pos + 8]);
            *pos += 8;
            Some(u64::from_le_bytes(b))
        }
        _ => None,
    }
}

pub fn enc_lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    enc_lenenc_int(out, s.len() as u64);
    out.extend_from_slice(s);
}

pub fn enc_lenenc_str3(out: &mut Vec<u8>, s: &str) {
    enc_lenenc_str(out, s.as_bytes());
}

pub fn read_nul_str(buf: &[u8], pos: &mut usize) -> Option<String> {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != 0 {
        *pos += 1;
    }
    if *pos >= buf.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[start..*pos]).into_owned();
    *pos += 1; // skip NUL
    Some(s)
}

pub fn read_le(buf: &[u8], pos: &mut usize, n: usize) -> Option<u64> {
    if *pos + n > buf.len() {
        return None;
    }
    let mut v = 0u64;
    for k in 0..n {
        v |= (buf[*pos + k] as u64) << (8 * k);
    }
    *pos += n;
    Some(v)
}

pub fn read_lenenc_bytes(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    if *pos >= buf.len() {
        return None;
    }
    let first = buf[*pos];
    *pos += 1;
    let len = match first {
        v if v < 251 => v as usize,
        0xFB => return Some(Vec::new()), // NULL marker in some contexts
        0xFC => read_le(buf, pos, 2)? as usize,
        0xFD => read_le(buf, pos, 3)? as usize,
        0xFE => read_le(buf, pos, 8)? as usize,
        _ => return None,
    };
    if *pos + len > buf.len() {
        return None;
    }
    let v = buf[*pos..*pos + len].to_vec();
    *pos += len;
    Some(v)
}

// ---------------------------------------------------------------------------
// Packet framing
// ---------------------------------------------------------------------------

pub fn read_packet<R: Read>(reader: &mut BufReader<R>, max: usize) -> std::io::Result<(u8, Vec<u8>)> {
    let mut first_seq: Option<u8> = None;
    let mut out = Vec::new();
    loop {
        let mut hdr = [0u8; 4];
        reader.read_exact(&mut hdr)?;
        let len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
        let seq = hdr[3];
        if first_seq.is_none() {
            first_seq = Some(seq);
        }
        if len > max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "packet too large",
            ));
        }
        let mut chunk = vec![0u8; len];
        if len > 0 {
            reader.read_exact(&mut chunk)?;
        }
        out.extend_from_slice(&chunk);
        if len < MAX_PACKET {
            return Ok((first_seq.unwrap_or(seq), out));
        }
        // len == 0xFFFFFF: continuation packet follows.
    }
}

pub fn write_packet<W: Write>(writer: &mut W, payload: &[u8], seq: &mut u8) -> std::io::Result<()> {
    let mut off = 0usize;
    if payload.is_empty() {
        let hdr = [0u8, 0u8, 0u8, *seq];
        writer.write_all(&hdr)?;
        *seq = seq.wrapping_add(1);
        return Ok(());
    }
    while off < payload.len() {
        let chunk = (payload.len() - off).min(MAX_PACKET);
        let h = [
            (chunk & 0xFF) as u8,
            ((chunk >> 8) & 0xFF) as u8,
            ((chunk >> 16) & 0xFF) as u8,
            *seq,
        ];
        writer.write_all(&h)?;
        writer.write_all(&payload[off..off + chunk])?;
        *seq = seq.wrapping_add(1);
        off += chunk;
        if chunk == MAX_PACKET && off >= payload.len() {
            // Terminating empty packet after exact-multiple payload.
            let h = [0u8, 0u8, 0u8, *seq];
            writer.write_all(&h)?;
            *seq = seq.wrapping_add(1);
        }
    }
    Ok(())
}

// Pure helpers for tests: frame a payload with a given seq.
#[allow(dead_code)]
pub fn frame_payload(payload: &[u8], seq: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.push((payload.len() & 0xFF) as u8);
    v.push(((payload.len() >> 8) & 0xFF) as u8);
    v.push(((payload.len() >> 16) & 0xFF) as u8);
    v.push(seq);
    v.extend_from_slice(payload);
    v
}

// ---------------------------------------------------------------------------
// OK / ERR / EOF
// ---------------------------------------------------------------------------

pub fn ok_payload(affected: u64, message: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + message.len());
    p.push(0x00);
    enc_lenenc_int(&mut p, affected);
    enc_lenenc_int(&mut p, 0); // last insert id
    p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes());
    if !message.is_empty() {
        p.extend_from_slice(message.as_bytes());
    }
    p
}

pub fn err_payload(code: u16, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + message.len());
    p.push(0xFF);
    p.extend_from_slice(&code.to_le_bytes());
    p.push(b'#');
    let mut st = [b'0'; 5];
    for (i, b) in sqlstate.bytes().take(5).enumerate() {
        st[i] = b;
    }
    p.extend_from_slice(&st);
    p.extend_from_slice(message.as_bytes());
    p
}

pub fn eof_payload() -> Vec<u8> {
    vec![0xFE, 0x00, 0x00, 0x02, 0x00]
}

pub fn mysql_error_for(e: &engine::Error) -> (u16, &'static str) {
    match e {
        engine::Error::TableNotFound(_) => (1146, "42S02"),
        engine::Error::TableExists(_) => (1050, "42S01"),
        engine::Error::DuplicateKey(_) => (1062, "23000"),
        engine::Error::ColumnNotFound(_) => (1054, "42S22"),
        engine::Error::ParseError(_) | engine::Error::NotSupported(_) => (1064, "42000"),
        _ => (1105, "HY000"),
    }
}

pub fn write_err<W: Write>(writer: &mut W, seq: &mut u8, e: &engine::Error) -> std::io::Result<()> {
    let (code, state) = mysql_error_for(e);
    write_packet(writer, &err_payload(code, state, &e.to_string()), seq)?;
    writer.flush()?;
    Ok(())
}

pub fn write_err_msg<W: Write>(writer: &mut W, seq: &mut u8, code: u16, msg: &str) -> std::io::Result<()> {
    write_packet(writer, &err_payload(code, "HY000", msg), seq)?;
    writer.flush()?;
    Ok(())
}
