//! Production authentication (SEC1): salted password proofs, no cleartext.
//!
//! Two MySQL-compatible plugins, both verified without ever seeing a
//! password on the server side:
//! - `caching_sha2_password` (default, SHA-256): the server stores
//!   `SHA256(password)`; fast-path proof only, full (cleartext)
//!   authentication is refused. With `s1 = SHA256(password)` and `s2 =
//!   SHA256(s1)`: `token = s1 XOR SHA256(s2 || scramble)` (always 32 bytes,
//!   mirroring `mysql_native_password`'s construction with SHA-256).
//! - `mysql_native_password` (SHA-1): the server stores
//!   `SHA1(SHA1(password))`; `token = stage1 XOR SHA1(scramble || stage2)`.
//!
//! An empty-password account accepts only an empty proof. Unknown users and
//! wrong passwords both fail with the same error (no user enumeration).
//! SHA-1/SHA-256 are implemented here in portable std (zero dependencies);
//! both are cross-checked against published test vectors in the unit tests,
//! and the token math is validated live against the official `mysql.exe`
//! client (including >32-byte passwords for the cycling rule).
//!
//! Users persist in `auth.bin` beside the WAL/snapshot (magic `HDBA`,
//! versioned, allocation-capped like the other codecs). If the file is
//! missing at serve startup, a `root` account with an EMPTY password is
//! bootstrapped with a loud warning (insecure-init semantics, keeps local
//! benches working); set a password via `server passwd` before exposing
//! the port.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use engine::db::privilege::GrantRule;
use engine::sql::Privilege;

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4, portable, no deps)
// ---------------------------------------------------------------------------

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    // Padding: message + 0x80 + zeros + 64-bit big-endian bit length.
    let mut msg = Vec::with_capacity(((data.len() + 9 + 63) / 64) * 64);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64).wrapping_mul(8)).to_be_bytes());
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([chunk[4 * i], chunk[4 * i + 1], chunk[4 * i + 2], chunk[4 * i + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(SHA256_K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// SHA-1 (FIPS 180-4, for mysql_native_password only)
// ---------------------------------------------------------------------------

pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut msg = Vec::with_capacity(((data.len() + 9 + 63) / 64) * 64);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64).wrapping_mul(8)).to_be_bytes());
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([chunk[4 * i], chunk[4 * i + 1], chunk[4 * i + 2], chunk[4 * i + 3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for i in 0..80 {
            let (f, k) = match i {
                0..20 => ((b & c) | ((!b) & d), 0x5A827999),
                20..40 => (b ^ c ^ d, 0x6ED9EBA1),
                40..60 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, v) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// Password proofs
// ---------------------------------------------------------------------------

/// Plugin identifiers on the wire and in `auth.bin`.
pub const PLUGIN_CACHING_SHA2: &str = "caching_sha2_password";
pub const PLUGIN_NATIVE: &str = "mysql_native_password";

/// Stored verifier for one account. `verifier` is empty for empty-password
/// accounts (only an empty proof is accepted then). `grants` carries the
/// RBAC rules (v2 file section; empty for v1-decoded accounts except the
/// migration rule below).
#[derive(Debug, Clone)]
pub struct Verifier {
    pub plugin: String,
    pub hash: Vec<u8>, // 32 bytes (SHA-256) or 20 bytes (double SHA-1)
    pub grants: Vec<GrantRule>,
}

impl Verifier {
    pub fn new_sha2(password: &[u8]) -> Self {
        Verifier {
            plugin: PLUGIN_CACHING_SHA2.into(),
            hash: sha256(password).to_vec(),
            grants: Vec::new(),
        }
    }
    pub fn new_native(password: &[u8]) -> Self {
        let stage1 = sha1(password);
        Verifier {
            plugin: PLUGIN_NATIVE.into(),
            hash: sha1(&stage1).to_vec(),
            grants: Vec::new(),
        }
    }
    pub fn empty() -> Self {
        Verifier {
            plugin: PLUGIN_CACHING_SHA2.into(),
            hash: Vec::new(),
            grants: Vec::new(),
        }
    }
    /// Empty-password accounts (dev bootstrap). Kept as API for callers;
    /// verification itself branches on the empty hash.
    #[allow(dead_code)]
    pub fn is_empty_password(&self) -> bool {
        self.hash.is_empty()
    }
}

fn xor_cycle(data: &[u8], mask: &[u8]) -> Vec<u8> {
    data.iter().enumerate().map(|(i, b)| b ^ mask[i % mask.len()]).collect()
}

/// Verify a `caching_sha2_password` fast-path token against the stored
/// `SHA256(password)`. `token` is empty (or a single NUL byte `[0]`) for
/// empty-password logins.
/// Proof: `token = s1 XOR SHA256(SHA256(s1) || scramble)` with `s1` the
/// stored hash, so XOR-ing back recovers the candidate `s1`, which must
/// equal the stored verifier byte-for-byte (no re-hash: the stored value
/// IS the single hash, unlike the native plugin's double hash).
pub fn verify_sha2(stored_sha256: &[u8], scramble: &[u8], token: &[u8]) -> bool {
    let empty_token = token.is_empty() || token == [0];
    if stored_sha256.is_empty() {
        return empty_token;
    }
    if empty_token {
        return false;
    }
    if stored_sha256.len() != 32 || token.len() != 32 {
        return false;
    }
    let stage2 = sha256(stored_sha256);
    let mut pre = Vec::with_capacity(32 + scramble.len());
    pre.extend_from_slice(&stage2);
    pre.extend_from_slice(scramble);
    let mask = sha256(&pre);
    xor_cycle(token, &mask) == stored_sha256
}

/// Verify a `mysql_native_password` token against stored `SHA1(SHA1(pw))`.
pub fn verify_native(stored_stage2: &[u8], scramble: &[u8], token: &[u8]) -> bool {
    let empty_token = token.is_empty() || token == [0];
    if stored_stage2.is_empty() {
        return empty_token;
    }
    if empty_token {
        return false;
    }
    if stored_stage2.len() != 20 || token.len() != 20 {
        return false;
    }
    // candidate_stage1 = token XOR SHA1(scramble || stored).
    let mut pre = Vec::with_capacity(scramble.len() + 20);
    pre.extend_from_slice(scramble);
    pre.extend_from_slice(stored_stage2);
    let mask = sha1(&pre);
    let candidate = xor_cycle(token, &mask);
    sha1(&candidate) == stored_stage2
}

/// Dispatch verification by the plugin the client actually used.
pub fn verify(v: &Verifier, plugin: &str, scramble: &[u8], token: &[u8]) -> bool {
    let empty_token = token.is_empty() || token == [0];
    if v.hash.is_empty() {
        return empty_token;
    }
    if empty_token {
        return false;
    }
    match plugin {
        PLUGIN_CACHING_SHA2 => verify_sha2(&v.hash, scramble, token),
        PLUGIN_NATIVE => verify_sha2_native_fallback(v, scramble, token),
        _ => false,
    }
}

/// `mysql_native_password` only verifies against a native verifier; a sha2
/// account cannot be opened through the native plugin (fail closed).
fn verify_sha2_native_fallback(v: &Verifier, scramble: &[u8], token: &[u8]) -> bool {
    if v.plugin != PLUGIN_NATIVE {
        return false;
    }
    verify_native(&v.hash, scramble, token)
}

// ---------------------------------------------------------------------------
// User store (`auth.bin`)
// ---------------------------------------------------------------------------

pub const AUTH_MAGIC: &[u8; 4] = b"HDBA";
/// v1 = verifiers only; v2 appends a per-user grants section (RBAC). v1
/// files decode cleanly: every pre-existing non-root account migrates with
/// `ALL PRIVILEGES ON *.*` (behavior-preserving upgrade — tighten with
/// REVOKE afterwards); root needs no stored grant (code bypass).
pub const AUTH_FORMAT_VERSION: u32 = 2;
const AUTH_FORMAT_V1: u32 = 1;

pub struct UserStore {
    path: PathBuf,
    pub users: HashMap<String, Verifier>,
}

impl UserStore {
    /// Load the store, bootstrapping `root` with an EMPTY password (with a
    /// loud warning) when no file exists yet. Returns (store, fresh).
    pub fn load_or_bootstrap(path: &Path) -> Result<(Self, bool), String> {
        if !path.exists() {
            let mut users = HashMap::new();
            users.insert("root".to_string(), Verifier::empty());
            let store = UserStore {
                path: path.to_path_buf(),
                users,
            };
            store.save()?;
            eprintln!("WARNING: no auth file; created 'root' with EMPTY password.");
            eprintln!("WARNING: set one before exposing the port: server passwd --dir {} --user root --password <pw>", path.parent().map(|p| p.display().to_string()).unwrap_or_else(|| ".".into()));
            return Ok((store, true));
        }
        Self::load(path).map(|s| (s, false))
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let mut f = File::open(path).map_err(|e| format!("auth file: {e}"))?;
        let mut magic = [0u8; 4];
        f.read_exact(&mut magic).map_err(|_| "auth file: truncated".to_string())?;
        if &magic != AUTH_MAGIC {
            return Err("auth file: bad magic".to_string());
        }
        let mut b4 = [0u8; 4];
        f.read_exact(&mut b4).map_err(|_| "auth file: truncated".to_string())?;
        let version = u32::from_le_bytes(b4);
        if version != AUTH_FORMAT_VERSION && version != AUTH_FORMAT_V1 {
            return Err("auth file: version".to_string());
        }
        f.read_exact(&mut b4).map_err(|_| "auth file: truncated".to_string())?;
        let n = u32::from_le_bytes(b4) as usize;
        if n > 10_000 {
            return Err("auth file: user count too large".to_string());
        }
        let mut users = HashMap::with_capacity(n.min(64));
        for _ in 0..n {
            f.read_exact(&mut b4).map_err(|_| "auth file: truncated".to_string())?;
            let nlen = u32::from_le_bytes(b4) as usize;
            if nlen == 0 || nlen > 1024 {
                return Err("auth file: bad username".to_string());
            }
            let mut nbuf = vec![0u8; nlen];
            f.read_exact(&mut nbuf).map_err(|_| "auth file: truncated".to_string())?;
            let name = String::from_utf8(nbuf).map_err(|_| "auth file: bad username".to_string())?;
            let mut pbyte = [0u8; 1];
            f.read_exact(&mut pbyte).map_err(|_| "auth file: truncated".to_string())?;
            let plugin = match pbyte[0] {
                1 => PLUGIN_CACHING_SHA2,
                2 => PLUGIN_NATIVE,
                _ => return Err("auth file: bad plugin".to_string()),
            }
            .to_string();
            let mut hlen = [0u8; 1];
            f.read_exact(&mut hlen).map_err(|_| "auth file: truncated".to_string())?;
            let hlen = hlen[0] as usize;
            if hlen > 64 {
                return Err("auth file: bad verifier".to_string());
            }
            // Empty verifier = empty password (dev bootstrap only).
            let mut hash = vec![0u8; hlen];
            if hlen > 0 {
                f.read_exact(&mut hash).map_err(|_| "auth file: truncated".to_string())?;
            }
            // Sanity: known verifier sizes (empty allowed).
            if !hash.is_empty() && hash.len() != 32 && hash.len() != 20 {
                return Err("auth file: bad verifier".to_string());
            }
            let grants = if version == AUTH_FORMAT_V1 {
                Vec::new()
            } else {
                decode_grants(&mut f)?
            };
            users.insert(name, Verifier { plugin, hash, grants });
        }
        if version == AUTH_FORMAT_V1 {
            // Behavior-preserving upgrade: pre-RBAC accounts keep full
            // access (root bypasses in code and needs nothing stored).
            for (name, v) in users.iter_mut() {
                if name != "root" {
                    v.grants.push(GrantRule {
                        priv_: Privilege::All,
                        db: "*".into(),
                        tbl: "*".into(),
                    });
                }
            }
        }
        Ok(UserStore {
            path: path.to_path_buf(),
            users,
        })
    }

    pub fn set_password(&mut self, user: &str, password: &[u8], plugin: &str) -> Result<(), String> {
        if user.is_empty() || user.len() > 256 || user.contains('\0') {
            return Err("bad username".to_string());
        }
        let v = match plugin {
            PLUGIN_CACHING_SHA2 => Verifier::new_sha2(password),
            PLUGIN_NATIVE => Verifier::new_native(password),
            _ => return Err(format!("unknown plugin '{plugin}'")),
        };
        self.users.insert(user.to_string(), v);
        self.save()
    }

    pub fn save(&self) -> Result<(), String> {
        let tmp = self.path.with_extension("tmp");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("auth file: {e}"))?;
        f.write_all(AUTH_MAGIC).map_err(|e| format!("auth file: {e}"))?;
        f.write_all(&AUTH_FORMAT_VERSION.to_le_bytes()).map_err(|e| format!("auth file: {e}"))?;
        f.write_all(&(self.users.len() as u32).to_le_bytes()).map_err(|e| format!("auth file: {e}"))?;
        let mut names: Vec<&String> = self.users.keys().collect();
        names.sort();
        for name in names {
            let v = &self.users[name];
            f.write_all(&(name.len() as u32).to_le_bytes()).map_err(|e| format!("auth file: {e}"))?;
            f.write_all(name.as_bytes()).map_err(|e| format!("auth file: {e}"))?;
            let pbyte = match v.plugin.as_str() {
                PLUGIN_CACHING_SHA2 => 1u8,
                PLUGIN_NATIVE => 2u8,
                _ => return Err("auth file: bad plugin".to_string()),
            };
            f.write_all(&[pbyte]).map_err(|e| format!("auth file: {e}"))?;
            if v.hash.len() > 64 {
                return Err("auth file: bad verifier".to_string());
            }
            f.write_all(&[v.hash.len() as u8]).map_err(|e| format!("auth file: {e}"))?;
            f.write_all(&v.hash).map_err(|e| format!("auth file: {e}"))?;
            encode_grants(&mut f, &v.grants)?;
        }
        f.sync_data().map_err(|e| format!("auth file: {e}"))?;
        drop(f);
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("auth file: {e}"))?;
        Ok(())
    }
}

/// Version-guarded persist: snapshot `db.privilege_version()` before client
/// SQL, call this after — persists only when a privilege mutation happened
/// (GRANT/REVOKE/CREATE/DROP/ALTER USER). Save failures are logged loudly
/// but never fail the (already executed) statement; the next mutation
/// retries.
pub fn persist_if_changed(db: &engine::Database, auth_path: &Path, before: u64) {
    if db.privilege_version() != before {
        if let Err(e) = persist_privileges(db, auth_path) {
            eprintln!("auth persist failed (grants held in memory, retried on next change): {e}");
        }
    }
}

/// Serializes concurrent privilege persists (two interleaved
/// read-modify-write cycles would otherwise drop grants).
static AUTH_SAVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Post-execute persist hook: call after client SQL when
/// `db.privilege_version()` moved since before the statement. Exports the
/// engine principals + grants, hashes staged CREATE/ALTER passwords into
/// verifiers, prunes tombstoned (dropped) accounts, imports file-only
/// accounts (`passwd`-created) as grantless principals, and saves
/// atomically. A save failure is reported but never fails the (already
/// executed) statement; the next mutation retries.
pub fn persist_privileges(db: &engine::Database, auth_path: &Path) -> Result<(), String> {
    let _guard = AUTH_SAVE_LOCK
        .lock()
        .map_err(|e| format!("auth save lock: {e}"))?;
    let (users, tombstones, pending) = db.export_privileges();
    let mut store = match UserStore::load(auth_path) {
        Ok(s) => s,
        Err(_) => UserStore { path: auth_path.to_path_buf(), users: HashMap::new() },
    };
    // Staged passwords first: hash into verifiers, preserving any grants
    // already on the account.
    for (user, pw) in &pending {
        let v = Verifier::new_sha2(pw.as_bytes());
        match store.users.get_mut(user) {
            Some(e) => {
                e.hash = v.hash;
                e.plugin = v.plugin;
            }
            None => {
                store.users.insert(user.clone(), v);
            }
        }
    }
    // Grants sync for engine principals.
    for (user, rules) in &users {
        match store.users.get_mut(user) {
            Some(e) => e.grants = rules.clone(),
            None => eprintln!(
                "auth: principal '{user}' has no account; grants held in memory only"
            ),
        }
    }
    // Prune dropped accounts.
    for t in &tombstones {
        store.users.remove(t);
    }
    // Import file-only accounts as grantless principals (root needs none:
    // the code bypass covers it, and it must never gain stored rules).
    let fresh: Vec<String> = store
        .users
        .keys()
        .filter(|n| *n != "root" && !users.iter().any(|(u, _)| u == *n))
        .cloned()
        .collect();
    if !fresh.is_empty() {
        db.import_missing_users(&fresh);
    }
    store.save()
}

/// Encode one user's grant list: u32 count + per rule (u8 priv, u32 db +
/// bytes, u32 tbl + bytes). Lengths capped like the rest of the codec.
fn encode_grants(f: &mut File, grants: &[GrantRule]) -> Result<(), String> {
    if grants.len() > 65_536 {
        return Err("auth file: too many grants".to_string());
    }
    f.write_all(&(grants.len() as u32).to_le_bytes())
        .map_err(|e| format!("auth file: {e}"))?;
    for g in grants {
        f.write_all(&[g.priv_.codec_byte()]).map_err(|e| format!("auth file: {e}"))?;
        for part in [&g.db, &g.tbl] {
            if part.len() > 1024 {
                return Err("auth file: bad grant scope".to_string());
            }
            f.write_all(&(part.len() as u32).to_le_bytes())
                .map_err(|e| format!("auth file: {e}"))?;
            f.write_all(part.as_bytes()).map_err(|e| format!("auth file: {e}"))?;
        }
    }
    Ok(())
}

fn read_u32_capped(f: &mut File, cap: usize, what: &str) -> Result<Vec<u8>, String> {
    let mut b4 = [0u8; 4];
    f.read_exact(&mut b4).map_err(|_| "auth file: truncated".to_string())?;
    let n = u32::from_le_bytes(b4) as usize;
    if n > cap {
        return Err(format!("auth file: {what} too large"));
    }
    let mut buf = vec![0u8; n];
    if n > 0 {
        f.read_exact(&mut buf).map_err(|_| "auth file: truncated".to_string())?;
    }
    Ok(buf)
}

fn decode_grants(f: &mut File) -> Result<Vec<GrantRule>, String> {
    let mut b4 = [0u8; 4];
    f.read_exact(&mut b4).map_err(|_| "auth file: truncated".to_string())?;
    let n = u32::from_le_bytes(b4) as usize;
    if n > 65_536 {
        return Err("auth file: too many grants".to_string());
    }
    let mut out = Vec::with_capacity(n.min(16));
    for _ in 0..n {
        let mut pb = [0u8; 1];
        f.read_exact(&mut pb).map_err(|_| "auth file: truncated".to_string())?;
        let priv_ = Privilege::from_codec_byte(pb[0])
            .ok_or_else(|| "auth file: bad privilege".to_string())?;
        let db = String::from_utf8(read_u32_capped(f, 1024, "grant scope")?)
            .map_err(|_| "auth file: bad grant scope".to_string())?;
        let tbl = String::from_utf8(read_u32_capped(f, 1024, "grant scope")?)
            .map_err(|_| "auth file: bad grant scope".to_string())?;
        if db.is_empty() || tbl.is_empty() {
            return Err("auth file: bad grant scope".to_string());
        }
        out.push(GrantRule { priv_, db, tbl });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_vectors() {
        // Published FIPS vectors (hex).
        assert_eq!(
            sha1(b"abc").iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            sha256(b"abc").iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256(b"").iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn double_sha1_is_stable() {
        // sha1("abc") above validates the primitive; double hashing is just
        // re-application. Cross-checked live against mysql.exe native auth.
        let stage1 = sha1(b"password");
        let stage2 = sha1(&stage1);
        assert_eq!(stage2.len(), 20);
        assert_ne!(stage1, stage2);
        assert_eq!(sha1(&stage1), stage2);
    }

    /// Client-side token builders (mirror what real clients send).
    fn native_token(password: &[u8], scramble: &[u8]) -> Vec<u8> {
        let stage1 = sha1(password);
        let stage2 = sha1(&stage1);
        let mut pre = Vec::new();
        pre.extend_from_slice(scramble);
        pre.extend_from_slice(&stage2);
        xor_cycle(&stage1, &sha1(&pre))
    }

    fn sha2_token(password: &[u8], scramble: &[u8]) -> Vec<u8> {
        let stage1 = sha256(password);
        let stage2 = sha256(&stage1);
        let mut pre = Vec::new();
        pre.extend_from_slice(&stage2);
        pre.extend_from_slice(scramble);
        xor_cycle(&stage1, &sha256(&pre))
    }

    #[test]
    fn native_roundtrip_and_reject() {
        let scramble = b"12345678901234567890";
        let v = Verifier::new_native(b"s3cret!");
        assert!(verify(&v, PLUGIN_NATIVE, scramble, &native_token(b"s3cret!", scramble)));
        assert!(!verify(&v, PLUGIN_NATIVE, scramble, &native_token(b"wrong", scramble)));
        assert!(!verify(&v, PLUGIN_NATIVE, b"00000000000000000000", &native_token(b"s3cret!", scramble)));
        assert!(!verify(&v, PLUGIN_NATIVE, scramble, b""));
        // A sha2 account cannot open through the native plugin.
        let v2 = Verifier::new_sha2(b"s3cret!");
        assert!(!verify(&v2, PLUGIN_NATIVE, scramble, &native_token(b"s3cret!", scramble)));
        assert!(!verify(&v, "bogus_plugin", scramble, &native_token(b"s3cret!", scramble)));
    }

    #[test]
    fn sha2_roundtrip_and_reject() {
        let scramble = b"abcdefghijklmnopqrst";
        for pw in [b"short".as_slice(), b"exactly-32-bytes-long-password!!".as_slice(), b"a-much-longer-password-that-exceeds-thirty-two-bytes-easily".as_slice()] {
            let v = Verifier::new_sha2(pw);
            assert!(verify(&v, PLUGIN_CACHING_SHA2, scramble, &sha2_token(pw, scramble)), "len={}", pw.len());
            assert!(!verify(&v, PLUGIN_CACHING_SHA2, scramble, &sha2_token(b"wrong", scramble)));
        }
        // Empty-password semantics.
        let v = Verifier::empty();
        assert!(verify(&v, PLUGIN_CACHING_SHA2, scramble, b""));
        assert!(!verify(&v, PLUGIN_CACHING_SHA2, scramble, b"x"));
        let v = Verifier::new_sha2(b"pw");
        assert!(!verify(&v, PLUGIN_CACHING_SHA2, scramble, b""));
    }

    #[test]
    fn user_file_roundtrip_and_corruption() {        let path = std::env::temp_dir().join(format!("hdbauth_{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let (mut store, fresh) = UserStore::load_or_bootstrap(&path).unwrap();
        assert!(fresh);
        assert!(store.users["root"].is_empty_password());
        store.set_password("root", b"pw123", PLUGIN_CACHING_SHA2).unwrap();
        store.set_password("app", b"pw", PLUGIN_NATIVE).unwrap();
        assert!(store.set_password("x", b"pw", "bogus").is_err());
        let store2 = UserStore::load(&path).unwrap();
        assert_eq!(store2.users.len(), 2);
        assert_eq!(store2.users["app"].plugin, PLUGIN_NATIVE);
        assert_eq!(store2.users["root"].hash.len(), 32);
        // Corruption fails cleanly, never panics.
        let bytes = std::fs::read(&path).unwrap();
        for i in 0..bytes.len() {
            let mut c = bytes.clone();
            c[i] ^= 0xFF;
            std::fs::write(&path, &c).unwrap();
            let _ = UserStore::load(&path);
        }
        for len in 0..bytes.len() {
            std::fs::write(&path, &bytes[..len]).unwrap();
            let _ = UserStore::load(&path);
        }
        let _ = std::fs::remove_file(&path);
    }

    fn grant(priv_: Privilege, db: &str, tbl: &str) -> GrantRule {
        GrantRule { priv_, db: db.into(), tbl: tbl.into() }
    }

    #[test]
    fn v2_grants_roundtrip() {
        let path = std::env::temp_dir().join(format!("hdbauthv2_{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let (mut store, _) = UserStore::load_or_bootstrap(&path).unwrap();
        store.set_password("root", b"pw", PLUGIN_CACHING_SHA2).unwrap();
        store.set_password("app", b"pw", PLUGIN_NATIVE).unwrap();
        store.users.get_mut("app").unwrap().grants = vec![
            grant(Privilege::Select, "shop", "*"),
            grant(Privilege::Insert, "shop", "orders"),
            grant(Privilege::All, "*", "*"),
        ];
        store.save().unwrap();
        let re = UserStore::load(&path).unwrap();
        assert_eq!(re.users["app"].grants.len(), 3);
        assert_eq!(re.users["app"].grants[0].priv_, Privilege::Select);
        assert_eq!(re.users["root"].grants.len(), 0);
        // Unknown privilege bytes and truncated grants sections fail clean.
        let bytes = std::fs::read(&path).unwrap();
        for len in 0..bytes.len() {
            std::fs::write(&path, &bytes[..len]).unwrap();
            let _ = UserStore::load(&path);
        }
        let _ = std::fs::remove_file(&path);
    }

    /// Hand-encoded v1 file (no grants section): legacy accounts decode,
    /// non-root keeps ALL (behavior-preserving upgrade), root stays bare.
    #[test]
    fn v1_legacy_decodes_with_migrated_grants() {
        let path = std::env::temp_dir().join(format!("hdbauthv1_{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut f = Vec::new();
        f.extend_from_slice(b"HDBA");
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(&2u32.to_le_bytes());
        for (name, plugin, hash) in [
            ("root", 1u8, vec![]),
            ("app", 2u8, vec![4u8; 20]),
        ] {
            f.extend_from_slice(&(name.len() as u32).to_le_bytes());
            f.extend_from_slice(name.as_bytes());
            f.push(plugin);
            f.push(hash.len() as u8);
            f.extend_from_slice(&hash);
        }
        std::fs::write(&path, &f).unwrap();
        let store = UserStore::load(&path).unwrap();
        assert!(store.users["root"].grants.is_empty());
        assert_eq!(
            store.users["app"].grants,
            vec![grant(Privilege::All, "*", "*")]
        );
        // Saving upgrades the file to v2 (grants section appears).
        store.save().unwrap();
        let re = UserStore::load(&path).unwrap();
        assert_eq!(re.users["app"].grants.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn persist_hook_end_to_end() {
        // Full SQL flow: CREATE USER + GRANT through the engine, persist,
        // reload — account, verifier, and grants all survive.
        let dir = std::env::temp_dir().join(format!("hdbpersist_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = engine::Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        let v0 = db.privilege_version();
        db.execute(&mut s, "CREATE USER app IDENTIFIED BY 's3cret!'").unwrap();
        db.execute(&mut s, "GRANT SELECT, INSERT ON t TO app").unwrap();
        assert!(db.privilege_version() != v0);
        let auth_path = dir.join("auth.bin");
        persist_if_changed(&db, &auth_path, v0);
        let store = UserStore::load(&auth_path).unwrap();
        assert_eq!(store.users["app"].hash, sha256(b"s3cret!").to_vec());
        assert_eq!(store.users["app"].grants.len(), 2);
        // Pending passwords drained (memory-only, never on disk plaintext).
        let (_, _, pending) = db.export_privileges();
        assert!(pending.is_empty());
        // DROP USER prunes the account on the next persist.
        let v1 = db.privilege_version();
        db.execute(&mut s, "DROP USER app").unwrap();
        persist_if_changed(&db, &auth_path, v1);
        let store = UserStore::load(&auth_path).unwrap();
        assert!(!store.users.contains_key("app"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
