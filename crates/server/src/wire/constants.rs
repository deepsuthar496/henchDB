//! Protocol constants for the MySQL client/server wire protocol.

use engine::PRODUCT_NAME;

pub const SERVER_VERSION_PREFIX: &str = "8.0.46";
/// Default authentication plugin (SHA-256, MySQL 8 default). The server also
/// accepts `mysql_native_password` when the client requests it; both are
/// verified in `auth.rs`, never with cleartext.
pub const AUTH_PLUGIN: &str = "caching_sha2_password";
pub const CHARSET_UTF8MB4: u8 = 255;
pub const STATUS_AUTOCOMMIT: u16 = 0x0002;

// Capability flags advertised to clients (subset of CLIENT_*).
pub const CAP_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CAP_FOUND_ROWS: u32 = 0x0000_0002;
pub const CAP_LONG_FLAG: u32 = 0x0000_0004;
pub const CAP_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CAP_PROTOCOL_41: u32 = 0x0000_0200;
pub const CAP_TRANSACTIONS: u32 = 0x0000_2000;
pub const CAP_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CAP_MULTI_STATEMENTS: u32 = 0x0001_0000;
pub const CAP_MULTI_RESULTS: u32 = 0x0002_0000;
pub const CAP_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CAP_DEPRECATE_EOF: u32 = 0x0100_0000;
/// Client requests a TLS upgrade (SSLRequest) before authenticating.
/// Advertised only when the server loaded `--tls-cert`/`--tls-key`.
pub const CAP_SSL: u32 = 0x0000_0800;

pub const SERVER_CAPS: u32 = CAP_LONG_PASSWORD
    | CAP_FOUND_ROWS
    | CAP_LONG_FLAG
    | CAP_CONNECT_WITH_DB
    | CAP_PROTOCOL_41
    | CAP_TRANSACTIONS
    | CAP_SECURE_CONNECTION
    | CAP_MULTI_STATEMENTS
    | CAP_MULTI_RESULTS
    | CAP_PLUGIN_AUTH;

// MySQL protocol commands.
pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_SHUTDOWN: u8 = 0x0B;
pub const COM_PING: u8 = 0x0E;
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
pub const COM_STMT_CLOSE: u8 = 0x19;
pub const COM_STMT_RESET: u8 = 0x1A;
pub const COM_STMT_FETCH: u8 = 0x1C;
pub const COM_RESET_CONNECTION: u8 = 0x1F;

// MySQL field / column types.
pub const TYPE_DECIMAL: u8 = 0x00;
pub const TYPE_TINY: u8 = 0x01;
pub const TYPE_SHORT: u8 = 0x02;
pub const TYPE_LONG: u8 = 0x03;
pub const TYPE_FLOAT: u8 = 0x04;
pub const TYPE_DOUBLE: u8 = 0x05;
pub const TYPE_NULL: u8 = 0x06;
pub const TYPE_TIMESTAMP: u8 = 0x07;
pub const TYPE_LONGLONG: u8 = 0x08;
pub const TYPE_INT24: u8 = 0x09;
pub const TYPE_DATE: u8 = 0x0A;
pub const TYPE_TIME: u8 = 0x0B;
pub const TYPE_DATETIME: u8 = 0x0C;
pub const TYPE_YEAR: u8 = 0x0D;
pub const TYPE_VARCHAR: u8 = 0x0F;
pub const TYPE_BIT: u8 = 0x10;
pub const TYPE_NEWDECIMAL: u8 = 0xF6;
pub const TYPE_ENUM: u8 = 0xF7;
pub const TYPE_SET: u8 = 0xF8;
pub const TYPE_TINY_BLOB: u8 = 0xF9;
pub const TYPE_MEDIUM_BLOB: u8 = 0xFA;
pub const TYPE_LONG_BLOB: u8 = 0xFB;
pub const TYPE_BLOB: u8 = 0xFC;
pub const TYPE_VAR_STRING: u8 = 0xFD;
pub const TYPE_STRING: u8 = 0xFE;
pub const TYPE_GEOMETRY: u8 = 0xFF;

// DoS guards for per-connection prepared state.
pub const MAX_PREPARED_PER_CONN: usize = 4096;
pub const MAX_PARAMS_PER_STMT: usize = 4096;
pub const MAX_LONG_DATA_PER_PARAM: usize = 16 * 1024 * 1024;

// Scrambles are fresh 20-byte values per connection (generated in
// `handshake.rs`); a fixed scramble would allow replay across sessions.
pub const MAX_PACKET: usize = 0xFF_FFFF;

pub fn server_version() -> String {
    format!("{SERVER_VERSION_PREFIX}-{PRODUCT_NAME}")
}
