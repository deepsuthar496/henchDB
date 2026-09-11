//! CLI usage and help formatters for server commands.
use engine::{PRODUCT_NAME, PRODUCT_TAGLINE, VERSION};
pub(crate) fn has_help_flag(args: &[String]) -> bool {
    args.iter().any(|a| a == "--help" || a == "-h" || a == "help")
}
pub(crate) fn banner() {
    println!("{PRODUCT_NAME} v{VERSION} — {PRODUCT_TAGLINE}");
}
pub(crate) fn print_main_usage() {
    eprintln!("usage: server [COMMAND] [--dir <data_dir>]");
    eprintln!("Commands: serve, bench, gcbench, clientbench, passwd, dump, restore, promote");
    eprintln!("Run 'server <command> --help' for command-specific options.");
}
pub(crate) fn print_main_help() {
    banner();
    println!("Usage: server [COMMAND] [OPTIONS]");
    println!();
    println!("Commands:");
    println!("  (no command)   Start interactive embedded SQL shell (default)");
    println!("  serve          Run TCP database server (MySQL & PostgreSQL wire protocols)");
    println!("  bench          Run in-process OLTP micro-benchmark");
    println!("  gcbench        Run group-commit and version GC micro-benchmark");
    println!("  clientbench    Run multi-threaded TCP client benchmark");
    println!("  passwd         Set or update user authentication password");
    println!("  dump           Take offline physical backup to .hdb archive");
    println!("  restore        Restore physical backup archive (with optional PITR replay)");
    println!("  promote        Promote replica directory to primary");
    println!();
    println!("Global Options:");
    println!("  --dir <path>   Database storage directory (default: data)");
    println!("  -h, --help     Print help information");
    println!();
    println!("Run 'server <command> --help' for details on a specific command.");
}
pub(crate) fn print_gcbench_help() {
    banner();
    println!("Usage: server gcbench [--threads <n>] [--dir <path>]");
    println!();
    println!("Options:");
    println!("  --threads <n>  Worker thread count (default: 8)");
    println!("  --dir <path>   Benchmark data directory (default: data)");
    println!("  -h, --help     Print help information");
}
pub(crate) fn print_clientbench_help() {
    banner();
    println!("Usage: server clientbench [--host <ip>] [--port <port>] [--threads <n>] [--ops <n>] [--mode point|range|update|txn] [--rows <n>]");
    println!();
    println!("Options:");
    println!("  --host <ip>    Server host (default: 127.0.0.1)");
    println!("  --port <port>  Server port (default: 3307)");
    println!("  --threads <n>  Concurrent client threads (default: 8)");
    println!("  --ops <n>      Total operations per thread (default: 10000)");
    println!("  --mode <mode>  Benchmark workload mode: point, range, update, txn (default: point)");
    println!("  --rows <n>     Working set row count (default: 50000)");
    println!("  -h, --help     Print help information");
}
pub(crate) fn print_serve_help() {
    banner();
    println!("Usage: server serve [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --port <port>             MySQL wire port (default: 3307)");
    println!("  --bind <addr>             Network bind address (default: 0.0.0.0)");
    println!("  --dir <path>              Data directory (default: data)");
    println!("  --max-connections <n>     Maximum concurrent client connections (default: 1024)");
    println!("  --wait-timeout <sec>      Client connection idle timeout in seconds (default: 28800)");
    println!("  --threads <n>             Worker pool thread count (default: 2x CPU count)");
    println!("  --pg-port <port>          PostgreSQL wire port (default: 5432, --no-pg to disable)");
    println!("  --metrics-port <port>     Prometheus metrics HTTP port (default: 9100, --no-metrics to disable)");
    println!("  --repl-port <port>        Replication primary feeder port (default: 3308, --no-repl to disable)");
    println!("  --replica-of <host:port>  Stream from upstream primary as read-only replica");
    println!("  --repl-user <user>        Replication upstream username (default: root)");
    println!("  --repl-password <pw>      Replication upstream password");
    println!("  --read-only               Start server in read-only mode");
    println!("  --wal-archive-dir <dir>   Archive directory for continuous WAL archiving / PITR");
    println!("  --tls-cert <file.pem>     TLS certificate PEM file");
    println!("  --tls-key <file.pem>      TLS private key PEM file");
    println!("  --allow-insecure-bind     Permit binding 0.0.0.0 with empty root password");
    println!("  --no-legacy               Disable legacy framed text protocol");
    println!("  -h, --help                Print help information");
}
pub(crate) fn print_bench_help() {
    banner();
    println!("Usage: server bench [OPTIONS]");
    println!("Options:");
    println!("  --rows <n>    Number of rows for insert/query benchmark (default: 50000)");
    println!("  --dir <path>  Benchmark database directory (default: data)");
    println!("  -h, --help    Print help information");
}
pub(crate) fn print_passwd_help() {
    banner();
    println!("Usage: server passwd --user <name> [--password <pw>] [--plugin sha2|native] [--dir <path>]");
    println!();
    println!("Options:");
    println!("  --user <name>        Username to set password for (required)");
    println!("  --password <pw>      New password (prompts securely on stdin if omitted)");
    println!("  --plugin <plugin>    Hash algorithm: sha2 (caching_sha2_password) or native (mysql_native)");
    println!("  --dir <path>         Data directory containing auth.bin (default: data)");
    println!("  -h, --help           Print help information");
}
pub(crate) fn print_dump_help() {
    banner();
    println!("Usage: server dump [--out <file.hdb>] [--dir <path>]");
    println!();
    println!("Options:");
    println!("  --out <file.hdb>   Output backup archive path (default: backup_<timestamp>.hdb)");
    println!("  --dir <path>      Database data directory to back up (default: data)");
    println!("  -h, --help         Print help information");
    println!();
    println!("Note: Server must be stopped before running offline dump.");
    println!("For online live backup, run SQL: BACKUP DATABASE TO '<path>';");
}
pub(crate) fn print_restore_help() {
    banner();
    println!("Usage: server restore --backup <file.hdb> [--dir <path>] [--force] [--archive-dir <dir>]");
    println!();
    println!("Options:");
    println!("  --backup <file.hdb>   Source backup archive (required)");
    println!("  --dir <path>          Target directory to restore into (default: data)");
    println!("  --force              Overwrite non-empty target directory");
    println!("  --archive-dir <dir>   WAL archive directory for Point-in-Time Recovery (PITR)");
    println!("  --target-time <ts>    Stop PITR replay at timestamp (YYYY-MM-DD [HH:MM:SS])");
    println!("  --target-txn <id>     Stop PITR replay before transaction ID");
    println!("  -h, --help            Print help information");
}
pub(crate) fn print_promote_help() {
    banner();
    println!("Usage: server promote [--dir <path>]");
    println!();
    println!("Options:");
    println!("  --dir <path>  Replica data directory to promote to primary (default: data)");
    println!("  -h, --help    Print help information");
}
