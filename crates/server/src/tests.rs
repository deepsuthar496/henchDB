//! Server configuration validation and security policy tests (§24, §49).

use super::*;

#[cfg(test)]
mod opts_tests {
    use super::*;

    #[test]
    fn server_opts_default_validation_succeeds() {
        let opts = ServerOpts::from_args(&["serve".into()]);
        assert_eq!(opts.port, 3307);
        assert_eq!(opts.bind, "0.0.0.0");
        assert!(!opts.allow_insecure_bind);
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn server_opts_insecure_bind_flag_parsed() {
        let opts = ServerOpts::from_args(& ["serve".into(), "--allow-insecure-bind".into()]);
        assert!(opts.allow_insecure_bind);
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn server_opts_rejects_invalid_port_zero() {
        let opts = ServerOpts::from_args(& ["serve".into(), "--port".into(), "0".into()]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid port 0"));
    }

    #[test]
    fn server_opts_rejects_max_connections_zero() {
        let opts = ServerOpts::from_args(& ["serve".into(), "--max-connections".into(), "0".into()]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid --max-connections 0"));
    }

    #[test]
    fn server_opts_rejects_threads_zero() {
        let opts = ServerOpts::from_args(& ["serve".into(), "--threads".into(), "0".into()]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid --threads 0"));
    }

    #[test]
    fn server_opts_rejects_invalid_bind_address() {
        let opts = ServerOpts::from_args(& ["serve".into(), "--bind".into(), "invalid.ip.format".into()]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid bind address"));
    }

    #[test]
    fn server_opts_rejects_port_conflict() {
        let opts = ServerOpts::from_args(& [
            "serve".into(),
            "--port".into(),
            "5432".into(),
            "--pg-port".into(),
            "5432".into(),
        ]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("port conflict"));
    }

    #[test]
    fn server_opts_rejects_incomplete_tls() {
        let opts = ServerOpts::from_args(&[
            "serve".into(),
            "--tls-cert".into(),
            "cert.pem".into(),
        ]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid TLS configuration"));
    }

    #[test]
    fn server_opts_rejects_nonexistent_tls_files() {
        let opts = ServerOpts::from_args(&[
            "serve".into(),
            "--tls-cert".into(),
            "nonexistent_cert.pem".into(),
            "--tls-key".into(),
            "nonexistent_key.pem".into(),
        ]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("file does not exist"));
    }

    #[test]
    fn server_opts_rejects_excessive_limits() {
        let opts_conn = ServerOpts::from_args(&[
            "serve".into(),
            "--max-connections".into(),
            "100000".into(),
        ]);
        let err = opts_conn.validate().unwrap_err();
        assert!(err.to_string().contains("between 1 and 65536"));

        let opts_th = ServerOpts::from_args(&[
            "serve".into(),
            "--threads".into(),
            "2000".into(),
        ]);
        let err2 = opts_th.validate().unwrap_err();
        assert!(err2.to_string().contains("between 1 and 1024"));
    }

    #[test]
    fn server_opts_rejects_zero_idle_timeout() {
        let opts = ServerOpts::from_args(&[
            "serve".into(),
            "--wait-timeout".into(),
            "0".into(),
        ]);
        let err = opts.validate().unwrap_err();
        assert!(err.to_string().contains("invalid --wait-timeout 0"));
    }
}

