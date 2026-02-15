//! Integration tests for ugnosd config: file, env overrides, CLI flags, and precedence.

use std::process::Command;

/// Run ugnosd in a subprocess with the given args and env. Returns (success, stdout, stderr).
fn run_ugnosd(args: &[&str], env_extra: &[(&str, &str)]) -> (bool, String, String) {
    let exe = env!("CARGO_BIN_EXE_ugnosd");
    let mut cmd = Command::new(exe);
    cmd.args(args);
    for (k, v) in env_extra {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run ugnosd");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    (out.status.success(), stdout, stderr)
}

#[test]
fn validate_config_no_config_uses_default_data_dir() {
    let (ok, stdout, stderr) = run_ugnosd(&["--validate-config", "--no-config"], &[]);
    assert!(ok, "stderr: {}", stderr);
    assert!(stdout.contains("data_dir="), "stdout: {}", stdout);
    assert!(
        stdout.contains("data_dir=./data") || stdout.contains("data_dir=.\\data"),
        "stdout: {}",
        stdout
    );
}

#[test]
fn validate_config_cli_data_dir_overrides() {
    let (ok, stdout, _) = run_ugnosd(
        &[
            "--validate-config",
            "--no-config",
            "--data-dir",
            "/var/lib/ugnos",
        ],
        &[],
    );
    assert!(ok);
    assert!(
        stdout.contains("data_dir=/var/lib/ugnos"),
        "stdout: {}",
        stdout
    );
}

#[test]
fn validate_config_env_data_dir_overridden_by_cli() {
    let (ok, stdout, _) = run_ugnosd(
        &["--validate-config", "--no-config", "--data-dir", "/cli/dir"],
        &[("UGNOS_DATA_DIR", "/env/dir")],
    );
    assert!(ok);
    assert!(
        stdout.contains("data_dir=/cli/dir"),
        "CLI should win: {}",
        stdout
    );
}

#[test]
fn validate_config_env_override_with_no_config() {
    let (ok, stdout, _) = run_ugnosd(
        &["--validate-config", "--no-config"],
        &[("UGNOS_DATA_DIR", "/env/override")],
    );
    assert!(ok);
    assert!(
        stdout.contains("data_dir=/env/override"),
        "stdout: {}",
        stdout
    );
}

#[test]
fn invalid_encoding_in_config_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("bad.toml");
    std::fs::write(
        &config_path,
        r#"
data_dir = "/tmp/ugnos"
[segment_store]
[segment_store.encoding]
float_encoding = "invalid_encoding"
"#,
    )
    .expect("write config");
    let (ok, _stdout, stderr) = run_ugnosd(
        &[
            "--validate-config",
            "--config",
            config_path.to_str().unwrap(),
        ],
        &[],
    );
    assert!(!ok, "invalid encoding should fail");
    assert!(
        stderr.contains("unknown variant")
            || stderr.contains("invalid_encoding")
            || stderr.contains("config error"),
        "stderr: {}",
        stderr
    );
}

#[test]
fn explicit_config_missing_file_fails() {
    let (ok, _stdout, stderr) = run_ugnosd(
        &["--validate-config", "--config", "/nonexistent/ugnosd.toml"],
        &[],
    );
    assert!(
        !ok,
        "missing config file with explicit --config should fail"
    );
    assert!(
        stderr.contains("not found") || stderr.contains("config error"),
        "stderr: {}",
        stderr
    );
}

#[test]
fn valid_config_file_merges_with_defaults() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("ugnosd.toml");
    std::fs::write(
        &config_path,
        r#"
data_dir = "/tmp/ugnos_test"
flush_interval_secs = 2
enable_wal = true
enable_segments = true
"#,
    )
    .expect("write config");
    let (ok, stdout, stderr) = run_ugnosd(
        &[
            "--validate-config",
            "--config",
            config_path.to_str().unwrap(),
        ],
        &[],
    );
    assert!(ok, "stderr: {}", stderr);
    assert!(stdout.contains("data_dir=/tmp/ugnos_test"));
    assert!(stdout.contains("flush_interval_secs=2"));
}

#[test]
fn validate_config_prints_http_bind() {
    let (ok, stdout, stderr) = run_ugnosd(&["--validate-config", "--no-config"], &[]);
    assert!(ok, "stderr: {}", stderr);
    assert!(
        stdout.contains("http_bind="),
        "validate-config should print http_bind: {}",
        stdout
    );
}
