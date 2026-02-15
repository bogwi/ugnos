//! Adversarial and integration tests for ugnosd health endpoints, readiness/liveness,
//! safe startup checks, and graceful shutdown.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

const HEALTH_PORT: u16 = 19499;
const STARTUP_WAIT_MS: u64 = 800;
const SHUTDOWN_WAIT_MS: u64 = 3000;

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

/// Start ugnosd in the background; returns the child process. Caller must kill it.
fn start_ugnosd_background(args: &[&str], env_extra: &[(&str, &str)]) -> Child {
    let exe = env!("CARGO_BIN_EXE_ugnosd");
    let mut cmd = Command::new(exe);
    cmd.args(args)
        .envs(env_extra.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.spawn().expect("spawn ugnosd")
}

/// GET /path on host:port, return (status_line, body_prefix).
fn http_get(host: &str, port: u16, path: &str) -> Option<(String, String)> {
    let mut stream = TcpStream::connect((host, port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream
        .write_all(format!("GET {} HTTP/1.0\r\nHost: {}\r\n\r\n", path, host).as_bytes())
        .ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let s = String::from_utf8_lossy(&buf).into_owned();
    let mut lines = s.lines();
    let status = lines.next()?.to_string();
    let body = lines
        .skip_while(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let body_prefix = if body.len() > 20 {
        body[..20].to_string()
    } else {
        body
    };
    Some((status, body_prefix))
}

// ---------- Safe startup (adversarial) ----------

#[test]
fn invalid_http_bind_fails_at_startup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let (ok, _stdout, stderr) = run_ugnosd(
        &[
            "--no-config",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--http-bind",
            "not-a-valid-address",
        ],
        &[],
    );
    assert!(!ok, "invalid http_bind should fail");
    assert!(
        stderr.contains("invalid http_bind") || stderr.contains("config error"),
        "stderr: {}",
        stderr
    );
}

#[test]
fn data_dir_as_file_fails_startup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file_as_data_dir = dir.path().join("file_not_dir");
    std::fs::write(&file_as_data_dir, b"x").expect("write file");
    let (ok, _stdout, stderr) = run_ugnosd(
        &[
            "--no-config",
            "--data-dir",
            file_as_data_dir.to_str().unwrap(),
        ],
        &[],
    );
    assert!(!ok, "data_dir as file should fail");
    assert!(
        stderr.contains("data_dir")
            && (stderr.contains("cannot be created")
                || stderr.contains("not writable")
                || stderr.contains("failed to open")),
        "stderr: {}",
        stderr
    );
}

// ---------- Health / readiness endpoints ----------

#[test]
fn healthz_returns_200_when_running() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let bind = format!("127.0.0.1:{}", HEALTH_PORT);
    let mut child = start_ugnosd_background(
        &[
            "--no-config",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--http-bind",
            &bind,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let result = http_get("127.0.0.1", HEALTH_PORT, "/healthz");
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        result.is_some(),
        "could not connect to health endpoint (port in use or daemon not ready?)"
    );
    let (status, _) = result.unwrap();
    assert!(
        status.contains("200"),
        "expected 200 OK for /healthz, got: {}",
        status
    );
}

#[test]
fn readyz_returns_200_after_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let bind = format!("127.0.0.1:{}", HEALTH_PORT + 1);
    let mut child = start_ugnosd_background(
        &[
            "--no-config",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--http-bind",
            &bind,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let result = http_get("127.0.0.1", HEALTH_PORT + 1, "/readyz");
    let _ = child.kill();
    let _ = child.wait();
    assert!(result.is_some(), "could not connect to readyz");
    let (status, _) = result.unwrap();
    assert!(
        status.contains("200"),
        "expected 200 OK for /readyz when ready, got: {}",
        status
    );
}

#[test]
fn unknown_path_returns_404() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let bind = format!("127.0.0.1:{}", HEALTH_PORT + 2);
    let mut child = start_ugnosd_background(
        &[
            "--no-config",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--http-bind",
            &bind,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let result = http_get("127.0.0.1", HEALTH_PORT + 2, "/nonexistent");
    let _ = child.kill();
    let _ = child.wait();
    assert!(result.is_some());
    let (status, _) = result.unwrap();
    assert!(
        status.contains("404"),
        "expected 404 for unknown path, got: {}",
        status
    );
}

// ---------- Graceful shutdown ----------

#[test]
fn graceful_shutdown_prints_flush_and_complete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let bind = format!("127.0.0.1:{}", HEALTH_PORT + 3);
    let mut child = start_ugnosd_background(
        &[
            "--no-config",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--http-bind",
            &bind,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let _ = http_get("127.0.0.1", HEALTH_PORT + 3, "/healthz");

    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
    }
    #[cfg(not(unix))]
    let _ = child.kill();

    let pid = child.id();
    let stderr_handle = child.stderr.take();
    let (tx, rx) = std::sync::mpsc::channel();
    let _ = thread::spawn(move || {
        let code = child.wait().ok().and_then(|s| s.code());
        let mut s = String::new();
        if let Some(mut h) = stderr_handle {
            let _ = h.read_to_string(&mut s);
        }
        let _ = tx.send((code, s));
    });
    let (exit_code, stderr) = rx
        .recv_timeout(Duration::from_millis(SHUTDOWN_WAIT_MS))
        .unwrap_or_else(|_| {
            #[cfg(unix)]
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
            #[cfg(not(unix))]
            let _ = std::process::Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/F"])
                .status();
            rx.recv().unwrap_or((None, String::new()))
        });
    assert!(
        stderr.contains("flushing")
            || stderr.contains("shutdown")
            || stderr.contains("Shutting")
            || stderr.contains("complete"),
        "stderr should mention flush/shutdown/complete: {}",
        stderr
    );
    // Acceptance: graceful shutdown must exit 0 (WAL flush and compaction stop completed).
    #[cfg(unix)]
    if let Some(code) = exit_code {
        assert_eq!(
            code, 0,
            "graceful SIGTERM should exit 0 (WAL flush + compaction stop); stderr: {}",
            stderr
        );
    }
}

/// Adversarial: after graceful SIGTERM, a second start on the same data_dir must recover
/// and serve readyz 200. Asserts that shutdown left WAL/segment state consistent.
#[test]
fn graceful_shutdown_then_restart_recovery_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config_path = dir.path().join("ugnosd.toml");
    std::fs::write(
        &config_path,
        r#"
data_dir = "DATA_DIR_PLACEHOLDER"
flush_interval_secs = 1
enable_wal = true
enable_segments = true
http_bind = "BIND_PLACEHOLDER"
[segment_store]
compaction_check_interval_secs = 2
"#,
    )
    .expect("write config");
    let config_content = std::fs::read_to_string(&config_path).unwrap();
    let config_content = config_content
        .replace("DATA_DIR_PLACEHOLDER", data_dir.to_str().unwrap())
        .replace(
            "BIND_PLACEHOLDER",
            &format!("127.0.0.1:{}", HEALTH_PORT + 10),
        );
    std::fs::write(&config_path, config_content).expect("write config");

    let bind = format!("127.0.0.1:{}", HEALTH_PORT + 10);
    let mut child = start_ugnosd_background(
        &[
            "--config",
            config_path.to_str().unwrap(),
            "--http-bind",
            &bind,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let first_readyz = http_get("127.0.0.1", HEALTH_PORT + 10, "/readyz");
    assert!(
        first_readyz.is_some(),
        "first run: readyz must be reachable"
    );
    let (status1, _) = first_readyz.unwrap();
    assert!(
        status1.contains("200"),
        "first run: readyz must be 200, got {}",
        status1
    );

    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
    }
    #[cfg(not(unix))]
    let _ = child.kill();

    let (tx, rx) = std::sync::mpsc::channel();
    let stderr_handle = child.stderr.take();
    let _ = thread::spawn(move || {
        let _ = child.wait();
        let mut s = String::new();
        if let Some(mut h) = stderr_handle {
            let _ = h.read_to_string(&mut s);
        }
        let _ = tx.send(s);
    });
    let _ = rx.recv_timeout(Duration::from_millis(SHUTDOWN_WAIT_MS));

    // Second run: same data_dir, different port so first process has released the socket.
    let bind2 = format!("127.0.0.1:{}", HEALTH_PORT + 11);
    let config_path2 = dir.path().join("ugnosd2.toml");
    let config_content2 = std::fs::read_to_string(&config_path)
        .unwrap()
        .replace(&format!("127.0.0.1:{}", HEALTH_PORT + 10), &bind2);
    std::fs::write(&config_path2, config_content2).expect("write config2");

    let mut child2 = start_ugnosd_background(
        &[
            "--config",
            config_path2.to_str().unwrap(),
            "--http-bind",
            &bind2,
        ],
        &[],
    );
    thread::sleep(Duration::from_millis(STARTUP_WAIT_MS));
    let second_readyz = http_get("127.0.0.1", HEALTH_PORT + 11, "/readyz");
    let _ = child2.kill();
    let _ = child2.wait();
    assert!(
        second_readyz.is_some(),
        "second run after graceful shutdown must be reachable (recovery succeeded)"
    );
    let (status2, _) = second_readyz.unwrap();
    assert!(
        status2.contains("200"),
        "second run: readyz must be 200 after recovery; got {}",
        status2
    );
}
