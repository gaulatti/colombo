use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Runtime {
    child: Child,
    _temp: tempfile::TempDir,
}
impl Runtime {
    fn start(settings: &[(&str, &str)]) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_colombo"));
        command
            .current_dir(temp.path())
            .env_clear()
            .env(
                "DATABASE_URL",
                std::env::var("COLOMBO_TEST_DATABASE_URL").unwrap_or_else(|_| {
                    "postgres://colombo:colombo@127.0.0.1:55436/colombo".into()
                }),
            )
            .env("COLOMBO_FTP_ENABLED", "false")
            .env("COLOMBO_SPOOL_PATH", temp.path().join("spool"))
            .env("TMPDIR", temp.path())
            .env("PORT", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Ok(profile) = std::env::var("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        for (key, value) in settings {
            command.env(key, value);
        }
        Self {
            child: command.spawn().unwrap(),
            _temp: temp,
        }
    }
    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "runtime did not exit within deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn invalid_configuration_exits_without_starting_listeners() {
    for settings in [
        vec![("PORT", "invalid")],
        vec![("COLOMBO_FTP_PORT", "invalid")],
        vec![("COLOMBO_FTP_ENABLED", "maybe")],
        vec![("FLYWAY_ENABLED", "maybe")],
        vec![("COLOMBO_FTPS_CERTIFICATE_PATH", "missing")],
        vec![("COLOMBO_FTPS_PRIVATE_KEY_PATH", "missing")],
        vec![("COLOMBO_METRICS_TOKEN", "short")],
        vec![("COLOMBO_FTP_PASSIVE_PORTS", "bad-range")],
        vec![("DATABASE_URL", "invalid")],
        vec![
            ("COLOMBO_FTP_ENABLED", "true"),
            ("COLOMBO_FTPS_CERTIFICATE_PATH", "missing"),
            ("COLOMBO_FTPS_PRIVATE_KEY_PATH", "missing"),
        ],
        vec![("COLOMBO_SPOOL_PATH", ""), ("PORT", "invalid")],
        vec![
            ("COLOMBO_FTP_ENABLED", ""),
            ("colombo.ftp.enabled", "maybe"),
        ],
    ] {
        assert!(
            !Runtime::start(&settings).wait().success(),
            "accepted invalid config {settings:?}"
        );
    }
    let occupied = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    for ftp in ["false", "true"] {
        assert!(
            !Runtime::start(&[("PORT", &port), ("COLOMBO_FTP_ENABLED", ftp)])
                .wait()
                .success()
        );
    }
}

#[tokio::test]
async fn http_process_starts_serves_and_shuts_down_cleanly() {
    for ftp_mode in ["disabled", "plain", "tls"] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port().to_string();
        drop(listener);
        let certificate_dir = tempfile::tempdir().unwrap();
        let certificate = certificate_dir.path().join("cert.pem");
        let key = certificate_dir.path().join("key.pem");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ftp_port = listener.local_addr().unwrap().port().to_string();
        drop(listener);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let passive_port = listener.local_addr().unwrap().port().to_string();
        drop(listener);
        let passive_range = format!("{passive_port}-{passive_port}");
        let mut settings: Vec<(&str, &str)> = vec![
            ("PORT", &port),
            ("COLOMBO_METRICS_TOKEN", "long-local-metrics-token"),
            ("RUST_LOG", "info"),
        ];
        if ftp_mode != "disabled" {
            settings.extend([
                ("COLOMBO_FTP_ENABLED", "true"),
                ("COLOMBO_FTP_PORT", ftp_port.as_str()),
                ("COLOMBO_FTP_PASSIVE_PORTS", passive_range.as_str()),
            ]);
        }
        if ftp_mode == "tls" {
            assert!(
                Command::new("openssl")
                    .args([
                        "req",
                        "-x509",
                        "-newkey",
                        "rsa:2048",
                        "-nodes",
                        "-days",
                        "1",
                        "-subj",
                        "/CN=localhost",
                        "-keyout"
                    ])
                    .arg(&key)
                    .arg("-out")
                    .arg(&certificate)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .unwrap()
                    .success()
            );
            settings.extend([
                (
                    "COLOMBO_FTPS_CERTIFICATE_PATH",
                    certificate.to_str().unwrap(),
                ),
                ("COLOMBO_FTPS_PRIVATE_KEY_PATH", key.to_str().unwrap()),
                ("COLOMBO_FTP_PASSIVE_EXTERNAL_ADDRESS", "127.0.0.1"),
            ]);
        }
        let mut runtime = Runtime::start(&settings);
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/actuator/health"))
                .send()
                .await
            {
                assert!(response.status().is_success());
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(20));
        }
        if ftp_mode != "disabled" {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
            let socket = tokio::net::TcpStream::connect(format!("127.0.0.1:{ftp_port}"))
                .await
                .unwrap();
            let mut socket = tokio::io::BufReader::new(socket);
            let mut greeting = String::new();
            socket.read_line(&mut greeting).await.unwrap();
            assert!(greeting.starts_with("220"));
            socket.get_mut().write_all(b"QUIT\r\n").await.unwrap();
            let mut response = String::new();
            socket.read_line(&mut response).await.unwrap();
            assert!(response.starts_with("221"));
        }
        assert!(
            Command::new("/bin/kill")
                .args(["-INT", &runtime.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert!(runtime.wait().success());
    }
}

#[tokio::test]
async fn unavailable_temporary_storage_rejects_authenticated_uploads() {
    let database = std::env::var("COLOMBO_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://colombo:colombo@127.0.0.1:55436/colombo".into());
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mock = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().route("/validate",axum::routing::post(||async {axum::Json(serde_json::json!({"assignmentId":"runtime-fixture","upload":{"accessKeyId":"access","secretAccessKey":"secret","sessionToken":"token","region":"us-east-1","bucket":"uploads","keyPrefix":"runtime-fixture","expiresAt":"2099-01-01T00:00:00Z"}}))}))).await.unwrap()
    });
    sqlx::query("INSERT INTO tenants(name,ftp_username,api_key,validation_endpoint,photo_endpoint) VALUES ('Runtime fixture','runtime-fixture','fictional-key',$1,$1) ON CONFLICT (ftp_username) DO UPDATE SET validation_endpoint=EXCLUDED.validation_endpoint")
        .bind(format!("{url}/validate")).execute(&pool).await.unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port().to_string();
    drop(socket);
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing");
    let mut runtime = Runtime::start(&[("PORT", &port), ("TMPDIR", missing.to_str().unwrap())]);
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/actuator/health"))
            .send()
            .await
        {
            assert!(response.status().is_success());
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let response=client.post(format!("http://127.0.0.1:{port}/upload")).header("X-Colombo-Username","runtime-fixture").header("X-Colombo-Password","key")
        .header("content-type","multipart/form-data; boundary=test").body("--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\n\r\ncontent\r\n--test--\r\n").send().await.unwrap();
    assert_eq!(response.status(), 500);
    assert!(
        Command::new("/bin/kill")
            .args(["-INT", &runtime.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(runtime.wait().success());
    let mut accepted_runtime = Runtime::start(&[
        ("PORT", &port),
        ("AWS_ENDPOINT_URL_S3", &url),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_MAX_ATTEMPTS", "1"),
    ]);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/actuator/health"))
            .send()
            .await
        {
            assert!(response.status().is_success());
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let response=client.post(format!("http://127.0.0.1:{port}/upload")).header("X-Colombo-Username","runtime-fixture").header("X-Colombo-Password","key")
        .header("content-type","multipart/form-data; boundary=test").body("--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\n\r\ncontent\r\n--test--\r\n").send().await.unwrap();
    assert_eq!(response.status(), 202);
    let receipt: serde_json::Value = response.json().await.unwrap();
    assert_eq!(receipt["status"], "accepted");
    assert_eq!(receipt["assignment_id"], "runtime-fixture");
    assert!(uuid::Uuid::parse_str(receipt["operation_id"].as_str().unwrap()).is_ok());
    let rejected=client.post(format!("http://127.0.0.1:{port}/upload")).header("X-Colombo-Username","runtime-fixture").header("X-Colombo-Password","key")
        .header("content-type","multipart/form-data; boundary=test").body("--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\n\r\ncontent\r\n--test\r\ninvalid headers\r\n\r\nunclosed").send().await.unwrap();
    assert_eq!(rejected.status(), 400);
    assert!(
        std::fs::read_dir(accepted_runtime._temp.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("colombo-upload-"))
    );
    assert!(
        Command::new("/bin/kill")
            .args(["-INT", &accepted_runtime.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(accepted_runtime.wait().success());
    mock.abort();
    sqlx::query("DELETE FROM tenants WHERE ftp_username='runtime-fixture'")
        .execute(&pool)
        .await
        .unwrap();
}
