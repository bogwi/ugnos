//! End-to-end integration tests for the gRPC API: ingest → persist → restart → query.
//!
//! Validates that data written via gRPC Write is durable across a restart (recovery) and
//! queryable via gRPC Query. Includes AuthN/AuthZ tests (deny-by-default, 401/403, permission scoping).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ugnos::grpc::{
    GrpcAuthConfig, GrpcAuthKey, GrpcAuthLayer, GrpcPermission, UgnosServiceClient,
    UgnosServiceImpl, UgnosServiceServer,
};
use ugnos::{DbConfig, DbCore};

fn ns_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// In-process gRPC server on the given address (no auth). Returns a guard that keeps the server running.
async fn run_grpc_server(
    db: Arc<DbCore>,
    addr: std::net::SocketAddr,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    let svc = UgnosServiceServer::new(UgnosServiceImpl::new(db));
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("gRPC server");
    });
    Ok(handle)
}

/// In-process gRPC server with AuthN/AuthZ layer. Deny-by-default: empty config ⇒ all requests denied.
async fn run_grpc_server_with_auth(
    db: Arc<DbCore>,
    addr: std::net::SocketAddr,
    auth_config: GrpcAuthConfig,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    let svc = UgnosServiceServer::new(UgnosServiceImpl::new(db));
    let layer = GrpcAuthLayer::new(auth_config);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(layer)
            .serve(addr, svc)
            .await
            .expect("gRPC server with auth");
    });
    Ok(handle)
}

fn auth_config_full_access(token: &str) -> GrpcAuthConfig {
    let perms: HashSet<GrpcPermission> = [
        GrpcPermission::Ingest,
        GrpcPermission::Query,
        GrpcPermission::Admin,
    ]
    .into_iter()
    .collect();
    GrpcAuthConfig {
        keys: vec![GrpcAuthKey {
            token: token.as_bytes().to_vec(),
            permissions: perms,
        }],
    }
}

fn auth_config_query_only(token: &str) -> GrpcAuthConfig {
    GrpcAuthConfig {
        keys: vec![GrpcAuthKey {
            token: token.as_bytes().to_vec(),
            permissions: [GrpcPermission::Query].into_iter().collect(),
        }],
    }
}

#[tokio::test]
async fn ingest_persist_restart_query_via_grpc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir: data_dir.clone(),
        flush_interval: Duration::from_millis(50),
        enable_wal: true,
        enable_snapshots: false,
        enable_segments: true,
        ..DbConfig::default()
    };

    // --- First run: create DB, recover (empty), start gRPC server ---
    let mut db = DbCore::with_config(config.clone()).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let grpc_addr: std::net::SocketAddr = "127.0.0.1:19500".parse().expect("grpc test addr");
    let _guard = run_grpc_server(Arc::clone(&db), grpc_addr)
        .await
        .expect("start gRPC server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", grpc_addr))
        .await
        .expect("connect client");

    // --- Ingest via gRPC Write ---
    let t0 = ns_since_epoch();
    let t1 = t0 + 1_000_000;
    let t2 = t0 + 2_000_000;
    let write_req = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "metrics.cpu".to_string(),
            points: vec![
                ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: t0,
                    value: 10.0,
                    tags: [("host".to_string(), "node1".to_string())]
                        .into_iter()
                        .collect(),
                },
                ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: t1,
                    value: 20.0,
                    tags: [("host".to_string(), "node1".to_string())]
                        .into_iter()
                        .collect(),
                },
                ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: t2,
                    value: 30.0,
                    tags: [("host".to_string(), "node1".to_string())]
                        .into_iter()
                        .collect(),
                },
            ],
        }],
    };
    let write_resp = client
        .write(tonic::Request::new(write_req))
        .await
        .expect("write rpc")
        .into_inner();
    assert_eq!(write_resp.points_written, 3, "all points should be written");
    assert!(write_resp.first_error.is_empty(), "no error expected");

    // --- Flush so data is persisted ---
    client
        .flush(tonic::Request::new(ugnos::grpc::ugnos::v1::FlushRequest {}))
        .await
        .expect("flush rpc");

    // Allow flush to complete and segments to be written
    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(client);
    drop(_guard);
    drop(db);

    // --- Restart: new DbCore with same data_dir, recover ---
    let mut db2 = DbCore::with_config(config).expect("open db again");
    db2.recover().expect("recover after restart");
    let db2 = Arc::new(db2);

    let grpc_addr2: std::net::SocketAddr = "127.0.0.1:19501".parse().expect("grpc test addr 2");
    let _guard2 = run_grpc_server(Arc::clone(&db2), grpc_addr2)
        .await
        .expect("start gRPC server again");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client2 = UgnosServiceClient::connect(format!("http://{}", grpc_addr2))
        .await
        .expect("connect client again");

    // --- Query via gRPC and assert data is present ---
    let query_req = ugnos::grpc::ugnos::v1::QueryRequest {
        series: "metrics.cpu".to_string(),
        start_ns: t0,
        end_ns: t2 + 1,
        tag_filter: [("host".to_string(), "node1".to_string())]
            .into_iter()
            .collect(),
        limit: 0,
    };
    let query_resp = client2
        .query(tonic::Request::new(query_req))
        .await
        .expect("query rpc")
        .into_inner();

    assert_eq!(
        query_resp.points.len(),
        3,
        "should return 3 points after restart"
    );
    let values: Vec<f64> = query_resp.points.iter().map(|p| p.value).collect();
    assert_eq!(values, [10.0, 20.0, 30.0], "values must match ingested");
}

/// Invalid time range (start_ns >= end_ns) returns gRPC INVALID_ARGUMENT.
#[tokio::test]
async fn query_invalid_time_range_returns_invalid_argument() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let addr: std::net::SocketAddr = "127.0.0.1:19502".parse().expect("addr");
    let _guard = run_grpc_server(Arc::clone(&db), addr)
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let req = ugnos::grpc::ugnos::v1::QueryRequest {
        series: "any".to_string(),
        start_ns: 100,
        end_ns: 100,
        tag_filter: std::collections::HashMap::new(),
        limit: 0,
    };
    let err = client.query(tonic::Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Query for non-existent series returns gRPC NOT_FOUND.
#[tokio::test]
async fn query_nonexistent_series_returns_not_found() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let addr: std::net::SocketAddr = "127.0.0.1:19503".parse().expect("addr");
    let _guard = run_grpc_server(Arc::clone(&db), addr)
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let req = ugnos::grpc::ugnos::v1::QueryRequest {
        series: "nonexistent.series".to_string(),
        start_ns: 0,
        end_ns: 1_000_000,
        tag_filter: std::collections::HashMap::new(),
        limit: 0,
    };
    let err = client.query(tonic::Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

// ---------- AuthN/AuthZ integration tests (deny-by-default, 401/403, permission scoping) ----------

fn request_with_api_key<T>(inner: T, key: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(inner);
    req.metadata_mut()
        .insert("x-api-key", key.parse().expect("ascii key"));
    req
}

/// No token → UNAUTHENTICATED (401).
#[tokio::test]
async fn grpc_auth_no_token_returns_unauthenticated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19510".parse().expect("addr");
    let _guard =
        run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_full_access("secret"))
            .await
            .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let req = ugnos::grpc::ugnos::v1::StatusRequest {};
    let err = client.status(tonic::Request::new(req)).await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "no token must yield 401"
    );
}

/// Wrong token → UNAUTHENTICATED (401).
#[tokio::test]
async fn grpc_auth_wrong_token_returns_unauthenticated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19511".parse().expect("addr");
    let _guard =
        run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_full_access("correct"))
            .await
            .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let req = request_with_api_key(ugnos::grpc::ugnos::v1::StatusRequest {}, "wrong");
    let err = client.status(req).await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "wrong token must yield 401"
    );
}

/// Valid token but missing permission for RPC → PERMISSION_DENIED (403).
#[tokio::test]
async fn grpc_auth_valid_token_missing_permission_returns_permission_denied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19512".parse().expect("addr");
    let _guard =
        run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_query_only("readonly"))
            .await
            .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    // Write requires Ingest; key has only Query → 403.
    let write_req = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "x".to_string(),
            points: vec![],
        }],
    };
    let err = client
        .write(request_with_api_key(write_req, "readonly"))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::PermissionDenied,
        "token without ingest must yield 403"
    );
}

/// Valid token with required permission → success. Covers Write (ingest), Query, Status (admin).
#[tokio::test]
async fn grpc_auth_valid_token_with_permission_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19513".parse().expect("addr");
    let _guard = run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_full_access("full"))
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let status_req = request_with_api_key(ugnos::grpc::ugnos::v1::StatusRequest {}, "full");
    let _ = client
        .status(status_req)
        .await
        .expect("status with valid key");
    let write_req = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "test".to_string(),
            points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                timestamp_ns: 1000,
                value: 1.0,
                tags: std::collections::HashMap::new(),
            }],
        }],
    };
    let _ = client
        .write(request_with_api_key(write_req, "full"))
        .await
        .expect("write with valid key");
    let _ = client
        .flush(request_with_api_key(
            ugnos::grpc::ugnos::v1::FlushRequest {},
            "full",
        ))
        .await
        .expect("flush with valid key");
    let query_req = ugnos::grpc::ugnos::v1::QueryRequest {
        series: "test".to_string(),
        start_ns: 0,
        end_ns: 2000,
        tag_filter: std::collections::HashMap::new(),
        limit: 10,
    };
    let _ = client
        .query(request_with_api_key(query_req, "full"))
        .await
        .expect("query with valid key");
}

/// Empty auth config (no keys) ⇒ all requests denied (deny-by-default).
#[tokio::test]
async fn grpc_auth_empty_config_denies_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19514".parse().expect("addr");
    let _guard = run_grpc_server_with_auth(Arc::clone(&db), addr, GrpcAuthConfig::default())
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let req = request_with_api_key(ugnos::grpc::ugnos::v1::StatusRequest {}, "any");
    let err = client.status(req).await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "empty config must deny all"
    );
}

/// Compact RPC with valid admin token succeeds through the auth layer.
#[tokio::test]
async fn grpc_auth_compact_with_valid_token_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19516".parse().expect("addr");
    let _guard =
        run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_full_access("admin-key"))
            .await
            .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let _ = client
        .compact(request_with_api_key(
            ugnos::grpc::ugnos::v1::CompactRequest {},
            "admin-key",
        ))
        .await
        .expect("compact with valid admin key must succeed");
}

/// Compact RPC with query-only token is denied (403).
#[tokio::test]
async fn grpc_auth_compact_without_admin_denied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19517".parse().expect("addr");
    let _guard =
        run_grpc_server_with_auth(Arc::clone(&db), addr, auth_config_query_only("readonly"))
            .await
            .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let err = client
        .compact(request_with_api_key(
            ugnos::grpc::ugnos::v1::CompactRequest {},
            "readonly",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::PermissionDenied,
        "compact without admin permission must be denied"
    );
}

/// Bearer token in Authorization header is accepted.
#[tokio::test]
async fn grpc_auth_bearer_token_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");
    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);
    let addr: std::net::SocketAddr = "127.0.0.1:19515".parse().expect("addr");
    let _guard = run_grpc_server_with_auth(
        Arc::clone(&db),
        addr,
        auth_config_full_access("bearer-token"),
    )
    .await
    .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");
    let mut req = tonic::Request::new(ugnos::grpc::ugnos::v1::StatusRequest {});
    req.metadata_mut().insert(
        "authorization",
        "Bearer bearer-token".parse().expect("ascii"),
    );
    let _ = client
        .status(req)
        .await
        .expect("Bearer token must be accepted");
}

// ---------- Backpressure / RESOURCE_EXHAUSTED integration tests (AC2) ----------

/// Write RPC returns RESOURCE_EXHAUSTED when series cardinality limit is exceeded.
/// Verifies the end-to-end backpressure path: gRPC Write → DbCore insert → cardinality check
/// → SeriesCardinalityLimitExceeded → RESOURCE_EXHAUSTED at the wire level.
#[tokio::test]
async fn grpc_write_cardinality_limit_returns_resource_exhausted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        max_series_cardinality: Some(2),
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let addr: std::net::SocketAddr = "127.0.0.1:19520".parse().expect("addr");
    let _guard = run_grpc_server(Arc::clone(&db), addr)
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");

    let ts = ns_since_epoch();

    // Write 2 distinct series: should succeed (at the limit).
    let write_req = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![
            ugnos::grpc::ugnos::v1::SeriesWrite {
                series: "series_a".to_string(),
                points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: ts,
                    value: 1.0,
                    tags: std::collections::HashMap::new(),
                }],
            },
            ugnos::grpc::ugnos::v1::SeriesWrite {
                series: "series_b".to_string(),
                points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: ts,
                    value: 2.0,
                    tags: std::collections::HashMap::new(),
                }],
            },
        ],
    };
    let resp = client
        .write(tonic::Request::new(write_req))
        .await
        .expect("first 2 series must succeed");
    assert_eq!(resp.into_inner().points_written, 2);

    // Write a 3rd distinct series: must return RESOURCE_EXHAUSTED.
    let write_req_overflow = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "series_c".to_string(),
            points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                timestamp_ns: ts,
                value: 3.0,
                tags: std::collections::HashMap::new(),
            }],
        }],
    };
    let err = client
        .write(tonic::Request::new(write_req_overflow))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "exceeding cardinality limit must return RESOURCE_EXHAUSTED"
    );
    assert!(
        err.message().contains("cardinality"),
        "error message must mention cardinality: {}",
        err.message()
    );
}

/// Batch write where cardinality limit is hit mid-batch returns RESOURCE_EXHAUSTED and reports
/// points_written in the error message. Verifies partial-progress visibility under backpressure.
#[tokio::test]
async fn grpc_write_cardinality_mid_batch_reports_progress() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        max_series_cardinality: Some(1),
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let addr: std::net::SocketAddr = "127.0.0.1:19521".parse().expect("addr");
    let _guard = run_grpc_server(Arc::clone(&db), addr)
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");

    let ts = ns_since_epoch();

    // Batch: series_a (should succeed), then series_b (should hit cardinality limit).
    let write_req = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![
            ugnos::grpc::ugnos::v1::SeriesWrite {
                series: "series_a".to_string(),
                points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: ts,
                    value: 1.0,
                    tags: std::collections::HashMap::new(),
                }],
            },
            ugnos::grpc::ugnos::v1::SeriesWrite {
                series: "series_b".to_string(),
                points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                    timestamp_ns: ts,
                    value: 2.0,
                    tags: std::collections::HashMap::new(),
                }],
            },
        ],
    };
    let err = client
        .write(tonic::Request::new(write_req))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(
        err.message().contains("points_written=1"),
        "error must report 1 point written before limit: {}",
        err.message()
    );
}

/// Write to an existing series (same cardinality key) after hitting the limit still succeeds.
/// Backpressure only applies to NEW series, not additional points on existing ones.
#[tokio::test]
async fn grpc_write_existing_series_after_limit_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data_dir");

    let config = DbConfig {
        data_dir,
        enable_segments: true,
        enable_wal: false,
        max_series_cardinality: Some(1),
        ..DbConfig::default()
    };
    let mut db = DbCore::with_config(config).expect("open db");
    db.recover().expect("recover");
    let db = Arc::new(db);

    let addr: std::net::SocketAddr = "127.0.0.1:19522".parse().expect("addr");
    let _guard = run_grpc_server(Arc::clone(&db), addr)
        .await
        .expect("start server");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = UgnosServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect");

    let ts = ns_since_epoch();

    // First write: creates series_a (at limit).
    let req1 = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "series_a".to_string(),
            points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                timestamp_ns: ts,
                value: 1.0,
                tags: std::collections::HashMap::new(),
            }],
        }],
    };
    client
        .write(tonic::Request::new(req1))
        .await
        .expect("first write must succeed");

    // Second write: still series_a (same cardinality key), should succeed despite being at limit.
    let req2 = ugnos::grpc::ugnos::v1::WriteRequest {
        series_writes: vec![ugnos::grpc::ugnos::v1::SeriesWrite {
            series: "series_a".to_string(),
            points: vec![ugnos::grpc::ugnos::v1::DataPoint {
                timestamp_ns: ts + 1_000_000,
                value: 2.0,
                tags: std::collections::HashMap::new(),
            }],
        }],
    };
    let resp = client
        .write(tonic::Request::new(req2))
        .await
        .expect("write to existing series must succeed even at cardinality limit");
    assert_eq!(resp.into_inner().points_written, 1);
}
