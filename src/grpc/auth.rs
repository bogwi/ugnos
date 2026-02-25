//! gRPC AuthN/AuthZ: deny-by-default enforcement for the server API surface.
//!
//! Authenticates requests via API key (metadata: `authorization: Bearer <token>` or
//! `x-api-key: <token>`). Authorizes by mapping each RPC path to a permission and
//! checking the principal's allowed set. No valid config or no matching key ⇒ deny.

use constant_time_eq::constant_time_eq;
use std::collections::HashSet;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

/// Permission required to call an RPC. Deny-by-default: only explicitly granted
/// permissions allow access.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GrpcPermission {
    /// Ingest: `Write` RPC.
    Ingest,
    /// Read: `Query` RPC.
    Query,
    /// Administration: `Flush`, `Compact`, `Status` RPCs.
    Admin,
}

impl GrpcPermission {
    /// Returns the permission required for the given gRPC path (e.g. `/ugnos.v1.UgnosService/Write`).
    /// Returns `None` for unknown paths (caller should deny).
    pub fn required_for_path(path: &str) -> Option<Self> {
        if path.ends_with("/Write") {
            Some(GrpcPermission::Ingest)
        } else if path.ends_with("/Query") {
            Some(GrpcPermission::Query)
        } else if path.ends_with("/Flush")
            || path.ends_with("/Compact")
            || path.ends_with("/Status")
        {
            Some(GrpcPermission::Admin)
        } else {
            None
        }
    }
}

/// Single principal: token (secret) and set of allowed permissions.
#[derive(Clone, Debug)]
pub struct GrpcAuthKey {
    /// Raw token; compare with constant_time_eq. Config file should be restricted (e.g. chmod 600).
    pub token: Vec<u8>,
    pub permissions: HashSet<GrpcPermission>,
}

impl GrpcAuthKey {
    /// Checks whether this key's token matches the given slice (constant-time) and has the permission.
    #[inline]
    pub fn matches_and_has(&self, token: &[u8], permission: GrpcPermission) -> bool {
        constant_time_eq(&self.token, token) && self.permissions.contains(&permission)
    }
}

/// Auth config: list of keys. Empty list ⇒ no valid principals ⇒ deny all (deny-by-default).
#[derive(Clone, Debug, Default)]
pub struct GrpcAuthConfig {
    pub keys: Vec<GrpcAuthKey>,
}

impl GrpcAuthConfig {
    /// Resolves token to a permission check. Returns true only if some key matches the token
    /// and has the required permission. Constant-time for token comparison; no early exit.
    pub fn check(&self, token: &[u8], required: GrpcPermission) -> bool {
        self.keys.iter().any(|k| k.matches_and_has(token, required))
    }

    /// Returns (any_key_matched, has_required_permission). Used to return 401 vs 403 without
    /// short-circuiting (iterates all keys so timing does not leak match position).
    fn auth_result(&self, token: &[u8], required: GrpcPermission) -> (bool, bool) {
        let mut matched = false;
        let mut has_perm = false;
        for k in &self.keys {
            if constant_time_eq(&k.token, token) {
                matched = true;
                if k.permissions.contains(&required) {
                    has_perm = true;
                }
            }
        }
        (matched, has_perm)
    }
}

/// Extracts the API token from gRPC metadata (headers). Supports:
/// - `authorization: Bearer <token>`
/// - `x-api-key: <token>`
///   Returns None if missing or invalid format.
fn extract_token_from_headers(headers: &http::HeaderMap) -> Option<Vec<u8>> {
    if let Some(v) = headers.get("x-api-key") {
        if let Ok(s) = v.to_str() {
            return Some(s.as_bytes().to_vec());
        }
    }
    if let Some(v) = headers.get("authorization") {
        if let Ok(s) = v.to_str() {
            let s = s.trim();
            if s.starts_with("Bearer ") && s.len() > 7 {
                return Some(s[7..].trim().as_bytes().to_vec());
            }
        }
    }
    None
}

/// Tower layer that enforces AuthN/AuthZ for gRPC. Deny-by-default: no token or no permission ⇒ deny.
/// Inner service must use `tonic::body::Body` as response body so error responses can be constructed.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    config: Arc<GrpcAuthConfig>,
}

impl GrpcAuthLayer {
    pub fn new(config: GrpcAuthConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl<S> Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuthService {
            inner,
            config: Arc::clone(&self.config),
        }
    }
}

/// Service that wraps the gRPC router and enforces auth before forwarding.
#[derive(Clone)]
pub struct GrpcAuthService<S> {
    inner: S,
    config: Arc<GrpcAuthConfig>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for GrpcAuthService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path();
        let required = match GrpcPermission::required_for_path(path) {
            Some(p) => p,
            None => {
                return Box::pin(async move {
                    Ok(grpc_error_response(
                        http::StatusCode::FORBIDDEN,
                        "unknown or unsupported RPC path",
                    ))
                });
            }
        };

        let token = match extract_token_from_headers(req.headers()) {
            Some(t) => t,
            None => {
                return Box::pin(async move {
                    Ok(grpc_error_response(
                        http::StatusCode::UNAUTHORIZED,
                        "missing or invalid authorization",
                    ))
                });
            }
        };

        let config = Arc::clone(&self.config);
        let (matched, has_perm) = config.auth_result(&token, required);

        if !matched {
            return Box::pin(async move {
                Ok(grpc_error_response(
                    http::StatusCode::UNAUTHORIZED,
                    "invalid or unknown token",
                ))
            });
        }
        if !has_perm {
            return Box::pin(async move {
                Ok(grpc_error_response(
                    http::StatusCode::FORBIDDEN,
                    "forbidden: token missing required permission",
                ))
            });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

/// Builds an HTTP response suitable for gRPC clients: status code and empty body.
/// gRPC-over-HTTP/2 maps 401 → UNAUTHENTICATED, 403 → FORBIDDEN (PERMISSION_DENIED).
fn grpc_error_response(
    status: http::StatusCode,
    _message: &str,
) -> http::Response<tonic::body::Body> {
    http::Response::builder()
        .status(status)
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::empty())
        .expect("response build")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_required_for_path_covers_all_rpcs() {
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Write"),
            Some(GrpcPermission::Ingest)
        );
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Query"),
            Some(GrpcPermission::Query)
        );
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Flush"),
            Some(GrpcPermission::Admin)
        );
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Compact"),
            Some(GrpcPermission::Admin)
        );
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Status"),
            Some(GrpcPermission::Admin)
        );
    }

    #[test]
    fn permission_required_for_unknown_path_returns_none() {
        assert_eq!(GrpcPermission::required_for_path("/unknown/Bar"), None);
        assert_eq!(
            GrpcPermission::required_for_path("/ugnos.v1.UgnosService/Other"),
            None
        );
    }

    #[test]
    fn auth_config_check_requires_matching_token_and_permission() {
        let mut perms = HashSet::new();
        perms.insert(GrpcPermission::Query);
        let config = GrpcAuthConfig {
            keys: vec![GrpcAuthKey {
                token: b"secret".to_vec(),
                permissions: perms,
            }],
        };
        assert!(config.check(b"secret", GrpcPermission::Query));
        assert!(!config.check(b"secret", GrpcPermission::Ingest));
        assert!(!config.check(b"wrong", GrpcPermission::Query));
        assert!(!config.check(b"", GrpcPermission::Query));
    }

    #[test]
    fn auth_config_empty_denies_all() {
        let config = GrpcAuthConfig::default();
        assert!(!config.check(b"any", GrpcPermission::Admin));
    }
}
