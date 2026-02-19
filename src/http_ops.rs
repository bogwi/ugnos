//! HTTP ops endpoints for liveness and readiness (e.g. `GET /healthz`, `GET /readyz`).
//!
//! Designed for use behind hyper: the handler is pure and testable; the daemon builds
//! the HTTP response and sets readiness to `false` during shutdown so probes see 503.

use std::sync::atomic::{AtomicBool, Ordering};

use http::StatusCode;

/// Shared state for the ops HTTP handler.
///
/// Readiness is typically set to `true` after the database has been opened and recovery
/// has completed, and set to `false` when shutdown is initiated so orchestrators
/// (e.g. Kubernetes) stop sending traffic before the process exits.
#[derive(Debug)]
pub struct OpsState {
    /// When `true`, the service is ready to accept traffic (e.g. DB recovered, gRPC listening).
    /// When `false`, `/readyz` returns 503.
    ready: AtomicBool,
}

impl OpsState {
    /// Creates state with the given initial readiness.
    pub fn new(ready: bool) -> Self {
        Self {
            ready: AtomicBool::new(ready),
        }
    }

    /// Returns whether the service is currently ready.
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Sets readiness. Call with `false` when initiating graceful shutdown.
    #[inline]
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

/// Result of dispatching an ops request: status code and response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpsResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// Response body (plain text).
    pub body: &'static str,
}

/// Handles a single ops request given method, path, and current readiness.
///
/// **Liveness** (`GET /healthz`): Returns 200 if the process can respond. No dependency
/// on DB or gRPC; used to decide whether to restart the container.
///
/// **Readiness** (`GET /readyz`): Returns 200 only when `ready` is true (e.g. DB opened
/// and recovered). Returns 503 otherwise so load balancers remove the instance from
/// rotation.
///
/// **Method**: Only `GET` and `HEAD` are allowed on `/healthz` and `/readyz`; other
/// methods receive 405. Other paths receive 404.
///
/// This function is pure and does not perform I/O, so it is easy to unit-test.
#[must_use]
pub fn handle_ops_request(method: &http::Method, path: &str, ready: bool) -> OpsResponse {
    // Normalize: "/healthz" and "healthz" both match; trailing slash stripped.
    let path = path.trim_matches('/');

    let is_get = method == http::Method::GET;
    let is_head = method == http::Method::HEAD;
    let allowed = is_get || is_head;

    match path {
        "" => OpsResponse {
            status: StatusCode::NOT_FOUND,
            body: "not found",
        },
        "healthz" => {
            if allowed {
                OpsResponse {
                    status: StatusCode::OK,
                    body: if is_head { "" } else { "ok" },
                }
            } else {
                OpsResponse {
                    status: StatusCode::METHOD_NOT_ALLOWED,
                    body: "Method Not Allowed",
                }
            }
        }
        "readyz" => {
            if !allowed {
                return OpsResponse {
                    status: StatusCode::METHOD_NOT_ALLOWED,
                    body: "Method Not Allowed",
                };
            }
            if ready {
                OpsResponse {
                    status: StatusCode::OK,
                    body: if is_head { "" } else { "ok" },
                }
            } else {
                OpsResponse {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    body: if is_head { "" } else { "not ready" },
                }
            }
        }
        _ => OpsResponse {
            status: StatusCode::NOT_FOUND,
            body: "not found",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_ops_request, OpsResponse, OpsState};
    use http::Method;
    use http::StatusCode;

    fn get(path: &str, ready: bool) -> OpsResponse {
        handle_ops_request(&Method::GET, path, ready)
    }

    fn post(path: &str, ready: bool) -> OpsResponse {
        handle_ops_request(&Method::POST, path, ready)
    }

    fn head(path: &str, ready: bool) -> OpsResponse {
        handle_ops_request(&Method::HEAD, path, ready)
    }

    #[test]
    fn healthz_get_returns_200() {
        let r = get("/healthz", false);
        assert_eq!(r.status, StatusCode::OK);
        assert_eq!(r.body, "ok");
    }

    #[test]
    fn healthz_get_ignores_ready() {
        let r = get("/healthz", true);
        assert_eq!(r.status, StatusCode::OK);
        let r2 = get("/healthz", false);
        assert_eq!(r2.status, StatusCode::OK);
    }

    #[test]
    fn healthz_head_returns_200_empty_body() {
        let r = head("/healthz", false);
        assert_eq!(r.status, StatusCode::OK);
        assert_eq!(r.body, "");
    }

    #[test]
    fn readyz_get_returns_200_when_ready() {
        let r = get("/readyz", true);
        assert_eq!(r.status, StatusCode::OK);
        assert_eq!(r.body, "ok");
    }

    #[test]
    fn readyz_get_returns_503_when_not_ready() {
        let r = get("/readyz", false);
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.body, "not ready");
    }

    #[test]
    fn readyz_head_returns_503_when_not_ready() {
        let r = head("/readyz", false);
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.body, "");
    }

    #[test]
    fn post_healthz_returns_405() {
        let r = post("/healthz", true);
        assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(r.body, "Method Not Allowed");
    }

    #[test]
    fn post_readyz_returns_405() {
        let r = post("/readyz", true);
        assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test]
    fn unknown_path_returns_404() {
        let r = get("/nonexistent", true);
        assert_eq!(r.status, StatusCode::NOT_FOUND);
        assert_eq!(r.body, "not found");
    }

    #[test]
    fn path_with_leading_trailing_slash_normalized() {
        let r = get("healthz", true);
        assert_eq!(r.status, StatusCode::OK);
        let r2 = get("/readyz/", true);
        assert_eq!(r2.status, StatusCode::OK);
        let r3 = get("/healthz", true);
        assert_eq!(r3.status, StatusCode::OK);
    }

    #[test]
    fn root_path_returns_404() {
        let r = get("/", true);
        assert_eq!(r.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn ops_state_default_ready_toggle() {
        let state = OpsState::new(true);
        assert!(state.is_ready());
        state.set_ready(false);
        assert!(!state.is_ready());
        state.set_ready(true);
        assert!(state.is_ready());
    }
}
