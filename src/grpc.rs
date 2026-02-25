//! gRPC (Tonic) API for ingest, query, and administration.
//!
//! This module includes the generated protobuf types and service trait, and provides
//! an implementation that delegates to [`crate::DbCore`]. Errors are mapped to gRPC
//! status codes (e.g. `RESOURCE_EXHAUSTED` for cardinality limit).
//! AuthN/AuthZ is enforced deny-by-default via [`auth`].

mod auth;
mod service;

pub use auth::{GrpcAuthConfig, GrpcAuthKey, GrpcAuthLayer, GrpcPermission};

pub use service::UgnosServiceImpl;

/// Generated protobuf and service stubs (from `proto/ugnos/v1/ugnos.proto`).
pub mod ugnos {
    pub mod v1 {
        #![allow(clippy::all)]
        include!(concat!(env!("OUT_DIR"), "/ugnos.v1.rs"));
    }
}

pub use ugnos::v1::ugnos_service_client::UgnosServiceClient;
pub use ugnos::v1::ugnos_service_server::{UgnosService, UgnosServiceServer};
