//! Implementation of the gRPC UgnosService that delegates to DbCore.

use crate::DbCore;
use crate::error::DbError;
use crate::grpc::ugnos::v1::{
    CompactRequest, CompactResponse, DataPointResult, FlushRequest, FlushResponse, QueryRequest,
    QueryResponse, StatusRequest, StatusResponse, WriteRequest, WriteResponse,
    ugnos_service_server::UgnosService as UgnosServiceTrait,
};
use crate::telemetry::db_metrics;
use crate::types::TagSet;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

/// Maps [`DbError`] to gRPC [`Status`].
///
/// Cardinality limit and backpressure-related errors map to `RESOURCE_EXHAUSTED`;
/// invalid arguments to `INVALID_ARGUMENT`; not found to `NOT_FOUND`;
/// internal/background errors to `INTERNAL`.
fn db_error_to_status(e: DbError) -> Status {
    match e {
        DbError::SeriesCardinalityLimitExceeded {
            scope,
            limit,
            current,
        } => Status::resource_exhausted(format!(
            "series cardinality limit exceeded: scope={}, current={}, limit={}",
            scope, current, limit
        )),
        DbError::SeriesNotFound(name) => Status::not_found(format!("series not found: {}", name)),
        DbError::InvalidTimeRange { start, end } => Status::invalid_argument(format!(
            "invalid time range: start={}, end={} (start must be < end)",
            start, end
        )),
        DbError::ConfigError(msg)
        | DbError::NotImplemented(msg)
        | DbError::BackgroundTaskError(msg)
        | DbError::Internal(msg)
        | DbError::LockError(msg)
        | DbError::Serialization(msg) => Status::internal(msg),
        DbError::Corruption { details, .. } => Status::data_loss(details),
        DbError::Io(e) => Status::internal(e.to_string()),
    }
}

/// Returns a static label suitable for metrics given a [`tonic::Code`].
fn grpc_code_label(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

/// Records gRPC metrics for a completed RPC and returns the result through.
fn record_and_return<T>(
    method: &str,
    started: Instant,
    result: Result<Response<T>, Status>,
) -> Result<Response<T>, Status> {
    let code = match &result {
        Ok(_) => "ok",
        Err(s) => grpc_code_label(s.code()),
    };
    db_metrics::record_grpc_request(method, started.elapsed(), code);
    result
}

/// Service implementation that holds a shared reference to the database core.
#[derive(Clone)]
pub struct UgnosServiceImpl {
    db: Arc<DbCore>,
}

impl UgnosServiceImpl {
    /// Creates a new service implementation backed by the given database.
    pub fn new(db: Arc<DbCore>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl UgnosServiceTrait for UgnosServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let mut points_written: u64 = 0;
        let mut first_error = String::new();

        for sw in req.series_writes {
            let series = sw.series;
            for pt in sw.points {
                let tags: TagSet = pt.tags.into_iter().collect();
                match self.db.insert(&series, pt.timestamp_ns, pt.value, tags) {
                    Ok(()) => points_written = points_written.saturating_add(1),
                    Err(DbError::SeriesCardinalityLimitExceeded {
                        scope,
                        limit,
                        current,
                    }) => {
                        // Backpressure: signal the client to back off immediately.
                        // Include points_written so the client knows how many succeeded before
                        // the limit was hit, enabling retry from that offset.
                        let status = Status::resource_exhausted(format!(
                            "series cardinality limit exceeded: scope={scope}, \
                             current={current}, limit={limit}; \
                             points_written={points_written}"
                        ));
                        return record_and_return("Write", started, Err(status));
                    }
                    Err(e) => {
                        if first_error.is_empty() {
                            first_error = e.to_string();
                        }
                    }
                }
            }
        }

        record_and_return(
            "Write",
            started,
            Ok(Response::new(WriteResponse {
                points_written,
                first_error,
            })),
        )
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        if req.start_ns >= req.end_ns {
            return record_and_return(
                "Query",
                started,
                Err(Status::invalid_argument(
                    "query: start_ns must be less than end_ns",
                )),
            );
        }

        let tag_filter: Option<TagSet> = if req.tag_filter.is_empty() {
            None
        } else {
            Some(req.tag_filter.into_iter().collect())
        };

        let range = req.start_ns..req.end_ns;
        let points = match self.db.query(&req.series, range, tag_filter.as_ref()) {
            Ok(pts) => pts,
            Err(e) => {
                return record_and_return("Query", started, Err(db_error_to_status(e)));
            }
        };

        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };
        let points: Vec<_> = points
            .into_iter()
            .take(limit)
            .map(|(timestamp_ns, value)| DataPointResult {
                timestamp_ns,
                value,
            })
            .collect();

        record_and_return(
            "Query",
            started,
            Ok(Response::new(QueryResponse { points })),
        )
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let started = Instant::now();
        record_and_return(
            "Flush",
            started,
            self.db
                .flush()
                .map(|()| Response::new(FlushResponse {}))
                .map_err(db_error_to_status),
        )
    }

    async fn compact(
        &self,
        _request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        let started = Instant::now();
        record_and_return(
            "Compact",
            started,
            self.db
                .compact()
                .map(|()| Response::new(CompactResponse {}))
                .map_err(db_error_to_status),
        )
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let started = Instant::now();
        let config = self.db.get_config();
        // Segment count is not exposed on DbCore; report 0 when segments are enabled.
        let segment_count: u32 = if config.enable_segments { 0 } else { 0 };

        record_and_return(
            "Status",
            started,
            Ok(Response::new(StatusResponse {
                data_dir: config.data_dir.to_string_lossy().to_string(),
                segments_enabled: config.enable_segments,
                wal_enabled: config.enable_wal,
                snapshots_enabled: config.enable_snapshots,
                segment_count,
            })),
        )
    }
}
