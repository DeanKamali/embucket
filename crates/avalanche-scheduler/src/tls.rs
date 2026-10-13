//! TLS serving for the scheduler (M7.2, design §13.2).
//!
//! Ballista's stock `start_grpc_service_with_listener` assembles the
//! scheduler's axum router (gRPC + embedded Flight proxy + REST + health)
//! and serves it plaintext over a `TcpListener`; the assembly internals are
//! not exported. When `AVALANCHE_TLS_DIR` is set we reproduce that assembly
//! here (using only public ballista APIs) and serve it over a rustls
//! listener with *required* client certificates — mutual TLS end to end.
//!
//! Client-side TLS for the scheduler's own outbound connections (Flight
//! proxy fetches from executors) is configured on `SchedulerConfig` by the
//! binary before the cluster is created.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::connect_info::Connected;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::serve::{Listener, ListenerExt};
use axum::{Router, http};
use ballista_core::flight_proxy_service::BallistaFlightProxyService;
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::BALLISTA_VERSION;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::create_scheduler;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::DATAFUSION_VERSION;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tonic::service::RoutesBuilder;
use tower_http::set_header::SetResponseHeaderLayer;

use avalanche_executor_common::TlsMaterial;

/// A TCP listener that upgrades every accepted connection to TLS (mutual:
/// client certificates are required by the `ServerConfig`).
struct TlsListener {
    inner: tokio::net::TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    fn new(inner: tokio::net::TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            inner,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((tcp, peer)) => match self.acceptor.accept(tcp).await {
                    Ok(stream) => return (stream, peer),
                    Err(e) => {
                        tracing::warn!("tls handshake from {peer} failed: {e}");
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!("tls accept failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// Same server/x-powered-by response headers the stock scheduler adds.
fn with_version_headers(router: Router) -> Router {
    let server_value = HeaderValue::from_str(&format!("ballista/{BALLISTA_VERSION}"))
        .expect("BALLISTA_VERSION should be a valid header value");
    let datafusion_value = HeaderValue::from_str(&format!("datafusion/{DATAFUSION_VERSION}"))
        .expect("DATAFUSION_VERSION should be a valid header value");

    router
        .layer(SetResponseHeaderLayer::overriding(
            http::header::SERVER,
            server_value,
        ))
        .layer(SetResponseHeaderLayer::overriding(
            http::HeaderName::from_static("x-powered-by"),
            datafusion_value,
        ))
}

async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

async fn readyz(
    State(scheduler): State<Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>>,
) -> Response {
    if scheduler.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: no executors registered\n",
        )
            .into_response()
    }
}

fn health_routes(scheduler: Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(scheduler)
}

/// Start the scheduler with the full axum assembly (gRPC + embedded Flight
/// proxy + REST + health) served over mutual TLS.
///
/// # Errors
///
/// Errors on cluster creation, binding, or TLS setup failure.
pub async fn start_server_tls(
    cluster: BallistaCluster,
    address: SocketAddr,
    config: Arc<SchedulerConfig>,
    tls: &TlsMaterial,
) -> BallistaResult<()> {
    config.validate()?;
    let scheduler = Arc::new(create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, config).await?);

    let runtime_config = scheduler.state.config.clone();
    let scheduler_grpc_server = SchedulerGrpcServer::new((*scheduler).clone())
        .max_encoding_message_size(runtime_config.grpc_server_max_encoding_message_size as usize)
        .max_decoding_message_size(runtime_config.grpc_server_max_decoding_message_size as usize);

    let max_decoding = runtime_config.grpc_server_max_decoding_message_size as usize;
    let max_encoding = runtime_config.grpc_server_max_encoding_message_size as usize;

    // Wrap the endpoint override (client TLS to executors) the same way the
    // stock assembly does.
    let customize_endpoint = runtime_config
        .override_create_grpc_client_endpoint
        .clone()
        .map(|f| Arc::new(BallistaConfigGrpcEndpoint::new(f)));

    let flight_proxy = BallistaFlightProxyService::new(
        max_decoding,
        max_encoding,
        runtime_config.use_tls,
        customize_endpoint,
    );

    let mut tonic_builder = RoutesBuilder::default();
    tonic_builder.add_service(scheduler_grpc_server);
    // No flight-sql feature in this build, so the embedded proxy is the only
    // FlightService mounted (matching the stock assembly's else branch).
    if runtime_config.enable_embedded_flight_proxy {
        tracing::info!("Adding embedded flight proxy service on scheduler");
        tonic_builder.add_service(
            arrow_flight::flight_service_server::FlightServiceServer::new(flight_proxy)
                .max_decoding_message_size(max_decoding)
                .max_encoding_message_size(max_encoding),
        );
    }

    let tonic_router = tonic_builder
        .routes()
        .into_axum_router()
        .fallback(|| async { StatusCode::NOT_FOUND });

    let merged = if runtime_config.disable_rest_api {
        tonic_router
    } else {
        ballista_scheduler::api::get_routes(scheduler.clone())
            .merge(tonic_router)
            .merge(health_routes(scheduler.clone()))
    };

    let final_route =
        with_version_headers(merged).into_make_service_with_connect_info::<SocketAddr>();

    let rustls_config = Arc::new(
        tls.rustls_server_config()
            .map_err(|e| BallistaError::Configuration(e.to_string()))?,
    );
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(BallistaError::from)?;
    tracing::info!(
        "Ballista Scheduler v{BALLISTA_VERSION} (DataFusion v{DATAFUSION_VERSION}) serving mTLS on {address:?}"
    );

    // `tap_io` supplies the ConnectInfo plumbing axum needs for custom
    // listeners (the `Connected<...> for SocketAddr` impl is axum's, generic
    // over any Listener whose Addr is SocketAddr).
    let tls_listener = TlsListener::new(listener, rustls_config).tap_io(|_| {});

    axum::serve(tls_listener, final_route)
        .await
        .map_err(BallistaError::from)?;

    Ok(())
}