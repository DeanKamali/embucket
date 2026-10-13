//! Compute-provisioning abstraction for warehouses.
//!
//! A warehouse's Ballista scheduler and executor pool run as containers. v0
//! drives the local Docker CLI (`ContainerDriver` impl: [`DockerCliDriver`]);
//! ECS/Fargate/K8s drivers plug in behind the same trait later (design §5).

use crate::error::{ContainerDriverSnafu, Result};
use async_trait::async_trait;

pub mod fake;
pub use fake::FakeDriver;

/// One compute process of a warehouse (scheduler or single executor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSpec {
    /// Deterministic container name, e.g. `avalanche-wh-analytics-exec-2`.
    pub name: String,
    pub image: String,
    pub args: Vec<String>,
    /// env var name → value
    pub env: Vec<(String, String)>,
    /// host port → container port
    pub ports: Vec<(u16, u16)>,
    /// Docker network to attach the container to (container-name DNS).
    pub network: Option<String>,
}

/// Starts/stops the compute processes of a warehouse.
#[async_trait]
pub trait ContainerDriver: Send + Sync {
    /// Start a container in detached mode. Idempotent: starting an already
    /// running container is a no-op.
    async fn start(&self, spec: &ContainerSpec) -> Result<()>;

    /// Stop and remove a container. Idempotent.
    async fn stop(&self, name: &str) -> Result<()>;

    /// True if a container with this name exists (running or stopped).
    async fn exists(&self, name: &str) -> Result<bool>;
}

/// Runs containers through the local `docker` CLI.
#[derive(Debug, Clone, Default)]
pub struct DockerCliDriver {
    /// docker network to attach warehouse containers to (default: bridge).
    pub network: Option<String>,
}

impl DockerCliDriver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn run(args: &[&str]) -> Result<std::process::Output> {
        tokio::process::Command::new("docker")
            .args(args)
            .output()
            .await
            .map_err(|err| {
                ContainerDriverSnafu {
                    message: format!("failed to run docker {}", args.join(" ")),
                    detail: err.to_string(),
                }
                .build()
            })
    }
}

#[async_trait]
impl ContainerDriver for DockerCliDriver {
    async fn start(&self, spec: &ContainerSpec) -> Result<()> {
        // docker run --name X already exists? then ensure it is started.
        let exists = self.exists(&spec.name).await?;
        if exists {
            let out = Self::run(&["start", &spec.name]).await?;
            if out.status.success() {
                return Ok(());
            }
            return Err(ContainerDriverSnafu {
                message: format!("docker start {}", spec.name),
                detail: String::from_utf8_lossy(&out.stderr).to_string(),
            }
            .build());
        }

        let mut args: Vec<String> = vec![
            "run".to_string(),
            "-d".to_string(),
            "--name".to_string(),
            spec.name.clone(),
            "--rm".to_string(),
        ];
        if let Some(network) = spec.network.as_ref().or(self.network.as_ref()) {
            args.push("--network".to_string());
            args.push(network.clone());
        }
        for (k, v) in &spec.env {
            args.push("--env".to_string());
            args.push(format!("{k}={v}"));
        }
        for (host, container) in &spec.ports {
            args.push("-p".to_string());
            args.push(format!("{host}:{container}"));
        }
        args.push(spec.image.clone());
        args.extend(spec.args.iter().cloned());

        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = Self::run(&arg_refs).await?;
        if out.status.success() {
            return Ok(());
        }
        Err(ContainerDriverSnafu {
            message: format!("docker run {}", spec.name),
            detail: String::from_utf8_lossy(&out.stderr).to_string(),
        }
        .build())
    }

    async fn stop(&self, name: &str) -> Result<()> {
        let exists = self.exists(name).await?;
        if !exists {
            return Ok(());
        }
        // `docker rm -f` stops a running container and removes a stopped one.
        let out = Self::run(&["rm", "-f", name]).await?;
        if out.status.success() {
            return Ok(());
        }
        Err(ContainerDriverSnafu {
            message: format!("docker rm -f {name}"),
            detail: String::from_utf8_lossy(&out.stderr).to_string(),
        }
        .build())
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let out = Self::run(&["container", "inspect", name]).await?;
        Ok(out.status.success())
    }
}
