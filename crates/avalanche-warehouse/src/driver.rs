//! Compute-provisioning abstraction for warehouses.
//!
//! A warehouse's Ballista scheduler and executor pool run as containers. v0
//! drives the local Docker CLI (`ContainerDriver` impl: [`DockerCliDriver`]);
//! ECS/Fargate/K8s drivers plug in behind the same trait later (design §5).

use crate::error::{ContainerDriverSnafu, Result};
use async_trait::async_trait;

pub mod fake;
pub mod k8s;
pub use fake::FakeDriver;
pub use k8s::KubectlDriver;

/// One compute process of a warehouse (scheduler or single executor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSpec {
    /// Deterministic container name, e.g. `avalanche-wh-analytics-exec-2`.
    pub name: String,
    pub image: String,
    pub args: Vec<String>,
    /// env var name → value
    pub env: Vec<(String, String)>,
    /// host path → container path, bind-mounted read-only (M7.2 TLS
    /// material; k8s driver renders hostPath volumes)
    pub volumes: Vec<(String, String)>,
    /// host port → container port
    pub ports: Vec<(u16, u16)>,
    /// Docker network to attach the container to (container-name DNS).
    pub network: Option<String>,
    /// Extra DNS names (docker `--network-alias`) this container answers to
    /// on its network (M7.3): scheduler replicas share one alias so
    /// executors can dial any replica by the same name.
    pub network_aliases: Vec<String>,
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

    /// Gateway IP of `network` (docker bridge), used to rewrite host-dialing
    /// URLs (e.g. the Postgres state-store URL) for container reachability.
    /// `None` when unsupported (fake) or unknown network.
    async fn network_gateway(&self, network: Option<&str>) -> Option<String> {
        let _ = network;
        None
    }

    /// Address the host-side Ballista client dials for a warehouse's
    /// scheduler. Default: docker-style mapped host port on loopback
    /// (embucketd runs on the docker host). Cluster drivers override this —
    /// e.g. the k8s driver dials the scheduler pod IP directly.
    async fn scheduler_url(&self, _warehouse: &str, host_port: Option<u16>) -> Result<String> {
        let _ = _warehouse;
        let port = host_port.ok_or_else(|| {
            ContainerDriverSnafu {
                message: "scheduler host port not allocated".to_string(),
                detail: "warehouse record has no scheduler_host_port".to_string(),
            }
            .build()
        })?;
        Ok(format!("http://127.0.0.1:{port}"))
    }
}

/// Runs containers through the local `docker` CLI.
#[derive(Debug, Clone, Default)]
pub struct DockerCliDriver {
    /// docker network to attach warehouse containers to (default: bridge).
    pub network: Option<String>,
}

/// Marker appended to `/etc/hosts` entries maintained by the docker driver.
const HOST_DNS_MARKER: &str = "# avalanche-dns";

/// Path of the hosts file maintained for container-name resolution.
const HOST_DNS_FILE: &str = "/etc/hosts";

/// Upsert a `<bridge-ip> <container-name>` entry in the host's hosts file.
///
/// The host-side Ballista client (embucketd) dials warehouse containers by
/// their container DNS name (TLS server certs carry name SANs), but docker's
/// embedded DNS answers only *inside* the network — not on the host. Bridge
/// IPs are host-routable, so we pin the mapping on the host ourselves.
/// Entries carry `# avalanche-dns` so only our own lines are ever rewritten.
/// Name token of a hosts line (`<ip> <name> ...`), if it is one of ours.
fn host_dns_name(line: &str) -> Option<&str> {
    if !line.contains(HOST_DNS_MARKER) {
        return None;
    }
    line.split_whitespace().nth(1)
}

/// Pin `ip` for a container's own name (unique — the line is replaced) and
/// for each of its network aliases (M7.3). Alias lines carry the owning
/// container name after the marker (`<ip> <alias> <marker> <container>`) so
/// `remove_host_dns` can retract exactly this container's entries while
/// leaving a shared alias resolvable through the other replicas.
///
/// A shared alias deliberately accumulates one line per live replica: the
/// host-side router resolves the alias to all replica IPs and hyper's
/// connector iterates them until one accepts the connection — that iteration
/// is the failover mechanism.
fn upsert_host_dns(name: &str, ip: &str, aliases: &[String]) {
    let Ok(existing) = std::fs::read_to_string(HOST_DNS_FILE) else {
        tracing::warn!("could not read {HOST_DNS_FILE}: container names may not resolve from the host");
        return;
    };
    let self_alias_tag = format!("{HOST_DNS_MARKER} {name}");
    let mut out: Vec<String> = existing
        .lines()
        // replace our own name line and our own alias lines
        .filter(|l| host_dns_name(l) != Some(name))
        .filter(|l| !l.contains(&self_alias_tag))
        .map(String::from)
        .collect();
    // already-pinned alias entries from other replicas with the same ip
    for alias in aliases {
        if out
            .iter()
            .any(|l| host_dns_name(l) == Some(alias.as_str()) && l.starts_with(ip))
        {
            continue;
        }
        out.push(format!("{ip} {alias} {HOST_DNS_MARKER} {name}"));
    }
    let mut text = out.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(&format!("{ip} {name} {HOST_DNS_MARKER}\n"));
    if let Err(err) = std::fs::write(HOST_DNS_FILE, text) {
        tracing::warn!("could not update {HOST_DNS_FILE} for {name}: {err}");
    }
}

/// Remove our hosts-file entry for a stopped container (best-effort).
/// Remove our hosts-file entries for a stopped container (best-effort): its
/// own name line plus every alias line it owned (tagged with the container
/// name). Alias lines owned by sibling replicas survive.
fn remove_host_dns(name: &str) {
    let Ok(existing) = std::fs::read_to_string(HOST_DNS_FILE) else {
        return;
    };
    let self_alias_tag = format!("{HOST_DNS_MARKER} {name}");
    let kept: Vec<&str> = existing
        .lines()
        .filter(|l| host_dns_name(l) != Some(name))
        .filter(|l| !l.contains(&self_alias_tag))
        .collect();
    let mut out = kept.join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    if out != existing {
        let _ = std::fs::write(HOST_DNS_FILE, out);
    }
}

/// Query a container's bridge-network IP (`docker inspect`).
async fn container_ip(name: &str) -> Option<String> {
    let out = tokio::process::Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
            name,
        ])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .filter(|ip| !ip.is_empty())
        .map(ToOwned::to_owned)
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
    async fn network_gateway(&self, network: Option<&str>) -> Option<String> {
        let net = network.or(self.network.as_deref()).unwrap_or("bridge");
        let out = Self::run(&[
            "network",
            "inspect",
            "-f",
            "{{(index .IPAM.Config 0).Gateway}}",
            net,
        ])
        .await
        .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .filter(|g| !g.is_empty())
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<()> {
        // docker run --name X already exists? then ensure it is started.
        let exists = self.exists(&spec.name).await?;
        if exists {
            let out = Self::run(&["start", &spec.name]).await?;
            if out.status.success() {
                // The container may have re-attached with a different bridge
                // IP than the pinned hosts-file entry — refresh it.
                if let Some(ip) = container_ip(&spec.name).await {
                    upsert_host_dns(&spec.name, &ip, &spec.network_aliases);
                }
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
        for (host, container) in &spec.volumes {
            args.push("-v".to_string());
            args.push(format!("{host}:{container}:ro"));
        }
        for (host, container) in &spec.ports {
            args.push("-p".to_string());
            args.push(format!("{host}:{container}"));
        }
        for alias in &spec.network_aliases {
            args.push("--network-alias".to_string());
            args.push(alias.clone());
        }
        args.push(spec.image.clone());
        args.extend(spec.args.iter().cloned());

        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        tracing::info!(container = %spec.name, args = %arg_refs.join(" "), "docker run");
        let out = Self::run(&arg_refs).await?;
        if out.status.success() {
            // Pin the container's bridge IP in the host hosts-file so
            // host-side dials (e.g. the router's partition fetches) resolve
            // the container DNS name that TLS certificates carry.
            if let Some(ip) = container_ip(&spec.name).await {
                upsert_host_dns(&spec.name, &ip, &spec.network_aliases);
            }
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
            remove_host_dns(name);
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
