//! Kubernetes [`ContainerDriver`]: runs warehouse compute as pods, driven
//! through the `kubectl` CLI (same shelling-out pattern as
//! [`DockerCliDriver`](crate::driver::DockerCliDriver)).
//!
//! Mapping decisions (M6, design §5):
//! - container names become pod (and, for schedulers, Service) names after
//!   lowercasing — k8s object names must be RFC-1123;
//! - the scheduler gets a ClusterIP Service named like the pod, so executors
//!   dial `--scheduler-host <sched-name>:50050` via in-namespace DNS;
//! - executors advertise no external host — the scheduler falls back to the
//!   gRPC peer IP (ballista-scheduler `register_executor`), i.e. pod IPs;
//! - the host-side Ballista client (embucketd) dials the scheduler **pod IP**
//!   directly (`http://<podIP>:50050`), which works from the node host on
//!   typical CNIs (k3s flannel included) and from in-cluster clients alike.
//!   Re-resolved per call, so pod restarts don't leave stale addresses;
//! - optional `WAREHOUSE_HOST_DATA_PATH` hostPath mount for `file://` tables
//!   (single-node clusters; multi-node should use object storage).

use crate::driver::{ContainerDriver, ContainerSpec};
use crate::error::{ContainerDriverSnafu, Result};
use async_trait::async_trait;
use serde_json::json;
use std::path::PathBuf;

/// Ballista scheduler gRPC port inside the cluster.
const SCHEDULER_PORT: u16 = 50050;

/// `kubectl`-driven Kubernetes driver.
#[derive(Debug, Clone)]
pub struct KubectlDriver {
    /// Optional `--kubeconfig` path (`WAREHOUSE_KUBECONFIG`).
    pub kubeconfig: Option<PathBuf>,
    /// Namespace pods and services are created in (`WAREHOUSE_NAMESPACE`).
    pub namespace: String,
    /// Pod `imagePullPolicy` (`WAREHOUSE_IMAGE_PULL_POLICY`).
    pub image_pull_policy: String,
    /// Host path mounted at `/data` in every pod
    /// (`WAREHOUSE_HOST_DATA_PATH`); unset skips the mount.
    pub host_data_path: Option<PathBuf>,
}

impl KubectlDriver {
    #[must_use]
    pub fn from_env() -> Self {
        let env = |key: &str| {
            std::env::var(key).ok().filter(|v| !v.is_empty())
        };
        Self {
            kubeconfig: env("WAREHOUSE_KUBECONFIG").map(PathBuf::from),
            namespace: env("WAREHOUSE_NAMESPACE").unwrap_or_else(|| "avalanche".to_string()),
            image_pull_policy: env("WAREHOUSE_IMAGE_PULL_POLICY")
                .unwrap_or_else(|| "IfNotPresent".to_string()),
            host_data_path: env("WAREHOUSE_HOST_DATA_PATH").map(PathBuf::from),
        }
    }

    /// k8s object name for a docker-style container name: k8s names must be
    /// RFC-1123 lowercase.
    fn k8s_name(container: &str) -> String {
        container.to_ascii_lowercase()
    }

    async fn run(&self, args: &[&str]) -> Result<std::process::Output> {
        let mut cmd = tokio::process::Command::new("kubectl");
        if let Some(kubeconfig) = &self.kubeconfig {
            cmd.arg("--kubeconfig").arg(kubeconfig);
        }
        cmd.arg("--namespace").arg(&self.namespace).args(args);
        cmd.output().await.map_err(|err| {
            ContainerDriverSnafu {
                message: format!("failed to run kubectl {}", args.join(" ")),
                detail: err.to_string(),
            }
            .build()
        })
    }

    /// Apply a manifest via stdin.
    async fn apply(&self, manifest: serde_json::Value) -> Result<()> {
        let mut cmd = tokio::process::Command::new("kubectl");
        if let Some(kubeconfig) = &self.kubeconfig {
            cmd.arg("--kubeconfig").arg(kubeconfig);
        }
        cmd.arg("--namespace")
            .arg(&self.namespace)
            .args(["apply", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().map_err(|err| {
            ContainerDriverSnafu {
                message: "failed to spawn kubectl apply".to_string(),
                detail: err.to_string(),
            }
            .build()
        })?;
        use tokio::io::AsyncWriteExt as _;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&serde_json::to_vec(&manifest).map_err(|err| {
                    ContainerDriverSnafu {
                        message: "failed to serialize pod manifest".to_string(),
                        detail: err.to_string(),
                    }
                    .build()
                })?)
                .await
                .map_err(|err| {
                    ContainerDriverSnafu {
                        message: "failed to write manifest to kubectl stdin".to_string(),
                        detail: err.to_string(),
                    }
                    .build()
                })?;
        }
        let out = child.wait_with_output().await.map_err(|err| {
            ContainerDriverSnafu {
                message: "kubectl apply failed".to_string(),
                detail: err.to_string(),
            }
            .build()
        })?;
        if out.status.success() {
            return Ok(());
        }
        Err(ContainerDriverSnafu {
            message: "kubectl apply".to_string(),
            detail: String::from_utf8_lossy(&out.stderr).to_string(),
        }
        .build())
    }

    fn pod_manifest(&self, spec: &ContainerSpec) -> serde_json::Value {
        let name = Self::k8s_name(&spec.name);
        // docker driver args reference container names for peer addressing
        // (`--scheduler-host`, `--external-host`); k8s DNS needs lowercase.
        let args: Vec<String> = spec
            .args
            .iter()
            .map(|arg| {
                if arg.starts_with("avalanche-wh-") {
                    arg.to_ascii_lowercase()
                } else {
                    arg.clone()
                }
            })
            .collect();
        let mut container = json!({
            "name": "compute",
            "image": spec.image,
            "imagePullPolicy": self.image_pull_policy,
            "args": args,
            "env": spec.env.iter().map(|(k, v)| json!({"name": k, "value": v}))
                .collect::<Vec<_>>(),
        });
        if !spec.ports.is_empty() {
            container["ports"] = json!(spec.ports.iter()
                .map(|(_, container_port)| json!({"containerPort": container_port}))
                .collect::<Vec<_>>());
        }
        // Per-spec read-only bind mounts (M7.2 TLS material). Rendered as
        // hostPath volumes; the demo-data mount (host_data_path) is separate.
        let spec_volumes: Vec<serde_json::Value> = spec
            .volumes
            .iter()
            .enumerate()
            .map(|(i, (host, mount_path))| {
                let vol_name = format!("spec-vol-{i}");
                container["volumeMounts"] = {
                    let mut mounts = container
                        .as_object()
                        .and_then(|o| o.get("volumeMounts"))
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    mounts.push(json!({"name": vol_name, "mountPath": mount_path, "readOnly": true}));
                    json!(mounts)
                };
                json!({"name": vol_name, "hostPath": {"path": host, "type": "File"}})
            })
            .collect();
        if let Some(host_path) = &self.host_data_path {
            let mut volumes = vec![json!({"name": "demo-data", "hostPath": {
                "path": host_path.to_string_lossy(), "type": "Directory",
            }})];
            volumes.extend(spec_volumes);
            container["volumeMounts"] = {
                let mut mounts = vec![json!({
                    "name": "demo-data",
                    "mountPath": "/data",
                })];
                if let Some(existing) = container
                    .as_object()
                    .and_then(|o| o.get("volumeMounts"))
                    .and_then(|v| v.as_array())
                {
                    mounts.extend(existing.iter().cloned());
                }
                json!(mounts)
            };
            return json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {"app": name, "app.kubernetes.io/part-of": "avalanche"},
                },
                "spec": {
                    "restartPolicy": "Always",
                    "volumes": volumes,
                    "containers": [container],
                },
            });
        }
        if spec_volumes.is_empty() {
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {"app": name, "app.kubernetes.io/part-of": "avalanche"},
                },
                "spec": {
                    "restartPolicy": "Always",
                    "containers": [container],
                },
            })
        } else {
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {"app": name, "app.kubernetes.io/part-of": "avalanche"},
                },
                "spec": {
                    "restartPolicy": "Always",
                    "volumes": spec_volumes,
                    "containers": [container],
                },
            })
        }
    }

    fn service_manifest(&self, spec: &ContainerSpec) -> serde_json::Value {
        let name = Self::k8s_name(&spec.name);
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "namespace": self.namespace},
            "spec": {
                "selector": {"app": name},
                "ports": [{"port": SCHEDULER_PORT, "targetPort": SCHEDULER_PORT}],
            },
        })
    }

    /// Pod phase (`Some("Running")`/`Some("Terminating")`/...) or `None`
    /// when the pod object does not exist.
    async fn pod_phase(&self, name: &str) -> Result<Option<String>> {
        let name = Self::k8s_name(name);
        let out = self
            .run(&["get", "pod", &name, "-o", "jsonpath={.status.phase}"])
            .await?;
        if !out.status.success() {
            return Ok(None);
        }
        let phase = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok((!phase.is_empty()).then_some(phase))
    }
}

#[async_trait]
impl ContainerDriver for KubectlDriver {
    async fn start(&self, spec: &ContainerSpec) -> Result<()> {
        let name = Self::k8s_name(&spec.name);
        // Idempotent like DockerCliDriver: an existing pod is left as-is.
        // A Terminating pod still owns the object name — wait for it to be
        // reclaimed before reusing the name.
        for _ in 0..30 {
            match self.pod_phase(&spec.name).await? {
                Some(phase) if phase == "Terminating" => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Some(_) => return Ok(()),
                None => break,
            }
        }
        if self.exists(&spec.name).await? {
            return Ok(());
        }
        let is_scheduler = spec.ports.iter().any(|(_, p)| *p == SCHEDULER_PORT);
        self.apply(self.pod_manifest(spec)).await?;
        if is_scheduler {
            self.apply(self.service_manifest(spec)).await?;
            // Resume-on-query blocks until the scheduler can take jobs;
            // executors register asynchronously, so only the scheduler waits.
            let out = self
                .run(&[
                    "wait",
                    "--for=condition=Ready",
                    "pod",
                    &name,
                    "--timeout=90s",
                ])
                .await?;
            if !out.status.success() {
                return Err(ContainerDriverSnafu {
                    message: format!("scheduler pod {name} not ready in 90s"),
                    detail: String::from_utf8_lossy(&out.stderr).to_string(),
                }
                .build());
            }
        }
        Ok(())
    }

    async fn stop(&self, name: &str) -> Result<()> {
        if !self.exists(name).await? {
            return Ok(());
        }
        let name = Self::k8s_name(name);
        let out = self
            .run(&["delete", "pod", &name, "--ignore-not-found", "--wait=false"])
            .await?;
        // the pod's Service is part of the same compute unit
        let _ = self
            .run(&["delete", "service", &name, "--ignore-not-found"])
            .await;
        if out.status.success() {
            return Ok(());
        }
        Err(ContainerDriverSnafu {
            message: format!("kubectl delete pod {name}"),
            detail: String::from_utf8_lossy(&out.stderr).to_string(),
        }
        .build())
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        match self.pod_phase(name).await? {
            Some(phase) => Ok(phase != "Terminating"),
            None => Ok(false),
        }
    }

    async fn scheduler_url(&self, warehouse: &str, _host_port: Option<u16>) -> Result<String> {
        let pod = Self::k8s_name(&crate::manager::scheduler_container_name(warehouse));
        let out = self
            .run(&["get", "pod", &pod, "-o", "jsonpath={.status.podIP}"])
            .await?;
        let ip = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !out.status.success() || ip.is_empty() {
            return Err(ContainerDriverSnafu {
                message: format!("scheduler pod {pod} has no IP (not running?)"),
                detail: String::from_utf8_lossy(&out.stderr).to_string(),
            }
            .build());
        }
        Ok(format!("http://{ip}:{SCHEDULER_PORT}"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    fn driver() -> KubectlDriver {
        KubectlDriver {
            kubeconfig: None,
            namespace: "avalanche".to_string(),
            image_pull_policy: "IfNotPresent".to_string(),
            host_data_path: None,
        }
    }

    #[test]
    fn k8s_names_are_lowercased() {
        assert_eq!(
            KubectlDriver::k8s_name("avalanche-wh-ETL-scheduler"),
            "avalanche-wh-etl-scheduler"
        );
    }

    #[test]
    fn scheduler_manifest_has_service_ports_and_lowercased_peer_args() {
        let d = driver();
        let spec = ContainerSpec {
            name: "avalanche-wh-ETL-scheduler".to_string(),
            image: "avalanche-scheduler:m4".to_string(),
            args: vec![
                "--bind-host".to_string(),
                "0.0.0.0".to_string(),
                "--external-host".to_string(),
                "avalanche-wh-ETL-scheduler".to_string(),
            ],
            env: vec![],
            volumes: vec![],
            ports: vec![(30001, SCHEDULER_PORT)],
            network: None,
            network_aliases: Vec::new(),
        };
        let pod = d.pod_manifest(&spec);
        assert_eq!(pod["kind"], "Pod");
        assert_eq!(pod["metadata"]["name"], "avalanche-wh-etl-scheduler");
        assert_eq!(pod["metadata"]["namespace"], "avalanche");
        assert_eq!(pod["spec"]["containers"][0]["image"], "avalanche-scheduler:m4");
        assert_eq!(
            pod["spec"]["containers"][0]["args"][3],
            "avalanche-wh-etl-scheduler"
        );
        assert_eq!(
            pod["spec"]["containers"][0]["ports"][0]["containerPort"],
            SCHEDULER_PORT
        );
        let svc = d.service_manifest(&spec);
        assert_eq!(svc["kind"], "Service");
        assert_eq!(svc["spec"]["selector"]["app"], "avalanche-wh-etl-scheduler");
        assert_eq!(svc["spec"]["ports"][0]["port"], SCHEDULER_PORT);
    }

    #[test]
    fn executor_manifest_has_no_ports_and_mounts_host_data() {
        let d = KubectlDriver {
            host_data_path: Some("/root/avalanche-demo/data".into()),
            ..driver()
        };
        let spec = ContainerSpec {
            name: "avalanche-wh-ETL-exec-0".to_string(),
            image: "avalanche-executor:m4".to_string(),
            args: vec!["--scheduler-host".to_string(), "avalanche-wh-ETL-scheduler".to_string()],
            env: vec![("BALLISTA_CONFIG".to_string(), "x".to_string())],
            volumes: vec![],
            ports: vec![],
            network: None,
            network_aliases: Vec::new(),
        };
        let pod = d.pod_manifest(&spec);
        assert_eq!(pod["spec"]["containers"][0]["ports"], serde_json::Value::Null);
        assert_eq!(pod["spec"]["volumes"][0]["hostPath"]["path"], "/root/avalanche-demo/data");
        assert_eq!(
            pod["spec"]["containers"][0]["volumeMounts"][0]["mountPath"],
            "/data"
        );
        assert_eq!(pod["spec"]["containers"][0]["env"][0]["name"], "BALLISTA_CONFIG");
    }
}
