//! In-memory [`ContainerDriver`] for tests: records operations instead of
//! touching Docker.

#![allow(clippy::expect_used)]

use crate::driver::{ContainerDriver, ContainerSpec};
use crate::error::Result;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Shared log of driver operations (started/started names in order).
#[derive(Debug, Default, Clone)]
pub struct FakeDriver {
    inner: Arc<Mutex<FakeState>>,
}

#[derive(Debug, Default)]
struct FakeState {
    /// container names currently "existing" (running or stopped)
    containers: HashSet<String>,
    ops: Vec<(String, String)>, // ("start"/"stop", name)
}

impl FakeDriver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn running(&self, name: &str) -> bool {
        self.inner.lock().expect("poisoned").containers.contains(name)
    }

    #[must_use]
    pub fn ops(&self) -> Vec<(String, String)> {
        self.inner.lock().expect("poisoned").ops.clone()
    }

    #[must_use]
    pub fn started_count(&self, prefix: &str) -> usize {
        self.ops()
            .iter()
            .filter(|(op, name)| op == "start" && name.starts_with(prefix))
            .count()
    }

    #[must_use]
    pub fn stopped_count(&self, prefix: &str) -> usize {
        self.ops()
            .iter()
            .filter(|(op, name)| op == "stop" && name.starts_with(prefix))
            .count()
    }
}

#[async_trait::async_trait]
impl ContainerDriver for FakeDriver {
    async fn start(&self, spec: &ContainerSpec) -> Result<()> {
        let mut state = self.inner.lock().expect("poisoned");
        state.containers.insert(spec.name.clone());
        state.ops.push(("start".to_string(), spec.name.clone()));
        Ok(())
    }

    async fn stop(&self, name: &str) -> Result<()> {
        let mut state = self.inner.lock().expect("poisoned");
        state.containers.remove(name);
        state.ops.push(("stop".to_string(), name.to_string()));
        Ok(())
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        Ok(self.inner.lock().expect("poisoned").containers.contains(name))
    }
}
