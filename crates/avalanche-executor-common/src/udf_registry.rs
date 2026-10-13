//! Collection of the Embucket UDF catalog without a live session.
//!
//! [`FunctionMaps`] implements [`FunctionRegistry`] as a collector, hands
//! itself to `functions::register_udfs`, and exposes the resulting
//! name-to-UDF maps. The codecs in [`crate::codecs`] use these maps to
//! resolve UDFs by name on Ballista schedulers and executors.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::{
    AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF,
};
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_common::{Result, plan_datafusion_err};
use functions::session_params::SessionParams;

/// Name-keyed maps of every UDF the `functions` crate registers.
#[derive(Debug, Default)]
pub struct FunctionMaps {
    pub scalar: HashMap<String, Arc<ScalarUDF>>,
    pub aggregate: HashMap<String, Arc<AggregateUDF>>,
    pub window: HashMap<String, Arc<WindowUDF>>,
}

impl FunctionMaps {
    /// Collect all Embucket UDFs. Registration is deterministic (no session
    /// state dependence beyond default `SessionParams`), so this is cheap
    /// enough to call per-codec construction.
    pub fn embucket() -> Self {
        struct Collector {
            scalar: HashMap<String, Arc<ScalarUDF>>,
            aggregate: HashMap<String, Arc<AggregateUDF>>,
            window: HashMap<String, Arc<WindowUDF>>,
        }

        impl Default for Collector {
            fn default() -> Self {
                Self {
                    scalar: HashMap::new(),
                    aggregate: HashMap::new(),
                    window: HashMap::new(),
                }
            }
        }

        impl FunctionRegistry for Collector {
            fn udfs(&self) -> HashSet<String> {
                self.scalar.keys().cloned().collect()
            }

            fn higher_order_function_names(&self) -> HashSet<String> {
                HashSet::new()
            }

            fn udafs(&self) -> HashSet<String> {
                self.aggregate.keys().cloned().collect()
            }

            fn udwfs(&self) -> HashSet<String> {
                self.window.keys().cloned().collect()
            }

            fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
                self.scalar
                    .get(name)
                    .cloned()
                    .ok_or_else(|| plan_datafusion_err!("Scalar UDF '{name}' not registered"))
            }

            fn higher_order_function(&self, name: &str) -> Result<Arc<HigherOrderUDF>> {
                let _ = name;
                Err(plan_datafusion_err!(
                    "Higher-order functions are not registered by the functions crate"
                ))
            }

            fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
                self.aggregate
                    .get(name)
                    .cloned()
                    .ok_or_else(|| plan_datafusion_err!("Aggregate UDF '{name}' not registered"))
            }

            fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
                self.window
                    .get(name)
                    .cloned()
                    .ok_or_else(|| plan_datafusion_err!("Window UDF '{name}' not registered"))
            }

            fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
                self.scalar.insert(udf.name().to_string(), udf.clone());
                Ok(Some(udf))
            }

            fn register_udaf(
                &mut self,
                udaf: Arc<AggregateUDF>,
            ) -> Result<Option<Arc<AggregateUDF>>> {
                self.aggregate.insert(udaf.name().to_string(), udaf.clone());
                Ok(Some(udaf))
            }

            fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
                self.window.insert(udwf.name().to_string(), udwf.clone());
                Ok(Some(udwf))
            }

            fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
                Vec::new()
            }
        }

        let mut collector = Collector::default();
        functions::register_udfs(&mut collector, &Arc::new(SessionParams::default()))
            .expect("Embucket UDF registration must not fail with default session params");
        // Aggregates are exported separately by the functions crate.
        functions::register_udafs(&mut collector)
            .expect("Embucket UDAF registration must not fail");
        Self {
            scalar: collector.scalar,
            aggregate: collector.aggregate,
            window: collector.window,
        }
    }

    /// Look up a scalar UDF by name.
    pub fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.scalar
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Scalar UDF '{name}' is not an Embucket function"))
    }

    /// Look up an aggregate UDF by name.
    pub fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.aggregate
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Aggregate UDF '{name}' is not an Embucket function"))
    }

    /// Look up a window UDF by name.
    pub fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        self.window
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Window UDF '{name}' is not an Embucket function"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_embucket_udfs() {
        let maps = FunctionMaps::embucket();
        assert!(!maps.scalar.is_empty(), "no scalar UDFs collected");
        assert!(!maps.aggregate.is_empty(), "no aggregate UDFs collected");
        // Spot-check a semi-structured function that must exist.
        let resolved = maps.scalar.keys().cloned().collect::<Vec<_>>();
        assert!(
            resolved.iter().any(|n| n.contains("json") || n.contains("object")),
            "expected semi-structured UDFs among {resolved:?}"
        );
    }

    #[test]
    fn udf_lookup_roundtrip() {
        let maps = FunctionMaps::embucket();
        let name = maps.scalar.keys().next().expect("non-empty").clone();
        let udf = maps.udf(&name).expect("lookup");
        assert_eq!(udf.name(), name);
        assert!(maps.udf("__definitely_not_a_function__").is_err());
    }
}
