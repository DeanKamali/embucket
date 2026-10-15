use serde::{Deserialize, Serialize};

/// Snowflake-style warehouse sizes. Executor counts follow the Snowflake
/// doubling ladder (v1 static mapping; design §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarehouseSize {
    Xsmall,
    Small,
    Medium,
    Large,
    Xlarge,
    TwoXlarge,
    ThreeXlarge,
    FourXlarge,
}

impl WarehouseSize {
    /// Number of Ballista executors backing this size (v1: one executor
    /// process per size step).
    #[must_use]
    pub const fn executor_count(self) -> u32 {
        match self {
            Self::Xsmall => 1,
            Self::Small => 2,
            Self::Medium => 4,
            Self::Large => 8,
            Self::Xlarge => 16,
            Self::TwoXlarge => 32,
            Self::ThreeXlarge => 64,
            Self::FourXlarge => 128,
        }
    }

    /// Billing multiplier used later by metering (M5); executor-seconds are
    /// scaled by this factor.
    #[must_use]
    pub const fn credit_multiplier(self) -> u32 {
        self.executor_count()
    }

    #[must_use]
    pub fn valid_sizes() -> Vec<&'static str> {
        use WarehouseSize::FourXlarge;
        use WarehouseSize::Large;
        use WarehouseSize::Medium;
        use WarehouseSize::Small;
        use WarehouseSize::ThreeXlarge;
        use WarehouseSize::TwoXlarge;
        use WarehouseSize::Xlarge;
        use WarehouseSize::Xsmall;
        vec![
            Xsmall.as_str(),
            Small.as_str(),
            Medium.as_str(),
            Large.as_str(),
            Xlarge.as_str(),
            TwoXlarge.as_str(),
            ThreeXlarge.as_str(),
            FourXlarge.as_str(),
        ]
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Xsmall => "XSMALL",
            Self::Small => "SMALL",
            Self::Medium => "MEDIUM",
            Self::Large => "LARGE",
            Self::Xlarge => "XLARGE",
            Self::TwoXlarge => "2XLARGE",
            Self::ThreeXlarge => "3XLARGE",
            Self::FourXlarge => "4XLARGE",
        }
    }

    /// Accepts Snowflake spellings: `XSMALL`/`X-SMALL`/`XS`, `2XLARGE`/`DOUBLE`...
    #[must_use]
    pub fn parse(size: &str) -> Option<Self> {
        use WarehouseSize::FourXlarge;
        use WarehouseSize::Large;
        use WarehouseSize::Medium;
        use WarehouseSize::Small;
        use WarehouseSize::ThreeXlarge;
        use WarehouseSize::TwoXlarge;
        use WarehouseSize::Xlarge;
        use WarehouseSize::Xsmall;
        match size.to_ascii_uppercase().replace(['-', ' '], "").as_str() {
            "XSMALL" | "XS" => Some(Xsmall),
            "SMALL" | "S" => Some(Small),
            "MEDIUM" | "M" | "MEDIUMSTANDARD" => Some(Medium),
            "LARGE" | "L" => Some(Large),
            "XLARGE" | "XL" => Some(Xlarge),
            "2XLARGE" | "XXLARGE" | "XXL" | "2XL" | "DOUBLE" => Some(TwoXlarge),
            "3XLARGE" | "XXXLARGE" | "XXXL" | "3XL" | "TRIPLE" => Some(ThreeXlarge),
            "4XLARGE" | "XXXXLARGE" | "XXXXL" | "4XL" => Some(FourXlarge),
            _ => None,
        }
    }
}

impl std::fmt::Display for WarehouseSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Warehouse lifecycle state. `Stopped` means even the scheduler is down
/// (fully cold); `Suspended` keeps the scheduler up with zero executors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarehouseState {
    Running,
    Suspended,
    Stopped,
}

impl WarehouseState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Suspended => "SUSPENDED",
            Self::Stopped => "STOPPED",
        }
    }

    #[must_use]
    pub fn parse(state: &str) -> Option<Self> {
        match state.to_ascii_uppercase().as_str() {
            "RUNNING" | "STARTED" => Some(Self::Running),
            "SUSPENDED" => Some(Self::Suspended),
            "STOPPED" => Some(Self::Stopped),
            _ => None,
        }
    }
}

impl std::fmt::Display for WarehouseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The full warehouse definition (persisted + runtime view).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Warehouse {
    pub name: String,
    pub size: WarehouseSize,
    pub state: WarehouseState,
    /// Idle seconds before auto-suspend; `None`/`Some(0)` = never suspend.
    pub auto_suspend_secs: Option<u64>,
    pub auto_resume: bool,
    pub created_at: u64,
    /// Display-safe storage-credential summary (M7.1): never contains secret
    /// values; `None` = no scoped storage credentials.
    pub storage_summary: Option<String>,
    /// Mutual TLS (M7.2): compute started with `AVALANCHE_TLS_DIR`, scheduler
    /// advertised over `https://`.
    pub tls: bool,
    /// Number of scheduler replicas sharing the warehouse's cluster state
    /// (M7.3): >1 means the warehouse survives one scheduler's death.
    pub scheduler_replicas: u16,
    /// Multi-cluster warehouses (M12.2, design §19.1): independent
    /// scheduler+executor clusters behind one alias. `min_clusters` run;
    /// `max_clusters` scale-out is parsed/stored but dormant in v1.
    pub min_clusters: u16,
    pub max_clusters: u16,
}

impl Warehouse {
    #[must_use]
    pub const fn executor_count(&self) -> u32 {
        match self.state {
            WarehouseState::Running => self.size.executor_count(),
            WarehouseState::Suspended | WarehouseState::Stopped => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_parses_snowflake_spellings() {
        assert_eq!(WarehouseSize::parse("XSMALL"), Some(WarehouseSize::Xsmall));
        assert_eq!(WarehouseSize::parse("x-small"), Some(WarehouseSize::Xsmall));
        assert_eq!(WarehouseSize::parse("4xlarge"), Some(WarehouseSize::FourXlarge));
        assert_eq!(WarehouseSize::parse("2XL"), Some(WarehouseSize::TwoXlarge));
        assert_eq!(WarehouseSize::parse("DOUBLE"), Some(WarehouseSize::TwoXlarge));
        assert_eq!(WarehouseSize::parse("jumbo"), None);
    }

    #[test]
    fn executor_ladder_doubles() {
        assert_eq!(WarehouseSize::Xsmall.executor_count(), 1);
        assert_eq!(WarehouseSize::Small.executor_count(), 2);
        assert_eq!(WarehouseSize::Medium.executor_count(), 4);
        assert_eq!(WarehouseSize::FourXlarge.executor_count(), 128);
    }

    #[test]
    fn state_round_trip() {
        for state in [
            WarehouseState::Running,
            WarehouseState::Suspended,
            WarehouseState::Stopped,
        ] {
            assert_eq!(WarehouseState::parse(state.as_str()), Some(state));
        }
    }
}
