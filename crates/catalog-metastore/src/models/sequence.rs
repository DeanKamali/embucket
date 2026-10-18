//! Sequence objects (design §22.8, M16.4 deferred item).
//!
//! Sequences share the schema object namespace (keyed by [`TableIdent`],
//! where the `table` field carries the sequence name) and live in the
//! metastore alongside streams/tasks, so journal replay and restart
//! durability cover them.
//!
//! The `current` field is the metastore-side *reservation cursor*: it is
//! advanced by `NEXTVAL` evaluation in whole reservation chunks (see the
//! executor's sequence module), so restarts continue after the last chunk
//! handed out — matching Snowflake's documented CACHE behavior (values
//! reserved but not delivered are skipped, never reused, and never roll
//! back with the enclosing transaction).

use serde::{Deserialize, Serialize};

use super::TableIdent;

/// A named number generator (Snowflake `CREATE SEQUENCE`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Sequence {
    pub ident: TableIdent,
    /// First value the sequence delivers.
    pub start: i64,
    /// Step between successive values.
    pub increment: i64,
    /// Last delivered-or-reserved value. Initialized to `start - increment`
    /// so the first `NEXTVAL` yields `start`.
    pub current: i64,
    pub comment: Option<String>,
}

impl Sequence {
    /// The value the *next* reservation would deliver first.
    #[must_use]
    pub fn next(&self) -> i64 {
        self.current + self.increment
    }
}