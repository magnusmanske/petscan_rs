//! Narrow view of [`Platform`](crate::platform::Platform) consumed by
//! [`PageList`](crate::pagelist::PageList).
//!
//! `PageList` only needs two things from `Platform`:
//!   - access to the application state (for DB connections and the upstream
//!     `Api` per wiki);
//!   - a parameter-presence check (the `wdf_main` / `rxp_filter` short-circuit
//!     in `load_missing_metadata`).
//!
//! Routing those through a trait keeps `pagelist.rs` from transitively
//! importing the whole `Platform` god-object's surface, and lets tests inject
//! a stub instead of standing up a real `Platform` (which itself needs a live
//! Wikidata `Api` handshake). Mirrors the pattern P5 #37 used for renderers.

use crate::app_state::AppState;
use std::sync::Arc;

pub trait QueryContext: Send + Sync {
    /// Application state for database access.
    fn state(&self) -> Arc<AppState>;

    /// True iff the form parameter `key` is set and non-empty. Mirrors
    /// [`Platform::has_param`](crate::platform::Platform::has_param);
    /// callers that want a different semantic (e.g. key-presence regardless
    /// of value) should not piggy-back on this method.
    fn has_param(&self, key: &str) -> bool;
}
