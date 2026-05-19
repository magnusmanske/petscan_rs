//! Shared test helpers, gated behind `#[cfg(test)]` so they never enter the
//! production binary.
//!
//! Before this module existed, `make_platform` (build a `Platform` from
//! `(&str, &str)` form-parameter pairs over a default `AppState`) was
//! copy-pasted across 11 test modules. Centralising it means a single
//! edit propagates everywhere (P5 #37).
//!
//! `state_with_config` and the network-bound `get_state` / `get_new_state`
//! helpers stay in `app_state.rs::tests` where they need access to private
//! struct fields.

use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use crate::platform::Platform;
use std::collections::HashMap;
use std::sync::Arc;

/// Build a `Platform` for tests from a vec of `(key, value)` form-parameter
/// pairs. The platform's `AppState` is `Default::default()` — no network,
/// no DB, no SiteMatrix. Suitable for any test that doesn't call
/// `platform.run()` or anything that reaches the real DB/API.
pub(crate) fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
    let mut params = HashMap::new();
    for (k, v) in pairs {
        params.insert(k.to_string(), v.to_string());
    }
    let fp = FormParameters::new_from_pairs(params);
    Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
}
