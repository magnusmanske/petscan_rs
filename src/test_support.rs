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
use crate::render::NamespaceContext;
use std::collections::HashMap;
use std::sync::Arc;
use wikimisc::mediawiki::api::NamespaceID;
use wikimisc::mediawiki::title::Title;

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

/// In-memory [`NamespaceContext`] for tests — no network. Holds a
/// `HashMap<NamespaceID, (local, canonical)>` and synthesises
/// `full_pretty` / `full_with_underscores` from the title plus the
/// looked-up namespace name. Returning `None` for an unknown
/// namespace id matches the upstream `Api` behaviour.
#[derive(Debug, Default)]
pub(crate) struct StubNamespaceContext {
    /// `namespace_id → (local_name, canonical_name)`.
    pub namespaces: HashMap<NamespaceID, (String, String)>,
}

impl StubNamespaceContext {
    /// Builder convenience — adds one namespace mapping and returns self.
    pub fn with(mut self, id: NamespaceID, local: &str, canonical: &str) -> Self {
        self.namespaces
            .insert(id, (local.to_string(), canonical.to_string()));
        self
    }

    /// A pre-populated stub mirroring the most common Wikipedia namespaces
    /// so tests don't have to enumerate them every time.
    pub fn enwiki() -> Self {
        Self::default()
            .with(0, "", "")
            .with(1, "Talk", "Talk")
            .with(2, "User", "User")
            .with(3, "User talk", "User talk")
            .with(6, "File", "File")
            .with(10, "Template", "Template")
            .with(14, "Category", "Category")
    }
}

impl NamespaceContext for StubNamespaceContext {
    fn local_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str> {
        self.namespaces
            .get(&namespace_id)
            .map(|(local, _)| local.as_str())
    }

    fn canonical_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str> {
        self.namespaces
            .get(&namespace_id)
            .map(|(_, canonical)| canonical.as_str())
    }

    fn full_pretty(&self, title: &Title) -> Option<String> {
        let ns = self.local_namespace_name(title.namespace_id())?;
        Some(if ns.is_empty() {
            title.pretty().to_string()
        } else {
            format!("{ns}:{}", title.pretty())
        })
    }

    fn full_with_underscores(&self, title: &Title) -> Option<String> {
        let ns = self.local_namespace_name(title.namespace_id())?;
        Some(if ns.is_empty() {
            title.with_underscores()
        } else {
            format!("{}:{}", ns.replace(' ', "_"), title.with_underscores())
        })
    }

    fn for_each_local_namespace(&self, f: &mut dyn FnMut(&str, &str)) {
        for (id, (local, _)) in &self.namespaces {
            f(&id.to_string(), local);
        }
    }
}
