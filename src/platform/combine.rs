use crate::combination::{Combination, CombinationSequential};
use crate::key_set::{KeySet, KeySetLimits};
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

pub(super) static RE_PARSE_COMBINATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\w+(?:'\w+)?|[^\w\s]")
        .expect("Platform::parse_combination_string: Regex is invalid")
});

/// One step of the combination stack machine. `needs_entries` is false when the
/// value is only ever used for page identity (the subtracted or
/// intersected-away side), which is what allows it to spill to disk.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CombineStep {
    pub instruction: CombinationSequential,
    pub needs_entries: bool,
}

/// A data source's result as it enters the combination. Key-only sources may
/// have streamed their rows straight to disk and never built a page list.
#[derive(Debug)]
pub(super) enum SourceResult {
    Entries(PageList),
    Keys(KeySet),
}

impl SourceResult {
    pub(super) fn wiki(&self) -> Option<String> {
        match self {
            Self::Entries(list) => list.wiki(),
            Self::Keys(keys) => keys.wiki(),
        }
    }
}

impl Platform {
    /// Maps each source that should run seeded to the source that supplies its
    /// seed. Only an intersection of two single-source branches is planned;
    /// other shapes keep the materialise-then-combine path.
    ///
    /// `priorities` gives [`crate::datasource::DataSource::seed_priority`] per
    /// source. A lower value is the more expensive side and therefore the
    /// preferred target for a seed.
    pub(super) fn plan_seeds(
        combination: &Combination,
        priorities: &HashMap<String, Option<u8>>,
    ) -> HashMap<String, String> {
        let mut plan = HashMap::new();
        Self::collect_seed_candidates(combination, priorities, &mut plan);
        // A seed must itself run eagerly. Drop chained targets (A seeded by B,
        // B seeded by C) until only eager seeds remain; a cycle drops entirely.
        loop {
            let deferred: HashSet<String> = plan.keys().cloned().collect();
            let before = plan.len();
            plan.retain(|_, seed| !deferred.contains(seed));
            if plan.len() == before {
                break;
            }
        }
        plan
    }

    fn collect_seed_candidates(
        node: &Combination,
        priorities: &HashMap<String, Option<u8>>,
        plan: &mut HashMap<String, String>,
    ) {
        match node {
            Combination::Intersection((left, right)) => {
                if let (Combination::Source(a), Combination::Source(b)) =
                    (left.as_ref(), right.as_ref())
                    && a != b
                {
                    let pa = priorities.get(a).copied().flatten();
                    let pb = priorities.get(b).copied().flatten();
                    let target_seed = match (pa, pb) {
                        (Some(pa), Some(pb)) if pa <= pb => Some((a, b)),
                        (Some(_), Some(_)) => Some((b, a)),
                        (Some(_), None) => Some((a, b)),
                        (None, Some(_)) => Some((b, a)),
                        (None, None) => None,
                    };
                    if let Some((target, seed)) = target_seed {
                        plan.entry(target.clone()).or_insert_with(|| seed.clone());
                    }
                }
                Self::collect_seed_candidates(left, priorities, plan);
                Self::collect_seed_candidates(right, priorities, plan);
            }
            Combination::Union((left, right)) | Combination::Not((left, right)) => {
                Self::collect_seed_candidates(left, priorities, plan);
                Self::collect_seed_candidates(right, priorities, plan);
            }
            Combination::Source(_) | Combination::None => {}
        }
    }

    /// Extracts the content between the leading `(` and matching `)` from `parts`,
    /// consuming tokens from `parts` as it goes. Returns `None` on unbalanced parens.
    fn extract_parenthesized(parts: &mut Vec<String>) -> Option<String> {
        let mut depth: usize = 0;
        let mut inner: Vec<String> = vec![];
        loop {
            let token = parts.first()?.clone();
            parts.remove(0);
            match token.as_str() {
                "(" => {
                    if depth > 0 {
                        inner.push(token);
                    }
                    depth += 1;
                }
                ")" => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(inner.join(" "));
                    }
                    inner.push(token);
                }
                _ => inner.push(token),
            }
        }
    }

    pub(super) fn parse_combination_string(s: &str) -> Combination {
        match s.trim().to_lowercase().as_str() {
            "" => return Combination::None,
            "categories" | "sparql" | "manual" | "pagepile" | "wikidata" | "search" => {
                return Combination::Source(s.to_string());
            }
            _ => {}
        }
        let mut parts: Vec<String> = RE_PARSE_COMBINATION
            .captures_iter(s)
            .filter_map(|cap| cap.get(0))
            .map(|s| s.as_str().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if parts.len() < 3 {
            return Combination::None;
        }

        let left = if parts.first().map(|s2| s2.as_str()) == Some("(") {
            match Self::extract_parenthesized(&mut parts) {
                Some(inner) => inner,
                None => return Combination::None,
            }
        } else {
            parts.remove(0)
        };

        if parts.is_empty() {
            return Self::parse_combination_string(&left);
        }
        let comb = parts.remove(0);
        let left = Box::new(Self::parse_combination_string(&left));
        let rest = Box::new(Self::parse_combination_string(&parts.join(" ")));
        match comb.trim().to_lowercase().as_str() {
            "and" => Combination::Intersection((left, rest)),
            "or" => Combination::Union((left, rest)),
            "not" => Combination::Not((left, rest)),
            _ => Combination::None,
        }
    }

    pub(super) fn get_combination(&self, available_sources: &[String]) -> Combination {
        match self.get_param("source_combination") {
            Some(combination_string) => Self::parse_combination_string(&combination_string),
            None => {
                let mut comb = Combination::None;
                for source in available_sources {
                    if comb == Combination::None {
                        comb = Combination::Source(source.to_string());
                    } else {
                        comb = Combination::Intersection((
                            Box::new(Combination::Source(source.to_string())),
                            Box::new(comb),
                        ));
                    }
                }
                comb
            }
        }
    }

    /// Serializes a two-child combination node (Intersection / Union / Not).
    /// For `None`-child shortcuts:
    ///   - `allow_none_right`: if the right child is `None`, return just the left serialization.
    ///   - Any `None` child that is not covered by the above returns `Err`.
    fn serialize_binary_combination(
        a: &Combination,
        b: &Combination,
        op: CombinationSequential,
        needs_entries: bool,
        allow_none_right: bool,
        steps: &mut Vec<CombineStep>,
    ) -> Result<()> {
        match (a, b) {
            (Combination::None, _) => Err(anyhow!("{op:?} with left Combination::None found")),
            (c, Combination::None) if allow_none_right => {
                Self::push_combine_steps(c, needs_entries, steps)
            }
            (_, Combination::None) => Err(anyhow!("{op:?} with right Combination::None found")),
            (c, d) => {
                Self::push_combine_steps(c, needs_entries, steps)?;
                // The right side of an intersection or difference only supplies
                // membership, so it never needs to keep its entries.
                Self::push_combine_steps(d, false, steps)?;
                steps.push(CombineStep {
                    instruction: op,
                    needs_entries,
                });
                Ok(())
            }
        }
    }

    /// Turns the combination tree into stack-machine steps, recording for each
    /// value whether anything needs its metadata. Only the final result and the
    /// retained side of an intersection/difference do; everything else is used
    /// for page identity and can spill to disk.
    pub(super) fn serialize_combine_results(combination: &Combination) -> Result<Vec<CombineStep>> {
        let mut steps = vec![];
        Self::push_combine_steps(combination, true, &mut steps)?;
        Ok(steps)
    }

    fn push_combine_steps(
        combination: &Combination,
        needs_entries: bool,
        steps: &mut Vec<CombineStep>,
    ) -> Result<()> {
        match combination {
            Combination::Source(s) => steps.push(CombineStep {
                instruction: CombinationSequential::Source(s.to_string()),
                needs_entries,
            }),
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                // Either side being None collapses the Union to the other side
                (Combination::None, c) | (c, Combination::None) => {
                    Self::push_combine_steps(c, needs_entries, steps)?;
                }
                (c, d) => {
                    Self::push_combine_steps(c, needs_entries, steps)?;
                    Self::push_combine_steps(d, needs_entries, steps)?;
                    steps.push(CombineStep {
                        instruction: CombinationSequential::Union,
                        needs_entries,
                    });
                }
            },
            Combination::Intersection((a, b)) => Self::serialize_binary_combination(
                a.as_ref(),
                b.as_ref(),
                CombinationSequential::Intersection,
                needs_entries,
                false,
                steps,
            )?,
            Combination::Not((a, b)) => Self::serialize_binary_combination(
                a.as_ref(),
                b.as_ref(),
                CombinationSequential::Not,
                needs_entries,
                true, // Not(x, None) => just x
                steps,
            )?,
            Combination::None => return Err(anyhow!("Combination::None found")),
        }
        Ok(())
    }

    /// Pops two registers and returns `(r1, r2)`, or an error if fewer than 2 are available.
    fn pop_two<T>(registers: &mut Vec<T>, op_name: &str) -> Result<(T, T)> {
        if registers.len() < 2 {
            return Err(anyhow!(
                "combine_results: Not enough registers for {op_name}"
            ));
        }
        let r2 = registers
            .pop()
            .ok_or_else(|| anyhow!("combine_results: {op_name} pop r2"))?;
        let r1 = registers
            .pop()
            .ok_or_else(|| anyhow!("combine_results: {op_name} pop r1"))?;
        Ok((r1, r2))
    }

    pub(super) async fn combine_results(
        &self,
        results: &mut HashMap<String, SourceResult>,
        combination: Vec<CombineStep>,
    ) -> Result<PageList> {
        self.combine_results_with_limits(results, combination, KeySetLimits::from_env())
            .await
    }

    async fn combine_results_with_limits(
        &self,
        results: &mut HashMap<String, SourceResult>,
        combination: Vec<CombineStep>,
        limits: KeySetLimits,
    ) -> Result<PageList> {
        let mut registers: Vec<Combined> = vec![];
        for step in combination {
            let combined = match step.instruction {
                CombinationSequential::Source(source_key) => {
                    let result = results
                        .remove(&source_key)
                        .ok_or_else(|| anyhow!("No result for source {source_key}"))?;
                    match (step.needs_entries, result) {
                        (true, SourceResult::Entries(list)) => Combined::Entries(list),
                        (false, SourceResult::Keys(keys)) => Combined::Keys(keys),
                        // A seeded target or a source without a streaming path
                        // can still arrive as entries; it is converted to keys
                        // lazily by the operation that consumes it.
                        (false, SourceResult::Entries(list)) => Combined::Entries(list),
                        (true, SourceResult::Keys(_)) => {
                            return Err(anyhow!(
                                "combine_results: source {source_key} is key-only but its entries are needed"
                            ));
                        }
                    }
                }
                CombinationSequential::Union => {
                    let (left, right) = Self::pop_two(&mut registers, "Union")?;
                    self.combine_union(left, right, step.needs_entries, limits)
                        .await?
                }
                CombinationSequential::Intersection => {
                    let (left, right) = Self::pop_two(&mut registers, "Intersection")?;
                    self.combine_filter(left, right, true, step.needs_entries, limits)
                        .await?
                }
                CombinationSequential::Not => {
                    let (left, right) = Self::pop_two(&mut registers, "Not")?;
                    self.combine_filter(left, right, false, step.needs_entries, limits)
                        .await?
                }
            };
            registers.push(combined);
        }
        if registers.len() == 1 {
            return match registers.pop() {
                Some(Combined::Entries(result)) => Ok(result),
                Some(Combined::Keys(..)) => Err(anyhow!("combine_results: result is key-only")),
                None => Err(anyhow!("combine_results: registers unexpectedly empty")),
            };
        }
        Err(anyhow!(
            "combine_results: {} registers set",
            registers.len()
        ))
    }

    async fn combine_union(
        &self,
        left: Combined,
        right: Combined,
        needs_entries: bool,
        limits: KeySetLimits,
    ) -> Result<Combined> {
        if needs_entries {
            let (Combined::Entries(left), Combined::Entries(right)) = (left, right) else {
                return Err(anyhow!(
                    "combine_results: Union lost the entry side of its result"
                ));
            };
            left.union(right, Some(self)).await?;
            return Ok(Combined::Entries(left));
        }
        let left = Self::into_keys(left, limits).await?;
        let right = Self::into_keys(right, limits).await?;
        let (left, right) = self.align_key_wikis(left, right, limits).await?;
        let keys = run_blocking(move || left.union(right, limits)).await?;
        Ok(Combined::Keys(keys))
    }

    /// Intersection (`keep`) and difference (`!keep`). The retained side keeps
    /// its entries; the other side is only consulted for membership, so it may
    /// be a key set on disk.
    async fn combine_filter(
        &self,
        left: Combined,
        right: Combined,
        keep: bool,
        needs_entries: bool,
        limits: KeySetLimits,
    ) -> Result<Combined> {
        if needs_entries {
            let Combined::Entries(entries) = left else {
                return Err(anyhow!(
                    "combine_results: set operation lost the entry side of its result"
                ));
            };
            match right {
                Combined::Entries(other) if keep => {
                    entries.intersection(other, Some(self)).await?;
                }
                Combined::Entries(other) => {
                    entries.difference(other, Some(self)).await?;
                }
                Combined::Keys(mut keys) => {
                    if let Some(target) = entries.wiki() {
                        keys.convert_to_wiki(&target, self, limits).await?;
                    }
                    entries.retain_by_keys(keys, keep).await?;
                }
            }
            return Ok(Combined::Entries(entries));
        }
        let left = Self::into_keys(left, limits).await?;
        let right = Self::into_keys(right, limits).await?;
        let (left, right) = self.align_key_wikis(left, right, limits).await?;
        let keys = run_blocking(move || {
            if keep {
                left.intersect(right, limits)
            } else {
                left.difference(right, limits)
            }
        })
        .await?;
        Ok(Combined::Keys(keys))
    }

    /// Both key operands must be on one wiki before their identities are
    /// compared. Converting is a no-op for the common same-wiki case.
    async fn align_key_wikis(
        &self,
        mut left: KeySet,
        mut right: KeySet,
        limits: KeySetLimits,
    ) -> Result<(KeySet, KeySet)> {
        if let Some(target) = left.wiki().or_else(|| right.wiki()) {
            left.convert_to_wiki(&target, self, limits).await?;
            right.convert_to_wiki(&target, self, limits).await?;
        }
        Ok((left, right))
    }

    /// Converts a register to keys, whether it still holds entries or already
    /// spilled.
    async fn into_keys(value: Combined, limits: KeySetLimits) -> Result<KeySet> {
        match value {
            Combined::Keys(keys) => Ok(keys),
            Combined::Entries(list) => {
                run_blocking(move || KeySet::from_pagelist(list, limits)).await
            }
        }
    }
}

/// A combination register: either a full page list (metadata preserved) or a
/// set of page identities, possibly spilled to disk.
enum Combined {
    Entries(PageList),
    Keys(KeySet),
}

/// Runs blocking file I/O off the async runtime.
async fn run_blocking<T: Send + 'static>(
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(job)
        .await
        .map_err(|error| anyhow!("{error}"))?
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    #[test]
    fn test_parse_combination_string_empty() {
        assert_eq!(Platform::parse_combination_string(""), Combination::None);
        assert_eq!(Platform::parse_combination_string("   "), Combination::None);
    }

    #[test]
    fn test_parse_combination_string_single_source() {
        assert_eq!(
            Platform::parse_combination_string("categories"),
            Combination::Source("categories".to_string())
        );
        assert_eq!(
            Platform::parse_combination_string("sparql"),
            Combination::Source("sparql".to_string())
        );
        assert_eq!(
            Platform::parse_combination_string("manual"),
            Combination::Source("manual".to_string())
        );
        assert_eq!(
            Platform::parse_combination_string("pagepile"),
            Combination::Source("pagepile".to_string())
        );
        assert_eq!(
            Platform::parse_combination_string("wikidata"),
            Combination::Source("wikidata".to_string())
        );
        assert_eq!(
            Platform::parse_combination_string("search"),
            Combination::Source("search".to_string())
        );
    }

    #[test]
    fn test_parse_combination_string_and() {
        let res = Platform::parse_combination_string("categories AND sparql");
        let expected = Combination::Intersection((
            Box::new(Combination::Source("categories".to_string())),
            Box::new(Combination::Source("sparql".to_string())),
        ));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_combination_string_or() {
        let res = Platform::parse_combination_string("manual OR pagepile");
        let expected = Combination::Union((
            Box::new(Combination::Source("manual".to_string())),
            Box::new(Combination::Source("pagepile".to_string())),
        ));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_combination_string_not() {
        let res = Platform::parse_combination_string("categories NOT sparql");
        let expected = Combination::Not((
            Box::new(Combination::Source("categories".to_string())),
            Box::new(Combination::Source("sparql".to_string())),
        ));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_combination_string_nested() {
        let res = Platform::parse_combination_string("categories NOT (sparql OR pagepile)");
        let expected = Combination::Not((
            Box::new(Combination::Source("categories".to_string())),
            Box::new(Combination::Union((
                Box::new(Combination::Source("sparql".to_string())),
                Box::new(Combination::Source("pagepile".to_string())),
            ))),
        ));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_combination_string_too_short() {
        // Less than 3 tokens → None
        assert_eq!(
            Platform::parse_combination_string("categories AND"),
            Combination::None
        );
    }

    #[test]
    fn test_get_combination_single_source() {
        let p = make_platform(vec![]);
        let comb = p.get_combination(&["categories".to_string()]);
        assert_eq!(comb, Combination::Source("categories".to_string()));
    }

    #[test]
    fn test_get_combination_two_sources_default_intersection() {
        let p = make_platform(vec![]);
        let comb = p.get_combination(&["categories".to_string(), "sparql".to_string()]);
        // Default (no source_combination param) → intersection
        let expected = Combination::Intersection((
            Box::new(Combination::Source("sparql".to_string())),
            Box::new(Combination::Source("categories".to_string())),
        ));
        assert_eq!(comb, expected);
    }

    #[test]
    fn test_get_combination_from_param() {
        let p = make_platform(vec![("source_combination", "manual OR pagepile")]);
        let comb = p.get_combination(&["manual".to_string(), "pagepile".to_string()]);
        let expected = Combination::Union((
            Box::new(Combination::Source("manual".to_string())),
            Box::new(Combination::Source("pagepile".to_string())),
        ));
        assert_eq!(comb, expected);
    }

    fn ops(steps: &[CombineStep]) -> Vec<CombinationSequential> {
        steps.iter().map(|step| step.instruction.clone()).collect()
    }

    fn needs(steps: &[CombineStep]) -> Vec<bool> {
        steps.iter().map(|step| step.needs_entries).collect()
    }

    #[test]
    fn test_serialize_combine_results_single_source() {
        let comb = Combination::Source("categories".to_string());
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            ops(&result),
            vec![CombinationSequential::Source("categories".to_string())]
        );
        assert_eq!(needs(&result), vec![true]);
    }

    #[test]
    fn test_serialize_combine_results_intersection() {
        let comb = Combination::Intersection((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            ops(&result),
            vec![
                CombinationSequential::Source("a".to_string()),
                CombinationSequential::Source("b".to_string()),
                CombinationSequential::Intersection,
            ]
        );
        // Only the retained side needs its metadata; the subtracted side is
        // used for membership alone.
        assert_eq!(needs(&result), vec![true, false, true]);
    }

    #[test]
    fn test_serialize_combine_results_union() {
        let comb = Combination::Union((
            Box::new(Combination::Source("x".to_string())),
            Box::new(Combination::Source("y".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            ops(&result),
            vec![
                CombinationSequential::Source("x".to_string()),
                CombinationSequential::Source("y".to_string()),
                CombinationSequential::Union,
            ]
        );
        assert_eq!(needs(&result), vec![true, true, true]);
    }

    #[test]
    fn test_serialize_combine_results_not() {
        let comb = Combination::Not((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            ops(&result),
            vec![
                CombinationSequential::Source("a".to_string()),
                CombinationSequential::Source("b".to_string()),
                CombinationSequential::Not,
            ]
        );
        assert_eq!(needs(&result), vec![true, false, true]);
    }

    #[test]
    fn test_serialize_combine_results_key_only_union() {
        // A AND (B OR C): the union only feeds membership, so neither B nor C
        // needs its entries.
        let comb = Combination::Intersection((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Union((
                Box::new(Combination::Source("b".to_string())),
                Box::new(Combination::Source("c".to_string())),
            ))),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(needs(&result), vec![true, false, false, false, true]);
        assert_eq!(
            ops(&result),
            vec![
                CombinationSequential::Source("a".to_string()),
                CombinationSequential::Source("b".to_string()),
                CombinationSequential::Source("c".to_string()),
                CombinationSequential::Union,
                CombinationSequential::Intersection,
            ]
        );
    }

    #[test]
    fn test_serialize_combine_results_entry_side_union() {
        // (A OR B) AND C: the union is the retained side, so A and B keep
        // their entries and only C is key-only.
        let comb = Combination::Intersection((
            Box::new(Combination::Union((
                Box::new(Combination::Source("a".to_string())),
                Box::new(Combination::Source("b".to_string())),
            ))),
            Box::new(Combination::Source("c".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(needs(&result), vec![true, true, true, false, true]);
    }

    #[test]
    fn test_serialize_combine_results_none_errors() {
        let comb = Combination::None;
        assert!(Platform::serialize_combine_results(&comb).is_err());
    }

    #[test]
    fn test_serialize_combine_results_union_with_none_collapses() {
        // Union(None, X) → just X
        let comb = Combination::Union((
            Box::new(Combination::None),
            Box::new(Combination::Source("cats".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            ops(&result),
            vec![CombinationSequential::Source("cats".to_string())]
        );
        assert_eq!(needs(&result), vec![true]);
    }

    #[test]
    fn test_serialize_combine_results_intersection_with_none_errors() {
        let comb = Combination::Intersection((
            Box::new(Combination::None),
            Box::new(Combination::Source("cats".to_string())),
        ));
        assert!(Platform::serialize_combine_results(&comb).is_err());
    }

    #[test]
    fn test_extract_parenthesized_simple() {
        let mut parts = vec![
            "(".to_string(),
            "sparql".to_string(),
            "OR".to_string(),
            "pagepile".to_string(),
            ")".to_string(),
            "AND".to_string(),
            "cats".to_string(),
        ];
        let inner = Platform::extract_parenthesized(&mut parts).unwrap();
        assert_eq!(inner, "sparql OR pagepile");
        // Remaining parts should be the ones after the closing paren
        assert_eq!(parts, vec!["AND", "cats"]);
    }

    #[test]
    fn test_extract_parenthesized_nested() {
        let mut parts = vec![
            "(".to_string(),
            "(".to_string(),
            "a".to_string(),
            ")".to_string(),
            "OR".to_string(),
            "b".to_string(),
            ")".to_string(),
        ];
        let inner = Platform::extract_parenthesized(&mut parts).unwrap();
        assert_eq!(inner, "( a ) OR b");
        assert!(parts.is_empty());
    }

    #[test]
    fn test_extract_parenthesized_empty_returns_none_on_missing_open() {
        let mut parts: Vec<String> = vec![];
        let result = Platform::extract_parenthesized(&mut parts);
        assert!(result.is_none());
    }

    // ── seed planning ────────────────────────────────────────────────────────

    fn source(name: &str) -> Combination {
        Combination::Source(name.to_string())
    }

    fn intersect(a: Combination, b: Combination) -> Combination {
        Combination::Intersection((Box::new(a), Box::new(b)))
    }

    fn union(a: Combination, b: Combination) -> Combination {
        Combination::Union((Box::new(a), Box::new(b)))
    }

    fn priorities(entries: &[(&str, Option<u8>)]) -> HashMap<String, Option<u8>> {
        entries
            .iter()
            .map(|(name, priority)| ((*name).to_string(), *priority))
            .collect()
    }

    #[test]
    fn plan_seeds_database_from_non_seedable_side() {
        let p = priorities(&[("categories", Some(0)), ("manual", None)]);
        for tree in [
            intersect(source("categories"), source("manual")),
            intersect(source("manual"), source("categories")),
        ] {
            let plan = Platform::plan_seeds(&tree, &p);
            assert_eq!(plan.len(), 1);
            assert_eq!(plan.get("categories"), Some(&"manual".to_string()));
        }
    }

    #[test]
    fn plan_seeds_prefers_the_expensive_side() {
        // Both can be seeded; the lower priority value (categories) is the
        // expensive side and should be restricted by the other.
        let p = priorities(&[("categories", Some(0)), ("wikidata", Some(1))]);
        for tree in [
            intersect(source("categories"), source("wikidata")),
            intersect(source("wikidata"), source("categories")),
        ] {
            let plan = Platform::plan_seeds(&tree, &p);
            assert_eq!(plan.get("categories"), Some(&"wikidata".to_string()));
        }
    }

    #[test]
    fn plan_seeds_needs_one_seedable_side() {
        let p = priorities(&[("sparql", None), ("manual", None)]);
        let tree = intersect(source("sparql"), source("manual"));
        assert!(Platform::plan_seeds(&tree, &p).is_empty());
    }

    #[test]
    fn plan_seeds_drops_chained_targets() {
        // (a ∩ b) ∩ (b ∩ c): the inner plans are a←b and b←c. b is itself
        // deferred, so a←b must be dropped and a runs eagerly.
        let p = priorities(&[("a", Some(0)), ("b", Some(1)), ("c", Some(1))]);
        let tree = intersect(
            intersect(source("a"), source("b")),
            intersect(source("b"), source("c")),
        );
        let plan = Platform::plan_seeds(&tree, &p);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.get("b"), Some(&"c".to_string()));
    }

    #[test]
    fn plan_seeds_recurses_into_union() {
        let p = priorities(&[("categories", Some(0)), ("manual", None), ("search", None)]);
        let tree = union(
            intersect(source("categories"), source("manual")),
            source("search"),
        );
        let plan = Platform::plan_seeds(&tree, &p);
        assert_eq!(plan.get("categories"), Some(&"manual".to_string()));
    }

    #[test]
    fn plan_seeds_ignores_self_intersection() {
        let p = priorities(&[("categories", Some(0))]);
        let tree = intersect(source("categories"), source("categories"));
        assert!(Platform::plan_seeds(&tree, &p).is_empty());
    }

    // ── spill-aware combination ──────────────────────────────────────────────

    use crate::pagelist_entry::PageListEntry;
    use wikimisc::mediawiki::title::Title;

    fn page_list(titles: &[&str]) -> PageList {
        let list = PageList::new_from_wiki("enwiki");
        for title in titles {
            let mut entry = PageListEntry::new(Title::new(title, 0));
            entry.set_page_id(Some(title.len() as u32));
            list.add_entry(entry);
        }
        list
    }

    fn titles(list: &PageList) -> Vec<String> {
        let mut titles: Vec<String> = list
            .as_vec()
            .iter()
            .map(|entry| entry.title().pretty().to_string())
            .collect();
        titles.sort();
        titles
    }

    fn results(entries: Vec<(&str, PageList)>) -> HashMap<String, SourceResult> {
        entries
            .into_iter()
            .map(|(name, list)| (name.to_string(), SourceResult::Entries(list)))
            .collect()
    }

    /// A right side above the spill threshold takes the disk-backed path and
    /// still keeps the left side's entries and metadata.
    #[tokio::test]
    async fn combine_intersection_spills_the_right_side() {
        let left = page_list(&["A", "B", "C", "D"]);
        let right = page_list(&["B", "D", "E", "F", "G", "H"]);
        let combination = intersect(source("left"), source("right"));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = results(vec![("left", left), ("right", right)]);
        let platform = make_platform(vec![]);
        let limit = KeySetLimits { threshold: 2 };

        let combined = platform
            .combine_results_with_limits(&mut results, steps, limit)
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["B", "D"]);
        // The left side survives with its own metadata, not a rehydrated copy.
        assert!(
            combined
                .as_vec()
                .iter()
                .all(|entry| entry.page_id().is_some())
        );
    }

    /// `A NOT B` removes B's pages even when B is merged from several runs.
    #[tokio::test]
    async fn combine_difference_spills_the_right_side() {
        let left = page_list(&["A", "B", "C", "D"]);
        let right = page_list(&["B", "X", "Y", "D", "Z", "W"]);
        let combination = Combination::Not((Box::new(source("left")), Box::new(source("right"))));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = results(vec![("left", left), ("right", right)]);
        let platform = make_platform(vec![]);

        let combined = platform
            .combine_results_with_limits(&mut results, steps, KeySetLimits { threshold: 2 })
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["A", "C"]);
    }

    /// The right side of an intersection may itself be a union, which is
    /// evaluated entirely in key mode.
    #[tokio::test]
    async fn combine_intersection_with_spilled_union() {
        let left = page_list(&["A", "B", "C", "D"]);
        let b = page_list(&["A", "B", "X", "Y", "Z"]);
        let c = page_list(&["B", "C", "Q", "R", "S"]);
        let combination = intersect(source("left"), union(source("b"), source("c")));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = results(vec![("left", left), ("b", b), ("c", c)]);
        let platform = make_platform(vec![]);

        let combined = platform
            .combine_results_with_limits(&mut results, steps, KeySetLimits { threshold: 2 })
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["A", "B", "C"]);
    }

    /// Below the threshold everything stays in memory and matches the old path.
    #[tokio::test]
    async fn combine_small_lists_keep_the_in_memory_path() {
        let left = page_list(&["A", "B", "C"]);
        let right = page_list(&["B", "C", "D"]);
        let combination = intersect(source("left"), source("right"));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = results(vec![("left", left), ("right", right)]);
        let platform = make_platform(vec![]);

        let combined = platform
            .combine_results_with_limits(&mut results, steps, KeySetLimits { threshold: 1000 })
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["B", "C"]);
    }

    /// The right side may already be a spilled key set, as it is when the
    /// source streamed its rows. The retained side still supplies the entries.
    #[tokio::test]
    async fn combine_intersection_uses_pre_streamed_keys() {
        let left = page_list(&["A", "B", "C", "D"]);
        let right = KeySet::from_pagelist(
            page_list(&["B", "D", "E", "F"]),
            KeySetLimits { threshold: 1 },
        )
        .unwrap();
        assert!(right.is_spilled());
        let combination = intersect(source("left"), source("right"));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = HashMap::from([
            ("left".to_string(), SourceResult::Entries(left)),
            ("right".to_string(), SourceResult::Keys(right)),
        ]);
        let platform = make_platform(vec![]);

        let combined = platform
            .combine_results_with_limits(&mut results, steps, KeySetLimits { threshold: 2 })
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["B", "D"]);
    }

    /// A union inside a key-only branch is merged entirely from spilled sets.
    #[tokio::test]
    async fn combine_intersection_with_pre_streamed_union() {
        let left = page_list(&["A", "B", "C"]);
        let b = KeySet::from_pagelist(page_list(&["A", "B", "X"]), KeySetLimits { threshold: 1 })
            .unwrap();
        let c = KeySet::from_pagelist(page_list(&["B", "C", "Y"]), KeySetLimits { threshold: 1 })
            .unwrap();
        let combination = intersect(source("left"), union(source("b"), source("c")));
        let steps = Platform::serialize_combine_results(&combination).unwrap();
        let mut results = HashMap::from([
            ("left".to_string(), SourceResult::Entries(left)),
            ("b".to_string(), SourceResult::Keys(b)),
            ("c".to_string(), SourceResult::Keys(c)),
        ]);
        let platform = make_platform(vec![]);

        let combined = platform
            .combine_results_with_limits(&mut results, steps, KeySetLimits { threshold: 2 })
            .await
            .unwrap();

        assert_eq!(titles(&combined), vec!["A", "B", "C"]);
    }
}
