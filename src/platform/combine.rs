use crate::combination::{Combination, CombinationSequential};
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

pub(super) static RE_PARSE_COMBINATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\w+(?:'\w+)?|[^\w\s]")
        .expect("Platform::parse_combination_string: Regex is invalid")
});

impl Platform {
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
        allow_none_right: bool,
    ) -> Result<Vec<CombinationSequential>> {
        match (a, b) {
            (Combination::None, _) => Err(anyhow!("{op:?} with left Combination::None found")),
            (c, Combination::None) if allow_none_right => Self::serialize_combine_results(c),
            (_, Combination::None) => Err(anyhow!("{op:?} with right Combination::None found")),
            (c, d) => {
                let mut ret = Self::serialize_combine_results(c)?;
                ret.append(&mut Self::serialize_combine_results(d)?);
                ret.push(op);
                Ok(ret)
            }
        }
    }

    pub(super) fn serialize_combine_results(
        combination: &Combination,
    ) -> Result<Vec<CombinationSequential>> {
        match combination {
            Combination::Source(s) => Ok(vec![CombinationSequential::Source(s.to_string())]),
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                // Either side being None collapses the Union to the other side
                (Combination::None, c) | (c, Combination::None) => {
                    Self::serialize_combine_results(c)
                }
                (c, d) => {
                    Self::serialize_binary_combination(c, d, CombinationSequential::Union, false)
                }
            },
            Combination::Intersection((a, b)) => Self::serialize_binary_combination(
                a.as_ref(),
                b.as_ref(),
                CombinationSequential::Intersection,
                false,
            ),
            Combination::Not((a, b)) => Self::serialize_binary_combination(
                a.as_ref(),
                b.as_ref(),
                CombinationSequential::Not,
                true, // Not(x, None) => just x
            ),
            Combination::None => Err(anyhow!("Combination::None found")),
        }
    }

    /// Pops two registers and returns `(r1, r2)`, or an error if fewer than 2 are available.
    async fn pop_two_registers(
        registers: &mut Vec<PageList>,
        op_name: &str,
    ) -> Result<(PageList, PageList)> {
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
        results: &mut HashMap<String, PageList>,
        combination: Vec<CombinationSequential>,
    ) -> Result<PageList> {
        let mut registers: Vec<PageList> = vec![];
        for command in combination {
            match command {
                CombinationSequential::Source(source_key) => {
                    let source = results
                        .remove(&source_key)
                        .ok_or_else(|| anyhow!("No result for source {source_key}"))?;
                    registers.push(source);
                }
                CombinationSequential::Union => {
                    let (r1, r2) = Self::pop_two_registers(&mut registers, "Union").await?;
                    r1.union(&r2, Some(self)).await?;
                    registers.push(r1);
                }
                CombinationSequential::Intersection => {
                    let (r1, r2) = Self::pop_two_registers(&mut registers, "Intersection").await?;
                    r1.intersection(&r2, Some(self)).await?;
                    registers.push(r1);
                }
                CombinationSequential::Not => {
                    let (r1, r2) = Self::pop_two_registers(&mut registers, "Not").await?;
                    r1.difference(&r2, Some(self)).await?;
                    registers.push(r1);
                }
            }
        }
        if registers.len() == 1 {
            return registers
                .pop()
                .ok_or_else(|| anyhow!("combine_results: registers unexpectedly empty"));
        }
        Err(anyhow!(
            "combine_results: {} registers set",
            registers.len()
        ))
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

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

    #[test]
    fn test_serialize_combine_results_single_source() {
        let comb = Combination::Source("categories".to_string());
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            result,
            vec![CombinationSequential::Source("categories".to_string())]
        );
    }

    #[test]
    fn test_serialize_combine_results_intersection() {
        let comb = Combination::Intersection((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            result,
            vec![
                CombinationSequential::Source("a".to_string()),
                CombinationSequential::Source("b".to_string()),
                CombinationSequential::Intersection,
            ]
        );
    }

    #[test]
    fn test_serialize_combine_results_union() {
        let comb = Combination::Union((
            Box::new(Combination::Source("x".to_string())),
            Box::new(Combination::Source("y".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            result,
            vec![
                CombinationSequential::Source("x".to_string()),
                CombinationSequential::Source("y".to_string()),
                CombinationSequential::Union,
            ]
        );
    }

    #[test]
    fn test_serialize_combine_results_not() {
        let comb = Combination::Not((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        let result = Platform::serialize_combine_results(&comb).unwrap();
        assert_eq!(
            result,
            vec![
                CombinationSequential::Source("a".to_string()),
                CombinationSequential::Source("b".to_string()),
                CombinationSequential::Not,
            ]
        );
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
            result,
            vec![CombinationSequential::Source("cats".to_string())]
        );
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
}
