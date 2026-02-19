use crate::combination::{Combination, CombinationSequential};
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

pub(super) static RE_PARSE_COMBINATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\w+(?:'\w+)?|[^\w\s]")
        .expect("Platform::parse_combination_string: Regex is invalid")
});

impl Platform {
    pub(super) fn parse_combination_string(s: &str) -> Combination {
        match s.trim().to_lowercase().as_str() {
            "" => return Combination::None,
            "categories" | "sparql" | "manual" | "pagepile" | "wikidata" | "search" => {
                return Combination::Source(s.to_string())
            }
            _ => {}
        }
        let mut parts: Vec<String> = RE_PARSE_COMBINATION
            .captures_iter(s)
            .filter_map(|cap| cap.get(0))
            .map(|s| s.as_str().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // Problem?
        if parts.len() < 3 {
            return Combination::None;
        }

        let first_part = match parts.first() {
            Some(part) => part.to_owned(),
            None => String::new(),
        };
        let left = if first_part == "(" {
            let mut cnt = 0;
            let mut new_left: Vec<String> = vec![];
            loop {
                if parts.is_empty() {
                    return Combination::None; // Failure to parse
                }
                let x = parts.remove(0);
                if x == "(" {
                    if cnt > 0 {
                        new_left.push(x.to_string());
                    }
                    cnt += 1;
                } else if x == ")" {
                    cnt -= 1;
                    if cnt == 0 {
                        break;
                    } else {
                        new_left.push(x.to_string());
                    }
                } else {
                    new_left.push(x.to_string());
                }
            }
            new_left.join(" ")
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

    pub(super) fn serialize_combine_results(
        combination: &Combination,
    ) -> Result<Vec<CombinationSequential>> {
        match combination {
            Combination::Source(s) => Ok(vec![CombinationSequential::Source(s.to_string())]),
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, c) => Self::serialize_combine_results(c),
                (c, Combination::None) => Self::serialize_combine_results(c),
                (c, d) => {
                    let mut ret = vec![];
                    ret.append(&mut Self::serialize_combine_results(c)?);
                    ret.append(&mut Self::serialize_combine_results(d)?);
                    ret.push(CombinationSequential::Union);
                    Ok(ret)
                }
            },
            Combination::Intersection((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => {
                    Err(anyhow!("Intersection with Combination::None found"))
                }
                (_c, Combination::None) => {
                    Err(anyhow!("Intersection with Combination::None found"))
                }
                (c, d) => {
                    let mut ret = vec![];
                    ret.append(&mut Self::serialize_combine_results(c)?);
                    ret.append(&mut Self::serialize_combine_results(d)?);
                    ret.push(CombinationSequential::Intersection);
                    Ok(ret)
                }
            },
            Combination::Not((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => Err(anyhow!("Not with Combination::None found")),
                (c, Combination::None) => Self::serialize_combine_results(c),
                (c, d) => {
                    let mut ret = vec![];
                    ret.append(&mut Self::serialize_combine_results(c)?);
                    ret.append(&mut Self::serialize_combine_results(d)?);
                    ret.push(CombinationSequential::Not);
                    Ok(ret)
                }
            },
            Combination::None => Err(anyhow!("Combination::None found")),
        }
    }

    pub(super) async fn combine_results(
        &self,
        results: &mut HashMap<String, PageList>,
        combination: Vec<CombinationSequential>,
    ) -> Result<PageList> {
        let mut registers: Vec<PageList> = vec![];
        for command in combination {
            match command {
                CombinationSequential::Source(source_key) => match results.remove(&source_key) {
                    Some(source) => {
                        registers.push(source);
                    }
                    None => return Err(anyhow!("No result for source {source_key}")),
                },
                CombinationSequential::Union => {
                    if registers.len() < 2 {
                        return Err(anyhow!("combine_results: Not enough registers for Union"));
                    }
                    let r2 = registers.pop().ok_or_else(|| {
                        anyhow!("combine_results: CombinationSequential::Union r1")
                    })?;
                    let r1 = registers.pop().ok_or_else(|| {
                        anyhow!("combine_results: CombinationSequential::Union r2")
                    })?;
                    r1.union(&r2, Some(self)).await?;
                    registers.push(r1);
                }
                CombinationSequential::Intersection => {
                    if registers.len() < 2 {
                        return Err(anyhow!("combine_results: Not enough registers for Union"));
                    }
                    let r2 = registers.pop().ok_or_else(|| {
                        anyhow!("combine_results: CombinationSequential::Intersection r1")
                    })?;
                    let r1 = registers.pop().ok_or_else(|| {
                        anyhow!("combine_results: CombinationSequential::Intersection r2")
                    })?;
                    r1.intersection(&r2, Some(self)).await?;
                    registers.push(r1);
                }
                CombinationSequential::Not => {
                    if registers.len() < 2 {
                        return Err(anyhow!("combine_results: Not enough registers for Union"));
                    }
                    let r2 = registers
                        .pop()
                        .ok_or_else(|| anyhow!("combine_results: CombinationSequential::Not r1"))?;
                    let r1 = registers
                        .pop()
                        .ok_or_else(|| anyhow!("combine_results: CombinationSequential::Not r2"))?;
                    r1.difference(&r2, Some(self)).await?;
                    registers.push(r1);
                }
            }
        }
        if registers.len() == 1 {
            return registers
                .pop()
                .ok_or_else(|| anyhow!("combine_results registers.len()"));
        }
        Err(anyhow!("combine_results:{} registers set", registers.len()))
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
}
