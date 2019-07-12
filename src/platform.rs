use crate::app_state::AppState;
use crate::datasource::*;
use crate::datasource_database::SourceDatabase;
use crate::form_parameters::FormParameters;
use crate::pagelist::PageList;
//use rayon::prelude::*;
use rocket::request::State;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
enum Combination {
    None,
    Source(String),
    Intersection((Box<Combination>, Box<Combination>)),
    Union((Box<Combination>, Box<Combination>)),
    Not((Box<Combination>, Box<Combination>)),
}

#[derive(Debug, Clone)]
pub struct Platform {
    form_parameters: Arc<FormParameters>,
    pub state: Arc<AppState>,
    result: Option<PageList>,
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: State<AppState>) -> Self {
        Self {
            form_parameters: Arc::new((*form_parameters).clone()),
            state: Arc::new(state.inner().clone()),
            result: None,
        }
    }

    pub fn run(&mut self) {
        // TODO legacy parameters

        let mut candidate_sources: Vec<Box<dyn DataSource>> = vec![];
        candidate_sources.push(Box::new(SourceDatabase::new()));
        candidate_sources.push(Box::new(SourceSparql::new()));
        candidate_sources.push(Box::new(SourceManual::new()));
        candidate_sources.push(Box::new(SourcePagePile::new()));
        candidate_sources.push(Box::new(SourceSearch::new()));
        candidate_sources.push(Box::new(SourceWikidata::new()));

        if !candidate_sources.iter().any(|source| source.can_run(&self)) {
            //self.result.wiki = Some("NO CANDIDATES".to_string());
            // TODO alternative sources
            //candidate_sources.push(Box::new(SourceLabels::new()));
            return;
        }

        let mut results: HashMap<String, Option<PageList>> = HashMap::new();
        // TODO threads
        candidate_sources
            .iter()
            .filter(|source| source.can_run(&self))
            .for_each(|source| {
                results.insert(source.name(), source.run(&self));
            });

        let available_sources = candidate_sources
            .iter()
            .filter(|s| s.can_run(&self))
            .map(|s| s.name())
            .collect();
        let combination = self.get_combination(available_sources);

        println!("{:#?}", &combination);

        self.result = self.combine_results(&mut results, &combination);
    }

    pub fn get_label_sql(&self) -> SQLtuple {
        let ret: SQLtuple = ("".to_string(), vec![]);
        // TODO
        ret
    }

    pub fn just_to_suppress_warnings() {
        let _x =
            Combination::Intersection((Box::new(Combination::None), Box::new(Combination::None)));
        let _y = Combination::Not((Box::new(Combination::None), Box::new(Combination::None)));
    }

    fn parse_combination_string(&self, _s: &String) -> Combination {
        // TODO
        Combination::Source("".to_string())
    }

    /// Checks is the parameter is set, and non-blank
    pub fn has_param(&self, param: &str) -> bool {
        match self.form_parameters().params.get(&param.to_string()) {
            Some(s) => s != "",
            None => false,
        }
    }

    pub fn get_param(&self, param: &str) -> Option<String> {
        if self.has_param(param) {
            self.form_parameters()
                .params
                .get(&param.to_string())
                .map(|s| s.to_string())
        } else {
            None
        }
    }

    fn get_combination(&self, available_sources: Vec<String>) -> Combination {
        match self.get_param("source_combination") {
            Some(combination_string) => self.parse_combination_string(&combination_string),
            None => {
                let mut comb = Combination::None;
                for source in &available_sources {
                    if comb == Combination::None {
                        comb = Combination::Source(source.to_string());
                    } else {
                        comb = Combination::Union((
                            Box::new(Combination::Source(source.to_string())),
                            Box::new(comb),
                        ));
                    }
                }
                comb
            }
        }
    }

    fn combine_results(
        &self,
        results: &mut HashMap<String, Option<PageList>>,
        combination: &Combination,
    ) -> Option<PageList> {
        match combination {
            Combination::Source(s) => match results.get(s) {
                Some(r) => r.to_owned(),
                None => None,
            },
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, c) => self.combine_results(results, c),
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.union(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::Intersection((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => None,
                (_c, Combination::None) => None,
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.intersection(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::Not((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => None,
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.difference(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::None => None,
        }
    }

    pub fn result(&self) -> &Option<PageList> {
        &self.result
    }

    pub fn form_parameters(&self) -> &Arc<FormParameters> {
        &self.form_parameters
    }
}
