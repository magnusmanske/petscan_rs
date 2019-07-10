use crate::app_state::AppState;
use crate::datasource::*;
use crate::form_parameters::FormParameters;
use crate::pagelist::PageList;
use rocket::request::State;
use std::collections::HashMap;
use std::sync::Arc;

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

        if !candidate_sources.iter().any(|source| source.can_run(&self)) {
            //self.result.wiki = Some("NO CANDIDATES".to_string());
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

        self.combine_results(&mut results);
    }

    fn combine_results(&mut self, results: &mut HashMap<String, Option<PageList>>) {
        // TODO
        for (name, result) in results {
            println!("{}/{:?}", &name, &result);
            if self.result.is_none() {
                self.result = result.clone();
            } else if result.is_some() {
                self.result.as_mut().unwrap().union(result.clone()).unwrap();
            }
        }
    }

    pub fn result(&self) -> &Option<PageList> {
        &self.result
    }

    pub fn form_parameters(&self) -> &Arc<FormParameters> {
        &self.form_parameters
    }
}
