use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use rocket::request::State;
use std::sync::Arc;

#[derive(Clone)]
pub struct Platform {
    pub form_parameters: Arc<FormParameters>,
    pub state: Arc<AppState>,
}

pub trait DataSource {}

#[derive(Clone)]
pub struct SourceDatabase {}

impl DataSource for SourceDatabase {}

impl SourceDatabase {
    pub fn new_from_platform() -> Self {
        Self {}
    }
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: State<AppState>) -> Self {
        Self {
            form_parameters: Arc::new((*form_parameters).clone()),
            state: Arc::new(state.inner().clone()),
        }
    }

    pub fn run(&mut self) {
        // TODO legacy parameters

        let x = SourceDatabase::new_from_platform();
        let mut candidate_sources: Vec<dyn DataSource> = vec![];
        candidate_sources.push(x);
    }
}
