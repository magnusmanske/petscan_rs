use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use rocket::request::State;
use std::sync::Arc;

//#[derive(Copy, Clone)]
pub struct Platform {
    pub form_parameters: FormParameters,
    pub state: AppState,
}

pub trait DataSource {
    //fn new_from_platform(platform: &Platform) -> Self;
}

#[derive(Copy, Clone)]
pub struct SourceDatabase {
    platform: Arc<&Platform>,
}

impl DataSource for SourceDatabase {}

impl SourceDatabase {
    pub fn new_from_platform(platform: Arc<&Platform>) -> Self {
        Self { platform }
    }
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: State<AppState>) -> Self {
        Self {
            form_parameters: (*form_parameters).clone(),
            state: state.inner().clone(),
        }
    }

    pub fn run(&mut self) {
        // TODO legacy parameters

        let me = Arc::new(&self);

        let _x = SourceDatabase::new_from_platform(me.clone());
        //let mut candidate_sources: Vec<dyn DataSource> = vec![];
        //candidate_sources.push(SourceDatabase::new_from_platform(&self));
    }
}
