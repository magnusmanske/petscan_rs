use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use rocket::request::State;

pub struct Platform {
    pub form_parameters: FormParameters,
    pub state: AppState,
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: State<AppState>) -> Self {
        Self {
            form_parameters: (*form_parameters).clone(),
            state: state.inner().clone(),
        }
    }
}
