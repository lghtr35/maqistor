mod health;
mod jobs;

use axum::Router;
use maqistor_engine::Engine;
use maqistor_persistence::SqliteStore;
use tower_http::trace::TraceLayer;

pub type AppEngine = Engine<SqliteStore>;

#[derive(Clone)]
pub struct AppState {
    pub engine: AppEngine,
}

pub fn router(engine: AppEngine) -> Router {
    let state = AppState { engine };

    Router::new()
        .merge(health::routes())
        .merge(jobs::routes())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
