use axum::{routing::get, Router};

use super::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route("/health", get(health))
}

async fn health() -> &'static str {
    "ok"
}
