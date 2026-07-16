use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use maqistor_api::{JobRequest, JobResponse};
use maqistor_engine::EngineError;
use maqistor_persistence::StoreError;
use serde::Serialize;
use uuid::Uuid;

use super::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/jobs", post(submit_job))
        .route("/jobs/{id}", get(get_job))
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

impl From<EngineError> for ApiError {
    fn from(error: EngineError) -> Self {
        match &error {
            EngineError::Store(StoreError::QueueNotFound(_)) => {
                ApiError::new(StatusCode::BAD_REQUEST, error.to_string())
            }
            EngineError::Store(StoreError::NotFound(_)) => {
                ApiError::new(StatusCode::NOT_FOUND, error.to_string())
            }
            EngineError::Payload(_) => ApiError::new(StatusCode::BAD_REQUEST, error.to_string()),
            EngineError::Store(_) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            }
        }
    }
}

async fn submit_job(
    State(state): State<AppState>,
    Json(request): Json<JobRequest>,
) -> Result<(StatusCode, Json<JobResponse>), ApiError> {
    let response = state.engine.submit(request).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobResponse>, ApiError> {
    let response = state.engine.get_job(id).await?;
    Ok(Json(response))
}
