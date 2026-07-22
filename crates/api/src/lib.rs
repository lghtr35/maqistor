use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use maqistor_engine::{DurableStore, Engine, EngineError, JobView, SubmitJob, WorkerDispatcher};
use serde::{Deserialize, Serialize};
use tower_http::trace::TraceLayer;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRequest {
    pub name: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResponse {
    pub id: i64,
    pub name: String,
    pub status: String,
}

#[derive(Clone)]
struct ApiState<S: DurableStore, D: WorkerDispatcher> {
    engine: Engine<S, D>,
}

pub fn router<S, D>(engine: Engine<S, D>) -> Router
where
    S: DurableStore + Clone + Send + Sync + 'static,
    D: WorkerDispatcher + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(|| async { StatusCode::NO_CONTENT }))
        .route("/jobs", post(submit_job::<S, D>))
        .route("/jobs/{id}", get(get_job::<S, D>))
        .layer(TraceLayer::new_for_http())
        .with_state(ApiState { engine })
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

struct ApiError {
    status: StatusCode,
    message: String,
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
        let status = match &error {
            EngineError::UnknownQueue(_) | EngineError::Payload(_) => StatusCode::BAD_REQUEST,
            EngineError::JobNotFound(_) => StatusCode::NOT_FOUND,
            EngineError::Storage { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

async fn submit_job<S, D>(
    State(state): State<ApiState<S, D>>,
    Json(request): Json<JobRequest>,
) -> Result<(StatusCode, Json<JobResponse>), ApiError>
where
    S: DurableStore + Clone + Send + Sync + 'static,
    D: WorkerDispatcher + Clone + Send + Sync + 'static,
{
    let response = state
        .engine
        .submit(SubmitJob {
            name: request.name,
            payload: request.payload,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(to_response(response))))
}

async fn get_job<S, D>(
    State(state): State<ApiState<S, D>>,
    Path(id): Path<i64>,
) -> Result<Json<JobResponse>, ApiError>
where
    S: DurableStore + Clone + Send + Sync + 'static,
    D: WorkerDispatcher + Clone + Send + Sync + 'static,
{
    Ok(Json(to_response(state.engine.get_job(id).await?)))
}

fn to_response(job: JobView) -> JobResponse {
    JobResponse {
        id: job.id,
        name: job.name,
        status: job.status.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::{body::Body, http::Request};
    use maqistor_dispatcher::{RegistryDispatcher, WorkerRegistry};
    use maqistor_engine::{Job, JobQueue, JobStatus, StoreError};
    use tower::ServiceExt;

    use super::*;

    #[derive(Clone, Default)]
    struct MemoryStore {
        queues: Arc<Mutex<HashMap<String, JobQueue>>>,
        jobs: Arc<Mutex<HashMap<i64, Job>>>,
    }

    impl DurableStore for MemoryStore {
        async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
            self.queues
                .lock()
                .unwrap()
                .insert(queue.name.clone(), queue.clone());
            Ok(queue)
        }
        async fn get_queue(&self, name: &str) -> Result<Option<JobQueue>, StoreError> {
            Ok(self.queues.lock().unwrap().get(name).cloned())
        }
        async fn list_queues(&self) -> Result<Vec<JobQueue>, StoreError> {
            Ok(self.queues.lock().unwrap().values().cloned().collect())
        }
        async fn enqueue(&self, mut job: Job) -> Result<Job, StoreError> {
            if self.queues.lock().unwrap().contains_key(&job.name) {
                job.id = self.jobs.lock().unwrap().len() as i64 + 1;
                self.jobs.lock().unwrap().insert(job.id, job.clone());
                Ok(job)
            } else {
                Err(StoreError::QueueNotFound(job.name))
            }
        }
        async fn get_job(&self, id: i64) -> Result<Job, StoreError> {
            self.jobs
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or(StoreError::NotFound(id))
        }
        async fn status(&self, id: i64) -> Result<JobStatus, StoreError> {
            Ok(self.get_job(id).await?.status)
        }
        async fn claim_next(&self, _queue: &str, _lease: i64) -> Result<Option<Job>, StoreError> {
            Ok(None)
        }
        async fn recover_stale_leases(&self, _now: i64) -> Result<Vec<Job>, StoreError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn http_submission_is_persisted_through_engine() {
        let store = MemoryStore::default();
        store.upsert_queue(JobQueue::new("email")).await.unwrap();
        let app = router(Engine::with_dispatcher(
            store.clone(),
            RegistryDispatcher::new(WorkerRegistry::default()),
            maqistor_engine::DispatchOptions::default(),
        ));
        let request = Request::post("/jobs")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"name":"email","payload":{"to":"a@example.test"}}"#,
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(store.jobs.lock().unwrap().len(), 1);
    }
}
