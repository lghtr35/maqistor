use reqwest::{Client, StatusCode};
use thiserror::Error;

use crate::{ErrorBody, JobRequest, JobResponse, MaqistorClient};

#[derive(Debug, Clone)]
pub struct MaqistorHttpClient {
    base_url: String,
    http: Client,
}

impl MaqistorHttpClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_client(base_url, Client::new())
    }

    pub fn with_client(base_url: impl Into<String>, http: Client) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            http,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl MaqistorClient for MaqistorHttpClient {
    type Error = HttpError;

    async fn health(&self) -> Result<(), Self::Error> {
        let response = self.http.get(self.url("/health")).send().await?;
        if response.status() == StatusCode::NO_CONTENT {
            Ok(())
        } else {
            Err(HttpError::from_response(response).await)
        }
    }

    async fn enqueue(&self, request: JobRequest) -> Result<JobResponse, Self::Error> {
        let response = self
            .http
            .post(self.url("/jobs"))
            .json(&request)
            .send()
            .await?;
        if response.status() == StatusCode::CREATED {
            Ok(response.json().await?)
        } else {
            Err(HttpError::from_response(response).await)
        }
    }

    async fn get_job(&self, id: i64) -> Result<JobResponse, Self::Error> {
        let response = self
            .http
            .get(self.url(&format!("/jobs/{id}")))
            .send()
            .await?;
        if response.status() == StatusCode::OK {
            Ok(response.json().await?)
        } else {
            Err(HttpError::from_response(response).await)
        }
    }
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error("maqistor http {status}: {message}")]
    Api { status: u16, message: String },
}

impl HttpError {
    async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status().as_u16();
        match response.json::<ErrorBody>().await {
            Ok(body) => Self::Api {
                status,
                message: body.error,
            },
            Err(error) => Self::Api {
                status,
                message: error.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::Path,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::MaqistorClient;

    #[derive(Clone, Default)]
    struct State {
        jobs: Arc<Mutex<Vec<JobResponse>>>,
    }

    async fn submit(state: axum::extract::State<State>, Json(req): Json<JobRequest>) -> Response {
        let mut jobs = state.jobs.lock().unwrap();
        let job = JobResponse {
            id: jobs.len() as i64 + 1,
            name: req.name,
            status: "queued".into(),
        };
        jobs.push(job.clone());
        (StatusCode::CREATED, Json(job)).into_response()
    }

    async fn get_job(
        state: axum::extract::State<State>,
        Path(id): Path<i64>,
    ) -> Result<Json<JobResponse>, Response> {
        state
            .jobs
            .lock()
            .unwrap()
            .iter()
            .find(|job| job.id == id)
            .cloned()
            .map(Json)
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("job {id} not found"),
                    }),
                )
                    .into_response()
            })
    }

    async fn health() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    async fn serve() -> (SocketAddr, State) {
        let state = State::default();
        let app = Router::new()
            .route("/health", get(health))
            .route("/jobs", post(submit))
            .route("/jobs/{id}", get(get_job))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, state)
    }

    #[tokio::test]
    async fn enqueue_and_get_job_round_trip() {
        let (addr, _) = serve().await;
        let client = MaqistorHttpClient::new(format!("http://{addr}"));

        client.health().await.unwrap();
        let created = client
            .enqueue(JobRequest {
                name: "email".into(),
                payload: json!({"to": "a@example.test"}),
            })
            .await
            .unwrap();
        assert_eq!(created.id, 1);
        assert_eq!(created.name, "email");
        assert_eq!(created.status, "queued");

        let fetched = client.get_job(created.id).await.unwrap();
        assert_eq!(fetched, created);
    }

    #[tokio::test]
    async fn get_job_maps_api_errors() {
        let (addr, _) = serve().await;
        let client = MaqistorHttpClient::new(format!("http://{addr}"));

        let err = client.get_job(99).await.unwrap_err();
        match err {
            HttpError::Api { status, message } => {
                assert_eq!(status, 404);
                assert!(message.contains("99"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
