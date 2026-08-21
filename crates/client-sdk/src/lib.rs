pub mod http;

pub use http::{HttpError, MaqistorHttpClient};
pub use maqistor_types::{ErrorBody, JobRequest, JobResponse};

pub trait MaqistorClient {
    type Error;

    fn health(&self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    fn enqueue(
        &self,
        request: JobRequest,
    ) -> impl std::future::Future<Output = Result<JobResponse, Self::Error>> + Send;

    fn get_job(
        &self,
        id: i64,
    ) -> impl std::future::Future<Output = Result<JobResponse, Self::Error>> + Send;
}
