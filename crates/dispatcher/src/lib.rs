use std::future::Future;

use maqistor_engine::{DispatchError, Job, WorkerDispatcher};

/// Docker-backed worker adapter. Worker lifecycle and protocol support will
/// live here without changing Engine's dispatch port.
#[derive(Clone, Default)]
pub struct DockerDispatcher;

impl DockerDispatcher {
    pub fn new() -> Self {
        Self
    }
}

impl WorkerDispatcher for DockerDispatcher {
    fn dispatch(&self, _job: Job) -> impl Future<Output = Result<(), DispatchError>> + Send {
        async { Err(DispatchError::NoCapacity) }
    }
}
