use std::sync::{Arc, Mutex};

use maqistor_engine::{
    DispatchError, DispatchPermit, Job, ReservedDispatch, WorkerDispatcher,
};

#[derive(Clone, Default)]
struct RecordingDispatcher(Arc<Mutex<Vec<Job>>>);

struct TestPermit;

impl DispatchPermit for TestPermit {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

impl WorkerDispatcher for RecordingDispatcher {
    async fn dispatch(
        &self,
        _permit: ReservedDispatch,
        job: Job,
    ) -> Result<(), DispatchError> {
        self.0.lock().unwrap().push(job);
        Ok(())
    }
}

#[tokio::test]
async fn fake_dispatcher_accepts_a_job() {
    let dispatcher = RecordingDispatcher::default();
    let job = Job {
        id: 0,
        name: "email".into(),
        status: maqistor_engine::ExecutionStatus::Pending,
        payload: b"payload".to_vec(),
        execution_count: 0,
        lease_expires_at: None,
        dispatch_id: None,
        result_payload: None,
        result_error: None,
        created_at: 0,
        updated_at: 0,
    };
    let expected_id = job.id;
    let permit = ReservedDispatch::new("email".into(), Box::new(TestPermit));

    dispatcher.dispatch(permit, job).await.unwrap();

    assert_eq!(dispatcher.0.lock().unwrap()[0].id, expected_id);
}
