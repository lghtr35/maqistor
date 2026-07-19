use std::sync::{Arc, Mutex};

use maqistor_engine::{DispatchError, Job, WorkerDispatcher};

#[derive(Clone, Default)]
struct RecordingDispatcher(Arc<Mutex<Vec<Job>>>);

impl WorkerDispatcher for RecordingDispatcher {
    async fn dispatch(&self, job: Job) -> Result<(), DispatchError> {
        self.0.lock().unwrap().push(job);
        Ok(())
    }
}

#[tokio::test]
async fn fake_dispatcher_accepts_a_job() {
    let dispatcher = RecordingDispatcher::default();
    let job = Job::new_pending("email", b"payload".to_vec());
    let expected_id = job.id;

    dispatcher.dispatch(job).await.unwrap();

    assert_eq!(dispatcher.0.lock().unwrap()[0].id, expected_id);
}
