use std::sync::Mutex as StdMutex;

use tokio::task::JoinHandle;

/// Tracks background tasks that must be drained during shutdown.
///
/// Accept-loop registration and shutdown can race. This registry keeps
/// the handoff simple: registration appends under one lock, and
/// shutdown drains the full set under that same lock.
#[derive(Default)]
pub(super) struct TaskRegistry {
    tasks: StdMutex<Vec<JoinHandle<()>>>,
}

impl TaskRegistry {
    pub(super) fn push(&self, task: JoinHandle<()>) {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(task);
    }

    pub(super) fn drain(&self) -> Vec<JoinHandle<()>> {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect()
    }
}
