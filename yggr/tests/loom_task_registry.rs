use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

struct ModelTaskRegistry<T> {
    tasks: Mutex<Vec<T>>,
}

impl<T> ModelTaskRegistry<T> {
    fn new() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
        }
    }

    fn push(&self, task: T) {
        match self.tasks.lock() {
            Ok(mut tasks) => tasks.push(task),
            Err(poisoned) => poisoned.into_inner().push(task),
        }
    }

    fn drain(&self) -> Vec<T> {
        match self.tasks.lock() {
            Ok(mut tasks) => tasks.drain(..).collect(),
            Err(poisoned) => poisoned.into_inner().drain(..).collect(),
        }
    }
}

#[test]
fn reader_task_registry_handles_register_drain_race_without_loss_or_duplication() {
    loom::model(|| {
        let registry = Arc::new(ModelTaskRegistry::new());
        let drained_count = Arc::new(AtomicUsize::new(0));

        let register_registry = Arc::clone(&registry);
        let register = thread::spawn(move || {
            register_registry.push(1usize);
        });

        let drain_registry = Arc::clone(&registry);
        let drain_count = Arc::clone(&drained_count);
        let drain = thread::spawn(move || {
            let drained = drain_registry.drain();
            drain_count.fetch_add(drained.len(), Ordering::Relaxed);
        });

        if let Err(e) = register.join() {
            panic!("register thread panicked: {e:?}");
        }
        if let Err(e) = drain.join() {
            panic!("drain thread panicked: {e:?}");
        }

        let tail = registry.drain();
        let total = drained_count.load(Ordering::Relaxed) + tail.len();
        assert_eq!(total, 1, "registered task must appear exactly once");
    });
}
