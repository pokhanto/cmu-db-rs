use std::{
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
};

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

///  Thread pool
pub struct ThreadPool {
    sender: Sender<ThreadPoolMessage>,
}

impl ThreadPool {
    /// Create new thread pool with predefined number of threads
    pub fn new(threads: u32) -> Self
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::channel::<ThreadPoolMessage>();
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..threads {
            let receiver = Arc::clone(&receiver);
            thread::spawn(move || loop {
                let receiver = receiver.lock().unwrap();
                let message = receiver.recv();
                drop(receiver);

                match message {
                    Ok(ThreadPoolMessage::RunJob(job)) => {
                        job();
                    }
                    _ => {
                        break;
                    }
                }
            });
        }
        Self { sender }
    }

    /// start work on thread pool thread
    pub fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.sender.send(ThreadPoolMessage::Shutdown).unwrap();
    }
}
