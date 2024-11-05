use anyhow::Result;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
        Arc, Mutex, RwLock,
    },
    thread,
};

use crate::{
    disk_manager::DiskManager,
    page::{Page, PageId},
};

#[derive(Debug)]
struct DiskRequestQueue {
    queues: HashMap<PageId, VecDeque<DiskRequest>>,
    in_processing_ids: HashSet<PageId>,
}

impl DiskRequestQueue {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            in_processing_ids: HashSet::new(),
        }
    }

    pub fn push(&mut self, disk_request: DiskRequest) {
        let page = disk_request.page.write().unwrap();
        let page_id = page.get_id().unwrap();
        let queue = self.queues.entry(page_id).or_default();
        drop(page);
        queue.push_back(disk_request);
    }

    pub fn start_processing(&mut self) -> Option<DiskRequest> {
        for (&page_id, queue) in &mut self.queues {
            if !self.in_processing_ids.contains(&page_id) {
                self.in_processing_ids.insert(page_id);
                return queue.pop_front();
            }
        }
        None
    }

    pub fn end_processing(&mut self, page_id: &PageId) {
        self.in_processing_ids.remove(page_id);
        if let Some(queue) = self.queues.get_mut(page_id) {
            if queue.is_empty() {
                self.queues.remove(page_id);
            }
        }
    }
}

#[derive(Debug)]
struct Worker {
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(
        id: usize,
        queue: Arc<Mutex<DiskRequestQueue>>,
        disk_manager: Arc<DiskManager>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        let thread = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let mut pop_queue = queue.lock().unwrap();
                let disk_request = pop_queue.start_processing();
                drop(pop_queue);
                if let Some(disk_request) = disk_request {
                    let mut page = disk_request.page.write().unwrap();
                    let page_id = page.get_id().unwrap();
                    println!(
                        "start processing page {} with write {:?}",
                        &page_id, &disk_request.is_write
                    );

                    if disk_request.is_write {
                        disk_manager.write_page(&mut page);
                    } else {
                        disk_manager.read_page(&mut page);
                    }
                    println!(
                        "end processing page {} with write {:?}",
                        &page_id, &disk_request.is_write
                    );

                    disk_request.callback_sender.send(Ok(())).unwrap();
                    let mut end_queue = queue.lock().unwrap();
                    end_queue.end_processing(&page_id);
                }
            }
        });
        Self { thread }
    }
}

#[derive(Debug)]
struct WorkerPool {
    workers: Vec<Worker>,
    queue: Arc<Mutex<DiskRequestQueue>>,
    stop_flag: Arc<AtomicBool>,
}

impl WorkerPool {
    fn new(size: usize, disk_manager: DiskManager) -> Self {
        let queue: Arc<Mutex<DiskRequestQueue>> = Arc::new(Mutex::new(DiskRequestQueue::new()));
        let disk_manager = Arc::new(disk_manager);
        let mut workers = Vec::with_capacity(size);
        let stop_flag = Arc::new(AtomicBool::new(false));

        for id in 0..size {
            let queue = Arc::clone(&queue);
            let disk_manager = Arc::clone(&disk_manager);
            let stop_flag = Arc::clone(&stop_flag);
            workers.push(Worker::new(id, queue, disk_manager, stop_flag));
        }
        Self {
            workers,
            queue,
            stop_flag,
        }
    }

    fn execute(&self, disk_request: DiskRequest) {
        let mut queue = self.queue.lock().unwrap();
        queue.push(disk_request);
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        for worker in mem::take(&mut self.workers) {
            worker.thread.join().unwrap();
        }
    }
}

#[derive(Debug)]
struct DiskRequest {
    is_write: bool,
    page: Arc<RwLock<Page>>,
    callback_sender: Sender<Result<()>>,
}

#[derive(Debug)]
pub struct DiskScheduler {
    pool: WorkerPool,
}

impl DiskScheduler {
    pub fn new(disk_manager: DiskManager) -> Self {
        let pool = WorkerPool::new(4, disk_manager);

        Self { pool }
    }

    pub fn schedule_read(&self, page: Arc<RwLock<Page>>, callback_sender: Sender<Result<()>>) {
        self.pool.execute(DiskRequest {
            is_write: false,
            page,
            callback_sender,
        });
    }

    pub fn schedule_write(&self, page: Arc<RwLock<Page>>, callback_sender: Sender<Result<()>>) {
        self.pool.execute(DiskRequest {
            is_write: true,
            page,
            callback_sender,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread::JoinHandle};

    use super::*;
    // TODO: figure out how to test concurrency and queuing page by ids
    #[test]
    fn test_schedule_read_and_write() {
        let disk_manager = DiskManager::new();
        let scheduler = DiskScheduler::new(disk_manager);
        let mut handles: Vec<JoinHandle<()>> = vec![];
        let mut test_data: VecDeque<(Page, bool)> = VecDeque::default();
        test_data.push_back((Page::new_with_id(1), false));
        test_data.push_back((Page::new_with_id(1), true));
        test_data.push_back((Page::new_with_id(2), true));
        test_data.push_back((Page::new_with_id(1), false));
        test_data.push_back((Page::new_with_id(4), false));
        test_data.push_back((Page::new_with_id(2), false));

        let test_data = Arc::new(Mutex::new(test_data));
        let scheduler = Arc::new(scheduler);
        for _ in 0..8 {
            let test_data = Arc::clone(&test_data);
            let scheduler = Arc::clone(&scheduler);
            let handle = thread::spawn(move || {
                let mut test_data = test_data.lock().unwrap();
                let item = test_data.pop_front();
                let Some(item) = item else {
                    return;
                };

                let (page, is_write) = item;
                let (result_sender, result_receiver) = mpsc::channel::<Result<()>>();

                if is_write {
                    scheduler.schedule_write(Arc::new(RwLock::new(page)), result_sender);
                } else {
                    scheduler.schedule_read(Arc::new(RwLock::new(page)), result_sender);
                }
                drop(test_data);

                let result = result_receiver.recv().unwrap();

                assert!(result.is_ok());
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
