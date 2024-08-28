use anyhow::Result;
use std::{
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
};

use crate::{disk_manager::DiskManager, PageId};

struct DiskRequest {
    is_write: bool,
    page_id: PageId,
    data: Arc<Mutex<Vec<u8>>>,
    callback_sender: Sender<Result<()>>,
}

pub struct DiskScheduler {
    join_handle: Option<thread::JoinHandle<()>>,
    sender: Sender<Option<DiskRequest>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: DiskManager) -> Self {
        let (sender, receiver) = mpsc::channel::<Option<DiskRequest>>();

        Self {
            sender,
            join_handle: Some(thread::spawn(move || loop {
                let disk_request: Option<DiskRequest> = receiver.recv().unwrap();
                if disk_request.is_none() {
                    break;
                }
                let disk_request = disk_request.unwrap();
                let mut data = disk_request.data.lock().unwrap();

                if disk_request.is_write {
                    disk_manager.write_page(disk_request.page_id, data.as_mut_slice());
                } else {
                    disk_manager.read_page(disk_request.page_id, data.as_mut_slice());
                }

                // TODO: result is not processed, what are cases of error?
                disk_request.callback_sender.send(Ok(())).unwrap();
            })),
        }
    }

    pub fn schedule_read(
        &self,
        page_id: PageId,
        data: Arc<Mutex<Vec<u8>>>,
        callback_sender: Sender<Result<()>>,
    ) {
        self.sender
            .send(Some(DiskRequest {
                is_write: false,
                page_id,
                data,
                callback_sender,
            }))
            .unwrap();
    }

    pub fn schedule_write(
        &self,
        page_id: PageId,
        data: Arc<Mutex<Vec<u8>>>,
        callback_sender: Sender<Result<()>>,
    ) {
        self.sender
            .send(Some(DiskRequest {
                is_write: true,
                page_id,
                data,
                callback_sender,
            }))
            .unwrap();
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        let _ = self.sender.send(None);
        if let Some(handle) = self.join_handle.take() {
            handle.join().expect("Failed to join the thread");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_schedule_read_and_write() {
        let (sender_read, receiver_read) = mpsc::channel::<Result<()>>();
        let (sender_write, receiver_write) = mpsc::channel::<Result<()>>();
        let disk_manager = DiskManager::new();
        let scheduler = DiskScheduler::new(disk_manager);

        let data_to_read: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![0]));
        let data_to_write: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![0]));

        scheduler.schedule_read(0, data_to_read, sender_read);
        scheduler.schedule_write(1, data_to_write.clone(), sender_write);

        let result_read = receiver_read.recv().unwrap();
        let result_write = receiver_write.recv().unwrap();

        assert_eq!(result_read.is_ok(), true);
        assert_eq!(result_write.is_ok(), true);
    }
}
