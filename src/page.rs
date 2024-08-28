use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::PageId;

const PAGE_SIZE: usize = 4096;

pub struct Page {
    id: Option<PageId>,
    data: Arc<Mutex<Vec<u8>>>,
    pin_count: AtomicUsize,
    is_dirty: bool,
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: Arc::new(Mutex::new(vec![0; PAGE_SIZE])),
            pin_count: AtomicUsize::new(0),
            is_dirty: false,
            id: None,
        }
    }

    pub fn reset(&mut self) {
        self.id = None;
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty = false;
        // TODO: is it required to create new?
        self.data = Arc::new(Mutex::new(vec![0; PAGE_SIZE]));
    }

    pub fn data(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.data)
    }

    pub fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::SeqCst) > 0
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }
}
