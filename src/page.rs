use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type PageId = usize;

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    id: RwLock<Option<PageId>>,
    data: RwLock<Vec<u8>>,
    pin_count: AtomicUsize,
    is_dirty: AtomicBool,
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: RwLock::new(vec![0; PAGE_SIZE]),
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            id: RwLock::new(None),
        }
    }

    pub fn new_with_id(id: PageId) -> Self {
        Page {
            data: RwLock::new(vec![0; PAGE_SIZE]),
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            id: RwLock::new(Some(id)),
        }
    }

    pub fn reset(&self) {
        let mut id = self.id.write();
        *id = None;
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty.store(false, Ordering::SeqCst);
        let mut data = self.data.write();
        *data = vec![0; PAGE_SIZE];
    }

    pub fn get_data_read(&self) -> RwLockReadGuard<'_, Vec<u8>> {
        self.data.read()
    }

    pub fn get_data_write(&self) -> RwLockWriteGuard<'_, Vec<u8>> {
        self.data.write()
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

    pub fn set_dirty(&self, is_dirty: bool) {
        self.is_dirty.store(is_dirty, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::SeqCst)
    }

    pub fn get_id(&self) -> Option<PageId> {
        let id = self.id.read();
        *id
    }

    pub fn set_id(&self, id: PageId) {
        let mut old_id = self.id.write();
        *old_id = Some(id);
    }
}
