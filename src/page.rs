use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub type PageId = usize;

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    id: Option<PageId>,
    data: Vec<u8>,
    pin_count: AtomicUsize,
    is_dirty: bool,
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: vec![0; PAGE_SIZE],
            pin_count: AtomicUsize::new(0),
            is_dirty: false,
            id: None,
        }
    }

    pub fn new_with_id(id: PageId) -> Self {
        Page {
            data: vec![0; PAGE_SIZE],
            pin_count: AtomicUsize::new(0),
            is_dirty: false,
            id: Some(id),
        }
    }

    pub fn reset(&mut self) {
        self.id = None;
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty = false;
        // TODO: is it required to create new?
        self.data = vec![0; PAGE_SIZE];
    }

    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn set_data(&mut self, new_data: Vec<u8>) {
        self.data = new_data;
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

    pub fn get_id(&self) -> Option<PageId> {
        self.id
    }

    pub fn set_id(&mut self, id: PageId) {
        self.id = Some(id);
    }

    fn to_arc_slice(self) -> Arc<[u8]> {
        Arc::from(self.data.into_boxed_slice())
    }
}
