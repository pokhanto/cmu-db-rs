use std::{thread, time::Duration};

use crate::PageId;

pub struct DiskManager {}

impl DiskManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn read_page(&self, page_id: PageId, page_content: &mut [u8]) {
        thread::sleep(Duration::from_millis(500));
    }

    pub fn write_page(&self, page_id: PageId, page_content: &[u8]) {
        thread::sleep(Duration::from_millis(600));
    }
}
