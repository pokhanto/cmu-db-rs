use std::{thread, time::Duration};

use crate::page::Page;

pub struct DiskManager {}

impl DiskManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn read_page(&self, page: &Vec<u8>) -> Vec<u8> {
        thread::sleep(Duration::from_millis(300));

        vec![0]
    }

    pub fn write_page(&self, page: &Vec<u8>) {
        thread::sleep(Duration::from_millis(200));
    }
}
