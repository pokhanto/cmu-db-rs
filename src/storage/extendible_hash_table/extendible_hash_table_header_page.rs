use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use serde_derive::{Deserialize, Serialize};

use crate::page::{Page, PageId};

#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct ExtendibleHTableHeaderPage {
    directory_page_ids: Vec<Option<PageId>>,
    max_depth: u32,
}

impl ExtendibleHTableHeaderPage {
    pub fn new(max_depth: u32) -> Self {
        Self {
            max_depth,
            directory_page_ids: vec![None; 2_usize.pow(max_depth)],
        }
    }

    pub fn hash_to_directory_index(&self, hash: u32) -> usize {
        (hash & (2_u32.pow(self.max_depth) - 1)) as usize
    }

    pub fn get_directory_page_id(&self, directory_index: usize) -> Option<&PageId> {
        self.directory_page_ids
            .get(directory_index)
            .and_then(|opt| opt.as_ref())
    }

    pub fn set_directory_page_id(&mut self, directory_index: usize, directory_page_id: PageId) {
        self.directory_page_ids[directory_index] = Some(directory_page_id);
    }

    pub fn get_max_size(&self) -> usize {
        2_u32.pow(self.max_depth) as usize
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl From<&RwLockWriteGuard<'_, Vec<u8>>> for ExtendibleHTableHeaderPage {
    fn from(data: &RwLockWriteGuard<'_, Vec<u8>>) -> Self {
        bincode::deserialize(data).unwrap()
    }
}

impl From<&RwLockReadGuard<'_, Vec<u8>>> for ExtendibleHTableHeaderPage {
    fn from(data: &RwLockReadGuard<'_, Vec<u8>>) -> Self {
        bincode::deserialize(data).unwrap()
    }
}
