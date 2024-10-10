use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{page::Page, PageId};

use super::error::ExtendibleHashTableError;

type BucketIndex = usize;
type BucketDepth = u32;

#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct ExtendibleHTableDirectoryPage {
    bucket_page_ids: Vec<PageId>,
    local_depths: Vec<BucketDepth>,
    max_depth: u32,
    global_depth: u32,
}

impl ExtendibleHTableDirectoryPage {
    pub fn new(max_depth: u32) -> Self {
        Self {
            max_depth,
            global_depth: 0,
            bucket_page_ids: Vec::default(),
            local_depths: vec![0; 1],
        }
    }

    // TODO: rework
    pub fn init(&mut self, page_id: PageId) {
        if self.local_depths.len() == 0 {
            self.local_depths.push(0);
            self.bucket_page_ids.push(page_id);
        }
    }

    pub fn hash_to_bucket_index(&self, hash: u32) -> BucketIndex {
        (hash & self.get_global_depth_mask()) as usize
    }

    pub fn get_bucket_page_id(&self, bucket_index: BucketIndex) -> Option<&PageId> {
        self.bucket_page_ids.get(bucket_index as usize)
    }

    pub fn get_split_image_index(&mut self, bucket_index: BucketIndex) -> BucketIndex {
        let local_depth = self.get_local_depth(bucket_index).unwrap().to_owned();

        if local_depth == 0 {
            return 0;
        }

        bucket_index ^ (1 << (local_depth - 1))
    }

    pub fn get_global_depth_mask(&self) -> u32 {
        let global_depth = self.get_global_depth();
        if global_depth == 0 {
            0
        } else {
            (1 << global_depth) - 1
        }
    }

    pub fn get_local_depth_mask(&mut self, bucket_index: BucketIndex) -> usize {
        let local_depth = self.get_local_depth(bucket_index).unwrap();

        (1 << local_depth) - 1
    }

    pub fn get_global_depth(&self) -> u32 {
        self.global_depth
    }

    pub fn get_size(&self) -> usize {
        2_usize.pow(self.global_depth as u32)
    }

    pub fn increment_global_depth(&mut self) -> Result<(), ExtendibleHashTableError> {
        if self.global_depth == self.max_depth {
            return Err(ExtendibleHashTableError::DirectoryMaxSizeReached);
        }

        let old_size = self.bucket_page_ids.len();
        let new_size = 2 * old_size;

        let mut new_bucket_page_ids: Vec<PageId> = vec![0; new_size];
        let mut new_local_depths = vec![0; new_size];

        for i in 0..old_size {
            let bucket_page_id = self.bucket_page_ids[i];
            let local_depth = self.local_depths[i];

            new_local_depths[i] = local_depth;
            new_local_depths[i + old_size] = local_depth;
            new_bucket_page_ids[i] = bucket_page_id;
            new_bucket_page_ids[i + old_size] = bucket_page_id;
        }

        self.global_depth += 1;
        self.bucket_page_ids = new_bucket_page_ids;
        self.local_depths = new_local_depths;

        Ok(())
    }

    pub fn decrement_global_depth(&mut self) {
        let old_size = self.bucket_page_ids.len();

        self.global_depth -= 1;
        self.bucket_page_ids.resize(old_size / 2, 0);
        self.local_depths.resize(old_size / 2, 0);
    }

    pub fn get_local_depth(&mut self, bucket_index: BucketIndex) -> Option<u32> {
        self.local_depths.get(bucket_index).copied()
    }

    pub fn set_local_depth(&mut self, bucket_index: BucketIndex, local_depth: u32) {
        self.local_depths[bucket_index] = local_depth;
    }

    // TODO: return Result
    pub fn increment_local_depth(&mut self, bucket_index: BucketIndex) {
        self.local_depths[bucket_index] += 1;
    }

    // TODO: return Result
    pub fn decrement_local_depth(&mut self, bucket_index: BucketIndex) {
        if self.get_local_depth(bucket_index).unwrap() > 0 {
            self.local_depths[bucket_index] -= 1;
        }
    }

    pub fn set_bucket_page_id(&mut self, bucket_index: BucketIndex, bucket_page_id: PageId) {
        // TODO: review
        if self.bucket_page_ids.is_empty() {
            self.bucket_page_ids.push(0);
        }
        self.bucket_page_ids[bucket_index] = bucket_page_id;
    }

    pub fn is_full(&mut self, bucket_index: BucketIndex) -> bool {
        self.global_depth == self.max_depth
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }

    pub fn verify_integrity(&self) {
        let mut page_id_to_count: HashMap<usize, u32> = HashMap::new();
        let mut page_id_to_ld: HashMap<usize, u32> = HashMap::new();

        for curr_idx in 0..self.bucket_page_ids.len() {
            let curr_page_id = self.bucket_page_ids[curr_idx];
            let curr_ld = self.local_depths[curr_idx];

            assert!(
                curr_ld <= self.global_depth as u32,
                "Local depth exceeds global depth"
            );

            *page_id_to_count.entry(curr_page_id).or_insert(0) += 1;

            if let Some(&old_ld) = page_id_to_ld.get(&curr_page_id) {
                assert_eq!(
                    curr_ld, old_ld,
                    "Local depth mismatch for page_id: {}",
                    curr_page_id
                );
            } else {
                page_id_to_ld.insert(curr_page_id, curr_ld);
            }
        }

        for (&curr_page_id, &curr_count) in &page_id_to_count {
            let curr_ld = page_id_to_ld[&curr_page_id];
            let required_count = 1 << (self.global_depth - curr_ld);

            assert_eq!(
                curr_count, required_count,
                "Count mismatch for page_id: {}",
                curr_page_id
            );
        }
    }
}

impl From<&std::sync::MutexGuard<'_, Page>> for ExtendibleHTableDirectoryPage {
    fn from(page: &std::sync::MutexGuard<'_, Page>) -> Self {
        let data = page.get_data();
        bincode::deserialize(data).unwrap()
    }
}
