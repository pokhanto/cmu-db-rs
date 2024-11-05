use super::error::ExtendibleHashTableError;
use super::extendible_hash_table_bucket_page::ExtendibleHTableBucketPage;
use super::extendible_hash_table_directory_page::ExtendibleHTableDirectoryPage;
use super::extendible_hash_table_header_page::ExtendibleHTableHeaderPage;
use crate::page::Page;
use crate::{buffer_pool_manager::BufferPoolManager, page::PageId};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use std::{
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

fn hash_string(s: String) -> u32 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    let hash = hasher.finish();

    (hash % u32::MAX as u64) as u32
}

/*
    TODO:
    1. Unwraps -> Result
    2. Review pages locking on insert: page should be locked while inserting
    3. Get rid of recursive calls
    4. `Get` should return reference to value
    5. Process keys collision
*/
#[derive(Debug)]
pub struct ExtendibleHashTable<K, V> {
    name: String,
    directory_max_depth: u32,
    bucket_max_size: usize,
    header_page_id: PageId,
    buffer_pool_manager: Arc<BufferPoolManager>,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K, V> ExtendibleHashTable<K, V>
where
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + ToString,
    V: Copy + Clone + Debug + Serialize + DeserializeOwned,
{
    pub fn new(
        name: String,
        buffer_pool_manager: Arc<BufferPoolManager>,
        directory_max_depth: u32,
        bucket_max_size: usize,
    ) -> Self {
        let header_max_size = 0;

        // TODO: what if BPM is not able to create new page
        let buf = Arc::clone(&buffer_pool_manager);
        let mut header_page = buf.new_page().unwrap();
        let header = ExtendibleHTableHeaderPage::new(header_max_size);
        let header_data = header.to_bytes();
        header_page.set_data(header_data);

        Self {
            name,
            directory_max_depth,
            bucket_max_size,
            // TODO: for now we assume that BPM will return page with initialized PageId
            // consider have Frame and Page entities, where Page always have PageId
            header_page_id: header_page.get_id().unwrap(),
            buffer_pool_manager,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), ExtendibleHashTableError> {
        let mut header_page = self
            .buffer_pool_manager
            .fetch_page_write(self.header_page_id)
            .unwrap();
        let mut header = ExtendibleHTableHeaderPage::from(&header_page);

        let insertion_key_hash = hash_string(key.to_string());

        let directory_index = header.hash_to_directory_index(insertion_key_hash);
        let (mut directory, mut directory_page) =
            match header.get_directory_page_id(directory_index) {
                Some(directory_page_id) => {
                    let directory_page = self
                        .buffer_pool_manager
                        .fetch_page_write(*directory_page_id)
                        .unwrap();

                    (
                        ExtendibleHTableDirectoryPage::from(&directory_page),
                        directory_page,
                    )
                }
                None => {
                    let new_page = self.buffer_pool_manager.new_page().unwrap();
                    let directory_page_id = new_page.get_id().unwrap();
                    //let header_page = self.fetch_page(self.header_page_id).unwrap();
                    //let mut header_page = header_page.lock().unwrap();
                    //let mut header = ExtendibleHTableHeaderPage::from(&header_page);
                    header.set_directory_page_id(directory_index, directory_page_id);
                    header_page.set_data(header.to_bytes());
                    //drop(header_page);

                    (
                        ExtendibleHTableDirectoryPage::new(self.directory_max_depth),
                        new_page,
                    )
                }
            };
        //drop(header_page);

        self.insert_internal(key, value, &mut directory, &mut directory_page)?;

        Ok(())
    }

    fn insert_internal(
        &self,
        key: K,
        value: V,
        directory: &mut ExtendibleHTableDirectoryPage,
        directory_page: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<(), ExtendibleHashTableError> {
        let insertion_key_hash = hash_string(key.to_string());
        let bucket_index = directory.hash_to_bucket_index(insertion_key_hash);
        let (mut bucket, mut bucket_page) = match directory.get_bucket_page_id(bucket_index) {
            Some(bucket_page_id) => {
                let bucket_page = self
                    .buffer_pool_manager
                    .fetch_page_write(*bucket_page_id)
                    .unwrap();
                let data = bucket_page.get_data();

                (
                    ExtendibleHTableBucketPage::from_bytes(data.as_slice()),
                    bucket_page,
                )
            }
            None => {
                let new_page = self.buffer_pool_manager.new_page().unwrap();

                let bucket_page_id = new_page.get_id().unwrap();
                directory.set_bucket_page_id(bucket_index, bucket_page_id);

                (
                    ExtendibleHTableBucketPage::new(self.bucket_max_size),
                    new_page,
                )
            }
        };

        if !bucket.is_full() {
            bucket.insert(key, value);

            bucket_page.set_data(bucket.to_bytes());
            directory_page.set_data(directory.to_bytes());

            Ok(())
        } else {
            let local_depth = directory.get_local_depth(bucket_index).unwrap();
            let global_depth = directory.get_global_depth();
            let should_double_size = local_depth == global_depth;

            let new_bucket = ExtendibleHTableBucketPage::<K, V>::new(self.bucket_max_size);
            let mut new_page = self.buffer_pool_manager.new_page().unwrap();
            new_page.set_data(new_bucket.to_bytes());
            let new_page_id = new_page.get_id().unwrap();
            drop(new_page);

            let bucket_next_local_depth = directory.get_local_depth(bucket_index).unwrap() + 1;
            let local_depth_mask = (1 << bucket_next_local_depth) - 1;
            let aligned_bucket_index = bucket_index & local_depth_mask;

            if should_double_size {
                directory.increment_local_depth(bucket_index);
                directory.increment_global_depth()?;
                let split_image_index = directory.get_split_image_index(bucket_index);
                directory.set_bucket_page_id(split_image_index, new_page_id);
            } else {
                for index in 0..directory.get_size() {
                    let other_bucket_index = index & local_depth_mask;
                    if aligned_bucket_index == other_bucket_index {
                        directory.increment_local_depth(index);

                        let split_image_index = directory.get_split_image_index(index);
                        directory.increment_local_depth(split_image_index);
                        directory.set_bucket_page_id(split_image_index, new_page_id);
                    }
                }
            }

            // drain all entries from current bucket
            let mut all_entries = bucket.get_entries();

            // write data to pages
            directory_page.set_data(directory.to_bytes());

            bucket_page.set_data(bucket.to_bytes());
            drop(bucket_page);

            all_entries.push((key, value));
            for entry in all_entries {
                let key = entry.0;
                let value = entry.1;
                self.insert_internal(key, value, directory, directory_page)?
            }

            Ok(())
        }
    }

    // TODO: remove empty directories
    //pub fn remove(&self, key: K) -> Result<(), ExtendibleHashTableError> {
    //    let insertion_key_hash = hash_string(key.to_string());
    //    let mut buffer_pool_manager = self.buffer_pool_manager.lock().unwrap();
    //
    //    // header
    //    let header_page = buffer_pool_manager
    //        .fetch_page_read(self.header_page_id)
    //        .map(|p| Arc::clone(&p))
    //        .unwrap();
    //    let header = ExtendibleHTableHeaderPage::from(&header_page);
    //
    //    // directory
    //    let directory_index = header.hash_to_directory_index(insertion_key_hash);
    //    let directory_page_id = *header
    //        .get_directory_page_id(directory_index)
    //        .ok_or(ExtendibleHashTableError::NoDirectoryForPageId)?;
    //    let directory_page = buffer_pool_manager
    //        .fetch_page_write(directory_page_id)
    //        .unwrap();
    //    let mut directory = ExtendibleHTableDirectoryPage::from(&directory_page);
    //
    //    //bucket
    //    let bucket_index = directory.hash_to_bucket_index(insertion_key_hash);
    //    let bucket_page_id = *directory
    //        .get_bucket_page_id(bucket_index)
    //        .ok_or(ExtendibleHashTableError::NoBucketForPageId)?;
    //    let mut buffer_pool_manager = self.buffer_pool_manager.lock().unwrap();
    //    let bucket_page = buffer_pool_manager
    //        .fetch_page_write(bucket_page_id)
    //        .unwrap();
    //    let mut bucket = ExtendibleHTableBucketPage::<K, V>::from(&bucket_page);
    //    let value = bucket.delete(key);
    //    drop(buffer_pool_manager);
    //
    //    if value.is_some() && bucket.is_empty() {
    //        let local_depth_mask = (1 << directory.get_local_depth(bucket_index).unwrap()) - 1;
    //        let aligned_bucket_index = bucket_index & local_depth_mask;
    //
    //        for index in 0..directory.get_size() {
    //            let other_bucket_index = index & local_depth_mask;
    //            if aligned_bucket_index == other_bucket_index {
    //                let bucket_current_local_depth = directory.get_local_depth(index).unwrap();
    //                let split_image_index = directory.get_split_image_index(index);
    //                let split_image_bucket_local_depth =
    //                    directory.get_local_depth(split_image_index).unwrap();
    //
    //                if bucket_current_local_depth != split_image_bucket_local_depth {
    //                    continue;
    //                }
    //
    //                let split_image_page_id =
    //                    directory.get_bucket_page_id(split_image_index).unwrap();
    //                directory.set_bucket_page_id(index, *split_image_page_id);
    //
    //                directory.decrement_local_depth(index);
    //                directory.decrement_local_depth(split_image_index);
    //            }
    //        }
    //
    //        let mut should_shrink = true;
    //        let global_depth = directory.get_global_depth();
    //        for bucket_index in 0..directory.get_size() {
    //            let local_depth = directory.get_local_depth(bucket_index).unwrap();
    //
    //            if local_depth == global_depth {
    //                should_shrink = false;
    //            }
    //        }
    //
    //        if should_shrink {
    //            directory.decrement_global_depth();
    //        }
    //    }
    //
    //    directory_page.set_data(directory.to_bytes());
    //    bucket_page.set_data(bucket.to_bytes());
    //
    //    Ok(())
    //}

    pub fn get(&self, key: K) -> Option<V> {
        let hash = hash_string(key.to_string());

        let header_page = self
            .buffer_pool_manager
            .fetch_page_read(self.header_page_id)
            .unwrap();
        let header = ExtendibleHTableHeaderPage::from(&header_page);
        drop(header_page);

        let directory_index = header.hash_to_directory_index(hash);

        let directory_page_id = header.get_directory_page_id(directory_index).unwrap();
        let directory_page = self
            .buffer_pool_manager
            .fetch_page_read(*directory_page_id)
            .unwrap();
        let directory = ExtendibleHTableDirectoryPage::from(&directory_page);
        drop(directory_page);

        let bucket_index = directory.hash_to_bucket_index(hash);

        let bucket_page_id = directory.get_bucket_page_id(bucket_index).unwrap();
        let bucket_page = self
            .buffer_pool_manager
            .fetch_page_read(*bucket_page_id)
            .unwrap();
        let bucket = ExtendibleHTableBucketPage::<K, V>::from(&bucket_page);

        bucket.get(key).copied()
    }

    pub fn verify_integrity(&self) {
        //let header_page = self.fetch_page(self.header_page_id).unwrap();
        //let header_page = header_page.lock().unwrap();
        //let header = ExtendibleHTableHeaderPage::from(&header_page);
        //
        //for index in 0..header.get_max_size() {
        //    let directory_page_id = header.get_directory_page_id(index);
        //
        //    if let Some(directory_page_id) = directory_page_id {
        //        let directory_page = self.fetch_page(*directory_page_id).unwrap();
        //        let directory_page = directory_page.lock().unwrap();
        //        let directory = ExtendibleHTableDirectoryPage::from(&directory_page);
        //
        //        directory.verify_integrity();
        //    }
        //}
    }
}
//#[cfg(test)]
//mod tests {
//    use std::{
//        thread::{self, JoinHandle},
//        time::Duration,
//    };
//
//    use rand::Rng;
//
//    use super::*;
//    use crate::disk_manager::DiskManager;
//
//    #[test]
//    fn test_hash_table() {
//        let entry_value = 277;
//        let disk_manager = DiskManager::new();
//        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 12, 4);
//        let hash_table = ExtendibleHashTable::<String, u32>::new(
//            "Test".into(),
//            Arc::new(Mutex::new(buffer_pool_manager)),
//            6,
//            2,
//        );
//
//        let keys: Vec<String> = vec![
//            "asdasdsas".into(),
//            "b1211212c".into(),
//            "d1211212c".into(),
//            "s1211212c".into(),
//            "w1211212c".into(),
//            "jj1211212c".into(),
//            "jf1212c".into(),
//            "jfsds1212c".into(),
//            "gfghfg1212c".into(),
//            "gfghdfsdfsdf1212c".into(),
//            "gfisdisidighfg1212c".into(),
//            "sdfs921201".into(),
//        ];
//
//        for key in keys.clone() {
//            hash_table.insert(key, entry_value).unwrap();
//        }
//
//        hash_table.verify_integrity();
//
//        for key in keys.clone() {
//            let value = hash_table.get(key);
//            assert_eq!(value.unwrap(), entry_value);
//        }
//
//        let value = hash_table.get("absent key".into());
//        assert_eq!(value, None);
//
//        for key in keys.clone() {
//            hash_table.remove(key).unwrap();
//        }
//
//        for key in keys.clone() {
//            let value = hash_table.get(key);
//            assert_eq!(value, None);
//        }
//        hash_table.verify_integrity();
//        println!("Hash table test has passed!");
//    }
//
//    #[test]
//    fn test_hash_table_concurrency() {
//        let disk_manager = DiskManager::new();
//        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 12, 4);
//        let hash_table = ExtendibleHashTable::<String, u32>::new(
//            "Test".into(),
//            Arc::new(Mutex::new(buffer_pool_manager)),
//            6,
//            2,
//        );
//
//        let hash_table = Arc::new(hash_table);
//
//        let mut handles: Vec<JoinHandle<()>> = vec![];
//        for _ in 0..8 {
//            let handle = thread::spawn({
//                let hash_table = Arc::clone(&hash_table);
//                move || {
//                    let mut rng = rand::thread_rng();
//                    let random_number: u32 = rng.gen_range(0..50);
//                    thread::sleep(Duration::from_millis(random_number as u64));
//                    hash_table.insert("key".into(), 21).unwrap();
//                    let _ = hash_table.get("key".into());
//                    thread::sleep(Duration::from_millis(random_number as u64));
//                    hash_table.remove("key".into()).unwrap();
//                }
//            });
//
//            handles.push(handle);
//        }
//
//        for handle in handles {
//            handle.join().unwrap();
//        }
//    }
//}
