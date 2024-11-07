use std::sync::{atomic::AtomicU32, mpsc, Arc, Mutex};

use cmu_db_rs::{BufferPoolManager, DiskManager, ExtendibleHashTable, ThreadPool};
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;

const ENTRIES_NUMBER: u32 = 50;
const THREADS_NUMBER: u32 = 10;
const BUFFER_POOL_SIZE: usize = 1000;
const REPLACER_K: usize = 4;
const BUCKET_MAX_DEPTH: u32 = 14;
const PAGE_SIZE: usize = 200;

fn parallel_get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel get");

    for thread_number in &[1, 2] {
        group.bench_with_input(
            format!("{}-thread threadpool", thread_number),
            thread_number,
            |b, thread_number| {
                let client_thread_pool = ThreadPool::new(THREADS_NUMBER);
                let disk_manager = DiskManager::new();
                let buffer_pool_manager =
                    BufferPoolManager::new(disk_manager, BUFFER_POOL_SIZE, REPLACER_K);
                let hash_table = ExtendibleHashTable::<String, u32>::new(
                    "Test".into(),
                    Arc::new(buffer_pool_manager),
                    BUCKET_MAX_DEPTH,
                    PAGE_SIZE,
                );
                let (end_work_sender, end_work_receiver) = mpsc::channel::<()>();

                let data = (0..ENTRIES_NUMBER)
                    .map(|i| (format!("key{}", i), 111))
                    .collect::<Vec<(String, u32)>>();

                for (key, value) in data.clone() {
                    let _ = hash_table.insert(key, value);
                }
                hash_table.verify_integrity();
                let counter = Arc::new(AtomicU32::new(0));

                let client_thread_pool = Arc::new(client_thread_pool);
                let data = Arc::new(data);
                let hash_table = Arc::new(hash_table);
                let end_work_sender = Arc::new(end_work_sender);
                b.iter(|| {
                    counter.store(0, std::sync::atomic::Ordering::Release);
                    let client_thread_pool = Arc::clone(&client_thread_pool);

                    for i in 0..ENTRIES_NUMBER {
                        let hash_table = Arc::clone(&hash_table);
                        let data = Arc::clone(&data);
                        let counter = Arc::clone(&counter);
                        let end_work_sender = Arc::clone(&end_work_sender);

                        client_thread_pool.spawn(move || {
                            let (key, value) = data.get(i as usize).unwrap();
                            let result = match hash_table.get(key.to_string()) {
                                Some(value) => {
                                    println!("Found value for key {key}");
                                    value
                                }
                                None => {
                                    println!("missing value for key {key}");

                                    0
                                }
                            };

                            assert_eq!(&result, value);

                            let prev = counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                            if prev + 1 == ENTRIES_NUMBER {
                                end_work_sender.send(()).unwrap();
                            }
                        });
                    }
                    end_work_receiver.recv().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn parallel_mixed_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel mixed");

    for thread_number in &[1, 2] {
        group.bench_with_input(
            format!("{}-thread threadpool", thread_number),
            thread_number,
            |b, thread_number| {
                let read_thread_pool = ThreadPool::new(THREADS_NUMBER);
                let write_thread_pool = ThreadPool::new(THREADS_NUMBER);
                let disk_manager = DiskManager::new();
                let buffer_pool_manager =
                    BufferPoolManager::new(disk_manager, BUFFER_POOL_SIZE, REPLACER_K);
                let hash_table = ExtendibleHashTable::<String, u32>::new(
                    "Test".into(),
                    Arc::new(buffer_pool_manager),
                    BUCKET_MAX_DEPTH,
                    PAGE_SIZE,
                );
                let (end_work_sender, end_work_receiver) = mpsc::channel::<()>();

                let data_to_read = (0..ENTRIES_NUMBER)
                    .map(|i| {
                        let word = random_word::gen(random_word::Lang::En);
                        (format!("{word} read {i}"), 111)
                    })
                    .collect::<Vec<(String, u32)>>();
                let data_to_write = (0..ENTRIES_NUMBER)
                    .map(|i| {
                        let word = random_word::gen(random_word::Lang::En);
                        (format!("{word} write {i}"), 222)
                    })
                    .collect::<Vec<(String, u32)>>();

                for (key, value) in data_to_read.clone() {
                    let _ = hash_table.insert(key, value);
                }
                hash_table.verify_integrity();
                let counter = Arc::new(AtomicU32::new(0));

                let read_thread_pool = Arc::new(read_thread_pool);
                let write_thread_pool = Arc::new(write_thread_pool);
                let data_to_read = Arc::new(data_to_read);
                let data_to_write = Arc::new(data_to_write);
                let hash_table = Arc::new(hash_table);
                let end_work_sender = Arc::new(end_work_sender);
                b.iter(|| {
                    counter.store(0, std::sync::atomic::Ordering::Release);
                    let read_thread_pool = Arc::clone(&read_thread_pool);
                    let write_thread_pool = Arc::clone(&write_thread_pool);

                    for i in 0..ENTRIES_NUMBER {
                        let data_to_read = Arc::clone(&data_to_read);
                        let data_to_write = Arc::clone(&data_to_write);
                        let counter = Arc::clone(&counter);
                        let end_work_sender = Arc::clone(&end_work_sender);

                        let hash_table_write = Arc::clone(&hash_table);
                        write_thread_pool.spawn(move || {
                            let (key, value) = data_to_write.get(i as usize).unwrap();
                            let _ = hash_table_write.insert(key.to_string(), *value);
                        });

                        let hash_table_read = Arc::clone(&hash_table);
                        read_thread_pool.spawn(move || {
                            let (key, value) = data_to_read.get(i as usize).unwrap();
                            let result = match hash_table_read.get(key.to_string()) {
                                Some(value) => value,
                                None => {
                                    println!("missing value for key {key}");

                                    0
                                }
                            };

                            //assert_eq!(&result, value);

                            let prev = counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                            if prev + 1 == ENTRIES_NUMBER {
                                end_work_sender.send(()).unwrap();
                            }
                        });
                    }
                    end_work_receiver.recv().unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, parallel_mixed_bench, parallel_get_bench);
criterion_main!(benches);
