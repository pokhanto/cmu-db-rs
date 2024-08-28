use crate::lru_k_replacer::LruKReplacer;

mod buffer_pool_manager;
mod disk_manager;
mod disk_scheduler;
mod lru_k_replacer;
mod page;

pub type PageId = usize;

fn main() {
    println!("Hello, world!");
    let lru_k_replacer = LruKReplacer::new(5, 5);
    lru_k_replacer.evict();
}