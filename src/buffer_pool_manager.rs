use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::sync::{mpsc, Arc, Mutex};

use crate::{
    disk_manager::DiskManager,
    disk_scheduler::DiskScheduler,
    lru_k_replacer::{AccessType, FrameId, LruKReplacer},
    page::{Page, PageId},
};

#[derive(Debug)]
pub struct BufferPoolManager {
    free_list: Arc<Mutex<Vec<FrameId>>>,
    pages: Vec<Page>,
    replacer: Arc<Mutex<LruKReplacer>>,
    disk_scheduler: Arc<DiskScheduler>,
    pages_map: DashMap<PageId, FrameId>,
    // TODO: should be atomic
    next_page_id: Arc<Mutex<PageId>>,
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager, pool_size: usize, replacer_k: usize) -> Self {
        let replacer = LruKReplacer::new(pool_size, replacer_k);
        let disk_scheduler = DiskScheduler::new(disk_manager);
        let pages_map: DashMap<PageId, FrameId> = DashMap::default();
        let mut pages: Vec<Page> = Vec::with_capacity(pool_size);
        let mut free_list: Vec<FrameId> = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            free_list.push(i);
            pages.push(Page::new());
        }

        Self {
            pages,
            free_list: Arc::new(Mutex::new(free_list)),
            replacer: Arc::new(Mutex::new(replacer)),
            disk_scheduler: Arc::new(disk_scheduler),
            pages_map,
            next_page_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn new_page(&self) -> Option<(PageId, RwLockWriteGuard<'_, Vec<u8>>)> {
        let replacer = self.replacer.lock().unwrap();
        let mut free_list = self.free_list.lock().unwrap();
        let frame_id = free_list.pop().or_else(|| replacer.evict());
        drop(replacer);
        drop(free_list);

        frame_id.map(|frame_id| {
            let page_id = self.allocate_page();
            let page = self.pages.get(frame_id).unwrap();

            if page.is_dirty() {
                let (sender, receiver) = mpsc::channel::<Result<()>>();
                //self.disk_scheduler.schedule_write(&guard, sender);
                let _ = receiver.recv().unwrap();
            }
            page.reset();
            page.set_id(page_id);

            self.pages_map.insert(page_id, frame_id);
            let mut replacer = self.replacer.lock().unwrap();
            replacer.record_access(frame_id, AccessType::Unknown);
            replacer.set_evictable(frame_id, false);

            (page.get_id().unwrap(), page.get_data_write())
        })
    }

    pub fn fetch_page_read(&self, page_id: PageId) -> Option<RwLockReadGuard<'_, Vec<u8>>> {
        let frame_id = self.pages_map.get(&page_id);
        if let Some(frame_id) = frame_id {
            let page = self.pages.get(*frame_id).unwrap();

            return Some(page.get_data_read());
        }

        let replacer = self.replacer.lock().unwrap();
        let mut free_list = self.free_list.lock().unwrap();
        let frame_id = free_list.pop().or_else(|| replacer.evict());
        drop(free_list);
        drop(replacer);
        frame_id.map(|frame_id| {
            let page = self.pages.get(frame_id).unwrap();

            if page.is_dirty() {
                let (sender, receiver) = mpsc::channel::<Result<()>>();
                //self.disk_scheduler
                //    .schedule_write(Arc::clone(&page_arc), sender);
                let _ = receiver.recv().unwrap();
            }
            page.reset();
            page.set_id(page_id);
            let (sender, receiver) = mpsc::channel::<Result<()>>();
            //self.disk_scheduler
            //    .schedule_read(Arc::clone(&page_arc), sender);
            let _ = receiver.recv().unwrap();

            self.pages_map.insert(page_id, frame_id);
            let mut replacer = self.replacer.lock().unwrap();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Unknown);

            page.get_data_read()
        })
    }

    pub fn fetch_page_write(&self, page_id: PageId) -> Option<RwLockWriteGuard<'_, Vec<u8>>> {
        let frame_id = self.pages_map.get(&page_id);
        if let Some(frame_id) = frame_id {
            let page = self.pages.get(*frame_id).unwrap();

            return Some(page.get_data_write());
        }

        let replacer = self.replacer.lock().unwrap();
        let mut free_list = self.free_list.lock().unwrap();
        let frame_id = free_list.pop().or_else(|| replacer.evict());
        drop(replacer);
        drop(free_list);
        frame_id.map(|frame_id| {
            let page = self.pages.get(frame_id).unwrap();

            if page.is_dirty() {
                let (sender, receiver) = mpsc::channel::<Result<()>>();
                //self.disk_scheduler
                //    .schedule_write(Arc::clone(&page_arc), sender);
                let _ = receiver.recv().unwrap();
            }
            page.reset();
            page.set_id(page_id);
            let (sender, receiver) = mpsc::channel::<Result<()>>();
            //self.disk_scheduler
            //    .schedule_read(Arc::clone(&page_arc), sender);
            let _ = receiver.recv().unwrap();

            self.pages_map.insert(page_id, frame_id);
            let mut replacer = self.replacer.lock().unwrap();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Unknown);

            page.get_data_write()
        })
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame = self
            .pages
            .get(*frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        frame.unpin();
        frame.set_dirty(is_dirty);

        if !frame.is_pinned() {
            let mut replacer = self.replacer.lock().unwrap();
            replacer.set_evictable(*frame_id, true);
        }

        Ok(())
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame = self
            .pages
            .get(*frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        let (sender, receiver) = mpsc::channel::<Result<()>>();
        //self.disk_scheduler
        //    .schedule_write(Arc::clone(frame_arc), sender);
        let _ = receiver.recv().unwrap();
        frame.set_dirty(false);

        Ok(())
    }

    // pub fn flush_all_pages(&self) {
    //     let page_ids = self.pages_map.keys().to_owned().collect::<Vec<&usize>>();
    //     for page_id in page_ids {
    //         self.flush_page(*page_id).unwrap_or(())
    //     }
    // }

    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame_id = *frame_id;
        let frame = self
            .pages
            .get(frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        if frame.is_pinned() {
            bail!("Page {} is pinned and cannot be deleted.", page_id);
        }

        self.pages_map.remove(&page_id);
        let mut replacer = self.replacer.lock().unwrap();
        replacer.remove(frame_id);
        let mut free_list = self.free_list.lock().unwrap();
        free_list.push(frame_id);
        drop(free_list);
        frame.reset();
        drop(frame);

        self.deallocate_page(page_id)?;

        Ok(())
    }

    fn allocate_page(&self) -> PageId {
        let mut next_page_id = self.next_page_id.lock().unwrap();
        *next_page_id += 1;

        *next_page_id
    }

    fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        Ok(())
    }
}
