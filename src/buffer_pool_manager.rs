use anyhow::{bail, Context, Result};
use std::{collections::HashMap, sync::mpsc};

use crate::{
    disk_manager::DiskManager,
    disk_scheduler::DiskScheduler,
    lru_k_replacer::{AccessType, FrameId, LruKReplacer},
    page::Page,
    PageId,
};

struct BufferPoolFlushManager {}

pub struct BufferPoolManager {
    free_list: Vec<FrameId>,
    pages: Vec<Page>,
    replacer: LruKReplacer,
    disk_scheduler: DiskScheduler,
    pages_map: HashMap<PageId, FrameId>,
    next_page_id: PageId,
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager, pool_size: usize, replacer_k: usize) -> Self {
        let replacer = LruKReplacer::new(pool_size, replacer_k);
        // TODO: consider passing references
        let disk_scheduler = DiskScheduler::new(disk_manager);
        let pages_map: HashMap<PageId, FrameId> = HashMap::default();
        let mut pages: Vec<Page> = Vec::with_capacity(pool_size);
        let mut free_list: Vec<FrameId> = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            free_list.push(i);
            pages.push(Page::new());
        }

        Self {
            pages,
            free_list,
            replacer,
            disk_scheduler,
            pages_map,
            next_page_id: 0,
        }
    }

    pub fn new_page(&mut self) -> Option<&mut Page> {
        let frame_id = self.free_list.pop().or_else(|| self.replacer.evict());
        frame_id.map(|frame_id| {
            let page_id = self.allocate_page();
            let page = self.pages.get_mut(frame_id).unwrap();

            if page.is_dirty() {
                let (sender, receiver) = mpsc::channel::<Result<()>>();
                self.disk_scheduler
                    .schedule_write(page_id, page.data(), sender);
                let _ = receiver.recv().unwrap();
            }
            page.reset();

            self.pages_map.insert(page_id, frame_id);
            self.replacer.set_evictable(frame_id, false);
            self.replacer.record_access(frame_id, AccessType::Unknown);

            page
        })
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Option<&Page> {
        let frame_id = self.pages_map.get(&page_id);
        if let Some(frame_id) = frame_id {
            return self.pages.get(*frame_id);
        }

        let frame_id = self.free_list.pop().or_else(|| self.replacer.evict());
        frame_id.map(|frame_id| {
            let page = self.pages.get_mut(frame_id).unwrap();

            if page.is_dirty() {
                let (sender, receiver) = mpsc::channel::<Result<()>>();
                self.disk_scheduler
                    .schedule_write(page_id, page.data(), sender);
                let _ = receiver.recv().unwrap();
            }
            page.reset();
            let (sender, receiver) = mpsc::channel::<Result<()>>();
            self.disk_scheduler
                .schedule_read(page_id, page.data(), sender);
            let _ = receiver.recv().unwrap();

            self.pages_map.insert(page_id, frame_id);
            self.replacer.set_evictable(frame_id, false);
            self.replacer.record_access(frame_id, AccessType::Unknown);

            &*page
        })
    }

    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame = self
            .pages
            .get_mut(*frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        frame.unpin();
        frame.set_dirty(is_dirty);

        if !frame.is_pinned() {
            self.replacer.set_evictable(*frame_id, true);
        }

        Ok(())
    }

    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame = self
            .pages
            .get_mut(*frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        let (sender, receiver) = mpsc::channel::<Result<()>>();
        self.disk_scheduler
            .schedule_write(page_id, frame.data(), sender);
        let _ = receiver.recv().unwrap();
        frame.set_dirty(false);

        Ok(())
    }

    // pub fn flush_all_pages(&mut self) {
    //     let page_ids = self.pages_map.keys().to_owned().collect::<Vec<&usize>>();
    //     for page_id in page_ids {
    //         self.flush_page(*page_id).unwrap_or(())
    //     }
    // }

    pub fn delete_page(&mut self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .pages_map
            .get(&page_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;
        let frame_id = *frame_id;
        let frame = self
            .pages
            .get_mut(frame_id)
            .with_context(|| format!("Page {} is not in buffer pool.", page_id))?;

        if frame.is_pinned() {
            bail!("Page {} is pinned and cannot be deleted.", page_id);
        }

        self.pages_map.remove(&page_id);
        self.replacer.remove(frame_id);
        self.free_list.push(frame_id);
        frame.reset();

        self.deallocate_page(page_id)?;

        Ok(())
    }

    fn allocate_page(&mut self) -> PageId {
        self.next_page_id += 1;

        self.next_page_id
    }

    fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        Ok(())
    }
}