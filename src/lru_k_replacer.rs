use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub type FrameId = usize;
pub type Timestamp = u128;

fn get_now_ts() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

struct LruKNode {
    k: usize,
    frame_id: FrameId,
    is_evictable: bool,
    history: Vec<Timestamp>,
}

impl LruKNode {
    fn new(frame_id: FrameId, k: usize) -> Self {
        let mut history: Vec<Timestamp> = Vec::with_capacity(k);
        history.push(get_now_ts());

        Self {
            k,
            frame_id,
            history,
            is_evictable: false,
        }
    }

    fn record_access(&mut self) {
        self.history.push(get_now_ts());
    }

    fn k_distance(&self) -> Option<usize> {
        if self.history.len() < self.k {
            return None;
        }
        let history_entry = self.history[self.history.len() - self.k];
        Some((get_now_ts() - history_entry) as usize)
    }

    fn least_recent_access(&self) -> Timestamp {
        self.history[self.history.len() - 1]
    }
}

// TODO: original has DISALLOW_COPY_AND_MOVE
pub struct LruKReplacer {
    num_of_frames: usize,
    k: usize,
    node_store: HashMap<FrameId, LruKNode>,
}

pub enum AccessType {
    Unknown,
    Lookup,
    Scan,
    Index,
}

impl LruKReplacer {
    pub fn new(num_of_frames: usize, k: usize) -> Self {
        let node_store = HashMap::default();
        Self {
            num_of_frames,
            k,
            node_store,
        }
    }

    pub fn evict(&self) -> Option<FrameId> {
        // let rr = self
        //     .node_store
        //     .values()
        //     .map(|node| node.k_distance().unwrap_or(usize::MAX))
        //     .reduce(|max, distance| {
        //         if distance > max {
        //             distance
        //         }
        //     });
        None
    }

    pub fn record_access(&mut self, frame_id: FrameId, _access_type: AccessType) {
        let node = self.node_store.get_mut(&frame_id);

        match node {
            Some(node) => node.record_access(),
            _ => {
                let new_node = LruKNode::new(frame_id, self.k);
                self.node_store.insert(frame_id, new_node);
            }
        };
    }

    pub fn remove(&mut self, frame_id: FrameId) {
        self.node_store.remove(&frame_id);
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, is_evictable: bool) {
        let node = self.node_store.get_mut(&frame_id);
        match node {
            Some(node) => {
                node.is_evictable = is_evictable;
            }
            // TODO: return Result if no node for frame_id
            _ => {}
        };
    }

    pub fn size(&self) -> usize {
        self.node_store
            .values()
            .filter(|node| node.is_evictable)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_init_node() {
        let now = get_now_ts();
        let node = LruKNode::new(10, 2);

        // TODO: rework
        assert_eq!(node.least_recent_access() - now < 100, true);
        assert_eq!(node.k_distance(), None);
    }

    #[test]
    fn test_history() {
        let mut node = LruKNode::new(10, 3);
        node.record_access();
        node.record_access();

        assert_eq!(node.k_distance().is_some(), true);
    }

    #[test]
    fn test_init_replacer() {
        let replacer = LruKReplacer::new(10, 2);

        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_size_after_record_access() {
        let mut replacer = LruKReplacer::new(10, 2);

        replacer.record_access(12, AccessType::Unknown);
        replacer.record_access(13, AccessType::Unknown);

        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_size_after_set_evictable() {
        let frame_id = 12;
        let mut replacer = LruKReplacer::new(10, 2);

        replacer.record_access(frame_id, AccessType::Unknown);
        replacer.set_evictable(frame_id, true);
        assert_eq!(replacer.size(), 1);

        replacer.set_evictable(frame_id, false);
        assert_eq!(replacer.size(), 0);
    }
}
