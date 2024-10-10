use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use std::usize;

pub type FrameId = usize;
pub type Timestamp = u128;

fn get_now_ts() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[derive(Debug)]
struct LruKNode {
    k: usize,
    frame_id: FrameId,
    is_evictable: bool,
    history: VecDeque<Timestamp>,
}

impl LruKNode {
    fn new(frame_id: FrameId, k: usize) -> Self {
        assert!(k > 0);
        let mut history: VecDeque<Timestamp> = VecDeque::with_capacity(k);
        history.push_front(get_now_ts());

        Self {
            k,
            frame_id,
            history,
            is_evictable: false,
        }
    }

    fn record_access(&mut self) {
        self.history.push_front(get_now_ts());

        if self.history.len() > self.k {
            self.history.pop_back();
        }
    }

    fn k_distance(&self) -> Option<usize> {
        if self.history.len() < self.k {
            return None;
        }
        let kth_history_entry = self.history[self.history.len() - 1];

        Some((get_now_ts() - kth_history_entry) as usize)
    }

    fn least_recent_access(&self) -> Timestamp {
        self.history[0]
    }

    fn get_is_evictable(&self) -> bool {
        self.is_evictable
    }
}

#[derive(Debug)]
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
        let longest_k_distance_node = self.node_store.iter().max_by(|x, y| {
            x.1.k_distance()
                .unwrap_or(usize::MAX)
                .cmp(&y.1.k_distance().unwrap_or(usize::MAX))
        });

        let longest_k_distance = longest_k_distance_node?
            .1
            .k_distance()
            .unwrap_or(usize::MAX);

        // it is possible to have multiple nodes with same longest k distance
        let nodes_with_longest_k_distance = self
            .node_store
            .iter()
            .filter(|(_, value)| {
                value.get_is_evictable()
                    && value.k_distance().unwrap_or(usize::MAX) == longest_k_distance
            })
            .collect::<Vec<(&FrameId, &LruKNode)>>();

        if nodes_with_longest_k_distance.len() == 1 {
            return Some(*nodes_with_longest_k_distance[0].0);
        }

        let least_recent_accessed_node = nodes_with_longest_k_distance
            .iter()
            .map(|(key, value)| (*key, value.least_recent_access()))
            .min_by(|x, y| x.1.cmp(&y.1));

        least_recent_accessed_node.map(|(frame_id, _)| *frame_id)
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
        if let Some(node) = node {
            node.is_evictable = is_evictable;
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
        assert!(node.least_recent_access() - now < 1000000);
        assert_eq!(node.k_distance(), None);
    }

    #[test]
    fn test_history() {
        let mut node = LruKNode::new(10, 3);
        node.record_access();
        node.record_access();

        assert!(node.k_distance().is_some());
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

    #[test]
    fn test_eviction_1() {
        let mut replacer = LruKReplacer::new(10, 2);
        let first_frame_id = 10;
        let second_frame_id = 11;
        let third_frame_id = 12;
        replacer.record_access(first_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);
        // access first frame one more time
        replacer.record_access(first_frame_id, AccessType::Unknown);

        replacer.set_evictable(first_frame_id, true);
        replacer.set_evictable(second_frame_id, true);
        replacer.set_evictable(third_frame_id, true);

        let frame_id = replacer.evict();

        assert_eq!(frame_id, Some(second_frame_id));
    }

    // frame with no k acesses and least recent access should be evicted
    #[test]
    fn test_eviction_2() {
        let mut replacer = LruKReplacer::new(10, 3);
        let first_frame_id = 10;
        let second_frame_id = 11;
        let third_frame_id = 12;
        replacer.record_access(first_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);

        replacer.set_evictable(first_frame_id, true);
        replacer.set_evictable(second_frame_id, true);
        replacer.set_evictable(third_frame_id, true);

        let frame_id = replacer.evict();

        assert_eq!(frame_id, Some(first_frame_id));
    }

    // multiple frames have the same backward k-distance.
    #[test]
    fn test_eviction_3() {
        let mut replacer = LruKReplacer::new(10, 2);
        let first_frame_id = 10;
        let second_frame_id = 11;
        replacer.record_access(first_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);

        replacer.set_evictable(first_frame_id, true);
        replacer.set_evictable(second_frame_id, true);

        let frame_id = replacer.evict();

        assert_eq!(frame_id, Some(first_frame_id));
    }

    // ensure that a frame marked as non-evictable is not evicted.
    // first frame marked as not evictable, so it should not be evicted.
    #[test]
    fn test_eviction_4() {
        let mut replacer = LruKReplacer::new(10, 3);
        let first_frame_id = 10;
        let second_frame_id = 11;
        let third_frame_id = 12;
        replacer.record_access(first_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);
        replacer.record_access(second_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);
        replacer.record_access(third_frame_id, AccessType::Unknown);

        replacer.set_evictable(second_frame_id, true);
        replacer.set_evictable(third_frame_id, true);

        let frame_id = replacer.evict();

        assert_eq!(frame_id, Some(second_frame_id));
    }

    // remove frame from replacer
    #[test]
    fn test_eviction_5() {
        let mut replacer = LruKReplacer::new(10, 3);
        let first_frame_id = 10;
        replacer.record_access(first_frame_id, AccessType::Unknown);

        replacer.set_evictable(first_frame_id, true);
        replacer.remove(first_frame_id);
        
        let frame_id = replacer.evict();

        assert_eq!(frame_id, None);
    }
}
