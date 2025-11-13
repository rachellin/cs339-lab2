use super::replacer::Replacer;
use crate::typedef::FrameId;
use std::collections::{HashMap, VecDeque};

/// Represents a node in the LRUKReplacer, maintaining access history and evictability status.
#[derive(Debug)]
struct LrukNode {
    frame_id: FrameId,
    is_evictable: bool,
    history: VecDeque<u64>, // Stores the last K access timestamps
    k: usize,
}

impl LrukNode {
    /// Creates an LRUkNode, which is not evictable by default.
    fn new(frame_id: FrameId, k: usize) -> Self {
        Self {
            frame_id,
            is_evictable: false,
            history: VecDeque::with_capacity(k),
            k,
        }
    }

    /// Checks if the node has an infinite backward K-distance.
    fn has_inf_backward_k_dist(&self) -> bool {
        self.history.len() < self.k
    }

    /// Gets the earliest recorded timestamp.
    fn get_earliest_timestamp(&self) -> u64 {
        *self.history.front().unwrap()
    }

    /// Calculates the backward K-distance of this node.
    fn get_backwards_k_distance(&self, current_timestamp: u64) -> u64 {
        // 1. check if node has been acessed less than k times aka has infinite backward K-distance
        if self.has_inf_backward_k_dist() {
            u64::MAX // encode infinity
        } else {
            // if the node has been accessed at least k times
            // calculate the difference between the current timestamp and the last timestamp
            // large difference ==> last access was a long time ago → candidate for eviction
            // small difference ==> last access was recent → not a candidate for eviction
            current_timestamp - self.history.front().unwrap()
        }
    }

    /// Inserts a new access timestamp, maintaining the last K timestamps.
    fn insert_history_timestamp(&mut self, current_timestamp: u64) {
        assert!(self.history.is_empty() || current_timestamp > *self.history.back().unwrap());
        self.history.push_back(current_timestamp);
        if self.history.len() > self.k {
            self.history.pop_front();
        }
    }
}

/// Implements the LRU-K replacement policy.
#[derive(Debug)]
pub(crate) struct LrukReplacer {
    node_store: HashMap<FrameId, LrukNode>,
    evictable_size: usize, // Number of evictable nodes
    current_timestamp: u64,
    k: usize, // Number of accesses to track
}

impl LrukReplacer {
    /// Creates a new LRU-K replacer instance.
    pub(crate) fn new(k: usize) -> Self {
        LrukReplacer {
            node_store: HashMap::new(),
            evictable_size: 0,
            current_timestamp: 0,
            k,
        }
    }

    /// Increments and returns the current timestamp.
    fn advance_timestamp(&mut self) -> u64 {
        let old_timestamp = self.current_timestamp;
        self.current_timestamp += 1;
        old_timestamp
    }
}

impl Replacer for LrukReplacer {
    /// Records access to a frame and updates its history.
    fn record_access(&mut self, frame_id: FrameId) {
        // 1. get the current timestamp
        let current_ts = self.advance_timestamp();

        // 2. get the node for this frame id
        let node = self
            .node_store
            .entry(frame_id)
            .or_insert_with(|| LrukNode::new(frame_id, self.k));

        // 3. update the timestamp history
        node.insert_history_timestamp(current_ts);
    }

    /// Pins a frame, making it non-evictable.
    fn pin(&mut self, frame_id: FrameId) {
        // do not evict a frame that is in active use
        // 1. get the node for this frame id
        let node = self
            .node_store
            .entry(frame_id)
            .or_insert_with(|| LrukNode::new(frame_id, self.k));
        // 2. update the evictable status
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            // first check that the frame is in the replacer
            if node.is_evictable {
                node.is_evictable = false; // make non-evictable
                self.evictable_size -= 1; // update number of evictable frames
            }
        }
    }

    /// Unpins a frame, making it evictable.
    fn unpin(&mut self, frame_id: FrameId) {
        // 1. get the node for this frame id
        let node = self
            .node_store
            .entry(frame_id)
            .or_insert_with(|| LrukNode::new(frame_id, self.k));
        // 2. update the evictable status
        if !node.is_evictable {
            node.is_evictable = true; // make evictable
            self.evictable_size += 1; // update number of evictable frames
        }
    }

    /// Evicts the frame with the largest backward k-distance.
    fn evict(&mut self) -> Option<FrameId> {
        // 1. handle the case where there are no evictable frames
        if self.evictable_size == 0 {
            return None;
        }

        let current_ts = self.current_timestamp;
        let mut candidate: Option<(FrameId, u64, u64)> = None;

        // 2. iterate over all the frames in the replacer
        for node in self.node_store.values() {
            // skip frames that are not evictable
            if !node.is_evictable {
                continue;
            }

            // 3. calculate the backward k-distance and oldest timestamp for each frame
            let dist = node.get_backwards_k_distance(current_ts);
            let earliest = node.get_earliest_timestamp();

            // choose the best candidate
            match &candidate {
                None => candidate = Some((node.frame_id, dist, earliest)),
                Some((_, best_dist, best_ts)) => {
                    if dist > *best_dist // this frame's k-distance is bigger -> less recently used -> better eviction candidate
                        || (dist == *best_dist && earliest < *best_ts)
                    // k-distances are the same -> choose the one with the older timestamp
                    {
                        candidate = Some((node.frame_id, dist, earliest));
                    }
                }
            }
        }

        // 4. evict the candidate frame
        if let Some((frame_id, _, _)) = candidate {
            self.node_store.remove(&frame_id); // remove
            self.evictable_size -= 1; // update number of evictable frames
            Some(frame_id) // return evicted frame id so the buffer pool knows which one to evict
        } else {
            None
        }
    }

    /// Removes a frame from the replacer if it is evictable.
    fn remove(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get(&frame_id) {
            // first check that the frame is in the replacer
            if node.is_evictable {
                self.node_store.remove(&frame_id); // remove the frame
                self.evictable_size -= 1; // update number of evictable frames
            }
        }
    }


    /// Returns the number of evictable frames.
    fn evictable_count(&self) -> usize {
        self.evictable_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lruk_replacer_one() {
        let mut lru_replacer = LrukReplacer::new(2);

        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.unpin(1);
        lru_replacer.unpin(2);
        lru_replacer.unpin(3);
        lru_replacer.unpin(4);
        lru_replacer.unpin(5);
        lru_replacer.pin(6);

        assert_eq!(5, lru_replacer.evictable_count());

        lru_replacer.record_access(1);
        assert_eq!(Some(2), lru_replacer.evict());
        assert_eq!(Some(3), lru_replacer.evict());
        assert_eq!(Some(4), lru_replacer.evict());
        assert_eq!(2, lru_replacer.evictable_count());

        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.unpin(3);
        lru_replacer.unpin(4);
        assert_eq!(4, lru_replacer.evictable_count());

        assert_eq!(Some(3), lru_replacer.evict());
        assert_eq!(3, lru_replacer.evictable_count());

        lru_replacer.unpin(6);
        assert_eq!(4, lru_replacer.evictable_count());
        assert_eq!(Some(6), lru_replacer.evict());
        assert_eq!(3, lru_replacer.evictable_count());

        lru_replacer.pin(1);
        assert_eq!(2, lru_replacer.evictable_count());
        assert_eq!(Some(5), lru_replacer.evict());
        assert_eq!(1, lru_replacer.evictable_count());

        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.unpin(1);
        assert_eq!(2, lru_replacer.evictable_count());

        assert_eq!(Some(4), lru_replacer.evict());
        assert_eq!(1, lru_replacer.evictable_count());
        assert_eq!(Some(1), lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        lru_replacer.record_access(1);
        lru_replacer.pin(1);
        assert_eq!(0, lru_replacer.evictable_count());

        assert_eq!(None, lru_replacer.evict());

        lru_replacer.unpin(1);
        assert_eq!(1, lru_replacer.evictable_count());
        assert_eq!(Some(1), lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        assert_eq!(None, lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        lru_replacer.pin(6);
        lru_replacer.unpin(6);
    }

    #[test]
    fn test_lruk_replacer_two() {
        let mut lru_replacer = LrukReplacer::new(2);

        // Add six frames to the replacer. Frame 6 is non-evictable.
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.unpin(1);
        lru_replacer.unpin(2);
        lru_replacer.unpin(3);
        lru_replacer.unpin(4);
        lru_replacer.unpin(5);
        lru_replacer.pin(6);

        // The size of the replacer is the number of evictable frames
        assert_eq!(5, lru_replacer.evictable_count());

        // Record an access for frame 1
        lru_replacer.record_access(1);

        // Evict three pages
        assert_eq!(Some(2), lru_replacer.evict());
        assert_eq!(Some(3), lru_replacer.evict());
        assert_eq!(Some(4), lru_replacer.evict());
        assert_eq!(2, lru_replacer.evictable_count());

        // Insert new frames [3, 4] and update history
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.unpin(3);
        lru_replacer.unpin(4);
        assert_eq!(4, lru_replacer.evictable_count());

        // Expect frame 3 to be evicted next
        assert_eq!(Some(3), lru_replacer.evict());
        assert_eq!(3, lru_replacer.evictable_count());

        // Set frame 6 to be evictable and evict it
        lru_replacer.unpin(6);
        assert_eq!(4, lru_replacer.evictable_count());
        assert_eq!(Some(6), lru_replacer.evict());
        assert_eq!(3, lru_replacer.evictable_count());

        // Mark frame 1 as non-evictable
        lru_replacer.pin(1);
        assert_eq!(2, lru_replacer.evictable_count());

        // Expect frame 5 to be evicted next
        assert_eq!(Some(5), lru_replacer.evict());
        assert_eq!(1, lru_replacer.evictable_count());

        // Update history for frame 1 and make it evictable
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.unpin(1);
        assert_eq!(2, lru_replacer.evictable_count());

        // Evict the last two frames
        assert_eq!(Some(4), lru_replacer.evict());
        assert_eq!(1, lru_replacer.evictable_count());
        assert_eq!(Some(1), lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        // Insert frame 1 again and mark it as non-evictable
        lru_replacer.record_access(1);
        lru_replacer.pin(1);
        assert_eq!(0, lru_replacer.evictable_count());

        // A failed eviction should not change the size of the replacer
        assert_eq!(None, lru_replacer.evict());

        // Mark frame 1 as evictable again and evict it
        lru_replacer.unpin(1);
        assert_eq!(1, lru_replacer.evictable_count());
        assert_eq!(Some(1), lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        // Ensure that eviction on an empty replacer does nothing strange
        assert_eq!(None, lru_replacer.evict());
        assert_eq!(0, lru_replacer.evictable_count());

        // Ensure setting a non-existent frame as evictable does not cause issues
        lru_replacer.unpin(6);
    }

    #[test]
    fn test_lruk_replacer_evict() {
        {
            // Empty and try removing
            let mut lru_replacer = LrukReplacer::new(2);
            assert_eq!(None, lru_replacer.evict());
        }

        {
            // Can only evict element if evictable=true
            let mut lru_replacer = LrukReplacer::new(2);
            lru_replacer.record_access(2);
            lru_replacer.pin(2);
            assert_eq!(None, lru_replacer.evict());
            lru_replacer.unpin(2);
            assert_eq!(Some(2), lru_replacer.evict());
        }

        {
            // Elements with less than k history should have max backward k-dist and get evicted first
            let mut lru_replacer = LrukReplacer::new(3);
            lru_replacer.record_access(1);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(1);
            lru_replacer.unpin(2);
            lru_replacer.unpin(1);

            assert_eq!(Some(2), lru_replacer.evict());
            assert_eq!(Some(1), lru_replacer.evict());
        }

        {
            // Select element with largest backward k-dist to evict
            let mut lru_replacer = LrukReplacer::new(3);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(3);
            lru_replacer.record_access(3);
            lru_replacer.record_access(3);
            lru_replacer.record_access(2);
            lru_replacer.record_access(2);
            lru_replacer.record_access(1);
            lru_replacer.record_access(1);
            lru_replacer.record_access(3);
            lru_replacer.record_access(2);
            lru_replacer.record_access(1);
            lru_replacer.unpin(2);
            lru_replacer.unpin(1);
            lru_replacer.unpin(3);

            assert_eq!(Some(3), lru_replacer.evict());
            assert_eq!(Some(2), lru_replacer.evict());
            assert_eq!(Some(1), lru_replacer.evict());
        }

        {
            let mut lru_replacer = LrukReplacer::new(3);
            lru_replacer.record_access(2);
            lru_replacer.record_access(2);
            lru_replacer.record_access(2);
            lru_replacer.record_access(1);
            lru_replacer.record_access(1);
            lru_replacer.unpin(2);
            lru_replacer.unpin(1);

            assert_eq!(Some(1), lru_replacer.evict());

            lru_replacer.record_access(1);
            lru_replacer.unpin(1);

            assert_eq!(Some(1), lru_replacer.evict());
        }

        {
            let mut lru_replacer = LrukReplacer::new(3);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(3);
            lru_replacer.record_access(4);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(3);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.unpin(1);
            lru_replacer.unpin(2);
            lru_replacer.unpin(3);
            lru_replacer.unpin(4);

            assert_eq!(Some(3), lru_replacer.evict());
            lru_replacer.record_access(4);
            lru_replacer.record_access(4);

            assert_eq!(Some(1), lru_replacer.evict());
            assert_eq!(Some(2), lru_replacer.evict());
            assert_eq!(Some(4), lru_replacer.evict());
        }

        {
            let mut lru_replacer = LrukReplacer::new(2);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(3);
            lru_replacer.record_access(4);
            lru_replacer.record_access(1);
            lru_replacer.record_access(2);
            lru_replacer.record_access(3);
            lru_replacer.record_access(4);

            lru_replacer.unpin(2);
            lru_replacer.unpin(1);

            assert_eq!(Some(1), lru_replacer.evict());

            lru_replacer.record_access(5);
            lru_replacer.unpin(5);
            assert_eq!(Some(5), lru_replacer.evict());
        }

        {
            let mut lru_replacer = LrukReplacer::new(3);
            for j in 0..4 {
                for i in (j * 250)..1000 {
                    lru_replacer.record_access(i);
                    lru_replacer.unpin(i);
                }
            }
            assert_eq!(1000, lru_replacer.evictable_count());

            for i in 250..500 {
                lru_replacer.pin(i);
            }
            assert_eq!(750, lru_replacer.evictable_count());

            for i in 0..100 {
                lru_replacer.remove(i);
            }
            assert_eq!(650, lru_replacer.evictable_count());

            for i in 100..600 {
                if i < 250 || i >= 500 {
                    assert_eq!(Some(i), lru_replacer.evict());
                }
            }
            assert_eq!(400, lru_replacer.evictable_count());

            for i in 250..500 {
                lru_replacer.unpin(i);
            }
            assert_eq!(650, lru_replacer.evictable_count());

            for i in 600..750 {
                lru_replacer.record_access(i);
                lru_replacer.record_access(i);
            }
            assert_eq!(650, lru_replacer.evictable_count());

            for i in 250..500 {
                assert_eq!(Some(i), lru_replacer.evict());
            }
            assert_eq!(400, lru_replacer.evictable_count());

            for i in 750..1000 {
                assert_eq!(Some(i), lru_replacer.evict());
            }
            assert_eq!(150, lru_replacer.evictable_count());

            for i in 600..750 {
                assert_eq!(Some(i), lru_replacer.evict());
            }
            assert_eq!(0, lru_replacer.evictable_count());
        }
    }
}
