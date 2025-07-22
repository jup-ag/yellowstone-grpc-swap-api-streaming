use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use solana_sdk::clock::Slot;

#[derive(Clone)]
pub struct GeyserStreaming {
    pub chunk_count: usize,
    pub slot_tracker: Arc<SlotTracker>,
}

impl GeyserStreaming {
    pub fn new(chunk_count: usize) -> Self {
        Self {
            chunk_count,
            slot_tracker: Arc::new(SlotTracker::new(chunk_count)),
        }
    }
}

impl Default for GeyserStreaming {
    fn default() -> Self {
        let chunk_count = 12;
        Self {
            chunk_count,
            slot_tracker: Arc::new(SlotTracker::new(chunk_count)),
        }
    }
}

pub struct SlotTracker(Vec<AtomicU64>);

impl SlotTracker {
    pub fn new(chunk_count: usize) -> Self {
        Self(
            vec![0; chunk_count]
                .iter_mut()
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>(),
        )
    }

    pub fn update(&self, idx: usize, slot: Slot) {
        self.0[idx].store(slot, Ordering::Relaxed);
    }

    pub fn get_min_max_slow_index(&self) -> (Slot, Slot, usize) {
        let (min, max, slow_index) = self.0.iter().enumerate().fold(
            (u64::MAX, u64::MIN, 100),
            |(mut min, mut max, mut slow_index), (idx, slot)| {
                let slot = slot.load(Ordering::Relaxed);
                if slot < min {
                    min = slot;
                    slow_index = idx
                }
                if slot > max {
                    max = slot;
                }
                min = min.min(slot);
                max = max.max(slot);
                (min, max, slow_index)
            },
        );
        (min, max, slow_index)
    }

    pub fn get_slots(&self) -> Vec<Slot> {
        self.0
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .collect()
    }
}
