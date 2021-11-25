use super::{FrameID, PageID, StorageError};

#[derive(Clone, Default)]
struct ClockItem {
    /// mark whether this frame is unpinned after last handle slide
    ref_bit: bool,
    /// mark whether the frame is under using
    pinned: bool,
}

impl ClockItem {
    pub fn pin(&mut self) -> bool {
        let already_pinned = self.pinned;
        self.pinned = true;
        !already_pinned
    }
    pub fn unpin(&mut self) {
        assert!(self.pinned);
        self.pinned = false;
        self.ref_bit = true;
    }
    pub fn try_victim(&mut self) -> bool {
        if self.pinned {
            false
        } else if self.ref_bit {
            self.ref_bit = false;
            false
        } else {
            self.pinned = true;
            true
        }
    }
}

pub struct ClockReplacer {
    /// the clock
    clock: Vec<ClockItem>,
    /// clock handle
    handle: usize,
    /// size of unpinned frame
    num_unpinned: usize,
}

impl ClockReplacer {
    pub fn new(size: usize) -> Self {
        Self {
            clock: vec![ClockItem::default(); size],
            handle: 0,
            num_unpinned: size,
        }
    }

    #[allow(dead_code)]
    pub fn erase(&mut self) {
        self.handle = 0;
        self.num_unpinned = self.clock.len();
        self.clock.fill(ClockItem::default());
    }

    /// we only unpin a frame when the pin count is 0
    pub fn unpin(&mut self, frame_id: FrameID) {
        assert!(frame_id < self.clock.len());
        self.clock[frame_id].unpin();
        self.num_unpinned += 1;
    }

    /// we may pin a frame multiple times
    pub fn pin(&mut self, frame_id: FrameID) {
        assert!(frame_id < self.clock.len());
        if self.clock[frame_id].pin() {
            self.num_unpinned -= 1;
        }
    }

    /// advance the clock handle, return the old handle
    fn advance(&mut self) -> usize {
        let handle = self.handle;
        self.handle += 1;
        if self.handle == self.clock.len() {
            self.handle = 0;
        }
        handle
    }

    /// victim a frame from clock, the action will
    /// pin the frame automatically
    pub fn victim(&mut self) -> Result<PageID, StorageError> {
        if self.num_unpinned == 0 {
            return Err(StorageError::ReplacerError(
                "all frames are pinned".to_string(),
            ));
        }
        loop {
            let handle = self.advance();
            if self.clock[handle].try_victim() {
                self.num_unpinned -= 1;
                return Ok(handle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_clock_replacer_test() {
        // simple victim
        let mut replacer = ClockReplacer::new(5);
        assert_eq!(replacer.victim().unwrap(), 0);
        assert_eq!(replacer.victim().unwrap(), 1);
        assert_eq!(replacer.victim().unwrap(), 2);
        assert_eq!(replacer.victim().unwrap(), 3);
        assert_eq!(replacer.victim().unwrap(), 4);
        // assume no space
        assert!(replacer.victim().is_err());
        // unpin and pin
        replacer.unpin(1);
        replacer.unpin(2);
        replacer.unpin(3);
        replacer.pin(2);
        assert_eq!(replacer.victim().unwrap(), 1);
        assert_eq!(replacer.victim().unwrap(), 3);
    }
}
