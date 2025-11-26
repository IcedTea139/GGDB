/*
* The current implementation is a simple clock replacement, make sure to change in the future 
* Database engines can not use the OS's demand paging algo because they need to 
* optimize demand paging based on query plans and also ensure ACID compliance with the internal page metadata.
* I separated the replacement with the buffer_pool_manager because we are probably going to change it 
* and I'm trying to make it modular, we can add back to buffer_pool_manager.rs when its more concrete
*/

use super::page::Page;
use super::page_constants::FrameId;

pub struct ClockReplacer {
    hand: usize,      // clock hand pointer
    size: usize,      // total number of frames
}

impl ClockReplacer {
    pub fn new(size: usize) -> Self {
        Self { hand: 0, size }
    }

    // Find a victim FrameId to evict.
    // Returns None if all pages are pinned (Deadlock, memory is cooked).
    pub fn victim(&mut self, frames: &mut [Page]) -> Option<FrameId> {
        let start_hand = self.hand;
        
        // Rust loop syntax is interesting
        loop {
            let frame = &mut frames[self.hand];

            // skip pinned pages
            if frame.pin_count > 0 {
                self.advance();
                // if everything is pinned we are cooked
                if self.hand == start_hand { return None; }
                continue;
            }
            
            // clock algo
            if frame.ref_bit {
                frame.ref_bit = false;
                self.advance();
            } else {
                let victim_id = self.hand;
                self.advance();
                return Some(victim_id);
            }
        }
    }

    fn advance(&mut self) {
        self.hand = (self.hand + 1) % self.size;
    }
}