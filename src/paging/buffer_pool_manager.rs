/* 
* Implements reading, allocating, and writing to pages on RAM
* Controls what pages live in RAM, implements replacement algorithm (demand paging)
* Very similar to the global frame table from OS project 
*/


use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use super::page_constants::{PageId, PAGE_SIZE, FrameId, BUFFER_SIZE};
use super::page::Page;
use super::replacement::ClockReplacer;

/*
* Page guard is simply a structure to prevent race conditions with RAII.
* Will automatically unpin when the PageFrameRef goes out of scope.
* We may need a WriteGuard in the future too to ensure the dirty bit is guarded.
*/
pub struct PageFrameRef<'a> {
    pub bpm: &'a BufferPoolManager, // based on lifetime of the bpm
    pub page_id: PageId,
    pub frame_index: FrameId,
}

// When you implement Deref, users can treat this Guard like a Page
// 'a simply means lifetime impl<'a> here means it will live as long as bufferpoolmanager
//this method tells what do we do upon dereferencing a pageframeref
//TODO: research some rust and see how safe these functions actually are
impl<'a> std::ops::Deref for PageFrameRef<'a> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        // The lock should be automatically dropped after deref
        let state = self.bpm.state.lock().unwrap();
        unsafe { &*(&state.frames[self.frame_index] as *const Page) }
    }
}
//this ensures we never forget to unpin a page
impl<'a> Drop for PageFrameRef<'a> {
    fn drop(&mut self) {
        // Auto-Unpin when this variable goes out of scope
        self.bpm.unpin_page(self.page_id, false); 
        // Note: We default dirty=false here. 
        // TODO: WriteGuard that sets dirty=true.
    }
}

// RAM state and manager
pub struct BufferPoolState {
    // physical frames
    frames: Vec<Page>,
    
    // Maps pagid to frameid TODO: We probably need a more robust way to map page to frame
    page_mapping: HashMap<PageId, FrameId>,
    
    // List of frames that have NEVER been used (fast path)
    free_list: VecDeque<FrameId>,
    
    // The Eviction Algo
    replacer: ClockReplacer,
    
    // Helper to read/write disk TODO: implement the disk manager
    // disk_manager: DiskManager, 
}

pub struct BufferPoolManager {
    state: Mutex<BufferPoolState>, // We are currently wrapping the entire page table in a mutex, not very good performance,
                                   // we probably need to lock each structure individually later
}

impl BufferPoolManager {
    //initiates buffer pool to size of pool_size
    pub fn new(pool_size: usize) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::new();

        for i in 0..pool_size {
            frames.push(Page::default()); //use default pages (page types shouldn't matter)
            free_list.push_back(i);
        }

        let state = BufferPoolState {
            frames,
            page_mapping: HashMap::new(),
            free_list,
            replacer: ClockReplacer::new(pool_size),
            // disk_manager: DiskManager::new("data.db"), implement later
        };

        Self { state: Mutex::new(state) }
    }

    // fetches a page frame RAM if present, if not add it in and evict if needed
    pub fn fetch_page(&self, page_id: PageId) -> Option<PageFrameRef> {
        let mut state = self.state.lock().unwrap();

        // check if page in RAM
        if let Some(&frame_id) = state.page_mapping.get(&page_id) {
            state.frames[frame_id].pin_count += 1;
            state.frames[frame_id].ref_bit = true; 
            return Some(PageFrameRef { bpm: self, page_id, frame_index: frame_id });
        }

        // Not in RAM. Find a frame to use.
        let frame_id= self.find_free_frame(&mut state)?;

        // if the victim frame is dirty, write it to disk, TODO: need to implement disk writing
        let old_page_id = state.frames[frame_id].page_id;
        if let Some(old_pid) = old_page_id {
            if state.frames[frame_id].is_dirty {
                // state.disk_manager.write_page(old_pid, &state.frames[frame_id].data);
            }
            state.page_mapping.remove(&old_pid);
        }

        // Read new page from disk, TODO
        // state.disk_manager.read_page(page_id, &mut state.frames[frame_id].data);
        
        //Update Metadata
        state.frames[frame_id].page_id = Some(page_id);
        state.frames[frame_id].pin_count = 1;
        state.frames[frame_id].is_dirty = false;
        state.page_mapping.insert(page_id, frame_id);

        Some(PageFrameRef { bpm: self, page_id, frame_index: frame_id })
    }


    // Helper to find a free frame or evict one
    fn find_free_frame(&self, state: &mut BufferPoolState) -> Option<FrameId> {
        // Try free list first
        if let Some(fid) = state.free_list.pop_front() {
            return Some(fid);
        }
        // Run clock algo to find and return victim
        state.replacer.victim(&mut state.frames)
    }

    // Called by the PageGuard when it drops
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) {
        let mut state = self.state.lock().unwrap(); // heard unwrap caused cloudflare outage, might not be so safe
        if let Some(&frame_id) = state.page_mapping.get(&page_id) {
            let frame = &mut state.frames[frame_id];
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
            if is_dirty {
                frame.is_dirty = true;
            }
        }
    }
}