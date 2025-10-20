use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Page size constant - 8KB
const PAGE_SIZE: usize = 8192;

/// Size of the page header in bytes
const HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();

/// Page header containing metadata
#[repr(C)]
#[derive(Debug, Clone)]
pub struct PageHeader {
    /// Unique identifier for this page
    page_id: u64,
    
    /// Log Sequence Number for write-ahead logging and recovery
    lsn: u64,
    
    /// Pointer to the start of free space (offset from page start)
    free_space_pointer: u32,
    
    /// Pin count - number of threads currently using this page
    pin_count: u32,
}

impl PageHeader {
    /// Create a new page header with the given page ID
    pub fn new(page_id: u64) -> Self {
        Self {
            page_id,
            lsn: 0,
            free_space_pointer: HEADER_SIZE as u32,
            pin_count: 0,
        }
    }
}

/// A page in the database buffer pool
/// Uses heap storage with metadata header
pub struct Page {
    /// Page header containing metadata
    header: PageHeader,
    
    /// Raw data buffer (8KB total, including header)
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new empty page with the given page ID
    pub fn new(page_id: u64) -> Self {
        let mut page = Self {
            header: PageHeader::new(page_id),
            data: [0; PAGE_SIZE],
        };
        
        // Write header to the beginning of data
        page.write_header();
        page
    }
    
    /// Create a page from raw bytes (e.g., when reading from disk)
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        let mut page = Self {
            header: PageHeader::new(0),
            data,
        };
        
        // Read header from data
        page.read_header();
        page
    }
    
    // ==================== Header Management ====================
    
    /// Write the current header to the data buffer
    fn write_header(&mut self) {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &self.header as *const PageHeader as *const u8,
                HEADER_SIZE,
            )
        };
        self.data[..HEADER_SIZE].copy_from_slice(header_bytes);
    }
    
    /// Read the header from the data buffer
    fn read_header(&mut self) {
        unsafe {
            let header_ptr = self.data.as_ptr() as *const PageHeader;
            self.header = (*header_ptr).clone();
        }
    }
    
    // ==================== Getters ====================
    
    /// Get the page ID
    pub fn get_page_id(&self) -> u64 {
        self.header.page_id
    }
    
    /// Get the Log Sequence Number
    pub fn get_lsn(&self) -> u64 {
        self.header.lsn
    }
    
    /// Get the free space pointer (offset from start of page)
    pub fn get_free_space_pointer(&self) -> u32 {
        self.header.free_space_pointer
    }
    
    /// Get the pin count
    pub fn get_pin_count(&self) -> u32 {
        self.header.pin_count
    }
    
    /// Check if the page is dirty
    pub fn is_dirty(&self) -> bool {
        self.header.dirty
    }
    
    /// Check if the page is pinned (pin_count > 0)
    pub fn is_pinned(&self) -> bool {
        self.header.pin_count > 0
    }
    
    /// Get the amount of free space available in bytes
    pub fn get_free_space(&self) -> usize {
        PAGE_SIZE - self.header.free_space_pointer as usize
    }
    
    /// Get a reference to the raw data buffer
    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }
    
    /// Get a mutable reference to the raw data buffer
    pub fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
    
    /// Get a slice of the data segment (excluding header)
    pub fn get_data_segment(&self) -> &[u8] {
        &self.data[HEADER_SIZE..]
    }
    
    /// Get a mutable slice of the data segment (excluding header)
    pub fn get_data_segment_mut(&mut self) -> &mut [u8] {
        &mut self.data[HEADER_SIZE..]
    }
    
    // ==================== Setters ====================
    
    /// Set the page ID
    pub fn set_page_id(&mut self, page_id: u64) {
        self.header.page_id = page_id;
        self.write_header();
    }
    
    /// Set the Log Sequence Number
    pub fn set_lsn(&mut self, lsn: u64) {
        self.header.lsn = lsn;
        self.write_header();
    }
    
    /// Set the free space pointer
    pub fn set_free_space_pointer(&mut self, pointer: u32) {
        self.header.free_space_pointer = pointer;
        self.write_header();
    }
    
    /// Set the pin count directly (use with caution - prefer pin/unpin methods)
    pub fn set_pin_count(&mut self, count: u32) {
        self.header.pin_count = count;
        self.write_header();
    }
    
    /// Set the dirty bit
    pub fn set_dirty(&mut self, dirty: bool) {
        self.header.dirty = dirty;
        self.write_header();
    }
    
    // ==================== Pin Management ====================
    
    /// Increment the pin count (called when a thread starts using the page)
    pub fn pin(&mut self) {
        self.header.pin_count += 1;
        self.write_header();
    }
    
    /// Decrement the pin count (called when a thread is done with the page)
    /// Returns true if successful, false if already at 0
    pub fn unpin(&mut self) -> bool {
        if self.header.pin_count > 0 {
            self.header.pin_count -= 1;
            self.write_header();
            true
        } else {
            false
        }
    }
    
    // ==================== Dirty Bit Management ====================
    
    /// Mark the page as dirty (modified)
    pub fn mark_dirty(&mut self) {
        self.header.dirty = true;
        self.write_header();
    }
    
    /// Mark the page as clean (not modified)
    pub fn mark_clean(&mut self) {
        self.header.dirty = false;
        self.write_header();
    }
    
    // ==================== Space Management ====================
    
    /// Check if the page has room for the given number of bytes
    pub fn has_room(&self, bytes_needed: usize) -> bool {
        self.get_free_space() >= bytes_needed
    }
    
    /// Allocate space in the page and return the offset where data can be written
    /// Returns None if there isn't enough space
    pub fn allocate(&mut self, size: usize) -> Option<u32> {
        if !self.has_room(size) {
            return None;
        }
        
        let offset = self.header.free_space_pointer;
        self.header.free_space_pointer += size as u32;
        self.mark_dirty();
        self.write_header();
        
        Some(offset)
    }
    
    /// Reset the page to its initial empty state (keeps page_id)
    pub fn reset(&mut self) {
        let page_id = self.header.page_id;
        self.header = PageHeader::new(page_id);
        self.data = [0; PAGE_SIZE];
        self.write_header();
    }
    
    // ==================== Data Access ====================
    
    /// Write data at the specified offset
    /// Returns true if successful, false if out of bounds
    pub fn write_at(&mut self, offset: u32, data: &[u8]) -> bool {
        let start = offset as usize;
        let end = start + data.len();
        
        if end > PAGE_SIZE {
            return false;
        }
        
        self.data[start..end].copy_from_slice(data);
        self.mark_dirty();
        true
    }
    
    /// Read data from the specified offset
    /// Returns None if out of bounds
    pub fn read_at(&self, offset: u32, length: usize) -> Option<&[u8]> {
        let start = offset as usize;
        let end = start + length;
        
        if end > PAGE_SIZE {
            return None;
        }
        
        Some(&self.data[start..end])
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(0)
    }
}