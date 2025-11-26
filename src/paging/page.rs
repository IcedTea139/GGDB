use super::page_constants::{PAGE_SIZE, HEADER_SIZE, PageId};

#[repr(u16)]
#[derive(Debug, Copy, Clone)]
pub enum PageType {
    NodeStore = 0,
    Relationship = 1,
    PropertyStore = 2,
}

// Page header containing metadata
//repr C forces the compiler to not optimize placements of each field as rust compiler will optimize the field placements
//to condense size and remove padding
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    pub lsn: u64,
    pub page_id: u64,
    pub checksum: u32, //TODO: add checksum, some kind of encryption to ensure page integrity when loading to and from disk since the OS moves 4k at a time but our page is 8k
    pub free_space_pointer: u32,
    pub item_count: u32, //necessary for fast aggregation queries
    pub page_type: PageType, //for debugging and robustness
    pub _padding: u16, //ensure 8 byte alignment
}

impl PageHeader {
    // Create a new page header with the given page ID
    pub fn new(page_id: u64, page_type: PageType) -> Self {
        Self {
            lsn: 0,
            page_id,
            checksum: 0,
            free_space_pointer: HEADER_SIZE as u32,
            item_count: 0,
            page_type,
            _padding: 0,
        }
    }
}

// A page in the database backed by a contiguous byte array
#[repr(C, align(8))]
pub struct Page {
    // header is overlayed into data for simplicity and to ensure proper alginment
    data: [u8; PAGE_SIZE],

    // Runtime metadata (will not be written to disk, only on RAM)
    pub page_id: Option<PageId>, // included this here so we don't have do fetch header every time we want page_id
    pub is_dirty: bool,
    pub pin_count: u32,
    pub ref_bit: bool,
}

impl Page {
    // Create a new page that already belongs to the given page ID and type
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE],
            page_id: Some(page_id),
            is_dirty: false,
            pin_count: 0,
            ref_bit: true,
        };
        page.write_header(PageHeader::new(page_id, page_type));
        page
    }

    // Create a page from raw bytes (e.g., after reading from disk)
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        let mut page = Self {
            data,
            page_id: None,
            is_dirty: false,
            pin_count: 0,
            ref_bit: false,
        };
        page.page_id = Some(page.get_header().page_id);
        page
    }

    // ==================== Header Management ====================
    // We cast to get the header (first 32 bytes of the page)
    pub fn get_header(&self) -> &PageHeader {
        unsafe { &*(self.data.as_ptr() as *const PageHeader) }
    }

    fn get_header_mut(&mut self) -> &mut PageHeader {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut PageHeader) }
    }

    fn write_header(&mut self, header: PageHeader) {
        *self.get_header_mut() = header;
    }

    // ==================== Getters ====================

    pub fn get_page_id(&self) -> u64 {
        self.get_header().page_id
    }

    pub fn get_lsn(&self) -> u64 {
        self.get_header().lsn
    }

    pub fn get_free_space_pointer(&self) -> u32 {
        self.get_header().free_space_pointer
    }

    pub fn get_pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn get_free_space(&self) -> usize {
        PAGE_SIZE - self.get_header().free_space_pointer as usize
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    pub fn get_data_segment(&self) -> &[u8] {
        &self.data[HEADER_SIZE..]
    }

    pub fn get_data_segment_mut(&mut self) -> &mut [u8] {
        &mut self.data[HEADER_SIZE..]
    }

    // ==================== Setters ====================

    pub fn set_page_id(&mut self, page_id: u64) {
        self.page_id = Some(page_id);
        self.get_header_mut().page_id = page_id;
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.get_header_mut().lsn = lsn;
    }

    pub fn set_free_space_pointer(&mut self, pointer: u32) {
        self.get_header_mut().free_space_pointer = pointer;
    }

    pub fn set_pin_count(&mut self, count: u32) {
        self.pin_count = count;
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }

    // ==================== Pin Management ====================

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) -> bool {
        assert!(self.pin_count > 0, "pin_count must be non negative");
        self.pin_count -= 1;
        true
    }

    // ==================== Space Management ====================

    pub fn has_room(&self, bytes_needed: usize) -> bool {
        self.get_free_space() >= bytes_needed
    }

    //writes size amount of data to the free_space pointer, returns none if no room
    pub fn allocate(&mut self, size: usize) -> Option<u32> {
        if !self.has_room(size) {
            return None;
        }

        let header = self.get_header_mut();
        let offset = header.free_space_pointer;
        header.free_space_pointer += size as u32;

        Some(offset)
    }

    //not sure when we will need this
    pub fn reset(&mut self) {
        let page_id = self.get_header().page_id;
        let page_type = self.get_header().page_type;
        self.data = [0; PAGE_SIZE];
        self.write_header(PageHeader::new(page_id, page_type));
        self.pin_count = 0;
        self.is_dirty = false;
        self.ref_bit = false;
        self.page_id = Some(page_id);
    }

    pub fn write_at(&mut self, offset: u32, data: &[u8]) -> bool {
        let start = offset as usize;
        let end = start + data.len();

        if end > PAGE_SIZE {
            return false;
        }

        self.data[start..end].copy_from_slice(data);
        self.is_dirty = true;
        true
    }

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
        Self::new(0, PageType::NodeStore)
    }
}
