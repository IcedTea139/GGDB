use super::page::PageHeader;

pub const PAGE_SIZE: usize = 8192;
pub const HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();
pub const BUFFER_SIZE: usize = 128; /* Temporary RAM size of 128 pages just for testing purposes */

pub type PageId = u64; /* Page identifier */
pub type FrameId = usize; /* ID of a frame in RAM, currently set to 0...127      TODO: extend frames depending on RAM contraints */