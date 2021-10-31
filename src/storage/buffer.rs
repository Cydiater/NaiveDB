use crate::storage::clock::ClockReplacer;
use crate::storage::disk::DiskManager;

#[allow(dead_code)]
pub struct BufferPoolMananger {
    disk: DiskManager,
    replacer: ClockReplacer,
}
