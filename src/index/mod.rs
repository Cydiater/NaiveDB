use crate::storage::PageID;
use crate::table::Datum;
use thiserror::Error;

#[allow(dead_code)]
pub type RecordID = (PageID, usize);

///
/// | num_key_data_types | type[0] | type[1] | ... | page_id_of_root_node | max_child
///

#[allow(dead_code)]
pub struct BPTIndex {
    page_id: PageID,
}

mod node;

#[allow(dead_code)]
impl BPTIndex {
    /// 1. fetch the root node;
    /// 2. find the leaf node corresponding to the inserting key;
    /// 3. have enough space ? insert => done : split => 4
    /// 4. split, insert into parent => 3
    pub fn insert(_key: Vec<Datum>, _rid: RecordID) -> Result<(), IndexError> {
        todo!()
    }
}

#[derive(Error, Debug)]
pub enum IndexError {}
