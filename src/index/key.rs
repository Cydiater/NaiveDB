use crate::datum::Datum;
use crate::index::RecordID;
use crate::storage::BufferPoolManagerRef;
use crate::table::SchemaRef;
use std::cmp::Ordering;

///
/// IndexKey Format:
///
///     | is_inlined | InlinedKey / NonInlinedKey |
///
/// InlinedKey Format:
///     
///     | Datum[0] | Datum[1] | ... |
///
/// NonInlinedKey Format:
///
///     | PageID | Offset |
///
/// TODO: Implement NonInlinedKey
///

pub enum IndexKey {
    Inlined((Vec<Datum>, SchemaRef)),
    #[allow(dead_code)]
    NonInlined((BufferPoolManagerRef, SchemaRef, RecordID)),
}

#[allow(dead_code)]
impl IndexKey {
    pub fn new_inlined(datums: Vec<Datum>, schema: SchemaRef) -> Self {
        Self::Inlined((datums, schema))
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Inlined((datums, schema)) => {
                let mut bytes = vec![0u8];
                for (datum, col) in datums.iter().zip(schema.iter()) {
                    bytes.extend_from_slice(&datum.to_bytes(&col.data_type));
                }
                bytes
            }
            Self::NonInlined(_) => {
                todo!()
            }
        }
    }
    pub fn from_bytes_and_schema(bytes: &[u8], schema: SchemaRef) -> Self {
        match bytes[0] {
            0u8 => {
                let mut datums = vec![];
                let offset = 1;
                for data_type in schema.iter().map(|c| c.data_type) {
                    let datum = Datum::from_bytes(&data_type, &bytes[offset..]);
                    datums.push(datum);
                }
                Self::Inlined((datums, schema))
            }
            1u8 => todo!(),
            _ => unreachable!(),
        }
    }
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &IndexKey) -> bool {
        match (self, other) {
            (IndexKey::Inlined((datums_lhs, _)), IndexKey::Inlined((datums_rhs, _))) => {
                datums_lhs == datums_rhs
            }
            _ => todo!(),
        }
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &IndexKey) -> Option<Ordering> {
        match (self, other) {
            (IndexKey::Inlined((datums_lhs, _)), IndexKey::Inlined((datums_rhs, _))) => {
                datums_lhs.partial_cmp(datums_rhs)
            }
            _ => todo!(),
        }
    }
}
