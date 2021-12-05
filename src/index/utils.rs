use crate::datum::Datum;
use crate::index::RecordID;
use crate::storage::BufferPoolManagerRef;
use crate::table::SchemaRef;

pub fn datums_from_bytes(
    _bpm: BufferPoolManagerRef,
    key_schema: SchemaRef,
    bytes: &[u8],
    is_inlined: bool,
) -> Vec<Datum> {
    let mut datums = vec![];
    if is_inlined {
        let mut offset = 0usize;
        for data_type in key_schema.iter().map(|c| c.data_type) {
            let width = data_type.width_of_value().unwrap();
            let datum = Datum::from_bytes(&data_type, bytes[offset..(offset + width)].to_vec());
            offset += width;
            datums.push(datum)
        }
        datums
    } else {
        todo!()
    }
}

pub fn bytes_from_datums(
    _bpm: BufferPoolManagerRef,
    key_schema: SchemaRef,
    datums: &[Datum],
    _rid: RecordID,
    is_inlined: bool,
) -> Vec<u8> {
    let bytes = if is_inlined {
        let mut bytes = vec![];
        for (data_type, datum) in key_schema.iter().map(|c| c.data_type).zip(datums.iter()) {
            bytes.extend_from_slice(datum.to_bytes(&data_type).as_slice());
        }
        bytes
    } else {
        todo!()
    };
    bytes
}
