use crate::datum::{DataType, Datum};
use crate::storage::BufferPoolManagerRef;

pub fn datums_from_index_key(
    _bpm: BufferPoolManagerRef,
    data_types: &[DataType],
    bytes: &[u8],
    is_inlined: bool,
) -> Vec<Datum> {
    let mut datums = vec![];
    if is_inlined {
        let mut offset = 0usize;
        for data_type in data_types {
            let width = data_type.width_of_value().unwrap();
            let datum = Datum::from_bytes(data_type, bytes[offset..(offset + width)].to_vec());
            offset += width;
            datums.push(datum)
        }
        datums
    } else {
        todo!()
    }
}

pub fn index_key_from_datums(
    _bpm: BufferPoolManagerRef,
    data_types: &[DataType],
    datums: &[Datum],
    is_inlined: bool,
) -> Vec<u8> {
    let bytes = if is_inlined {
        let mut bytes = vec![];
        for (data_type, datum) in data_types.iter().zip(datums.iter()) {
            bytes.extend_from_slice(datum.to_bytes(data_type).as_slice());
        }
        bytes
    } else {
        todo!()
    };
    bytes
}
