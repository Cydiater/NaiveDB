use crate::catalog::CatalogManagerRef;
use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::expr::ExprImpl;
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use itertools::Itertools;
use std::rc::Rc;

pub struct AddIndexExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    table_name: String,
    exprs: Vec<ExprImpl>,
    executed: bool,
}

impl AddIndexExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        exprs: Vec<ExprImpl>,
    ) -> Self {
        AddIndexExecutor {
            bpm,
            catalog,
            table_name,
            exprs,
            executed: false,
        }
    }
}

impl Executor for AddIndexExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_int(false),
            "Num Indexed Tuple".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let table = self.catalog.borrow().find_table(self.table_name.clone())?;
        let schema = Rc::new(Schema::from_exprs(&self.exprs));
        let mut index = BPTIndex::new(self.bpm.clone(), schema.clone());
        let slices = table.into_slice();
        let mut indexed_cnt = 0;
        for slice in slices {
            let rows = self.exprs.iter_mut().map(|e| e.eval(Some(&slice))).fold(
                vec![vec![]; slice.get_num_tuple()],
                |rows, col| {
                    rows.into_iter()
                        .zip(col.into_iter())
                        .map(|(mut r, d)| {
                            r.push(d);
                            r
                        })
                        .collect_vec()
                },
            );
            for (idx, row) in rows.iter().enumerate() {
                let record_id = slice.record_id_at(idx);
                index.insert(row, record_id).unwrap();
                indexed_cnt += 1;
            }
        }
        let page_id = index.get_page_id();
        self.catalog
            .borrow_mut()
            .add_index(self.table_name.clone(), schema, page_id)?;
        let mut msg = Slice::new(self.bpm.clone(), self.schema());
        msg.add(&[Datum::Int(Some(indexed_cnt))])?;
        Ok(Some(msg))
    }
}
