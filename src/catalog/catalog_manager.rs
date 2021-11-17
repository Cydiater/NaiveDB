use crate::catalog::{Catalog, CatalogError, CatalogIter};
use crate::storage::{BufferPoolManagerRef, PageID};
use log::info;
use std::cell::RefCell;
use std::rc::Rc;

pub struct CatalogManager {
    bpm: BufferPoolManagerRef,
    database_catalog: Catalog,
    table_catalog: Option<Catalog>,
}

pub type CatalogManagerRef = Rc<RefCell<CatalogManager>>;

impl CatalogManager {
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        Self {
            bpm: bpm.clone(),
            database_catalog: Catalog::new_database_catalog(bpm),
            table_catalog: None,
        }
    }
    pub fn new_shared(bpm: BufferPoolManagerRef) -> CatalogManagerRef {
        Rc::new(RefCell::new(Self::new(bpm)))
    }
    pub fn create_database(&mut self, database_name: String) -> Result<(), CatalogError> {
        // create table catalog
        let table_catalog = Catalog::new_empty(self.bpm.clone()).unwrap();
        let page_id = table_catalog.page_id;
        // add to database catalog
        self.database_catalog
            .insert(page_id, database_name)
            .unwrap();
        Ok(())
    }
    pub fn create_table(
        &mut self,
        table_name: String,
        page_id: PageID,
    ) -> Result<(), CatalogError> {
        if let Some(table_catalog) = self.table_catalog.as_mut() {
            info!("create table {}", table_name);
            table_catalog.insert(page_id, table_name).unwrap();
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn use_database(&mut self, database_name: String) -> Result<(), CatalogError> {
        if let Some(page_id) = self
            .database_catalog
            .iter()
            .filter(|(_, _, name)| name == &database_name)
            .map(|(_, page_id, _)| page_id)
            .next()
        {
            let table_catalog = Catalog::new_with_page_id(self.bpm.clone(), page_id);
            self.table_catalog = Some(table_catalog);
            info!("checkout to database {}", database_name);
            Ok(())
        } else {
            Err(CatalogError::EntryNotFound)
        }
    }
    pub fn iter(&self) -> CatalogIter {
        self.database_catalog.iter()
    }
}
