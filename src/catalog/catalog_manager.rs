use crate::catalog::{Catalog, CatalogError, CatalogIter};
use crate::index::BPTIndex;
use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{SchemaRef, Table};
use itertools::Itertools;
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
        let page_id = table_catalog.get_page_id();
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
    pub fn remove_table(&mut self, table_name: String) -> Result<(), CatalogError> {
        if let Some(table_catalog) = &mut self.table_catalog {
            table_catalog.remove(table_name)?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn find_table(&self, table_name: String) -> Result<Table, CatalogError> {
        if let Some(table_catalog) = &self.table_catalog {
            if let Some(page_id) = table_catalog
                .iter()
                .filter(|(_, _, name)| name == &table_name)
                .map(|(_, page_id, _)| page_id)
                .next()
            {
                Ok(Table::open(page_id, self.bpm.clone()))
            } else {
                Err(CatalogError::EntryNotFound)
            }
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn find_indexes_by_table(&self, table_name: String) -> Result<Vec<BPTIndex>, CatalogError> {
        if let Some(table_catalog) = &self.table_catalog {
            let mut indexes = vec![];
            for (_, page_id, name) in table_catalog.iter() {
                let parts = name.split(':').collect_vec();
                if parts.len() == 1 {
                    continue;
                }
                if parts[0] == table_name {
                    let index = BPTIndex::open(self.bpm.clone(), page_id);
                    indexes.push(index);
                }
            }
            Ok(indexes)
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn add_index(
        &mut self,
        table_name: String,
        schema: SchemaRef,
        page_id: PageID,
    ) -> Result<(), CatalogError> {
        if let Some(table_catalog) = self.table_catalog.as_mut() {
            let columns = schema.iter().map(|c| c.desc.clone()).collect_vec();
            let key = table_name + ":" + &columns.join(":");
            table_catalog.insert(page_id, key)?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn iter(&self) -> CatalogIter {
        self.database_catalog.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::storage::BufferPoolManager;
    use crate::table::Schema;
    use std::fs::remove_file;

    #[test]
    fn test_use_create_find() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let mut catalog_manager = CatalogManager::new(bpm.clone());
            // create database
            catalog_manager
                .create_database("sample_db".to_string())
                .unwrap();
            // use this database
            catalog_manager
                .use_database("sample_db".to_string())
                .unwrap();
            // create a table
            let table = Table::new(
                Rc::new(Schema::from_slice(&[(
                    DataType::new_int(false),
                    "v1".to_string(),
                )])),
                bpm,
            );
            // attach in catalog
            catalog_manager
                .create_table("sample_table".to_string(), table.get_page_id())
                .unwrap();
            // find this table
            assert!(catalog_manager
                .find_table("sample_table".to_string())
                .is_ok());
            filename
        };
        remove_file(filename).unwrap();
    }
}
