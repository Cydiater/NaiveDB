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
    current_database: Option<String>,
}

pub type CatalogManagerRef = Rc<RefCell<CatalogManager>>;

impl CatalogManager {
    pub fn current_database(&self) -> Option<String> {
        self.current_database.clone()
    }
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        Self {
            bpm: bpm.clone(),
            database_catalog: Catalog::new_for_database(bpm),
            table_catalog: None,
            current_database: None,
        }
    }
    pub fn new_shared(bpm: BufferPoolManagerRef) -> CatalogManagerRef {
        Rc::new(RefCell::new(Self::new(bpm)))
    }
    pub fn create_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        if self
            .database_catalog
            .iter()
            .any(|(name, _)| database_name == name)
        {
            return Err(CatalogError::Duplicated);
        }
        // create table catalog
        let table_catalog = Catalog::new(self.bpm.clone()).unwrap();
        let page_id = table_catalog.page_id();
        // add to database catalog
        self.database_catalog
            .insert(page_id, database_name)
            .unwrap();
        Ok(())
    }
    pub fn create_table(&mut self, table_name: &str, page_id: PageID) -> Result<(), CatalogError> {
        if let Some(table_catalog) = self.table_catalog.as_mut() {
            info!("create table {}", table_name);
            table_catalog.insert(page_id, table_name)?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn use_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        if let Some(page_id) = self.database_catalog.page_id_of(database_name) {
            let table_catalog = Catalog::open(self.bpm.clone(), page_id)?;
            self.table_catalog = Some(table_catalog);
            info!("checkout to database {}", database_name);
            self.current_database = Some(database_name.to_owned());
            Ok(())
        } else {
            Err(CatalogError::EntryNotFound)
        }
    }
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        if let Some(table_catalog) = &mut self.table_catalog {
            table_catalog.remove(table_name)?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn remove_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        if Some(database_name.to_string()) == self.current_database {
            self.table_catalog = None;
            self.current_database = None;
        }
        self.database_catalog.remove(database_name)?;
        Ok(())
    }
    pub fn find_table(&self, table_name: &str) -> Result<Table, CatalogError> {
        if let Some(table_catalog) = &self.table_catalog {
            if let Some(page_id) = table_catalog.page_id_of(table_name) {
                Ok(Table::open(page_id, self.bpm.clone()))
            } else {
                Err(CatalogError::EntryNotFound)
            }
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn remove_indexes_by_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        if let Some(table_catalog) = &mut self.table_catalog {
            table_catalog
                .prefix_with(&format!("{}:", table_name))
                .into_iter()
                .map(|index_name| index_name.to_owned())
                .collect_vec()
                .into_iter()
                .try_for_each(|index_name| table_catalog.remove(&index_name))?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn find_indexes_by_table(&self, table_name: &str) -> Result<Vec<BPTIndex>, CatalogError> {
        if let Some(table_catalog) = &self.table_catalog {
            let page_id_of_table = table_catalog.page_id_of(table_name).unwrap();
            let table = Table::open(page_id_of_table, self.bpm.clone());
            Ok(table_catalog
                .prefix_with(&format!("{}:", table_name))
                .into_iter()
                .map(|name| {
                    let page_id = table_catalog.page_id_of(name).unwrap();
                    BPTIndex::open(self.bpm.clone(), page_id, table.schema.as_ref())
                })
                .collect_vec())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
    }
    pub fn database_iter(&self) -> CatalogIter {
        self.database_catalog.iter()
    }
    pub fn table_names(&self) -> Result<Vec<String>, CatalogError> {
        let table_catalog = self
            .table_catalog
            .as_ref()
            .ok_or(CatalogError::NotUsingDatabase)?;
        let table_names = table_catalog
            .iter()
            .map(|(name, _)| name.to_string())
            .collect_vec();
        Ok(table_names)
    }
    pub fn add_index(
        &mut self,
        table_name: &str,
        schema: SchemaRef,
        page_id: PageID,
    ) -> Result<(), CatalogError> {
        if let Some(table_catalog) = self.table_catalog.as_mut() {
            let columns = schema.iter().map(|c| c.desc.clone()).collect_vec();
            let key = table_name.to_owned() + ":" + &columns.join(":");
            table_catalog.insert(page_id, &key)?;
            Ok(())
        } else {
            Err(CatalogError::NotUsingDatabase)
        }
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
            catalog_manager.create_database("sample_db").unwrap();
            // use this database
            catalog_manager.use_database("sample_db").unwrap();
            // create a table
            let table = Table::new(
                Rc::new(Schema::from_slice(&[(
                    DataType::new_as_int(false),
                    "v1".to_string(),
                )])),
                bpm,
            );
            // attach in catalog
            catalog_manager
                .create_table("sample_table", table.get_page_id())
                .unwrap();
            // find this table
            assert!(catalog_manager.find_table("sample_table").is_ok());
            filename
        };
        remove_file(filename).unwrap();
    }
}
