use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::DatabaseError;
use crate::error::Result;
use crate::error::ServerError;

/// The main trait to interact with the in-memory database.
pub trait Database {
    /// Gets the `key`'s value from the database.
    /// Returns `None` if the ket does not exist.
    fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>>;

    /// Inserts the `value` for `key`.
    /// Overwrites the potentially existing value.
    fn insert(
        &self,
        key: String,
        value: String,
    ) -> Result<()>;

    /// Removes `key` from the database.
    fn remove(
        &self,
        key: &str,
    ) -> Result<()>;

    /// Clears the entire database.
    fn clear(&self) -> Result<()>;
}

/// An w
#[derive(Debug, Clone)]
pub struct DB(Arc<Mutex<HashMap<String, String>>>);

impl DB {
    /// Creates a new instance of `DB`.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    /// Creates a new instance of `DB` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(HashMap::with_capacity(capacity))))
    }
}

impl Default for DB {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DB {
    type Target = Arc<Mutex<HashMap<String, String>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Database for DB {
    fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>> {
        let lock = self
            .0
            .lock()
            .map_err(|_| ServerError::Database(DatabaseError::DbLock))?;
        Ok(lock.get(key).map(ToString::to_string))
    }

    fn insert(
        &self,
        key: String,
        value: String,
    ) -> Result<()> {
        let mut lock = self
            .0
            .lock()
            .map_err(|_| ServerError::Database(DatabaseError::DbLock))?;
        lock.insert(key, value);
        Ok(())
    }

    fn remove(
        &self,
        key: &str,
    ) -> Result<()> {
        let mut lock = self
            .0
            .lock()
            .map_err(|_| ServerError::Database(DatabaseError::DbLock))?;
        lock.remove(key);
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let mut lock = self
            .0
            .lock()
            .map_err(|_| ServerError::Database(DatabaseError::DbLock))?;
        lock.clear();
        Ok(())
    }
}
