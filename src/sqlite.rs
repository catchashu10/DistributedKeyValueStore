use rusqlite::{params, Connection, Result};

pub struct KeyValueDataStore {
    conn: Connection,
}

impl KeyValueDataStore {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare("SELECT value FROM kv_store WHERE key = ?1")?;
        let result: Result<Option<String>> = stmt.query_row(params![key], |row| row.get(0)).optional();
        result
    }

    /// Inserts or updates a value associated with a key, returning the old value if it exists.
    pub fn put(&self, key: &str, value: &str) -> Result<String> {
        let mut stmt = self.conn.prepare("SELECT value FROM kv_store WHERE key = ?1")?;
        let current_value: Option<String> = stmt.query_row(params![key], |row| row.get(0)).optional()?;

        match current_value {
            Some(old_value) => {
                self.conn.execute(
                    "UPDATE kv_store SET value = ?2 WHERE key = ?1",
                    params![key, value],
                )?;
                Ok(old_value)
            },
            None => {
                self.conn.execute(
                    "INSERT INTO kv_store (key, value) VALUES (?1, ?2)",
                    params![key, value],
                )?;
                Ok(String::new())  // Return an empty string if there was no previous value
            }
        }
    }
}
