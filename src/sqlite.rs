use rusqlite::{params, Result, OptionalExtension};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub struct KeyValueDataStore {
    pool: Pool<SqliteConnectionManager>,
}

impl KeyValueDataStore {
    pub fn new(db_path: &str) -> Result<Self> {
        let manager = SqliteConnectionManager::file(db_path);
        let pool = Pool::new(manager).expect("Failed to create connection pool");
        let conn = pool.get().unwrap();

        // Enable write ahead logging; it is in DELETE mode by default
        conn.execute_batch("PRAGMA journal_mode = WAL")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            )",
            [],
        )?;
        Ok(Self { pool })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let conn = self.pool.get().unwrap();
        let mut stmt = conn.prepare("SELECT value FROM kv_store WHERE key = ?1")?;
        let result: Result<Option<String>> = stmt.query_row(params![key], |row| row.get(0)).optional();
        result
    }

    pub fn put(&self, key: &str, value: &str) -> Result<String> {
        let conn = self.pool.get().unwrap();
        let mut stmt = conn.prepare("SELECT value FROM kv_store WHERE key = ?1")?;
        let current_value: Option<String> = stmt.query_row(params![key], |row| row.get(0)).optional()?;

        match current_value {
            Some(old_value) => {
                conn.execute(
                    "UPDATE kv_store SET value = ?2 WHERE key = ?1",
                    params![key, value],
                )?;
                Ok(old_value)
            },
            None => {
                conn.execute(
                    "INSERT INTO kv_store (key, value) VALUES (?1, ?2)",
                    params![key, value],
                )?;
                Ok(String::new())
            }
        }
    }
}
