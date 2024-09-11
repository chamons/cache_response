use std::path::Path;

use eyre::Result;
use rusqlite::{params, Connection};

pub struct ResponseCache {
    connection: Connection,
}

impl ResponseCache {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let connection = rusqlite::Connection::open(path.as_ref())?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS cached (key TEXT PRIMARY KEY, value BLOB);",
            (),
        )?;

        Ok(ResponseCache { connection })
    }

    pub fn get<F>(&self, key: impl AsRef<str>, mut fetch: F) -> Result<Vec<u8>>
    where
        F: FnMut() -> Result<Vec<u8>>,
    {
        let mut fetch_statement = self
            .connection
            .prepare("SELECT value FROM cached WHERE key = (?1)")?;

        let mut query = fetch_statement.query([key.as_ref()]).unwrap();
        let cache_row = query.next()?;

        match cache_row {
            Some(cache_row) => return Ok(cache_row.get(0)?),
            None => {
                let value = fetch()?;

                self.connection.execute(
                    "INSERT INTO cached VALUES((?1), (?2))",
                    params![key.as_ref(), value],
                )?;

                Ok(value)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use crate::ResponseCache;

    struct TestHarness {
        file: NamedTempFile,
        cache: ResponseCache,
    }

    impl TestHarness {
        pub fn new() -> TestHarness {
            let file = NamedTempFile::new().unwrap();
            let cache = ResponseCache::new(file.path()).unwrap();
            TestHarness { file, cache }
        }
    }

    #[test]
    fn round_trip_multiple() {
        let test = TestHarness::new();

        let value_one = "1".as_bytes().to_vec();
        let value_two = "2".as_bytes().to_vec();

        assert_eq!(
            test.cache.get("1", || Ok(value_one.clone())).unwrap(),
            value_one
        );
        assert_eq!(
            test.cache.get("2", || Ok(value_two.clone())).unwrap(),
            value_two
        );
        assert_eq!(
            test.cache.get("1", || Ok(value_one.clone())).unwrap(),
            value_one
        );
    }

    #[test]
    fn fetch_count() {
        let test = TestHarness::new();

        let mut fetched_count = 0;
        let mut fetch = || {
            fetched_count += 1;
            Ok("1".as_bytes().to_vec())
        };

        let result1 = test.cache.get("1", &mut fetch).unwrap();
        let result2 = test.cache.get("1", &mut fetch).unwrap();
        let result3 = test.cache.get("1", &mut fetch).unwrap();

        assert_eq!(result1, "1".as_bytes().to_vec());
        assert_eq!(result2, "1".as_bytes().to_vec());
        assert_eq!(result3, "1".as_bytes().to_vec());
        assert_eq!(1, fetched_count);
    }

    #[test]
    fn sustains_data() {
        let test = TestHarness::new();

        assert_eq!(
            test.cache.get("1", || Ok("1".as_bytes().to_vec())).unwrap(),
            "1".as_bytes().to_vec()
        );

        let cache = ResponseCache::new(test.file.path()).unwrap();
        assert_eq!(
            cache.get("1", || panic!()).unwrap(),
            "1".as_bytes().to_vec()
        );
    }

    #[test]
    fn stores_uuid() {
        let test = TestHarness::new();

        let data = uuid::Uuid::new_v4();

        let fetched_data = test
            .cache
            .get("1", || Ok(data.as_bytes().to_vec()))
            .unwrap();

        assert_eq!(uuid::Uuid::from_slice(&fetched_data).unwrap(), data);
    }
}
