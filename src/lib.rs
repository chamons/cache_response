use std::{future::Future, path::Path};

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

    pub async fn get(
        &self,
        key: impl AsRef<str>,
        fetch: impl Future<Output = eyre::Result<Vec<u8>>>,
    ) -> Result<Vec<u8>> {
        let mut fetch_statement = self
            .connection
            .prepare("SELECT value FROM cached WHERE key = (?1)")?;

        let mut query = fetch_statement.query([key.as_ref()]).unwrap();
        let cache_row = query.next()?;

        match cache_row {
            Some(cache_row) => Ok(cache_row.get(0)?),
            None => {
                let value = fetch.await?;

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

    #[tokio::test]
    async fn round_trip_multiple() {
        let test = TestHarness::new();

        let value_one = "1".as_bytes().to_vec();
        let value_two = "2".as_bytes().to_vec();

        assert_eq!(
            test.cache
                .get("1", async { Ok(value_one.clone()) })
                .await
                .unwrap(),
            value_one
        );
        assert_eq!(
            test.cache
                .get("2", async { Ok(value_two.clone()) })
                .await
                .unwrap(),
            value_two
        );
        assert_eq!(
            test.cache
                .get("1", async { Ok(value_one.clone()) })
                .await
                .unwrap(),
            value_one
        );
    }

    #[tokio::test]
    async fn fetch_count() {
        let test = TestHarness::new();

        let mut fetched_count = 0;

        for _ in 0..3 {
            let result = test
                .cache
                .get("1", async {
                    fetched_count += 1;
                    Ok("1".as_bytes().to_vec())
                })
                .await
                .unwrap();

            assert_eq!(result, "1".as_bytes().to_vec());
        }

        assert_eq!(1, fetched_count);
    }

    #[tokio::test]
    async fn sustains_data() {
        let test = TestHarness::new();

        assert_eq!(
            test.cache
                .get("1", async { Ok("1".as_bytes().to_vec()) })
                .await
                .unwrap(),
            "1".as_bytes().to_vec()
        );

        let cache = ResponseCache::new(test.file.path()).unwrap();
        assert_eq!(
            cache.get("1", async { panic!() }).await.unwrap(),
            "1".as_bytes().to_vec()
        );
    }

    #[tokio::test]
    async fn stores_uuid() {
        let test = TestHarness::new();

        let data = uuid::Uuid::new_v4();

        let fetched_data = test
            .cache
            .get("1", async { Ok(data.as_bytes().to_vec()) })
            .await
            .unwrap();

        assert_eq!(uuid::Uuid::from_slice(&fetched_data).unwrap(), data);
    }
}
