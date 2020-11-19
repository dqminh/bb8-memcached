#![allow(clippy::needless_doctest_main)]

pub use bb8;
pub use memcache_async;

mod client;

use futures::future::{Future, FutureExt};
use async_trait::async_trait;
use client::{Connectable, Connection};
use std::io;
use url::Url;

/// `MemcachePool` is a convenience wrapper around `bb8::Pool` that hides the fact that
/// `RedisConnectionManager` uses an `Option<Connection>` to smooth over the API incompatibility.
#[derive(Debug, Clone)]
pub struct MemcachePool {
    pool: bb8::Pool<MemcacheConnectionManager>,
}

impl MemcachePool {
    pub fn new(pool: bb8::Pool<MemcacheConnectionManager>) -> MemcachePool {
        MemcachePool { pool }
    }

    /// Access the `bb8::Pool` directly.
    pub fn pool(&self) -> &bb8::Pool<MemcacheConnectionManager> {
        &self.pool
    }

    /// Retrieve the pooled connection
    pub async fn get(
        &self,
    ) -> Result<bb8::PooledConnection<'_, MemcacheConnectionManager>, bb8::RunError<io::Error>> {
        self.pool().get().await
    }

    // Run the function with a connection provided by the pool.
    // pub async fn run<'a, T, E, U, F>(&self, f: F) -> Result<T, bb8::RunError<E>>
    // where
    //     F: FnOnce(Connection) -> U + Send + 'a,
    //     U: Future<Output = Result<(Connection, T), E>> + Send + 'a,
    //     E: From<<MemcacheConnectionManager as bb8::ManageConnection>::Error> + Send + 'a,
    //     T: Send + 'a,
    // {
    //     let f = move |conn: Option<Connection>| {
    //         let conn = conn.unwrap();
    //         f(conn).map(|res| match res {
    //             Ok((conn, item)) => Ok((item, Some(conn))),
    //             Err(err) => Err((err, None)),
    //         })
    //     };
    //     self.pool.run(f).await
    // }
}

/// A `bb8::ManageConnection` for `memcache_async::ascii::Protocol`.
#[derive(Clone, Debug)]
pub struct MemcacheConnectionManager {
    uri: Url,
}

impl MemcacheConnectionManager {
    pub fn new<U: Connectable>(u: U) -> Result<MemcacheConnectionManager, io::Error> {
        Ok(MemcacheConnectionManager {
            uri: u.get_uri(),
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for MemcacheConnectionManager {
    type Connection = Option<Connection>;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = Connection::connect(&self.uri).await?;
        Ok(Some(conn))
    }

  //  async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error>;

    async fn is_valid(&self, conn:  &mut bb8::PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        // The connection should only be None after a failure.
        conn.as_mut().unwrap().version().await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_none()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bb8;
    use std::io::ErrorKind;

    #[tokio::test]
    async fn test_cache_get() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
        let pool = MemcachePool::new(bb8::Pool::builder().build(manager).await.unwrap());

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();
        let conn = conn.as_mut().unwrap();

        assert!(conn.flush().await.is_ok());

        let ( key, val ) = ("hello", "world");
        assert_eq!(conn.get(&key).await.unwrap_err().kind(), ErrorKind::NotFound);
        assert!(conn.set(&key, val.as_bytes(), 0).await.is_ok());
        assert_eq!(conn.get(&key).await.unwrap(), val.as_bytes());
    }

    #[tokio::test]
    async fn test_cache_add() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
        let pool = MemcachePool::new(bb8::Pool::builder().build(manager).await.unwrap());

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();
        let conn = conn.as_mut().unwrap();

        assert!(conn.flush().await.is_ok());
        let ( key, val ) = ("hello", "world");
        assert!(conn.add(&key, val.as_bytes(), 0).await.is_ok());
    }

    #[tokio::test]
    async fn test_cache_unix_socket() {
        let manager = MemcacheConnectionManager::new("unix:/tmp/memcached.sock").unwrap();
        let pool = MemcachePool::new(bb8::Pool::builder().build(manager).await.unwrap());

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();
        let conn = conn.as_mut().unwrap();

        assert!(conn.flush().await.is_ok());
    }
}
