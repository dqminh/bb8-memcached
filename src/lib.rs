//! Memcached support for the `bb8` connection pool.
//!
//! # Example
//! ```
//! use futures::future::join_all;
//! use bb8_memcached::{bb8, MemcacheConnectionManager};
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
//!     let pool = bb8::Pool::builder().build(manager).await.unwrap();
//!
//!     let mut handles = vec![];
//!
//!     for _i in 0..10 {
//!         let pool = pool.clone();
//!
//!         handles.push(tokio::spawn(async move {
//!             let mut conn = pool.get().await.unwrap();
//!
//!             let version = conn.version().await.unwrap();
//!         }));
//!     }
//!
//!     join_all(handles).await;
//! }
//! ```

#![allow(clippy::needless_doctest_main)]

pub use bb8;
pub use memcache_async;

mod client;

use async_trait::async_trait;
use client::{Connectable, Connection};
use std::{
    io::{self, ErrorKind},
    time::Duration,
};
use url::Url;

/// A `bb8::ManageConnection` for `memcache_async::ascii::Protocol`.
#[derive(Clone, Debug)]
pub struct MemcacheConnectionManager {
    uri: Url,
    /// A tokio controlled timeout for operations get, get_multi, version.
    memcache_read_timeout: Option<Duration>,
    /// A tokio controlled timeout for operations set, add, delete, incr, flush.
    memcache_write_timeout: Option<Duration>,
}

impl MemcacheConnectionManager {
    pub fn new<U: Connectable>(u: U) -> Result<MemcacheConnectionManager, io::Error> {
        Ok(MemcacheConnectionManager {
            uri: u.get_uri(),
            memcache_read_timeout: None,
            memcache_write_timeout: None,
        })
    }

    pub fn with_timeouts(mut self, read_timeout: Duration, write_timeout: Duration) -> Self {
        self.memcache_read_timeout = Some(read_timeout);
        self.memcache_write_timeout = Some(write_timeout);
        self
    }
}

#[async_trait]
impl bb8::ManageConnection for MemcacheConnectionManager {
    type Connection = Connection;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Connection::connect(
            &self.uri,
            self.memcache_read_timeout,
            self.memcache_write_timeout,
        )
        .await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if conn.is_tainted() {
            return Err(ErrorKind::ConnectionAborted.into());
        }
        conn.version().await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_tainted()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{io::ErrorKind, time::Duration};

    #[tokio::test]
    async fn test_cache_get() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();

        assert!(conn.flush().await.is_ok());

        let (key, val) = ("hello", "world");
        assert_eq!(
            conn.get(&key).await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
        assert!(conn.set(&key, val.as_bytes(), 0).await.is_ok());
        assert_eq!(conn.get(&key).await.unwrap(), val.as_bytes());
    }

    #[tokio::test]
    async fn test_cache_add_delete() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();

        assert!(conn.flush().await.is_ok());

        let (key, val) = ("hello_add_delete", "world");
        assert!(conn.add(&key, val.as_bytes(), 0).await.is_ok());
        assert_eq!(conn.get(&key).await.unwrap(), val.as_bytes());

        // add the same key will fail
        assert!(conn.add(&key, val.as_bytes(), 0).await.is_err());

        assert!(conn.delete(&key).await.is_ok());
        assert_eq!(
            conn.get(&key).await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn test_increment() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211").unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();

        assert!(conn.flush().await.is_ok());

        let (key, val) = ("increment", "0");
        assert!(conn.set(&key, val.as_bytes(), 0).await.is_ok());
        assert_eq!(conn.increment(&key, 1).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_cache_unix_socket() {
        let manager = MemcacheConnectionManager::new("unix:/tmp/memcached.sock").unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();

        assert!(conn.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_connection_timeouts() {
        let manager = MemcacheConnectionManager::new("tcp://localhost:11211")
            .unwrap()
            .with_timeouts(Duration::from_millis(2), Duration::from_millis(5));
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        {
            let mut conn = pool.get().await.unwrap();

            assert!(conn.flush().await.is_ok());

            let key = "hello";
            assert_eq!(
                conn.get(&key).await.unwrap_err().kind(),
                ErrorKind::NotFound
            );

            assert_eq!(pool.state().connections, 1);
            assert_eq!(pool.state().statistics.connections_closed_broken, 0);
        }

        {
            let mut conn = pool.get().await.unwrap();

            assert!(conn.flush().await.is_ok());

            let key = "hello";
            // test timeout
            let v = vec![1; 204_800_000];

            assert_eq!(
                conn.set(&key, &v, 0).await.unwrap_err().kind(),
                ErrorKind::TimedOut
            );
        }

        // connection should be tainted
        assert_eq!(pool.state().connections, 0);
        // has_broken happens first because its called when we try to put a connection back into the pool.
        // is_valid is called when we try to get a connection from the pool.
        assert_eq!(pool.state().statistics.connections_closed_broken, 1);
    }
}
