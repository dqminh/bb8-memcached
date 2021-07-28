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
use std::io;
use url::Url;

/// A `bb8::ManageConnection` for `memcache_async::ascii::Protocol`.
#[derive(Clone, Debug)]
pub struct MemcacheConnectionManager {
    uri: Url,
}

impl MemcacheConnectionManager {
    pub fn new<U: Connectable>(u: U) -> Result<MemcacheConnectionManager, io::Error> {
        Ok(MemcacheConnectionManager { uri: u.get_uri() })
    }
}

#[async_trait]
impl bb8::ManageConnection for MemcacheConnectionManager {
    type Connection = Connection;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Connection::connect(&self.uri).await
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        conn.version().await.map(|_| ())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bb8;
    use std::io::ErrorKind;

    #[tokio::test]
    async fn test_cache_get() {
        let manager = MemcacheConnectionManager::new("tcp://memcached-tcp:11211").unwrap();
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
        let manager = MemcacheConnectionManager::new("tcp://memcached-tcp:11211").unwrap();
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
    async fn test_cache_unix_socket() {
        let manager = MemcacheConnectionManager::new("unix:/tmp/memcached.sock").unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        let pool = pool.clone();
        let mut conn = pool.get().await.unwrap();

        assert!(conn.flush().await.is_ok());
    }
}
