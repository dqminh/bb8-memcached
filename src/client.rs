use memcache_async::ascii;
use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use url::Url;

pub trait Connectable {
    fn get_uri(self) -> Url;
}

impl Connectable for String {
    fn get_uri(self) -> Url {
        Url::parse(&self).unwrap()
    }
}

impl Connectable for &str {
    fn get_uri(self) -> Url {
        Url::parse(self).unwrap()
    }
}

pub struct Connection {
    pub(crate) connection: ConnectionInner,
    tainted: bool,
    memcache_read_timeout: Option<Duration>,
    memcache_write_timeout: Option<Duration>,
}

pub enum ConnectionInner {
    Unix(ascii::Protocol<StreamCompat<UnixStream>>),
    Tcp(ascii::Protocol<StreamCompat<TcpStream>>),
}

impl ConnectionInner {
    /// Returns the value for given key as bytes. If the value doesn't exist, std::io::ErrorKind::NotFound is returned.
    async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.get(key).await,
            ConnectionInner::Tcp(ref mut c) => c.get(key).await,
        }
    }

    async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.get_multi(keys).await,
            ConnectionInner::Tcp(ref mut c) => c.get_multi(keys).await,
        }
    }

    async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.delete(key).await,
            ConnectionInner::Tcp(ref mut c) => c.delete(key).await,
        }
    }

    async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.add(key, val, expiration).await,
            ConnectionInner::Tcp(ref mut c) => c.add(key, val, expiration).await,
        }
    }

    async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.set(key, val, expiration).await,
            ConnectionInner::Tcp(ref mut c) => c.set(key, val, expiration).await,
        }
    }

    async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.increment(key, amount).await,
            ConnectionInner::Tcp(ref mut c) => c.increment(key, amount).await,
        }
    }

    async fn version(&mut self) -> Result<String, io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.version().await,
            ConnectionInner::Tcp(ref mut c) => c.version().await,
        }
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        match self {
            ConnectionInner::Unix(ref mut c) => c.flush().await,
            ConnectionInner::Tcp(ref mut c) => c.flush().await,
        }
    }
}

impl Connection {
    pub async fn connect(
        uri: &Url,
        memcache_read_timeout: Option<Duration>,
        memcache_write_timeout: Option<Duration>,
    ) -> Result<Connection, io::Error> {
        let connection = if uri.has_authority() {
            let addr = uri.socket_addrs(|| None)?;
            let sock = TcpStream::connect(addr.first().unwrap()).await?;
            ConnectionInner::Tcp(ascii::Protocol::new(StreamCompat::new(sock)))
        } else {
            let sock = UnixStream::connect(uri.path()).await?;
            ConnectionInner::Unix(ascii::Protocol::new(StreamCompat::new(sock)))
        };
        Ok(Connection {
            connection,
            tainted: false,
            memcache_read_timeout,
            memcache_write_timeout,
        })
    }

    /// Returns the value for given key as bytes. If the value doesn't exist, std::io::ErrorKind::NotFound is returned.
    pub async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error> {
        match self.memcache_read_timeout {
            Some(read_timeout) => match timeout(read_timeout, self.connection.get(key)).await {
                Err(_elapsed_err) => {
                    self.taint();
                    Err(io::ErrorKind::TimedOut.into())
                }
                Ok(data) => data,
            },
            _ => self.connection.get(key).await,
        }
    }

    /// Returns values for multiple keys in a single call as a HashMap from keys to found values. If a key is not present in memcached it will be absent from returned map.
    pub async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error> {
        match self.memcache_read_timeout {
            Some(read_timeout) => {
                match timeout(read_timeout, self.connection.get_multi(keys)).await {
                    Err(_elapsed_err) => {
                        self.taint();
                        Err(io::ErrorKind::TimedOut.into())
                    }
                    Ok(data) => data,
                }
            }
            _ => self.connection.get_multi(keys).await,
        }
    }

    /// Delete a key
    pub async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error> {
        match self.memcache_write_timeout {
            Some(write_timeout) => {
                match timeout(write_timeout, self.connection.delete(key)).await {
                    Err(_elapsed_err) => {
                        self.taint();
                        Err(io::ErrorKind::TimedOut.into())
                    }
                    Ok(data) => data,
                }
            }
            _ => self.connection.delete(key).await,
        }
    }

    /// Add key to given value
    pub async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self.memcache_write_timeout {
            Some(write_timeout) => {
                match timeout(write_timeout, self.connection.add(key, val, expiration)).await {
                    Err(_elapsed_err) => {
                        self.taint();
                        Err(io::ErrorKind::TimedOut.into())
                    }
                    Ok(data) => data,
                }
            }
            _ => self.connection.add(key, val, expiration).await,
        }
    }

    /// Set key to given value and don't wait for response.
    pub async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self.memcache_write_timeout {
            Some(write_timeout) => {
                match timeout(write_timeout, self.connection.set(key, val, expiration)).await {
                    Err(_elapsed_err) => {
                        self.taint();
                        Err(io::ErrorKind::TimedOut.into())
                    }
                    Ok(data) => data,
                }
            }
            _ => self.connection.set(key, val, expiration).await,
        }
    }

    /// Increment a value and return the updated value.
    pub async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error> {
        match self.memcache_write_timeout {
            Some(write_timeout) => {
                match timeout(write_timeout, self.connection.increment(key, amount)).await {
                    Err(_elapsed_err) => {
                        self.taint();
                        Err(io::ErrorKind::TimedOut.into())
                    }
                    Ok(data) => data,
                }
            }
            _ => self.connection.increment(key, amount).await,
        }
    }

    pub async fn version(&mut self) -> Result<String, io::Error> {
        match self.memcache_read_timeout {
            Some(read_timeout) => match timeout(read_timeout, self.connection.version()).await {
                Err(_elapsed_err) => {
                    self.taint();
                    Err(io::ErrorKind::TimedOut.into())
                }
                Ok(data) => data,
            },
            _ => self.connection.version().await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        match self.memcache_write_timeout {
            Some(write_timeout) => match timeout(write_timeout, self.connection.flush()).await {
                Err(_elapsed_err) => {
                    self.taint();
                    Err(io::ErrorKind::TimedOut.into())
                }
                Ok(data) => data,
            },
            _ => self.connection.flush().await,
        }
    }

    fn taint(&mut self) {
        self.tainted = true
    }

    pub(crate) fn is_tainted(&self) -> bool {
        self.tainted
    }
}

/// Compatibility layer for `futures::{AsyncRead, AsyncWrite}`.
pub struct StreamCompat<T> {
    inner: T,
}

impl<T> StreamCompat<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Deref for StreamCompat<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for StreamCompat<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: AsyncRead> futures::AsyncRead for StreamCompat<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut b = ReadBuf::new(buf);
        match AsyncRead::poll_read(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            cx,
            &mut b,
        ) {
            Poll::Ready(_) => Poll::Ready(Ok(b.filled().len())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncWrite> futures::AsyncWrite for StreamCompat<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, cx)
    }
}
