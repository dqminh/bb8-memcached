use memcache_async::ascii;
use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
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

pub enum Connection {
    Unix(ascii::Protocol<StreamCompat<UnixStream>>),
    Tcp(ascii::Protocol<StreamCompat<TcpStream>>),
}

impl Connection {
    pub async fn connect(uri: &Url) -> Result<Connection, io::Error> {
        let connection = if uri.has_authority() {
            let addr = uri.socket_addrs(|| None)?;
            let sock = TcpStream::connect(addr.first().unwrap()).await?;
            Self::parse_query_string(uri, &sock);
            Connection::Tcp(ascii::Protocol::new(StreamCompat::new(sock)))
        } else {
            let sock = UnixStream::connect(uri.path()).await?;
            Connection::Unix(ascii::Protocol::new(StreamCompat::new(sock)))
        };
        Ok(connection)
    }

    /// Returns the value for given key as bytes. If the value doesn't exist, std::io::ErrorKind::NotFound is returned.
    pub async fn get<'a, K: AsRef<[u8]>>(&'a mut self, key: &'a K) -> Result<Vec<u8>, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.get(key).await,
            Connection::Tcp(ref mut c) => c.get(key).await,
        }
    }

    /// Returns values for multiple keys in a single call as a HashMap from keys to found values. If a key is not present in memcached it will be absent from returned map.
    pub async fn get_multi<'a, K: AsRef<[u8]>>(
        &'a mut self,
        keys: &'a [K],
    ) -> Result<HashMap<String, Vec<u8>>, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.get_multi(keys).await,
            Connection::Tcp(ref mut c) => c.get_multi(keys).await,
        }
    }

    /// Delete a key
    pub async fn delete<'a, K: Display>(&'a mut self, key: &'a K) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.delete(key).await,
            Connection::Tcp(ref mut c) => c.delete(key).await,
        }
    }

    /// Add key to given value
    pub async fn add<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.add(key, val, expiration).await,
            Connection::Tcp(ref mut c) => c.add(key, val, expiration).await,
        }
    }

    /// Set key to given value and don't wait for response.
    pub async fn set<'a, K: Display>(
        &'a mut self,
        key: &'a K,
        val: &'a [u8],
        expiration: u32,
    ) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.set(key, val, expiration).await,
            Connection::Tcp(ref mut c) => c.set(key, val, expiration).await,
        }
    }

    /// Increment a value and return the updated value.
    pub async fn increment<'a, K: AsRef<[u8]>>(
        &'a mut self,
        key: &'a K,
        amount: u64,
    ) -> Result<u64, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.increment(key, amount).await,
            Connection::Tcp(ref mut c) => c.increment(key, amount).await,
        }
    }

    pub async fn version(&mut self) -> Result<String, io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.version().await,
            Connection::Tcp(ref mut c) => c.version().await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        match self {
            Connection::Unix(ref mut c) => c.flush().await,
            Connection::Tcp(ref mut c) => c.flush().await,
        }
    }

    /// Parse query string for tcp options and set it to TcpStream
    fn parse_query_string(uri: &Url, sock: &TcpStream) {
        if let Some(query) = uri.query() {
            match query {
                "tcp_nodelay=true" => sock.set_nodelay(true).unwrap_or_default(),
                _ => (),
            }
        }
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
