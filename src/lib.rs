//! Minimal iroh/QUIC fabric for cross-device rows.
//!
//! The wire protocol is deliberately tiny: one bi-directional QUIC stream
//! per message, carrying a `u32` little-endian length prefix followed by
//! a postcard-serialized value. No per-message id on the wire — the stream
//! *is* the correlation — and no Arrow IPC: serde rows are just `T`, like
//! on any other fabric. Arrow IPC stays in `fabric::codec::ArrowCodec` (the
//! `arrow` feature of the `fabric` crate) for local shared-memory rows.
//!
//! Because iroh is async and fabric's core is synchronous, the fabric owns
//! a tokio runtime. `Requester::call` and `Subscriber::next` drive the async
//! work through that runtime on the caller's thread. Callers already on a
//! tokio reactor should use the `async` adapter instead of blocking.
//!
//! Pub/sub peer discovery is explicit: [`IrohFabric::with_peer`] sets the
//! address subscribers dial. A subscriber registers a topic by sending a
//! one-frame handshake on the connection; the publisher's accept loop keeps
//! one connection per (topic, subscriber) and pushes each published value
//! on a fresh bi-stream. There is no discovery protocol, name service, or
//! retransmission — this is the smallest transport that still implements
//! the two fabric patterns.

use fabric::{
    Codec, Error, Fabric, PubSubPair, Publisher, ReqRepPair, Requester, Responder, ServeHandle,
    Subscriber,
};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// Maximum encoded frame size. Larger values are refused (`Error::Refused`)
/// before a loan is attempted, matching the iceoryx2 fabric's capacity
/// contract.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

fn ps_alpn(alpn: &[u8]) -> Vec<u8> {
    let mut v = alpn.to_vec();
    v.extend_from_slice(b"-ps");
    v
}

/// Write one length-prefixed frame to any async writer. The caller calls
/// `finish()` on the concrete stream afterwards.
async fn write_frame<W: AsyncWriteExt + Unpin>(send: &mut W, bytes: &[u8]) -> Result<(), Error> {
    let len = u32::try_from(bytes.len()).map_err(|_| Error::Refused)?;
    let mut buf = Vec::with_capacity(bytes.len() + 4);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Other(format!("iroh write: {e}")))?;
    send.flush()
        .await
        .map_err(|e| Error::Other(format!("iroh flush: {e}")))
}

/// Read one length-prefixed frame from any async reader.
async fn read_frame<R: AsyncReadExt + Unpin>(recv: &mut R) -> Result<Vec<u8>, Error> {
    let mut head = [0u8; 4];
    recv.read_exact(&mut head)
        .await
        .map_err(|e| Error::Other(format!("iroh frame header: {e}")))?;
    let len = u32::from_le_bytes(head) as usize;
    if len > MAX_FRAME {
        return Err(Error::Refused);
    }
    let mut bytes = vec![0u8; len];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|e| Error::Other(format!("iroh frame body: {e}")))?;
    Ok(bytes)
}

/// A tokio runtime handle suitable for [`IrohFabric::from_endpoint`].
///
/// # Errors
/// Runtime creation failure is [`Error::Other`].
pub fn runtime() -> Result<Arc<tokio::runtime::Runtime>, Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|e| Error::Other(format!("tokio runtime: {e}")))
}

/// Runtime handle used by the fabric: either a runtime it owns or a borrowed
/// handle to a caller-owned runtime (e.g. a test running inside `#[tokio::test]`).
#[derive(Clone)]
enum Rt {
    Owned(Arc<tokio::runtime::Runtime>),
    Handle(tokio::runtime::Handle),
}

impl Rt {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            Self::Owned(rt) => rt.spawn(future),
            Self::Handle(h) => h.spawn(future),
        }
    }
}


/// A fabric over an iroh endpoint. Bind one with [`IrohFabric::bind`], set
/// the pub/sub peer with [`IrohFabric::with_peer`], then use the ordinary
/// [`Fabric`] patterns.
pub struct IrohFabric<C> {
    endpoint: Endpoint,
    alpn: Vec<u8>,
    peer: Option<EndpointAddr>,
    codec: Arc<C>,
    rt: Rt,
}

impl<C> IrohFabric<C> {
    /// Bind a new iroh endpoint with `alpn` and an owned tokio runtime.
    ///
    /// # Errors
    /// Runtime or endpoint creation failure is [`Error::Other`].
    pub fn bind(codec: C, alpn: &[u8]) -> Result<Self, Error> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Other(format!("tokio runtime: {e}")))?;
        let endpoint = rt
            .block_on(async {
                Endpoint::builder(iroh::endpoint::presets::N0)
                    .alpns(vec![alpn.to_vec(), ps_alpn(alpn)])
                    .bind()
                    .await
            })
            .map_err(|e| Error::Other(format!("iroh bind: {e}")))?;
        Ok(Self {
            endpoint,
            alpn: alpn.to_vec(),
            peer: None,
            codec: Arc::new(codec),
            rt: Rt::Owned(Arc::new(rt)),
        })
    }

    /// Build a fabric around an existing iroh endpoint and tokio runtime.
    /// Use this when the caller already owns an endpoint (e.g. one shared
    /// with iroh-blobs) — the fabric does not take ownership of the
    /// endpoint's lifecycle. `alpn` must be one the endpoint advertises.
    ///
    /// `rt` is shared: blocking pattern calls are driven on this runtime.
    #[must_use]
    pub fn from_endpoint(
        codec: C,
        alpn: &[u8],
        endpoint: Endpoint,
        rt: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            endpoint,
            alpn: alpn.to_vec(),
            peer: None,
            codec: Arc::new(codec),
            rt: Rt::Owned(rt),
        }
    }

    /// Set the peer that pub/sub subscribers dial and req/rep callers use
    /// when no per-call address is supplied.
    #[must_use]
    pub fn with_peer(mut self, peer: EndpointAddr) -> Self {
        self.peer = Some(peer);
        self
    }

    /// This endpoint's address, to hand to peers.
    #[must_use]
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// This endpoint's id.
    #[must_use]
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// The endpoint this fabric uses, for consumers that need to pass it to
    /// other iroh-based components (e.g. blobs) or to another fabric.
    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    /// The runtime this fabric drives its async work on.
    ///
    /// # Panics
    /// Panics if the fabric was built with a borrowed runtime handle rather
    /// than an owned runtime; use [`Self::handle`] in that case.
    #[must_use]
    pub fn runtime(&self) -> Arc<tokio::runtime::Runtime> {
        match &self.rt {
            Rt::Owned(rt) => Arc::clone(rt),
            Rt::Handle(_) => panic!("fabric uses a borrowed runtime handle; no owned runtime"),
        }
    }

    /// The runtime handle, available for both owned and borrowed runtimes.
    #[must_use]
    pub fn handle(&self) -> tokio::runtime::Handle {
        match &self.rt {
            Rt::Owned(rt) => rt.handle().clone(),
            Rt::Handle(h) => h.clone(),
        }
    }

    /// Build a fabric around an existing endpoint and a borrowed tokio handle.
    #[must_use]
    pub fn from_endpoint_on_handle(
        codec: C,
        alpn: &[u8],
        endpoint: Endpoint,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            endpoint,
            alpn: alpn.to_vec(),
            peer: None,
            codec: Arc::new(codec),
            rt: Rt::Handle(handle),
        }
    }
}

/// Caller half of an iroh request/reply channel.
pub struct IrohRequester<Req, Res, C> {
    endpoint: Endpoint,
    peer: Option<EndpointAddr>,
    alpn: Vec<u8>,
    codec: Arc<C>,
    _p: std::marker::PhantomData<fn(Req, Res)>,
}

impl<Req, Res, C> Requester<Req, Res> for IrohRequester<Req, Res, C>
where
    Req: Send + 'static,
    Res: Send + 'static,
    C: Codec<Req> + Codec<Res>,
{
    async fn call(&self, req: Req, deadline: Option<Duration>) -> Result<Res, Error> {
        let req_bytes = self.codec.encode(&req)?;
        let endpoint = self.endpoint.clone();
        let peer = self.peer.clone();
        let alpn = self.alpn.clone();
        let codec = Arc::clone(&self.codec);
        {
            let fut = async {
                let peer = peer.ok_or_else(|| {
                    Error::Other("iroh requester has no peer: call with_peer() first".into())
                })?;
                let conn = endpoint
                    .connect(peer, &alpn)
                    .await
                    .map_err(|e| Error::Other(format!("iroh connect: {e}")))?;
                let (mut send, mut recv) = conn
                    .open_bi()
                    .await
                    .map_err(|e| Error::Other(format!("iroh open_bi: {e}")))?;
                write_frame(&mut send, &req_bytes).await?;
                let bytes = read_frame(&mut recv).await?;
                codec.decode(&bytes).map_err(Error::Codec)
            };
            match deadline {
                Some(d) => tokio::time::timeout(d, fut)
                    .await
                    .map_err(|_| Error::Deadline)?,
                None => fut.await,
            }
        }
    }
}

/// Server half of an iroh request/reply channel.
pub struct IrohResponder<Req, Res, C> {
    endpoint: Endpoint,
    alpn: Vec<u8>,
    codec: Arc<C>,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
    rt: Rt,
    _p: std::marker::PhantomData<fn(Req, Res)>,
}

impl<Req, Res, C> Responder<Req, Res> for IrohResponder<Req, Res, C>
where
    Req: Send + 'static,
    Res: Send + 'static,
    C: Codec<Req> + Codec<Res>,
{
    fn serve<H, F>(mut self, handler: H) -> ServeHandle
    where
        H: Fn(Req) -> F + Send + Sync + 'static,
        F: Future<Output = Res> + Send + 'static,
    {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let stop = Arc::clone(&self.stop);
        let endpoint = self.endpoint.clone();
        let codec = Arc::clone(&self.codec);
        let alpn = self.alpn.clone();
        let handler = Arc::new(handler);
        let task = self.rt.spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Some(incoming) = endpoint.accept().await else {
                    break;
                };
                let Ok(conn) = incoming.await else {
                    continue;
                };
                if conn.alpn() != alpn.as_slice() {
                    continue;
                }
                let codec = Arc::clone(&codec);
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    loop {
                        let Ok((mut send, mut recv)) = conn.accept_bi().await else {
                            break;
                        };
                        let Ok(bytes) = read_frame(&mut recv).await else {
                            break;
                        };
                        let Ok(req) = codec.decode(&bytes) else {
                            break;
                        };
                        let res = handler(req).await;
                        let Ok(res_bytes) = codec.encode(&res) else {
                            break;
                        };
                        if write_frame(&mut send, &res_bytes).await.is_err() {
                            break;
                        }
                        let _ = send.finish();
                    }
                });
            }
        });
        ServeHandle::new(move || task.abort())
    }
}

/// Publisher half of an iroh pub/sub channel.
pub struct IrohPublisher<T, C> {
    topic: Vec<u8>,
    subscribers: Arc<std::sync::Mutex<Vec<iroh::endpoint::Connection>>>,
    codec: Arc<C>,
    rt: Rt,
    _p: std::marker::PhantomData<fn(T)>,
}

impl<T, C> Publisher<T> for IrohPublisher<T, C>
where
    T: Clone + Send + 'static,
    C: Codec<T>,
{
    fn publish(&self, value: T) -> Result<(), Error> {
        let bytes = self.codec.encode(&value)?;
        let subscribers: Vec<_> = self.subscribers.lock().map_err(|_| Error::Closed)?.clone();
        let topic = self.topic.clone();
        self.rt.spawn(async move {
            for conn in subscribers {
                let result = async {
                    let (mut send, _) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Other(format!("open_bi: {e}")))?;
                    // topic discriminator is the first frame on each
                    // publication stream — one shared -ps connection can
                    // carry several topics.
                    write_frame(&mut send, &topic).await?;
                    write_frame(&mut send, &bytes).await?;
                    let _ = send.finish();
                    Ok::<(), Error>(())
                }
                .await;
                // Fire-and-forget: a dead subscriber does not fail the
                // publication to the others.
                let _ = result;
            }
        });
        Ok(())
    }
}

/// Subscriber half of an iroh pub/sub channel.
pub struct IrohSubscriber<T, C> {
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>>,
    codec: Arc<C>,
    _p: std::marker::PhantomData<fn(T)>,
}

impl<T, C> Subscriber<T> for IrohSubscriber<T, C>
where
    T: Clone + Send + 'static,
    C: Codec<T>,
{
    async fn next(&self) -> Result<T, Error> {
        let bytes = self.rx.lock().await.recv().await.ok_or(Error::Closed)?;
        self.codec.decode(&bytes).map_err(Error::Codec)
    }

    fn try_next(&self) -> Result<Option<T>, Error> {
        // try_next is only valid when no other task is awaiting `next`;
        // try_lock keeps it non-blocking as the trait promises.
        let mut rx = self.rx.try_lock().map_err(|_| Error::Refused)?;
        match rx.try_recv() {
            Ok(bytes) => Ok(Some(self.codec.decode(&bytes).map_err(Error::Codec)?)),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Err(Error::Closed),
        }
    }
}

impl<C> Fabric<C> for IrohFabric<C> {
    type Requester<Req, Res>
        = IrohRequester<Req, Res, C>
    where
        Req: Send + 'static,
        Res: Send + 'static,
        C: Codec<Req> + Codec<Res>;
    type Responder<Req, Res>
        = IrohResponder<Req, Res, C>
    where
        Req: Send + 'static,
        Res: Send + 'static,
        C: Codec<Req> + Codec<Res>;
    type Publisher<T>
        = IrohPublisher<T, C>
    where
        T: Clone + Send + 'static,
        C: Codec<T>;
    type Subscriber<T>
        = IrohSubscriber<T, C>
    where
        T: Clone + Send + 'static,
        C: Codec<T>;

    fn request_reply<Req, Res>(
        &self,
        _channel: &str,
    ) -> Result<ReqRepPair<Self, C, Req, Res>, Error>
    where
        Req: Send + 'static,
        Res: Send + 'static,
        C: Codec<Req> + Codec<Res>,
    {
        Ok((
            IrohRequester {
                endpoint: self.endpoint.clone(),
                peer: self.peer.clone(),
                alpn: self.alpn.clone(),
                codec: Arc::clone(&self.codec),
                _p: std::marker::PhantomData,
            },
            IrohResponder {
                endpoint: self.endpoint.clone(),
                alpn: self.alpn.clone(),
                codec: Arc::clone(&self.codec),
                stop: Arc::new(AtomicBool::new(false)),
                task: None,
                rt: self.rt.clone(),
                _p: std::marker::PhantomData,
            },
        ))
    }

    fn publish_subscribe<T>(&self, topic: &str) -> Result<PubSubPair<Self, C, T>, Error>
    where
        T: Clone + Send + 'static,
        C: Codec<T>,
    {
        let peer = self.peer.clone();
        let topic_bytes = topic.as_bytes().to_vec();
        let subscribers: Arc<std::sync::Mutex<Vec<iroh::endpoint::Connection>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        // Publisher-side accept loop: register connections that handshake
        // for this topic. Detached; lives as long as the endpoint.
        let endpoint = self.endpoint.clone();
        let pub_alpn = ps_alpn(&self.alpn);
        let registry = Arc::clone(&subscribers);
        let topic_key = topic_bytes.clone();
        self.rt.spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let Ok(conn) = incoming.await else {
                    continue;
                };
                if conn.alpn() != pub_alpn.as_slice() {
                    continue;
                }
                let registry = Arc::clone(&registry);
                let topic_key = topic_key.clone();
                tokio::spawn(async move {
                    // First stream is the subscribe handshake: [topic].
                    let Ok((_, mut recv)) = conn.accept_bi().await else {
                        return;
                    };
                    let Ok(t) = read_frame(&mut recv).await else {
                        return;
                    };
                    if t != topic_key {
                        return;
                    }
                    if let Ok(mut reg) = registry.lock() {
                        reg.push(conn);
                    }
                });
            }
        });

        // Subscriber side: dial the peer, handshake the topic, then read
        // published values off accepted bi-streams into a std channel.
        let endpoint = self.endpoint.clone();
        let sub_alpn = ps_alpn(&self.alpn);
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let topic_key = topic_bytes.clone();
        self.rt.spawn(async move {
            let Some(peer) = peer else {
                // Publisher-only channel: no peer to dial, so no subscriber.
                return;
            };
            let conn = loop {
                match endpoint.connect(peer.clone(), &sub_alpn).await {
                    Ok(c) => break c,
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            };
            if let Ok((mut send, _)) = conn.open_bi().await {
                let _ = write_frame(&mut send, &topic_key).await;
                let _ = send.finish();
            }
            loop {
                let Ok((_, mut recv)) = conn.accept_bi().await else {
                    break;
                };
                // skip the topic discriminator, read the value
                if read_frame(&mut recv).await.is_err() {
                    break;
                }
                match read_frame(&mut recv).await {
                    Ok(bytes) => {
                        if tx.send(bytes).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok((
            IrohPublisher {
                topic: topic_bytes,
                subscribers,
                codec: Arc::clone(&self.codec),
                rt: self.rt.clone(),
                _p: std::marker::PhantomData,
            },
            IrohSubscriber {
                rx: Arc::new(tokio::sync::Mutex::new(rx)),
                codec: Arc::clone(&self.codec),
                _p: std::marker::PhantomData,
            },
        ))
    }
}
