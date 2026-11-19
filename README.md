# fabric-iroh

Minimal iroh/QUIC fabric for cross-device rows. Implements the `fabric::Fabric<C>`
patterns (request/reply and publish/subscribe) over iroh bi-directional QUIC
streams, with postcard-serialized serde values on the wire.

This is the row transport for cross-device communication (e.g. flow devices).
It deliberately does **not** use Arrow IPC on the network: a row is just a
serde `T`, encoded by the caller-chosen fabric codec. For local shared-memory
rows, use `fabric`'s `arrow` feature (`ArrowCodec`) with the iceoryx2 fabrics.

## Wire protocol

One bi-directional QUIC stream per message:

```
[u32 LE length][postcard bytes]
```

The stream itself is the correlation — there is no request id on the wire,
no pending map, and no multiplexing. Deadlines are applied locally with
`tokio::time::timeout`. Pub/sub peer discovery is explicit via
`IrohFabric::with_peer(addr)`; a subscriber handshakes a topic on the
connection, and the publisher pushes each published value on a fresh stream
(fire-and-forget).

## Usage

```rust,no_run
use fabric::{Fabric, PostcardCodec, Requester, Responder};
use fabric_iroh::IrohFabric;

# fn main() -> Result<(), fabric::Error> {
// Bind a fabric with its own iroh endpoint + tokio runtime...
let local = IrohFabric::bind(PostcardCodec, b"my-app")?;
// ...or inject an endpoint the caller already owns (e.g. flow's):
let rt = fabric_iroh::runtime()?;
let server = IrohFabric::from_endpoint(PostcardCodec, b"my-app", endpoint, rt)?;
# Ok(())
# }
```

The `fabric` crate exposes the pattern traits; this crate supplies the
iroh medium. Callers already on a tokio reactor should use `fabric`'s
`async` adapter rather than the blocking `call`/`next` methods.

## Gates

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
