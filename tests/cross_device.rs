//! Cross-device row transport over iroh/QUIC: the ordinary fabric
//! request/reply pattern carrying postcard-serialized serde rows.

use fabric::{Error, Fabric, PostcardCodec, Publisher, Requester, Responder, Subscriber};
use fabric_iroh::{IrohFabric, runtime};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const ALPN: &[u8] = b"fabric-test-rows";

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Row {
    id: u64,
    name: String,
}

async fn bind(disco: &MemoryLookup) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .address_lookup(disco.clone())
        .alpns(vec![ALPN.to_vec(), {
            let mut p = ALPN.to_vec();
            p.extend_from_slice(b"-ps");
            p
        }])
        .bind()
        .await
        .expect("bind local endpoint")
}

#[test]
fn a_serde_row_round_trips_between_two_iroh_endpoints() {
    let rt = runtime().expect("runtime");
    let disco = MemoryLookup::new();
    let (server_ep, client_ep) = rt.block_on(async {
        let a = bind(&disco).await;
        let b = bind(&disco).await;
        disco.add_endpoint_info(a.addr());
        disco.add_endpoint_info(b.addr());
        (a, b)
    });

    let server = IrohFabric::from_endpoint(PostcardCodec, ALPN, server_ep, rt.clone());
    let client = IrohFabric::from_endpoint(PostcardCodec, ALPN, client_ep, rt.clone())
        .with_peer(server.addr());

    // `request_reply` is symmetrical: each side calls it on its own fabric.
    // The server uses the responder half, the client the requester half.
    let (requester, _client_responder) = client
        .request_reply::<Row, Row>("rows")
        .expect("client req/rep pair");
    let (_server_requester, responder) = server
        .request_reply::<Row, Row>("rows")
        .expect("server req/rep pair");
    let _serve = responder.serve(|row: Row| async move {
        Row {
            id: row.id + 1,
            name: row.name,
        }
    });

    let answer = requester
        .call(
            Row {
                id: 41,
                name: "row".into(),
            },
            Some(Duration::from_secs(10)),
        )
        .expect("a row crossed the iroh fabric");
    assert_eq!(
        answer,
        Row {
            id: 42,
            name: "row".into()
        }
    );
}

#[test]
fn a_deadline_holds_when_no_responder_serves() {
    let rt = runtime().expect("runtime");
    let disco = MemoryLookup::new();
    let (server_ep, client_ep) = rt.block_on(async {
        let a = bind(&disco).await;
        let b = bind(&disco).await;
        disco.add_endpoint_info(a.addr());
        disco.add_endpoint_info(b.addr());
        (a, b)
    });
    let server = IrohFabric::from_endpoint(PostcardCodec, ALPN, server_ep, rt.clone());
    let client =
        IrohFabric::from_endpoint(PostcardCodec, ALPN, client_ep, rt).with_peer(server.addr());

    let (requester, _responder) = client
        .request_reply::<Row, Row>("no-server")
        .expect("req/rep pair");
    let err = requester
        .call(
            Row {
                id: 1,
                name: "x".into(),
            },
            Some(Duration::from_millis(100)),
        )
        .expect_err("no responder is serving");
    assert!(matches!(err, Error::Deadline | Error::Other(_)));
}

#[test]
fn a_serde_row_reaches_a_subscriber_across_iroh() {
    let rt = runtime().expect("runtime");
    let disco = MemoryLookup::new();
    let (pub_ep, sub_ep) = rt.block_on(async {
        let a = bind(&disco).await;
        let b = bind(&disco).await;
        disco.add_endpoint_info(a.addr());
        disco.add_endpoint_info(b.addr());
        (a, b)
    });
    let publisher_fabric = IrohFabric::from_endpoint(PostcardCodec, ALPN, pub_ep, rt.clone());
    let subscriber_fabric = IrohFabric::from_endpoint(PostcardCodec, ALPN, sub_ep, rt.clone())
        .with_peer(publisher_fabric.addr());

    let (publisher, _subscriber) = publisher_fabric
        .publish_subscribe::<Row>("rows-topic")
        .expect("publisher half");
    let (_other_pub, subscriber) = subscriber_fabric
        .publish_subscribe::<Row>("rows-topic")
        .expect("subscriber half");
    let row = Row {
        id: 7,
        name: "pub".into(),
    };

    // Registration is async; retry until the subscriber is connected.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let got = loop {
        publisher.publish(row.clone()).expect("publish row");
        if let Some(got) = subscriber.try_next().expect("try_next") {
            break got;
        }
        assert!(std::time::Instant::now() < deadline, "row never arrived");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(got, row);
}
