// Sanity tests for the Local backend.

use peerrpc_signal::{Local, SignalBody, SdpOffer};

#[tokio::test]
async fn local_exchange_by_service_and_broadcasts() {
    let local = Local::new();
    let mut alice = local.exchange("svc.1", "alice").await.unwrap();
    let mut bob = local.exchange("svc.1", "bob").await.unwrap();

    assert_eq!(alice.service(), "svc.1");
    assert_eq!(alice.peer_id(), "alice");

    alice
        .send(peerrpc_signal::SignalMessage {
            service: "svc.1".into(),
            body: SignalBody {
                offer: Some(SdpOffer { sdp: "v=0".into() }),
                ..Default::default()
            },
        })
        .unwrap();

    let msg = bob.recv().await.expect("bob should receive");
    assert_eq!(msg.body.offer.unwrap().sdp, "v=0");
}
