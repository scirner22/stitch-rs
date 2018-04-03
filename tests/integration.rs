extern crate futures;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate stitch;
extern crate tokio_core;

use std::vec::Vec;
use std::thread;

use futures::sync::mpsc::{ channel, Receiver, Sender };
use stitch::{ Message, StitchClient, UpsertRequest };
use tokio_core::reactor::Core;

const STITCH_AUTH_FIXTURE: &'static str = env!("STITCH_AUTH_FIXTURE");
const STITCH_CLIENT_ID: &'static str = env!("STITCH_CLIENT_ID");

#[derive(Debug, Clone, Serialize)]
struct Inner {
    description: String,
}

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: u32,
    name: String,
    inner: Inner,
}

impl Message for TestRecord {
    fn get_table_name(&self) -> String {
        String::from("test_integration")
    }

    fn get_keys(&self) -> Vec<String> {
        vec![
            String::from("id"),
            String::from("name"),
        ]
    }
}

#[test]
pub fn test_buffered_stream() {
    // setup
    let mut core = Core::new().unwrap();
    let client_id = STITCH_CLIENT_ID.parse().unwrap();
    let client = StitchClient::new(&core.handle(), client_id, STITCH_AUTH_FIXTURE).unwrap();

    // test records
    let r1 = TestRecord {
        id: 1,
        name: String::from("name_1"),
        inner: Inner { description: String::from("description_1") }
    };
    let r2 = TestRecord {
        id: 2,
        name: String::from("name_2"),
        inner: Inner { description: String::from("description_2") }
    };
    let r3 = TestRecord {
        id: 3,
        name: String::from("name_3"),
        inner: Inner { description: String::from("description_3") }
    };
    let r4 = TestRecord {
        id: 4,
        name: String::from("name_4"),
        inner: Inner { description: String::from("description_4") }
    };
    let r5 = TestRecord {
        id: 5,
        name: String::from("name_5"),
        inner: Inner { description: String::from("description_5") }
    };
    let r6 = TestRecord {
        id: 6,
        name: String::from("name_6"),
        inner: Inner { description: String::from("description_6") }
    };
    let r7 = TestRecord {
        id: 7,
        name: String::from("name_7"),
        inner: Inner { description: String::from("description_7") }
    };

    // test validate
    let f = client.validate_batch(vec![client.upsert_record(r1.clone()), client.upsert_record(r2.clone())]);
    assert!(core.run(f).is_ok());

    // create channel
    let (mut tx, mut rx): (Sender<UpsertRequest<TestRecord>>, Receiver<UpsertRequest<TestRecord>>) = channel(10);

    // seed channel
    assert!(tx.try_send(client.upsert_record(r1)).is_ok());
    assert!(tx.try_send(client.upsert_record(r2)).is_ok());
    assert!(tx.try_send(client.upsert_record(r3)).is_ok());
    assert!(tx.try_send(client.upsert_record(r4)).is_ok());
    assert!(tx.try_send(client.upsert_record(r5)).is_ok());
    assert!(tx.try_send(client.upsert_record(r6)).is_ok());
    assert!(tx.try_send(client.upsert_record(r7)).is_ok());
    rx.close();

    // test stream
    let t = thread::spawn(move || {
        let mut t_core = Core::new().unwrap();
        let t_client = StitchClient::new(&t_core.handle(), client_id, STITCH_AUTH_FIXTURE).unwrap();
        assert!(t_core.run(t_client.buffer_batches(rx, 3)).is_ok());
    });

    assert!(t.join().is_ok());
}
