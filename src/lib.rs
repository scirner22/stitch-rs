extern crate futures;
extern crate hyper;
extern crate hyper_tls;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;

mod message;
mod stitch;
mod util;

use std::rc::Rc;
use std::fmt::Debug;

use futures::{Future, Stream};
use futures::sync::mpsc::Receiver;
use hyper::{Chunk, Method, Request, Response, Uri};
use hyper::header::{Authorization, Bearer, ContentLength, ContentType};
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;
use serde::ser::Serialize;
use tokio_core::reactor;

use util::futures::*;
use util::*;

pub use message::{Message, RawUpsertRequest, UpsertRequest};
pub use stitch::error::Error;

static BASE_URL: &'static str = "https://api.stitchdata.com/v2/import";

struct Inner {
    bearer: Authorization<Bearer>,
    client_id: u32,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl Inner {
    pub fn get_status(&self) -> stitch::Future<Response> {
        let url = format!("{}/status", BASE_URL);
        let url = url.parse().unwrap();

        let f = self.client
            .get(url)
            .map_err(Into::into)
            .and_then(|res| into_result(res));

        into_future_trait(f)
    }

    fn upsert_helper<T>(&self, url: Uri, batch: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
    where
        T: Message + Serialize,
    {
        let batch = batch
            .into_iter()
            .map(Into::<RawUpsertRequest<T>>::into)
            .collect::<Vec<_>>();
        let json = serde_json::to_string(&batch).unwrap();
        let mut req = Request::new(Method::Post, url);

        req.headers_mut().set(self.bearer.clone());
        req.headers_mut().set(ContentType::json());
        req.headers_mut().set(ContentLength(json.len() as u64));
        req.set_body(json);

        let f = self.client
            .request(req)
            .map_err(Into::into)
            .and_then(|res| into_result(res))
            .and_then(|res| res.body().concat2().map_err(Into::into));

        into_future_trait(f)
    }

    pub fn validate_batch<T>(&self, batch: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
    where
        T: Message + Serialize,
    {
        let url = format!("{}/validate", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_helper(url, batch)
    }

    pub fn upsert_batch<T>(&self, batch: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
    where
        T: Message + Serialize,
    {
        let url = format!("{}/push", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_helper(url, batch)
    }

    // TODO add switch_view record
    // TODO do not cancel stream on errs.. just log
}

#[derive(Clone)]
pub struct StitchClient {
    inner: Rc<Inner>,
}

impl StitchClient {
    pub fn new<S>(handle: &reactor::Handle, client_id: u32, auth_token: S) -> stitch::Result<Self>
    where
        S: Into<String>,
    {
        let client = ::Client::configure()
            .connector(HttpsConnector::new(4, handle)?)
            .build(handle);
        let bearer = Authorization(Bearer {
            token: auth_token.into(),
        });

        Ok(StitchClient {
            inner: Rc::new(Inner {
                bearer,
                client_id,
                client,
            }),
        })
    }

    pub fn client_id(&self) -> u32 {
        self.inner.client_id
    }

    pub fn upsert_record<T>(&self, data: T) -> UpsertRequest<T>
    where
        T: Message + Serialize,
    {
        UpsertRequest::new(self.client_id(), 1, data)
    }

    pub fn get_status(&self) -> stitch::Future<Response> {
        self.inner.get_status()
    }

    pub fn validate_batch<T>(&self, record: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
    where
        T: Message + Serialize,
    {
        self.inner.validate_batch(record)
    }

    pub fn upsert_batch<T>(&self, record: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
    where
        T: Message + Serialize,
    {
        self.inner.upsert_batch(record)
    }

    pub fn buffer_batches<T>(
        &self,
        stream: Receiver<UpsertRequest<T>>,
        batch_size: usize,
    ) -> stitch::Future<()>
    where
        T: Message + Serialize + Debug + 'static,
    {
        let inner = Rc::clone(&self.inner);
        let f = stream
            .chunks(batch_size)
            .map_err(|_| Error::Buffer(""))
            .for_each(move |chunk| {
                info!("Persisting batch of {} records", chunk.len());
                inner.upsert_batch(chunk).map(|_| ())
            });

        into_future_trait(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;
    use std::str;

    use futures::sync::mpsc::{channel, Sender};
    use tokio_core::reactor::Core;

    const STITCH_AUTH_FIXTURE: &'static str = env!("STITCH_AUTH_FIXTURE");
    const STITCH_CLIENT_ID: &'static str = env!("STITCH_CLIENT_ID");

    fn get_client() -> (Core, StitchClient) {
        let core = Core::new().unwrap();
        let client_id = STITCH_CLIENT_ID.parse().unwrap();
        let client = StitchClient::new(&core.handle(), client_id, STITCH_AUTH_FIXTURE).unwrap();

        (core, client)
    }

    #[test]
    fn get_status() {
        let (mut core, client) = get_client();
        let res = core.run(client.get_status());

        assert_eq!(res.is_ok(), true);
    }

    #[derive(Debug, Serialize)]
    struct Testing {
        id: u32,
    }

    impl Message for Testing {
        fn get_table_name(&self) -> String {
            String::from("table_name")
        }

        fn get_keys(&self) -> Vec<String> {
            vec![String::from("id")]
        }
    }

    #[test]
    fn validate_message_simple() {
        let (mut core, client) = get_client();
        let record = client.upsert_record(Testing { id: 1 });
        let right = r#"{"status":"OK","message":"Batch is valid!"}"#;
        let res = core.run(client.validate_batch::<Testing>(vec![record]))
            .unwrap();

        assert_eq!(str::from_utf8(&res).unwrap(), right);
    }

    #[test]
    fn buffer_batches() {
        let (mut core, client) = get_client();
        let (mut tx, mut rx): (
            Sender<UpsertRequest<Testing>>,
            Receiver<UpsertRequest<Testing>>,
        ) = channel(4);
        tx.try_send(client.upsert_record(Testing { id: 1 }))
            .unwrap();
        tx.try_send(client.upsert_record(Testing { id: 2 }))
            .unwrap();
        tx.try_send(client.upsert_record(Testing { id: 3 }))
            .unwrap();
        rx.close();
        let res = core.run(client.buffer_batches(rx, 2));

        assert!(res.is_ok());
    }
}
