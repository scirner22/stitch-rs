extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;

mod message;
mod stitch;

use std::rc::Rc;

use futures::{ Future, Stream };
//use futures::sync::mpsc::Receiver;
use hyper::{ Chunk, Method, Request, Response, Uri };
use hyper::header::{ Authorization, Bearer, ContentLength, ContentType };
use hyper::client::{ Client, HttpConnector };
use hyper_tls::HttpsConnector;
use serde::ser::Serialize;
use tokio_core::reactor;

pub use message::{ Message, RawUpsertRequest, UpsertRequest };

static BASE_URL: &'static str = "https://api.stitchdata.com/v2/import";

pub struct StitchClient {
    inner: Rc<Inner>,
}

struct Inner {
    bearer: Authorization<Bearer>,
    client_id: u32,
    client: Client <HttpsConnector<HttpConnector>>,
    //receiver: Receiver<>,
}

impl StitchClient {
    pub fn new<S>(handle: &reactor::Handle, client_id: u32, auth_token: S) -> stitch::Result<Self>
        where S: Into<String>
    {
        let client = ::Client::configure()
            .connector(HttpsConnector::new(4, handle)?)
            .build(handle);
        let bearer = Authorization(
            Bearer {
                token: auth_token.into(),
            }
        );

        Ok(StitchClient {
            inner: Rc::new(
                Inner { bearer, client_id, client }
            )
        })
    }

    pub fn client_id(&self) -> u32 {
        self.inner.client_id
    }

    pub fn upsert_record<T>(&self, data: T) -> UpsertRequest<T>
        where T: Message + Serialize
    {
        UpsertRequest::new(self.inner.client_id, 1, data)
    }

    pub fn get_status(&self) -> stitch::Future<Response> {
        let url = format!("{}/status", BASE_URL);
        let url = url.parse().unwrap();

        Box::new(
            self.inner.client.get(url)
                .map_err(Into::into)
        )
    }

    fn upsert_batch<T>(&self, url: Uri, batch: Vec<UpsertRequest<T>>) -> stitch::Future<Chunk>
        where T: Message + Serialize
    {
        let batch = batch.into_iter()
            .map(Into::<RawUpsertRequest<T>>::into)
            .collect::<Vec<_>>();
        let json = serde_json::to_string(&batch).unwrap();
        let mut req = Request::new(Method::Post, url);

        req.headers_mut().set(self.inner.bearer.clone());
        req.headers_mut().set(ContentType::json());
        req.headers_mut().set(ContentLength(json.len() as u64));
        req.set_body(json);

        Box::new(
            self.inner.client.request(req)
                .and_then(|res| {
                    res.body().concat2()
                })
                .map_err(Into::into)
        )
    }

    pub fn upsert_message<T>(&self, record: UpsertRequest<T>) -> stitch::Future<Chunk>
        where T: Message + Serialize
    {
        let url = format!("{}/push", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_batch(url, vec![record])
    }

    pub fn validate_message<T>(&self, record: UpsertRequest<T>) -> stitch::Future<Chunk>
        where T: Message + Serialize
    {
        let url = format!("{}/validate", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_batch(url, vec![record])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;
    use std::str;
    use tokio_core::reactor::Core;

    const STITCH_AUTH_FIXTURE: &'static str = env!("STITCH_AUTH_FIXTURE");
    const STITCH_CLIENT_ID: &'static str = env!("STITCH_CLIENT_ID");

    #[test]
    fn get_status() {
        let mut core = Core::new().unwrap();
        let client_id = STITCH_CLIENT_ID.parse().unwrap();
        let client = StitchClient::new(&core.handle(), client_id, STITCH_AUTH_FIXTURE).unwrap();

        let res = core.run(client.get_status()).unwrap();
        assert_eq!(res.status(), ::hyper::Ok);
    }

    #[derive(Serialize)]
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
        let mut core = Core::new().unwrap();
        let client_id = STITCH_CLIENT_ID.parse().unwrap();
        let client = StitchClient::new(&core.handle(), client_id, STITCH_AUTH_FIXTURE).unwrap();
        let record = client.upsert_record(Testing { id: 1 });

        let res = core.run(client.validate_message::<Testing>(record)).unwrap();
        let right = r#"{"status":"OK","message":"Batch is valid!"}"#;
        assert_eq!(str::from_utf8(&res).unwrap(), right);
    }
}
