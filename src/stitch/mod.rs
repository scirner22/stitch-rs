//! Stitch Client

use std::rc::Rc;

use futures::{Future as _Future, Stream};
use futures::sync::mpsc::Receiver;
use hyper::{Chunk, Method, Request, Response, Uri};
use hyper::header::{Authorization, Bearer, ContentLength, ContentType};
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;
use serde::ser::Serialize;
use serde_json;
use tokio_core::reactor;

pub mod error;
pub mod message;
pub mod types;
mod util;

use self::error::Error;
use self::message::{Message, RawUpsertRequest, UpsertRequest};
use self::types::{Future, Result};

static BASE_URL: &'static str = "https://api.stitchdata.com/v2/import";

/// Inner representation of a stitch client.
struct Inner {
    bearer: Authorization<Bearer>,
    client_id: u32,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl Inner {
    pub fn get_status(&self) -> Future<Response> {
        let url = format!("{}/status", BASE_URL);
        let url = url.parse().unwrap();

        let f = self.client
            .get(url)
            .map_err(Into::into)
            .and_then(util::into_result);

        util::into_future_trait(f)
    }

    fn upsert_helper<T>(&self, url: Uri, batch: Vec<T>) -> Future<Chunk>
    where
        T: Message + Serialize + 'static,
    {
        let time_ms = util::get_unix_timestamp_ms();
        let batch = batch
            .into_iter()
            .map(|msg| UpsertRequest::new(self.client_id, time_ms, msg))
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
            .and_then(util::into_result)
            .and_then(|res| res.body().concat2().map_err(Into::into));

        util::into_future_trait(f)
    }

    pub fn validate_batch<T>(&self, batch: Vec<T>) -> Future<Chunk>
    where
        T: Message + Serialize + 'static,
    {
        let url = format!("{}/validate", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_helper(url, batch)
    }

    pub fn upsert_batch<T>(&self, batch: Vec<T>) -> Future<Chunk>
    where
        T: Message + Serialize + 'static,
    {
        let url = format!("{}/push", BASE_URL);
        let url = url.parse().unwrap();

        self.upsert_helper(url, batch)
    }
}

/// Holds an inner representation of a stitch client.
/// This type can be created on a separate thread and sent messages
/// over a channel, or can be safely cloned many times on a single
/// thread.
#[derive(Clone)]
pub struct StitchClient {
    inner: Rc<Inner>,
}

impl StitchClient {
    /// Create a new stitch client.
    pub fn new<S>(handle: &reactor::Handle, client_id: u32, auth_token: S) -> Result<Self>
    where
        S: Into<String>,
    {
        let client = Client::configure()
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

    /// Returns the `client_id`.
    pub fn client_id(&self) -> u32 {
        self.inner.client_id
    }

    /// Upserts a single `Message`, as a future.
    pub fn upsert_record<T>(&self, data: T) -> UpsertRequest<T>
    where
        T: Message + Serialize + 'static,
    {
        UpsertRequest::new(self.client_id(), 1, data)
    }

    /// Returns the current status of the stitch api, as a future.
    pub fn get_status(&self) -> Future<Response> {
        self.inner.get_status()
    }

    /// Validates a batch of `Messages`, as a future.
    pub fn validate_batch<T>(&self, record: Vec<T>) -> Future<Chunk>
    where
        T: Message + Serialize + 'static,
    {
        self.inner.validate_batch(record)
    }

    /// Upserts a batch of `Messages`, as a future.
    pub fn upsert_batch<T>(&self, record: Vec<T>) -> Future<Chunk>
    where
        T: Message + Serialize + 'static,
    {
        self.inner.upsert_batch(record)
    }

    /// Returns a future that represents a buffered stream,
    /// represented by a `futures::sync::mpsc::Receiver`.
    /// The stream is chunked and each batch is sent to stitch.
    /// Failures are logged and dropped.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// extern crate futures;
    /// extern crate stitch;
    /// extern crate tokio_core;
    ///
    /// use std::thread;
    ///
    /// use futures::sync::mpsc::channel;
    /// use stitch::StitchClient;
    /// use tokio_core::reactor::Core;
    ///
    /// fn main() {
    ///     let (mut tx, mut rx) = channel(10);
    ///
    ///     let t = thread::spawn(move || {
    ///         let mut core = Core::new().unwrap();
    ///         let client = StitchClient::new(&core.handle(), 1, "auth_token_1").unwrap();
    ///
    ///         core.run(client.buffer_channel(20, rx)).unwrap();
    ///     });
    /// }
    /// ```
    ///
    /// ```ignore
    /// extern crate futures;
    /// extern crate stitch;
    /// extern crate tokio_core;
    ///
    /// use futures::sync::mpsc::channel;
    /// use stitch::StitchClient;
    /// use tokio_core::reactor::Core;
    ///
    /// fn main() {
    ///     let mut core = Core::new().unwrap();
    ///     let client = StitchClient::new(&core.handle(), 1, "auth_token_1").unwrap();
    ///     let (mut tx, mut rx) = channel(10);
    ///
    ///     core.run(client.buffer_channel(20, rx)).unwrap();
    /// }
    /// ```
    pub fn buffer_channel<T>(&self, batch_size: usize, stream: &'static mut Receiver<T>) -> Future<()>
    where
        T: Message + Serialize + 'static,
    {
        let inner = Rc::clone(&self.inner);
        let f = stream
            .chunks(batch_size)
            .map_err(|_| Error::Buffer("Could not chunk stream"))
            .for_each(move |chunk| {
                info!("Persisting batch of {} records", chunk.len());

                inner.upsert_batch(chunk).then(|res| match res {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("{}", e);
                        Ok(())
                    }
                })
            });

        util::into_future_trait(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;
    use std::str;

    use futures::sync::mpsc::channel;
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
        let record = Testing { id: 1 };
        let right = r#"{"status":"OK","message":"Batch is valid!"}"#;
        let res = core.run(client.validate_batch(vec![record]))
            .unwrap();

        assert_eq!(str::from_utf8(&res).unwrap(), right);
    }

    #[test]
    fn buffer_channel() {
        let (mut core, client) = get_client();
        let (mut tx, mut rx) = channel(4);
        tx.try_send(Testing { id: 1 }).unwrap();
        tx.try_send(Testing { id: 2 }).unwrap();
        tx.try_send(Testing { id: 3 }).unwrap();
        rx.close();
        let res = core.run(client.buffer_channel(2, rx));

        assert!(res.is_ok());
    }
}
