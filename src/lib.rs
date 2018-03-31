extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;

mod message;

use std::rc::Rc;

use futures::Future;
use hyper::{ Method, Request };
use hyper::header::{ ContentLength, ContentType };
use hyper::client::{ Client, FutureResponse, HttpConnector };
use hyper_tls::HttpsConnector;
use tokio_core::reactor;

pub use message::{ Message, UpsertRequest };

static BASE_URL: &'static str = "https://api.stitchdata.com/v2/import";

pub struct StitchClient {
    inner: Rc<Inner>,
}

struct Inner {
    auth_token: String,
    client: Client <HttpsConnector<HttpConnector>>,
}

impl StitchClient {
    pub fn new<S>(handle: &reactor::Handle, auth_token: S) -> Self
        where S: Into<String>
    {
        // TODO resolve unwrap
        let client = ::Client::configure()
            .connector(HttpsConnector::new(4, handle).unwrap())
            .build(handle);

        StitchClient {
            inner: Rc::new(
                Inner {
                    auth_token: auth_token.into(),
                    client,
                }
            )
        }
    }

    pub fn get_status(&self) -> FutureResponse {
        let url = format!("{}/status", BASE_URL);
        self.inner.client.get(url.parse().unwrap())
    }

    pub fn upsert_message<T>(&self, record: UpsertRequest<T>) -> Box<futures::Future<Item=(), Error=::hyper::error::Error>>
        where T: Message
    {
        let json = r#"{"library":"hyper"}"#;
        let url = format!("{}/push", BASE_URL);
        println!("{}", url);
        let mut req = Request::new(Method::Post, url.parse().unwrap());
        req.headers_mut().set(ContentType::json());
        req.headers_mut().set(ContentLength(json.len() as u64));
        req.set_body(json);

        Box::new(self.inner.client.request(req).and_then(|res| {
            println!("{:#?}", res.status());
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;
    use tokio_core::reactor::Core;

    const STITCH_AUTH_FIXTURE: &'static str = env!("STITCH_AUTH_FIXTURE");

    #[test]
    fn get_status() {
        let mut core = Core::new().unwrap();
        let client = StitchClient::new(&core.handle(), STITCH_AUTH_FIXTURE);

        let res = core.run(client.get_status()).unwrap();
        assert_eq!(res.status(), ::hyper::Ok);
    }

    struct Testing {
        test: String,
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
    fn upsert_message_simple() {
        let mut core = Core::new().unwrap();
        let client = StitchClient::new(&core.handle(), STITCH_AUTH_FIXTURE);
        let record = UpsertRequest::new(Testing { test: String::from("12345") });

        let res = core.run(client.upsert_message::<Testing>(record)).unwrap();
        assert!(false);
    }
}
