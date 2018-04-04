use futures::Future;
use hyper::Response;

use stitch::{self, error::Error};

pub fn into_future_trait<F, I, E>(f: F) -> Box<Future<Item = I, Error = E>>
where
    F: 'static + Future<Item = I, Error = E>,
{
    Box::new(f)
}

pub fn into_result(res: Response) -> stitch::Result<Response> {
    if res.status().is_success() {
        Ok(res)
    } else {
        Err(Error::HyperStatus(res.status()))
    }
}
