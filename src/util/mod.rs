use hyper::Response;

use stitch::{self, error::Error};

pub mod futures;

pub fn into_result(res: Response) -> stitch::Result<Response> {
    if res.status().is_success() {
        Ok(res)
    } else {
        Err(Error::HyperStatus(res.status()))
    }
}
