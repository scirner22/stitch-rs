use hyper::Response;

use stitch::{ self, error::Error };

pub mod futures;

pub fn into_result(res: Response) -> stitch::Result<Response> {
    match res.status().is_success() {
        true => Ok(res),
        false => Err(Error::HyperStatus(res.status())),
    }
}
