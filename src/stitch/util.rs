//! Utils for various conversions

use chrono::prelude::*;
use futures::Future;
use hyper::Response;

use stitch::{self, error::Error};

/// Boxes a future. This should be removed once `impl Trait`
/// is released on stable.
pub fn into_future_trait<F, I, E>(f: F) -> Box<Future<Item = I, Error = E>>
where
    F: 'static + Future<Item = I, Error = E>,
{
    Box::new(f)
}

/// Converts non 2xx `hyper::Response`s to an error.
pub fn into_result(res: Response) -> stitch::Result<Response> {
    if res.status().is_success() {
        Ok(res)
    } else {
        Err(Error::HyperStatus(res.status()))
    }
}

/// Returns the current time in milliseconds.
pub fn get_unix_timestamp_ms() -> u64 {
    let now = Utc::now();
    let seconds = now.timestamp() as u64;
    let nanoseconds = u64::from(now.nanosecond());

    (seconds * 1000) + (nanoseconds / 1000 / 1000)
}
