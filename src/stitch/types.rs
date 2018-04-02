use std::result;

use futures;

use stitch::error;

pub type Result<T> = result::Result<T, error::Error>;
pub type Future<T> = Box<futures::Future<Item=T, Error=error::Error>>;
