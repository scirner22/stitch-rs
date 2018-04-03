use std::result;

use futures;

use stitch::error::Error;

pub type Result<T> = result::Result<T, Error>;
pub type Future<T> = Box<futures::Future<Item=T, Error=Error>>;
