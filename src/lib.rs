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

mod stitch;

/// Re-exports
pub use stitch::error::Error;
pub use stitch::message::{Message, UpsertRequest};
pub use stitch::types::{Future, Result};
pub use stitch::StitchClient;
