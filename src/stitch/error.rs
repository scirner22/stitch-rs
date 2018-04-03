use std::{error, fmt};

#[derive(Debug)]
pub enum Error {
    HyperStatus(::hyper::StatusCode),
    Buffer(&'static str),
    Hyper(::hyper::Error),
    HyperTls(::hyper_tls::Error),
}

impl From<::hyper::Error> for Error {
    fn from(e: ::hyper::Error) -> Self {
        Error::Hyper(e)
    }
}

impl From<::hyper_tls::Error> for Error {
    fn from(e: ::hyper_tls::Error) -> Self {
        Error::HyperTls(e)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        use self::Error::*;
        match *self {
            HyperStatus(ref code) => code.canonical_reason()
                .unwrap_or("Unregistered Status Code"),
            Buffer(msg) => msg,
            Hyper(ref e) => e.description(),
            HyperTls(ref e) => e.description(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            HyperStatus(ref code) => write!(
                f,
                "{}",
                code.canonical_reason()
                    .unwrap_or("Unregistered Status Code")
            ),
            Buffer(msg) => write!(f, "{}", msg),
            Hyper(ref err) => write!(f, "{}", err),
            HyperTls(ref err) => write!(f, "{}", err),
        }
    }
}
