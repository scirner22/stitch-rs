use std::{ error, fmt };

#[derive(Debug)]
pub enum Error {
    //// 400
    //BadRequest,
    //// 401
    //Unauthorized,
    //// 403
    //Forbidden,
    //// 405
    //MethodNotAllowed,
    //// 413
    //RequestEntityTooLarge,
    //// 415
    //UnsupportedMediaType,
    //// 422
    //UnprocessableEntity,
    //// 503
    //ServiceUnavailable,
    //// 504
    //GatewayTimeout,
    // Non status related hyper errors
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
            Hyper(ref e) => e.description(),
            HyperTls(ref e) => e.description(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            Hyper(ref err) => write!(f, "{}", err),
            HyperTls(ref err) => write!(f, "{}", err),
        }
    }
}
