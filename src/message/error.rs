pub enum Error {
    // 400
    BadRequest,
    // 401
    Unauthorized,
    // 403
    Forbidden,
    // 405
    MethodNotAllowed,
    // 413
    RequestEntityTooLarge,
    // 415
    UnsupportedMediaType,
    // 422
    UnprocessableEntity,
    // 503
    ServiceUnavailable,
    // 504
    GatewayTimeout,
}

