use futures::*;

pub fn into_future_trait<F, I, E>(f: F) -> Box<Future<Item=I, Error=E>>
    where F: 'static + Future<Item=I, Error=E>
{
    Box::new(f)
}
