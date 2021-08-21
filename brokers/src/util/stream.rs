use futures::Stream;
use std::{future::Future, task::Poll};

pub fn repeat_fn<F, R, O>(mut func: F) -> impl Stream<Item = O>
where
    R: Future<Output = Option<O>>,
    F: FnMut() -> R,
{
    let mut fut = Box::pin(func());

    futures::stream::poll_fn(move |ctx| {
        let out = fut.as_mut().poll(ctx);

        if let Poll::Ready(_) = out {
            fut.set(func());
        }

        out
    })
}
