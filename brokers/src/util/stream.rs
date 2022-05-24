use futures::{stream::poll_fn, Stream};
use std::future::Future;

pub fn repeat_fn<F, R, O>(mut func: F) -> impl Stream<Item = O>
where
    R: Future<Output = Option<O>>,
    F: FnMut() -> R,
{
    let mut fut = Box::pin(func());

    poll_fn(move |ctx| {
        let out = fut.as_mut().poll(ctx);

        if out.is_ready() {
            fut.set(func());
        }

        out
    })
}
