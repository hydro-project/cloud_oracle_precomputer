use hydroflow::{tokio_stream::Stream};
use std::task::{Poll, Context};

/// Returns an unfused iterator that emits `n` items from `iter` at a time, with `None`s in-between.
pub fn iter_stream_batches<I>(iter: I, n: usize) -> impl Stream<Item = I::Item>
where
    I: Iterator + Unpin,
{
    struct UnfusedBatches<I> {
        iter: I,
        n: usize,
        count: usize,
    }
    impl<I> Stream for UnfusedBatches<I>
    where
        I: Iterator + Unpin,
    {
        type Item = I::Item;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            ctx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.count += 1;
            if this.n <= this.count {
                this.count = 0;
                ctx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(this.iter.next())
            }
        }
    }
    UnfusedBatches { iter, n, count: 0 }
}