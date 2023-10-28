use hydroflow::tokio_stream::Stream;

trait ConditionalOpOnStream
where
{
    fn map<T, U>(self, _: T) -> U;
}