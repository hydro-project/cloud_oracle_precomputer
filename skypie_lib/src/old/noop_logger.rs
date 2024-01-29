use std::fmt::Debug;
use std::{
    future::Future,
};

use hydroflow::futures::{self, Sink};

#[derive(Debug)]
pub struct NoopLogger {
}

impl NoopLogger {
    pub fn new() -> Self {
        NoopLogger {}
    }

    
    pub fn log<T>(&self, _data: T) -> impl Future<Output = Result<String, influxdb::Error>> + '_
    {
        futures::future::ready(Ok("".to_string()))
    }

    pub fn into_sink<T>(self) -> impl Sink<T, Error = influxdb::Error>
    {
        futures::sink::unfold(self, |logger, data| async move {
            logger.log(data).await.map(|_str| logger)
        })
    }
}
