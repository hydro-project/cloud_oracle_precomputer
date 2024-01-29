use std::fmt::Debug;
use std::{
    future::Future,
};

use hydroflow::futures::{self, Sink};
use influxdb::{Client, InfluxDbWriteable};

pub struct InfluxLoggerConfig {
    pub measurement: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl InfluxLoggerConfig {
    pub fn get_connection_string(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn get_database(&self) -> String {
        self.database.clone()
    }

    pub fn get_measurement(&self) -> String {
        self.measurement.clone()
    }
}

#[derive(Debug)]
pub struct InfluxLogger {
    client: Client,
    measurement: String,
}

impl InfluxLogger {
    pub fn new(config: InfluxLoggerConfig) -> Self {
        let client = Client::new(config.get_connection_string(), config.get_database());
        InfluxLogger {
            client,
            measurement: config.get_measurement(),
        }
    }

    /// Asynchronously logs data to an InfluxDB database.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be logged. It must implement the `InfluxDbWriteable` trait. It should have a time field of type `DateTime<Utc>`.
    ///
    /// # Returns
    ///
    /// A future that resolves to a `Result` indicating whether the logging operation was successful or not.
    /// If successful, it returns the string representation of the logged data. Otherwise, it returns an `influxdb::Error`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use influxdb::InfluxDbWriteable;
    ///
    /// #[derive(InfluxDbWriteable)]
    /// struct Measurement {
    ///     // fields and tags
    /// }
    ///
    /// async fn example() {
    ///     let logger = Logger::new();
    ///     let measurement = Measurement { /* initialize measurement */ };
    ///     let result = logger.log(measurement).await;
    ///
    ///     match result {
    ///         Ok(logged_data) => {
    ///             println!("Data logged successfully: {}", logged_data);
    ///         }
    ///         Err(error) => {
    ///             eprintln!("Failed to log data: {:?}", error);
    ///         }
    ///     }
    /// }
    /// ```
    pub fn log<T>(&self, data: T) -> impl Future<Output = Result<String, influxdb::Error>> + '_
    where
        T: InfluxDbWriteable,
    {
        self.client.query(data.into_query(&self.measurement))
    }

    pub fn into_sink<T>(self) -> impl Sink<T, Error = influxdb::Error>
    where
        T: InfluxDbWriteable,
    {
        futures::sink::unfold(self, |logger, data| async move {
            logger.log(data).await.map(|_str| logger)
        })
    }
}

// impl<T> Sink<T> for InfluxLogger
// where
//     T: InfluxDbWriteable + Unpin,  // Unpin is required by Sink trait
// {
//     type Error = influxdb::Error;

//     fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))  // Always ready to accept data
//     }

//     fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
//         // In this case, we will send immediately
//         let _res = futures::executor::block_on(self.log(item));
//         Ok(())
//     }

//     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))  // No need to flush, since we send immediately
//     }

//     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))  // No need to close, since we send immediately
//     }
// }
