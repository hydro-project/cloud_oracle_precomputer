use influxdb::{Client, ReadQuery};
use influxdb::InfluxDbWriteable;
use chrono::{DateTime, Utc};
//use crate::SystemTime;
use std::time::SystemTime;

#[tokio::main]
// or #[async_std::main] if you prefer
async fn main() {
    // Connect to db `test` on `http://localhost:8086`
    let client = Client::new("http://localhost:8086", "skypie");
    
    // Authenticate

    #[derive(InfluxDbWriteable)]
    struct WeatherReading {
        time: DateTime<Utc>,
        humidity: i32,
        #[influxdb(tag)] wind_direction: String,
    }

    // Let's write some data into a measurement called `weather`
    let weather_readings = vec!(
        WeatherReading {
            time: SystemTime::now().into(), //Timestamp::Hours(1).into(),
            humidity: 31,
            wind_direction: String::from("north"),
        }.into_query("weather"),
        WeatherReading {
            time: SystemTime::now().into(), //Timestamp::Hours(2).into(),
            humidity: 41,
            wind_direction: String::from("west"),
        }.into_query("weather"),
    );

    let _write_result = client
        .query(weather_readings);
        //.await;
    //assert!(write_result.is_ok(), "Write result was not okay");

    // Wait for the data to be written
    std::thread::sleep(std::time::Duration::from_secs(5));

    // Let's see if the data we wrote is there
    let read_query = ReadQuery::new("SELECT * FROM weather");

    let read_result = client.query(read_query).await;
    assert!(read_result.is_ok(), "Read result was not ok");
    println!("{}", read_result.unwrap());
}