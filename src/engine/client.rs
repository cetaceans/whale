use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::util::pretty;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;

#[derive(Debug)]
pub struct Client {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl Client {
    pub async fn connect(url: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = FlightServiceClient::connect(format!("http://{}", url)).await?;
        Ok(Self { client })
    }

    pub async fn exec_query(&mut self, sql: String) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(Ticket { ticket: sql.into() });

        let mut stream = self.client.do_get(request).await?.into_inner();

        let flight_data = stream.message().await?.unwrap();
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        let mut results = vec![];
        while let Some(flight_data) = stream.message().await? {
            let record_batch = flight_data_to_arrow_batch(&flight_data, schema.clone()).unwrap()?;
            results.push(record_batch);
        }
        pretty::print_batches(&results)?;
        Ok(())
    }
}
