use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;

use crate::common::config::WhaleConfig;
use crate::engine::service::flight::WhaleFlightService;

pub struct WhaleServer {}

impl WhaleServer {
    #[tokio::main]
    pub async fn start(config: WhaleConfig) -> Result<(), Box<dyn std::error::Error>> {
        let addr = config.clone().server_url;
        let addr = addr.parse()?;
        let service = WhaleFlightService {};
        let svc = FlightServiceServer::new(service);
        info!("Listening on {:?}", addr);
        Server::builder().add_service(svc).serve(addr).await?;
        Ok(())
    }
}
