use arrow_flight::flight_service_server::FlightServiceServer;
use log::{error, info};
use tonic::transport::Server;

use crate::common::config::WhaleConfig;
use crate::engine::service::flight::WhaleFlightService;
use crate::WHALE_VERSION;

pub struct WhaleServer {}

impl WhaleServer {
    #[tokio::main]
    pub async fn start(config: WhaleConfig) -> Result<(), Box<dyn std::error::Error>> {
        let addr = config.clone().base.server_url;
        let addr = addr.parse()?;
        info!("Whale(v{}) server listening on {:?}", WHALE_VERSION, addr);

        let service = WhaleFlightService {};
        let svc = FlightServiceServer::new(service);

        match Server::builder().add_service(svc).serve(addr).await {
            Ok(_) => {}
            Err(err) => error!("Failed to start server \n{}", err),
        };
        Ok(())
    }
}
