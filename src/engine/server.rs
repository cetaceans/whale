use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;

use crate::common::config::WhaleConfig;
use crate::common::logger::Logger;
use crate::engine::service::flight::WhaleFlightService;
use crate::WHALE_VERSION;

pub struct WhaleServer {}

impl WhaleServer {
    #[tokio::main]
    pub async fn start(config: WhaleConfig) -> Result<(), Box<dyn std::error::Error>> {
        Logger::level("info");
        let addr = config.clone().server_url;
        let addr = addr.parse()?;
        let service = WhaleFlightService {};
        let svc = FlightServiceServer::new(service);
        info!("WhaleServer (V:{}) listening on {:?}", WHALE_VERSION, addr);
        Server::builder().add_service(svc).serve(addr).await?;
        Ok(())
    }
}
