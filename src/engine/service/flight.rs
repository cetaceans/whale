use std::pin::Pin;

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion::{datasource::parquet::ParquetTable, physical_plan::collect};
use futures::Stream;
use log::debug;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct WhaleFlightService {}

#[tonic::async_trait]
impl FlightService for WhaleFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        debug!("get_schema");

        let request = request.into_inner();
        let table = ParquetTable::try_new(&request.path[0], 200).unwrap();

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_result =
            arrow_flight::utils::flight_schema_from_arrow_schema(table.schema().as_ref(), &options);

        Ok(Response::new(schema_result))
    }

    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        match String::from_utf8(ticket.ticket.to_vec()) {
            Ok(sql) => {
                debug!("do_get: {}", sql);
                let mut ctx = ExecutionContext::new();

                ctx.register_parquet(
                    "test",
                    "F:/Rust/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet",
                )
                .map_err(|e| to_tonic_err(&e))?;

                // create the query plan
                let plan = ctx
                    .create_logical_plan(&sql)
                    .and_then(|plan| ctx.optimize(&plan))
                    .and_then(|plan| ctx.create_physical_plan(&plan))
                    .map_err(|e| to_tonic_err(&e))?;

                // execute the query
                let results = collect(plan.clone()).await.map_err(|e| to_tonic_err(&e))?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }

                // add an initial FlightData message that sends schema
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let schema = plan.schema();
                let schema_flight_data =
                    arrow_flight::utils::flight_data_from_arrow_schema(schema.as_ref(), &options);

                let mut flights: Vec<Result<FlightData, Status>> = vec![Ok(schema_flight_data)];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .iter()
                    .flat_map(|batch| {
                        let flight_data =
                            arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);
                        flight_data.into_iter().map(Ok)
                    })
                    .collect();

                // append batch vector to schema vector, so that the first message sent is the schema
                flights.append(&mut batches);

                let output = futures::stream::iter(flights);

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {:?}", e))),
        }
    }

    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_tonic_err(e: &datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{:?}", e))
}
