mod bind;
mod config;
mod db;
mod insert;
mod sink;
mod upsert;

use config::SqlConfig;
use fluvio_connector_common::{connector, consumer::ConsumerStream, tracing::trace, Result, Sink};
use fluvio_model_sql::Operation;
use futures::SinkExt;
use sink::SqlSink;

#[connector(sink)]
async fn start(config: SqlConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let sink = SqlSink::new(&config)?;
    let mut sink = sink.connect(None).await?;
    while let Some(item) = stream.next().await {
        let operation: Operation = serde_json::from_slice(item?.as_ref())?;
        trace!(?operation);
        sink.send(operation).await?;
    }
    Ok(())
}
