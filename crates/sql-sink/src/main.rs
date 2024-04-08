mod backoff;
mod bind;
mod config;
mod db;
mod insert;
mod sink;
mod upsert;

use std::time::Duration;

use anyhow::Result;
use config::SqlConfig;
use futures::SinkExt;

use fluvio_connector_common::{
    connector,
    consumer::ConsumerStream,
    tracing::{error, trace, warn},
    Sink,
};
use fluvio_model_sql::Operation;

use sink::SqlSink;

use crate::backoff::Backoff;

const BACKOFF_LIMIT: Duration = Duration::from_secs(1000);

#[connector(sink)]
async fn start(config: SqlConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        let wait = backoff.next();

        if wait > BACKOFF_LIMIT {
            error!("Max retry reached, exiting");
        }
        let sink = SqlSink::new(&config)?;
        let mut sink = match sink.connect(None).await {
            Ok(sink) => sink,
            Err(err) => {
                warn!(
                    "Error connecting to streaming source: \"{}\", reconnecting in {}.",
                    err,
                    humantime::format_duration(wait)
                );
                async_std::task::sleep(wait).await;
                continue; // loop and retry
            }
        };
        while let Some(item) = stream.next().await {
            let operation: Operation = serde_json::from_slice(item?.as_ref())?;
            trace!(?operation);
            sink.send(operation).await?;
        }
    }
}
