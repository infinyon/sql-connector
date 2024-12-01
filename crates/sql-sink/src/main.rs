mod bind;
mod config;
mod db;
mod insert;
mod sink;
mod upsert;

use std::time::Duration;

use adaptive_backoff::prelude::{
    Backoff, BackoffBuilder, ExponentialBackoff, ExponentialBackoffBuilder,
};
use anyhow::{anyhow, Result};
use config::SqlConfig;
use futures::{SinkExt, StreamExt};

use fluvio_connector_common::{
    connector,
    consumer::ConsumerStream,
    tracing::{error, trace, warn},
    LocalBoxSink, Sink,
};
use fluvio_model_sql::Operation;

use sink::SqlSink;

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(3600 * 24);

#[connector(sink)]
async fn start(config: SqlConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let mut backoff = backoff_init()?;
    let mut sink = start_sink(&mut backoff, config.clone()).await?;

    while let Some(item_result) = stream.next().await {
        match item_result {
            Ok(item) => {
                match serde_json::from_slice::<Operation>(item.as_ref()) {
                    Ok(operation) => {
                        trace!(?operation);
                        // Retry sending the operation to the sink if it fails
                        while let Err(e) = sink.send(serde_json::from_slice(item.as_ref())?).await {
                            error!("Error sending operation to sink: {}", e);
                            sink = start_sink(&mut backoff, config.clone()).await?;
                        }
                    }
                    Err(err) => {
                        error!("Failed to deserialize operation: {}", err);
                        continue;
                    }
                }
            }
            Err(err) => {
                error!("Error reading from stream: {}", err);
                // Handle backoff retries for stream errors
                backoff_and_wait(&mut backoff).await?;
            }
        }
    }

    Ok(())
}

async fn backoff_and_wait(backoff: &mut ExponentialBackoff) -> Result<()> {
    let wait = backoff.wait();
    if wait < BACKOFF_MAX {
        warn!("Retrying in {}", humantime::format_duration(wait));
        async_std::task::sleep(wait).await;
        Ok(())
    } else {
        let err_msg = "Max retry wait time exceeded, shutting down";
        error!(err_msg);
        Err(anyhow!(err_msg))
    }
}

async fn start_sink(
    backoff: &mut ExponentialBackoff,
    config: SqlConfig,
) -> Result<LocalBoxSink<Operation>> {
    loop {
        match SqlSink::new(&config)?.connect(None).await {
            Ok(sink) => {
                // Reset backoff on a successful connection and return the sink
                return Ok(sink);
            }
            Err(err) => {
                error!("Error connecting to sink: \"{}\".", err,);
                backoff_and_wait(backoff).await?;
            }
        }
    }
}

fn backoff_init() -> Result<ExponentialBackoff> {
    ExponentialBackoffBuilder::default()
        .factor(1.1)
        .min(BACKOFF_MIN)
        .max(BACKOFF_MAX)
        .build()
}
