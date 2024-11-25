mod bind;
mod config;
mod db;
mod insert;
mod sink;
mod upsert;

use std::time::Duration;

use anyhow::{anyhow, Result};
use config::SqlConfig;
use futures::{SinkExt, StreamExt};

use fluvio_connector_common::{
    connector,
    consumer::ConsumerStream,
    future::retry::ExponentialBackoff,
    tracing::{error, trace, warn},
    LocalBoxSink, Sink,
};
use fluvio_model_sql::Operation;

use sink::SqlSink;

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(3600 * 24);

#[connector(sink)]
async fn start(config: SqlConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let mut sink = start_sink(config).await?;
    while let Some(item) = stream.next().await {
        let operation: Operation = serde_json::from_slice(item?.as_ref())?;
        trace!(?operation);
        sink.send(operation).await?;
    }

    Ok(())
}

async fn start_sink(config: SqlConfig) -> Result<LocalBoxSink<Operation>> {
    let mut backoff = backoff_init()?;
    loop {
        let Some(wait) = backoff.next() else {
            // not currently possible, but if backoff strategy is changed later
            // this could kick in
            let msg = "Retry backoff exhausted";
            error!(msg);
            return Err(anyhow!(msg));
        };
        if wait >= BACKOFF_MAX {
            // max retry set to 24hrs
            error!("Max retry reached");
            continue;
        }
        let sink = match SqlSink::new(&config)?.connect(None).await {
            Ok(sink) => sink,
            Err(err) => {
                warn!(
                    "Error connecting to sink: \"{}\", reconnecting in {}.",
                    err,
                    humantime::format_duration(wait)
                );
                async_std::task::sleep(wait).await;
                continue; // loop and retry
            }
        };
        // reset the backoff on successful connect
        return Ok(sink);
    }
}

fn backoff_init() -> Result<ExponentialBackoff> {
    let bmin: u64 = BACKOFF_MIN.as_millis().try_into()?;
    Ok(ExponentialBackoff::from_millis(bmin).max_delay(BACKOFF_MAX))
}
