mod bind;
mod config;
mod db;
mod insert;
mod sink;
mod upsert;

use adaptive_backoff::prelude::{
    Backoff, BackoffBuilder, ExponentialBackoff, ExponentialBackoffBuilder,
};
use anyhow::{anyhow, Result};
use async_std::task::sleep;
use config::SqlConfig;
use futures::{select, FutureExt, SinkExt, StreamExt};

use fluvio_connector_common::{
    connector,
    consumer::ConsumerStream,
    tracing::{error, info, trace, warn},
    LocalBoxSink, Sink,
};
use fluvio_model_sql::Operation;

use sink::SqlSink;

#[connector(sink)]
async fn start(
    config: SqlConfig,
    stream: impl ConsumerStream + futures::stream::FusedStream,
) -> Result<()> {
    match config.batch_size {
        None => start_single(config, stream).await?,
        Some(size) => start_batch(config, stream, size).await?,
    }

    info!("Stream ended, shutting down");

    Ok(())
}

async fn start_single(
    config: SqlConfig,
    mut stream: impl ConsumerStream + futures::stream::FusedStream,
) -> Result<()> {
    let mut backoff = backoff_init(&config)?;
    let mut sink = start_sink(&mut backoff, &config).await?;

    info!("Starting to process items one by one");
    while let Some(item_result) = stream.next().await {
        match item_result {
            Ok(item) => {
                let operation: Operation = match serde_json::from_slice(item.as_ref()) {
                    Ok(op) => {
                        info!("{:?}", &op);
                        op
                    }
                    Err(err) => {
                        error!("Failed to deserialize operation: {}", err);
                        continue;
                    }
                };
                info!(?operation, "Deserialized operation");
                if let Err(err) = process_item(&mut sink, &mut backoff, &config, &operation).await {
                    error!("Error processing item: {}", err);
                }
            }
            Err(err) => {
                error!("Error reading from stream: {}", err);
                break;
            }
        }
    }

    Ok(())
}

async fn start_batch(
    config: SqlConfig,
    mut stream: impl ConsumerStream + futures::stream::FusedStream,
    batch_size: usize,
) -> Result<()> {
    let mut backoff = backoff_init(&config)?;
    let mut sink = start_sink(&mut backoff, &config).await?;
    let mut collected_operation = Operation::UnInit;

    info!(
        "Starting to process items in {} batches and {:?} interval",
        batch_size, config.batch_interval
    );
    loop {
        let do_operation = select! {
            item_result = stream.next() => {
                let Some(item_result) = item_result else {
                    error!("Erred out collecting next item from topic");
                    break;
                };

                match item_result {
                    Ok(item) => {
                        let operation: Operation = match serde_json::from_slice(item.as_ref()) {
                            Ok(op) => op,
                            Err(err) => {
                                error!("Failed to deserialize operation: {}", err);
                                continue;
                            }
                        };

                        trace!(?operation, "Deserialized operation");
                        collected_operation.push(operation);

                        // execute operation if batch_size reached
                        collected_operation.len() >= batch_size
                    }
                    Err(err) => {
                        error!("Error reading from stream: {}", err);
                        break;
                    }
                }
            },
            _ = sleep(config.batch_interval).fuse() => {
               // execute operation if operation is not empty
                !collected_operation.is_empty()
            }
        };

        if do_operation {
            if let Err(err) =
                process_item(&mut sink, &mut backoff, &config, &collected_operation).await
            {
                error!("Error processing item: {}", err);
            }
            collected_operation.clear();
        }
    }

    Ok(())
}

async fn process_item(
    sink: &mut LocalBoxSink<Operation>,
    backoff: &mut ExponentialBackoff,
    config: &SqlConfig,
    operation: &Operation,
) -> Result<()> {
    loop {
        match sink.send(operation.clone()).await {
            Ok(_) => {
                backoff.reset();
                break;
            }
            Err(err) => {
                error!("Error sending operation to sink: {}", err);
                *sink = start_sink(backoff, config).await?;
                backoff_and_wait(backoff, config).await?;
            }
        }
    }

    Ok(())
}

async fn backoff_and_wait(backoff: &mut ExponentialBackoff, config: &SqlConfig) -> Result<()> {
    let wait = backoff.wait();
    if wait < config.backoff_max {
        warn!(
            "Waiting {} before next attempting to db",
            humantime::format_duration(wait)
        );
        async_std::task::sleep(wait).await;
        Ok(())
    } else {
        let err_msg = "Max retry on SQL Execution, shutting down";
        error!(err_msg);
        Err(anyhow!(err_msg))
    }
}

async fn start_sink(
    backoff: &mut ExponentialBackoff,
    config: &SqlConfig,
) -> Result<LocalBoxSink<Operation>> {
    loop {
        match SqlSink::new(config)?.connect(None).await {
            Ok(sink) => {
                // Reset backoff on a successful connection and return the sink
                backoff.reset();
                return Ok(sink);
            }
            Err(err) => {
                error!("Error connecting to sink: \"{}\".", err);
                backoff_and_wait(backoff, config).await?;
            }
        }
    }
}

fn backoff_init(config: &SqlConfig) -> Result<ExponentialBackoff> {
    ExponentialBackoffBuilder::default()
        .factor(1.5)
        .min(config.backoff_min)
        .max(config.backoff_max)
        .build()
}
