use std::time::Duration;

use anyhow::{Context, Result};
use fluvio_future::timer::sleep;
use log::{debug, info};
use serde::Deserialize;
use serde_json::json;

use crate::utils::{
    self, ctx::TestContext, generate_raw_records, new_config_path, produce_to_fluvio,
    read_from_postgres,
};

pub async fn test_postgres_consumer_offsets(ctx: &mut TestContext) -> Result<()> {
    // given
    info!("running 'test_postgres_consumer_offsets' test");
    let config_path = new_config_path("test_postgres_consumer_offsets.yaml")?;
    debug!("{config_path:?}");
    let config: TestConfig = serde_yaml::from_reader(std::fs::File::open(&config_path)?)?;
    let table = "test_postgres_consumer_offsets";
    sqlx::query(&format!(
        "CREATE TABLE {} (device_id int, record json)",
        table
    ))
    .execute(&mut (ctx.pg_conn))
    .await
    .context(format!("unable to create table {table})"))?;

    utils::cdk::cdk_deploy_start(&config_path, None).await?;
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name)?;
    info!("connector: {connector_name}, status: {connector_status:?}");

    sleep(Duration::from_secs(3)).await;
    let records = generate_raw_records(table, 0, 2)?;
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records).await?;
    let records = generate_raw_records(table, 2, 4)?;
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records).await?;
    info!("waiting for connector to catch up");
    sleep(Duration::from_secs(3)).await;

    // when
    info!("shutting down connector");
    utils::cdk::cdk_deploy_shutdown(connector_name)?;

    info!("producing more records with connector down");
    sleep(Duration::from_secs(3)).await;
    let records = generate_raw_records(table, 4, 6)?;
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records).await?;

    info!("restarting connector");
    utils::cdk::cdk_deploy_start(&config_path, None).await?;
    let records = generate_raw_records(table, 6, 8)?;
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records).await?;
    sleep(Duration::from_secs(3)).await;
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name)?;
    info!("connector: {connector_name}, status: {connector_status:?}");

    let read_result = read_from_postgres(table, 8).await;
    let received_records: Vec<TestRecord> = read_result?;

    // then
    assert_eq!(received_records.len(), 8);
    for (i, record) in received_records.into_iter().enumerate() {
        assert_eq!(record.device_id as usize, i);
        assert_eq!(record.record, json!({"device": { "device_id" : i }}));
    }

    let consumer = ctx
        .fluvio
        .consumer_offsets()
        .await?
        .into_iter()
        .find(|c| c.consumer_id.eq("test-postgres-consumer-offsets"));

    utils::cdk::cdk_deploy_shutdown(connector_name)?;
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic).await?;

    // then
    assert!(consumer.is_some());
    assert!(consumer.unwrap().offset >= 0);
    info!("test 'test_postgres_consumer_offsets' passed");
    Ok(())
}

#[derive(Debug, Deserialize)]
struct MetaConfig {
    name: String,
    topic: String,
}

#[derive(Debug, Deserialize)]
struct TestConfig {
    meta: MetaConfig,
}

#[derive(sqlx::FromRow, Debug)]
struct TestRecord {
    device_id: i32,
    record: serde_json::Value,
}
