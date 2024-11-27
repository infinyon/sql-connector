use anyhow::{Context, Result};
use log::{debug, info};
use serde::Deserialize;
use serde_json::json;

use crate::utils::{
    self, ctx::TestContext, generate_json_records, new_config_path, produce_to_fluvio,
    read_from_postgres,
};

pub async fn test_postgres_with_json_sql_transformations(ctx: &mut TestContext) -> Result<()> {
    // given
    info!("running 'test_postgres_with_json_sql_transformations' test");
    let config_path = new_config_path("test_postgres_with_json_sql_transformations.yaml")?;
    debug!("{config_path:?}");
    let config: TestConfig = serde_yaml::from_reader(std::fs::File::open(&config_path)?)?;
    let table = "test_postgres_with_json_sql_transformations";
    sqlx::query(&format!(
        "CREATE TABLE {} (device_id int, record json)",
        table
    ))
    .execute(&mut ctx.pg_conn)
    .await
    .context(format!("unable to create table {table})"))?;

    utils::cdk::cdk_deploy_start(&config_path, None).await?;
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name)?;
    info!("connector: {connector_name}, status: {connector_status:?}");

    let count = 10;
    let records = generate_json_records(count);

    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records.clone()).await?;

    // when
    let read_result = read_from_postgres(table, count).await;
    utils::cdk::cdk_deploy_shutdown(connector_name)?;
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic).await?;
    let received_records: Vec<TestRecord> = read_result?;

    // then
    assert_eq!(received_records.len(), count);
    for (i, record) in received_records.into_iter().enumerate() {
        assert_eq!(record.device_id as usize, i);
        assert_eq!(record.record, json!({"device": { "device_id" : i }}));
    }
    info!("test 'test_postgres_with_json_sql_transformations' passed");
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
