use std::time::Duration;

use fluvio_future::timer::sleep;
use log::info;
use once_cell::sync::Lazy;
use serde_json::json;

use crate::utils::{
    self, ctx::TestContext, generate_raw_records, new_config_path, produce_to_fluvio,
    read_from_postgres,
};

const TABLE: &str = "test_postgres_consumer_offsets";
static CREATE_TABLE: Lazy<String> =
    Lazy::new(|| format!("CREATE TABLE {TABLE} (device_id int, record json)"));

pub(crate) async fn test_postgres_consumer_offsets(ctx: &mut TestContext) {
    // given
    info!("running 'test_postgres_consumer_offsets' test");
    let config_path = new_config_path("test_postgres_consumer_offsets.yaml").unwrap();
    sqlx::query(&CREATE_TABLE)
        .execute(&mut (ctx.pg_conn))
        .await
        .unwrap();

    let config = utils::cdk::cdk_deploy_start(&config_path, None)
        .await
        .unwrap();
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name).unwrap();
    info!("connector: {connector_name}, status: {connector_status:?}");

    sleep(Duration::from_secs(3)).await;
    let records = generate_raw_records(TABLE, 0, 2).unwrap();
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
        .await
        .unwrap();
    let records = generate_raw_records(TABLE, 2, 4).unwrap();
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
        .await
        .unwrap();
    info!("waiting for connector to catch up");
    sleep(Duration::from_secs(3)).await;

    // when
    info!("shutting down connector");
    utils::cdk::cdk_deploy_shutdown(connector_name).unwrap();

    info!("producing more records with connector down");
    sleep(Duration::from_secs(3)).await;
    let records = generate_raw_records(TABLE, 4, 6).unwrap();
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
        .await
        .unwrap();

    info!("restarting connector");
    utils::cdk::cdk_deploy_start(&config_path, None)
        .await
        .unwrap();
    let records = generate_raw_records(TABLE, 6, 8).unwrap();
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
        .await
        .unwrap();
    sleep(Duration::from_secs(3)).await;
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name).unwrap();
    info!("connector: {connector_name}, status: {connector_status:?}");

    utils::cdk::cdk_deploy_shutdown(connector_name).unwrap();
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic)
        .await
        .unwrap();
    sleep(Duration::from_secs(3)).await;

    let read_result = read_from_postgres(TABLE, 8).await;
    let received_records: Vec<TestRecord> = read_result.unwrap();

    // then
    assert_eq!(received_records.len(), 8);
    for (i, record) in received_records.into_iter().enumerate() {
        assert_eq!(record.device_id as usize, i);
        assert_eq!(record.record, json!({"device": { "device_id" : i }}));
    }

    let consumer = ctx
        .fluvio
        .consumer_offsets()
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.consumer_id.eq("test-postgres-consumer-offsets"));

    // then
    assert!(consumer.is_some());
    assert!(consumer.unwrap().offset >= 0);
    info!("test 'test_postgres_consumer_offsets' passed");
}

#[derive(sqlx::FromRow, Debug)]
struct TestRecord {
    device_id: i32,
    record: serde_json::Value,
}
