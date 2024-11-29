use log::info;
use serde_json::json;

use crate::utils::{
    self, ctx::TestContext, generate_json_records, new_config_path, produce_to_fluvio,
    read_from_postgres,
};

const TABLE: &str = "test_postgres_with_json_sql_transformations";

pub(crate) async fn test_postgres_with_json_sql_transformations(ctx: &mut TestContext) {
    // given
    info!("running 'test_postgres_with_json_sql_transformations' test");
    let config_path = new_config_path("test_postgres_with_json_sql_transformations.yaml").unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {} (device_id int, record json)",
        TABLE
    ))
    .execute(&mut ctx.pg_conn)
    .await
    .unwrap();

    let config = utils::cdk::cdk_deploy_start(&config_path, None)
        .await
        .unwrap();

    let count = 10;
    let records = generate_json_records(count);

    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records.clone())
        .await
        .unwrap();

    // when
    let read_result = read_from_postgres(TABLE, count).await;
    utils::cdk::cdk_deploy_shutdown(&config.meta.name).unwrap();
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic)
        .await
        .unwrap();
    let received_records: Vec<TestRecord> = read_result.unwrap();

    // then
    assert_eq!(received_records.len(), count);
    for (i, record) in received_records.into_iter().enumerate() {
        assert_eq!(record.device_id as usize, i);
        assert_eq!(record.record, json!({"device": { "device_id" : i }}));
    }
    info!("test 'test_postgres_with_json_sql_transformations' passed");
}

#[derive(sqlx::FromRow, Debug)]
struct TestRecord {
    device_id: i32,
    record: serde_json::Value,
}
