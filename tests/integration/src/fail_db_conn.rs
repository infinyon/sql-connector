use std::time::Duration;

use fluvio_future::timer::sleep;
use log::info;
use once_cell::sync::Lazy;

use crate::utils::{
    self, ctx::TestContext, generate_raw_records, new_config_path, produce_to_fluvio,
};

const TABLE: &str = "test_fail_db_conn";
static CREATE_TABLE: Lazy<String> =
    Lazy::new(|| format!("CREATE TABLE {TABLE} (device_id int, record json)"));

pub(crate) async fn test(ctx: &mut TestContext) {
    // given
    info!("running 'test_fail_db_conn' test");
    let config_path = new_config_path("test_fail_db_conn.yaml").unwrap();
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
    sleep(Duration::from_secs(3)).await;

    // when
    info!("stoping db");
    ctx.stop_postgres().await.unwrap();
    let records = generate_raw_records(TABLE, 4, 6).unwrap();
    produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
        .await
        .unwrap();
    // using 10 seconds of max backoff with factor 1.5 should fail the connector in 20 seconds:
    // 1 1.5s
    // 2 2.25s
    // 3 3.375s
    // 4 5.0625s
    // 5 7.59375s
    // 6 10s
    // total without the max: ~20s

    sleep(Duration::from_secs(10)).await;
    let connector_name = &config.meta.name;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name).unwrap();
    info!("connector: {connector_name}, status: {connector_status:?}");
    assert_eq!(connector_status.unwrap(), "Running");

    sleep(Duration::from_secs(15)).await;
    let connector_status = utils::cdk::cdk_deploy_status(connector_name).unwrap();
    info!("connector: {connector_name}, status: {connector_status:?}");
    assert_eq!(connector_status.unwrap(), "Stopped");

    utils::cdk::cdk_deploy_shutdown(connector_name).unwrap();

    info!("restarting db");
    ctx.start_postgres().await.unwrap();

    info!("test 'test_fail_db_conn' passed");

    // cleanup
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic)
        .await
        .unwrap();
}
