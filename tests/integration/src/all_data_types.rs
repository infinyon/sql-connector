use std::time::Duration;

use async_std::task;
use fluvio_model_sql::{Operation, Upsert};
use log::info;
use once_cell::sync::Lazy;

use crate::utils::{
    self, ctx::TestContext, generate_records, new_config_path, produce_to_fluvio,
    read_from_postgres,
};

const TABLE: &str = "test_postgres_all_data_types";
static CREATE_TABLE: Lazy<String> = Lazy::new(|| {
    format!(
        r#"
        CREATE TABLE {TABLE} (
            bool_col bool NOT NULL,
            smallint_col smallint NOT NULL,
            int_col int NOT NULL,
            bigint_col bigint NOT NULL,
            float_col float4 NOT NULL,
            double_col float8 NOT NULL,
            text_col varchar NOT NULL,
            bytes_col bytea NOT NULL,
            numeric_col numeric NOT NULL,
            timestamp_col timestamp NOT NULL,
            date_col date NOT NULL,
            time_col time NOT NULL,
            uuid_col uuid NOT NULL PRIMARY KEY,
            json_col json NOT NULL,
            char_col "char" NOT NULL
        )
        "#
    )
});

pub(crate) async fn test_postgres_all_data_types(ctx: &mut TestContext) {
    #[derive(sqlx::FromRow, Debug)]
    #[allow(dead_code)]
    struct TestRecord {
        bool_col: bool,
        smallint_col: i16,
        int_col: i32,
        bigint_col: i64,
        float_col: f32,
        double_col: f64,
        text_col: String,
        bytes_col: Vec<u8>,
        numeric_col: rust_decimal::Decimal,
        timestamp_col: chrono::NaiveDateTime,
        date_col: chrono::NaiveDate,
        time_col: chrono::NaiveTime,
        uuid_col: uuid::Uuid,
        json_col: serde_json::Value,
        char_col: i8,
    }

    info!("running 'test_postgres_all_data_types' test");
    let config_path = new_config_path("test_postgres_all_data_types.yaml").unwrap();
    sqlx::query(&CREATE_TABLE)
        .execute(&mut ctx.pg_conn)
        .await
        .unwrap();

    let config = utils::cdk::cdk_deploy_start(&config_path, None)
        .await
        .unwrap();

    let count = 10;
    let records = generate_records(TABLE, count).unwrap();

    assert_eq!(records.len(), count);

    {
        produce_to_fluvio(
            &ctx.fluvio,
            &config.meta.topic,
            records
                .iter()
                .map(|op| serde_json::to_string(&Operation::Insert(op.clone())))
                .collect::<serde_json::Result<_>>()
                .unwrap(),
        )
        .await
        .unwrap();
        let mut received_records: Vec<TestRecord> = read_from_postgres(TABLE, count).await.unwrap();

        received_records.sort_by_key(|r| r.smallint_col);

        assert_eq!(received_records.len(), count);
        for (i, record) in received_records.into_iter().enumerate() {
            assert_eq!(record.int_col as usize, i);
            assert_eq!(record.smallint_col as usize, i);
            assert_eq!(record.bigint_col as usize, i);
        }
    }

    // first upsert should do nothing
    {
        let records = records
            .iter()
            .map(|op| {
                let op = Upsert {
                    table: op.table.clone(),
                    values: op.values.clone(),
                    uniq_idx: "uuid_col".into(),
                };
                serde_json::to_string(&Operation::Upsert(op))
            })
            .collect::<serde_json::Result<_>>()
            .unwrap();
        produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
            .await
            .unwrap();

        task::sleep(Duration::from_secs(3)).await;

        let mut received_records: Vec<TestRecord> = read_from_postgres(TABLE, count).await.unwrap();

        received_records.sort_by_key(|r| r.smallint_col);

        assert_eq!(received_records.len(), count);
        for (i, record) in received_records.into_iter().enumerate() {
            assert_eq!(record.int_col as usize, i);
            assert_eq!(record.smallint_col as usize, i);
            assert_eq!(record.bigint_col as usize, i);
        }
    }

    // second upsert should do update
    {
        let records = records
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let mut op = Upsert {
                    table: op.table.clone(),
                    values: op.values.clone(),
                    uniq_idx: "uuid_col".into(),
                };
                op.values[2].raw_value = (i + 1).to_string();
                op.values[3].raw_value = (i + 2).to_string();
                op.values[4].raw_value = (i + 4).to_string();
                serde_json::to_string(&Operation::Upsert(op))
            })
            .collect::<serde_json::Result<_>>()
            .unwrap();
        produce_to_fluvio(&ctx.fluvio, &config.meta.topic, records)
            .await
            .unwrap();

        task::sleep(Duration::from_secs(3)).await;

        let mut received_records: Vec<TestRecord> = read_from_postgres(TABLE, count).await.unwrap();

        received_records.sort_by_key(|r| r.smallint_col);

        assert_eq!(received_records.len(), count);
        for (i, record) in received_records.into_iter().enumerate() {
            assert_eq!(record.smallint_col as usize, i + 1);
            assert_eq!(record.int_col as usize, i + 2);
            assert_eq!(record.bigint_col as usize, i + 4);
        }
    }

    utils::cdk::cdk_deploy_shutdown(&config.meta.name).unwrap();
    utils::fluvio_conn::remove_topic(&ctx.fluvio, &config.meta.topic)
        .await
        .unwrap();

    info!("test 'test_postgres_all_data_types' passed");
}
