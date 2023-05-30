use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{Context, Result};
use async_std::task;
use bollard::{
    container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions},
    image::CreateImageOptions,
    service::{HostConfig, PortBinding},
    Docker,
};
use futures_util::stream::TryStreamExt;
use log::{debug, info};
use serde::Deserialize;
use serde_json::json;
use sqlx::{postgres::PgRow, Connection, FromRow, PgConnection};

use fluvio::{metadata::topic::TopicSpec, Fluvio, RecordKey};
use fluvio_future::retry::{retry, ExponentialBackoff};
use fluvio_model_sql::{Insert, Operation, Type, Upsert, Value};

const POSTGRES_IMAGE: &str = "postgres:15.2";
const POSTGRES_HOST_PORT: &str = "5432";
const POSTGRES_PASSWORD: &str = "passpass";
const POSTGRES_USER: &str = "pguser";
const POSTGRES_DB: &str = POSTGRES_USER;
const JSON_SQL_PACKAGE_NAME: &str = "infinyon/json-sql@0.1.0";

#[async_std::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("preparing environment");
    let docker = connect_docker()
        .await
        .context("unable to connect to docker engine")?;

    let fluvio = connect_fluvio()
        .await
        .context("unable to connect to fluvio cluster")?;

    let mut pg_conn = run_postgres(&docker)
        .await
        .context("unable to run posgres container")?;

    info!("running sql-connector integration tests");

    let result1 = test_postgres_all_data_types(&fluvio, &mut pg_conn)
        .await
        .context("test_postgres_all_data_types failed");

    let result2 = test_postgres_with_json_sql_transformations(&fluvio, &mut pg_conn)
        .await
        .context("test_postgres_with_json_sql_transformations failed");

    let _ = remove_postgres(&docker).await;

    result1?;
    result2?;

    info!("ALL PASSED");

    Ok(())
}

async fn test_postgres_all_data_types(fluvio: &Fluvio, pg_conn: &mut PgConnection) -> Result<()> {
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
    let config_path = new_config_path("test_postgres_all_data_types.yaml")?;
    debug!("{config_path:?}");
    let config: TestConfig = serde_yaml::from_reader(std::fs::File::open(&config_path)?)?;
    let table = "test_postgres_all_data_types";
    sqlx::query(&format!(
        "CREATE TABLE {} (
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
                char_col \"char\" NOT NULL
                )",
        table
    ))
    .execute(&mut *pg_conn)
    .await
    .context(format!("unable to create table {table})"))?;

    cdk_deploy_start(&config_path, None).await?;
    let connector_name = &config.meta.name;
    let connector_status = cdk_deploy_status(connector_name)?;
    info!("connector: {connector_name}, status: {connector_status:?}");

    let count = 10;
    let records = generate_records(table, count)?;

    assert_eq!(records.len(), count);

    {
        produce_to_fluvio(
            fluvio,
            &config.meta.topic,
            records
                .iter()
                .map(|op| serde_json::to_string(&Operation::Insert(op.clone())))
                .collect::<serde_json::Result<_>>()?,
        )
        .await?;
        let mut received_records: Vec<TestRecord> = read_from_postgres(table, count).await?;

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
            .collect::<serde_json::Result<_>>()?;
        produce_to_fluvio(fluvio, &config.meta.topic, records).await?;

        task::sleep(Duration::from_secs(3)).await;

        let mut received_records: Vec<TestRecord> = read_from_postgres(table, count).await?;

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
            .collect::<serde_json::Result<_>>()?;
        produce_to_fluvio(fluvio, &config.meta.topic, records).await?;

        task::sleep(Duration::from_secs(3)).await;

        let mut received_records: Vec<TestRecord> = read_from_postgres(table, count).await?;

        received_records.sort_by_key(|r| r.smallint_col);

        assert_eq!(received_records.len(), count);
        for (i, record) in received_records.into_iter().enumerate() {
            assert_eq!(record.smallint_col as usize, i + 1);
            assert_eq!(record.int_col as usize, i + 2);
            assert_eq!(record.bigint_col as usize, i + 4);
        }
    }

    cdk_deploy_shutdown(connector_name)?;
    remove_topic(fluvio, &config.meta.topic).await?;

    info!("test 'test_postgres_all_data_types' passed");
    Ok(())
}

async fn test_postgres_with_json_sql_transformations(
    fluvio: &Fluvio,
    pg_conn: &mut PgConnection,
) -> Result<()> {
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
    .execute(&mut *pg_conn)
    .await
    .context(format!("unable to create table {table})"))?;

    fluvio_download_smartmodule(JSON_SQL_PACKAGE_NAME)?;
    cdk_deploy_start(&config_path, None).await?;
    let connector_name = &config.meta.name;
    let connector_status = cdk_deploy_status(connector_name)?;
    info!("connector: {connector_name}, status: {connector_status:?}");

    let count = 10;
    let records = generate_json_records(count);

    produce_to_fluvio(fluvio, &config.meta.topic, records.clone()).await?;

    // when
    #[derive(sqlx::FromRow, Debug)]
    struct TestRecord {
        device_id: i32,
        record: serde_json::Value,
    }
    let read_result = read_from_postgres(table, count).await;
    cdk_deploy_shutdown(connector_name)?;
    remove_topic(fluvio, &config.meta.topic).await?;
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

async fn connect_docker() -> Result<Docker> {
    info!("checking docker engine availability");

    let docker = Docker::connect_with_local_defaults()?;
    let version = docker.version().await?;
    info!(
        "connected to docker version: {:?}, api_version: {:?}",
        &version.version, &version.api_version
    );
    Ok(docker)
}

async fn connect_fluvio() -> Result<Fluvio> {
    info!("checking fluvio cluster availability");
    let fluvio = fluvio::Fluvio::connect().await?;
    info!("connected to fluvio version: {}", fluvio.platform_version());
    Ok(fluvio)
}

async fn run_postgres(docker: &Docker) -> Result<PgConnection> {
    info!("starting postgres container");

    let config: Config<String> = Config {
        image: Some(POSTGRES_IMAGE.to_owned()),
        exposed_ports: Some(HashMap::from([(
            POSTGRES_HOST_PORT.to_owned(),
            Default::default(),
        )])),
        host_config: Some(HostConfig {
            port_bindings: Some(HashMap::from([(
                POSTGRES_HOST_PORT.to_owned(),
                Some(vec![PortBinding {
                    host_ip: Some("0.0.0.0".to_owned()),
                    host_port: Some(POSTGRES_HOST_PORT.to_owned()),
                }]),
            )])),
            ..Default::default()
        }),
        env: Some(vec![
            format!("POSTGRES_PASSWORD={POSTGRES_PASSWORD}"),
            format!("POSTGRES_USER={POSTGRES_USER}"),
        ]),
        ..Default::default()
    };
    let _ = &docker
        .create_image(
            Some(CreateImageOptions {
                from_image: POSTGRES_IMAGE,
                ..Default::default()
            }),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await?;

    let _ = &docker
        .create_container(
            Some(CreateContainerOptions {
                name: "postgres",
                platform: None,
            }),
            config,
        )
        .await?;

    let _ = &docker
        .start_container("postgres", None::<StartContainerOptions<String>>)
        .await?;

    info!("postgres container created, waiting for readiness");
    let conn = retry(ExponentialBackoff::from_millis(10).take(6), || {
        connect_postgres()
    })
    .await?;

    info!("postgres container started with {POSTGRES_IMAGE} image");

    Ok(conn)
}

async fn connect_postgres() -> Result<PgConnection> {
    let connection_str = postgres_connection_str();
    debug!("connecting to {connection_str}");
    let mut conn = PgConnection::connect(&connection_str).await?;

    sqlx::query("SELECT count(*) FROM pg_catalog.pg_tables")
        .fetch_one(&mut conn)
        .await?;
    Ok(conn)
}

fn postgres_connection_str() -> String {
    format!(
        "postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{POSTGRES_HOST_PORT}/{POSTGRES_DB}"
    )
}

fn new_config_path(name: &str) -> Result<PathBuf> {
    let package_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::new();
    path.push(package_dir);
    path.push("configs");
    path.push(name);
    Ok(path)
}

fn connector_dir() -> Result<PathBuf> {
    let package_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::new();
    path.push(package_dir);
    path.push("..");
    path.push("sql-sink");
    Ok(path.canonicalize()?)
}

async fn cdk_deploy_start(config_path: &Path, env: Option<(&str, &str)>) -> Result<()> {
    let connector_dir = connector_dir()?;
    info!(
        "deploying connector with config from {config_path:?}, connector_dir: {}",
        connector_dir.to_string_lossy()
    );
    let mut command = Command::new("cdk");
    command.current_dir(&connector_dir);
    command
        .arg("deploy")
        .arg("start")
        .arg("--config")
        .arg(config_path);
    if let Some((env_name, env_value)) = env {
        command.env(env_name, env_value);
    }
    let output = command.output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy start` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    task::sleep(Duration::from_secs(10)).await; // time for connector to start
    Ok(())
}

fn cdk_deploy_shutdown(connector_name: &str) -> Result<()> {
    info!("shutting down connector {connector_name}");
    let output = Command::new("cdk")
        .arg("deploy")
        .arg("shutdown")
        .arg("--name")
        .arg(connector_name)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy shutdown` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    Ok(())
}

fn cdk_deploy_status(connector_name: &str) -> Result<Option<String>> {
    let output = Command::new("cdk").arg("deploy").arg("list").output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy list` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    for line in String::from_utf8_lossy(output.stdout.as_slice())
        .lines()
        .skip(1)
    {
        let mut column_iter = line.split_whitespace();
        match column_iter.next() {
            Some(name) if name.eq(connector_name) => {
                return Ok(column_iter.next().map(|s| s.to_owned()))
            }
            _ => {}
        }
    }
    Ok(None)
}

fn generate_records(table: &str, count: usize) -> Result<Vec<Insert>> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let op = Insert {
            table: table.to_string(),
            values: vec![
                Value {
                    column: "json_col".to_string(),
                    raw_value: "{\"json_key\":\"json_value\"}".to_string(),
                    type_: Type::Json,
                },
                Value {
                    column: "bool_col".to_string(),
                    raw_value: "true".to_string(),
                    type_: Type::Bool,
                },
                Value {
                    column: "smallint_col".to_string(),
                    raw_value: i.to_string(),
                    type_: Type::SmallInt,
                },
                Value {
                    column: "int_col".to_string(),
                    raw_value: i.to_string(),
                    type_: Type::Int,
                },
                Value {
                    column: "bigint_col".to_string(),
                    raw_value: i.to_string(),
                    type_: Type::BigInt,
                },
                Value {
                    column: "text_col".to_string(),
                    raw_value: "some text".to_string(),
                    type_: Type::Text,
                },
                Value {
                    column: "bytes_col".to_string(),
                    raw_value: "some bytes".to_string(),
                    type_: Type::Bytes,
                },
                Value {
                    column: "float_col".to_string(),
                    raw_value: "3.123".to_string(),
                    type_: Type::Float,
                },
                Value {
                    column: "double_col".to_string(),
                    raw_value: "3.333333333".to_string(),
                    type_: Type::DoublePrecision,
                },
                Value {
                    column: "numeric_col".to_string(),
                    raw_value: rust_decimal::Decimal::TEN.to_string(),
                    type_: Type::Numeric,
                },
                Value {
                    column: "timestamp_col".to_string(),
                    raw_value: chrono::Utc::now().naive_local().to_string(),
                    type_: Type::Timestamp,
                },
                Value {
                    column: "date_col".to_string(),
                    raw_value: chrono::Utc::now().naive_local().date().to_string(),
                    type_: Type::Date,
                },
                Value {
                    column: "time_col".to_string(),
                    raw_value: chrono::Utc::now()
                        .naive_local()
                        .time()
                        .format("%H:%M:%S")
                        .to_string(),
                    type_: Type::Time,
                },
                Value {
                    column: "uuid_col".to_string(),
                    raw_value: uuid::Uuid::new_v4().to_string(),
                    type_: Type::Uuid,
                },
                Value {
                    column: "char_col".to_string(),
                    raw_value: "126".to_string(),
                    type_: Type::Char,
                },
            ],
        };
        result.push(op);
    }
    Ok(result)
}

fn generate_json_records(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{{\"device\":{{\"device_id\":{i}}}}}"))
        .collect()
}

async fn produce_to_fluvio<V: Into<Vec<u8>> + std::fmt::Debug + Send + Sync + 'static>(
    fluvio: &Fluvio,
    fluvio_topic: &str,
    records: Vec<V>,
) -> Result<()> {
    let producer = fluvio.topic_producer(fluvio_topic).await?;
    for record in records {
        producer.send(RecordKey::NULL, record).await?;
        producer.flush().await?;
    }
    Ok(())
}

async fn remove_topic(fluvio: &Fluvio, topic: &str) -> Result<()> {
    fluvio.admin().await.delete::<TopicSpec, _>(topic).await?;
    Ok(())
}

async fn read_from_postgres<R>(table: &str, count: usize) -> Result<Vec<R>>
where
    R: for<'r> FromRow<'r, PgRow> + Unpin + Send,
{
    let result = retry(ExponentialBackoff::from_millis(10).take(5), || {
        let sql = format!("SELECT * FROM {table} LIMIT {count}");
        async move {
            let mut pg_conn = connect_postgres().await?;
            let result: Vec<R> = sqlx::query_as(&sql).fetch_all(&mut pg_conn).await?;
            if result.len() != count {
                anyhow::bail!("not all expected records are ready yet");
            }
            Ok::<Vec<R>, anyhow::Error>(result)
        }
    })
    .await?;

    Ok(result)
}

async fn remove_postgres(docker: &Docker) -> Result<()> {
    let _ = &docker
        .remove_container(
            "postgres",
            Some(RemoveContainerOptions {
                v: true,
                force: true,
                ..Default::default()
            }),
        )
        .await?;
    info!("postgres container removed");
    Ok(())
}

fn fluvio_download_smartmodule(smartmodule_name: &str) -> Result<()> {
    info!("downloading {smartmodule_name} to the cluster");
    let output = Command::new("fluvio")
        .arg("hub")
        .arg("download")
        .arg(smartmodule_name)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`fluvio hub download` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
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
