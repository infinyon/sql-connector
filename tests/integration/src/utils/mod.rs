pub mod cdk;
pub mod ctx;
pub mod db;
pub mod docker_conn;
pub mod fluvio_conn;
pub mod smdk;

use std::path::PathBuf;

use anyhow::Result;
use sqlx::{postgres::PgRow, FromRow};

use fluvio::{Fluvio, RecordKey};
use fluvio_future::retry::{retry, ExponentialBackoff};
use fluvio_model_sql::{Insert, Operation, Type, Value};

pub fn new_config_path(name: &str) -> Result<PathBuf> {
    let package_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    println!("package_dir: {}", package_dir);
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
    path.push("..");
    path.push("crates");
    path.push("sql-sink");
    Ok(path.canonicalize()?)
}

fn sm_dir() -> Result<PathBuf> {
    let package_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::new();
    path.push(package_dir);
    path.push("..");
    path.push("..");
    path.push("crates");
    path.push("json-sql");
    Ok(path.canonicalize()?)
}

pub fn generate_records(table: &str, count: usize) -> Result<Vec<Insert>> {
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

pub fn generate_raw_records(table: &str, start: usize, end: usize) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(end);
    for i in start..end {
        let op = Insert {
            table: table.to_string(),
            values: vec![
                Value {
                    column: "device_id".to_string(),
                    raw_value: i.to_string(),
                    type_: Type::Int,
                },
                Value {
                    column: "record".to_string(),
                    raw_value: format!("{{\"device\":{{\"device_id\":{i}}}}}"),
                    type_: Type::Json,
                },
            ],
        };
        result.push(op);
    }
    Ok(result
        .iter()
        .map(|op| serde_json::to_string(&Operation::Insert(op.clone())))
        .collect::<serde_json::Result<_>>()?)
}

pub fn generate_json_records(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{{\"device\":{{\"device_id\":{i}}}}}"))
        .collect()
}

pub async fn produce_to_fluvio<V: Into<Vec<u8>> + std::fmt::Debug + Send + Sync + 'static>(
    fluvio: &Fluvio,
    fluvio_topic: &str,
    records: Vec<V>,
) -> Result<()> {
    let producer = fluvio.topic_producer(fluvio_topic).await?;
    for record in records {
        producer.send(RecordKey::NULL, record).await?;
    }
    producer.flush().await?;
    Ok(())
}

pub async fn read_from_postgres<R>(table: &str, count: usize) -> Result<Vec<R>>
where
    R: for<'r> FromRow<'r, PgRow> + Unpin + Send,
{
    let result = retry(ExponentialBackoff::from_millis(10).take(5), || {
        let sql = format!("SELECT * FROM {table} LIMIT {count}");
        async move {
            let mut pg_conn = db::connect_postgres().await?;
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
