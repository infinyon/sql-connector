use anyhow::anyhow;
use async_trait::async_trait;
use fluvio_connector_common::tracing::{debug, error};
use fluvio_model_sql::{Insert, Operation, Type, Upsert, Value};
use itertools::Itertools;
use rust_decimal::Decimal;
use sqlx::any::{AnyConnectOptions, AnyKind};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Connection, Executor, PgConnection, Postgres, Sqlite, SqliteConnection};
use std::str::FromStr;

const NAIVE_DATE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

#[async_trait]
trait DbConnection {
    fn kind(&self) -> &'static str;

    async fn insert(&mut self, insert: &Insert) -> anyhow::Result<()>;

    async fn upsert(&mut self, upsert: &Upsert) -> anyhow::Result<()>;

    #[cfg(test)]
    fn as_postgres_conn(&mut self) -> Option<&mut PgConnection> {
        None
    }

    #[cfg(test)]
    fn as_sqlite_conn(&mut self) -> Option<&mut SqliteConnection> {
        None
    }
}

#[async_trait]
impl DbConnection for SqliteConnection {
    fn kind(&self) -> &'static str {
        "sqlite"
    }

    async fn insert(&mut self, insert: &Insert) -> anyhow::Result<()> {
        let query = build_insert_query_sqlite(insert);
        debug!(query, "sending");
        let mut query = sqlx::query(&query);

        for value in insert.values.iter() {
            query = match sqlite_bind(query, value) {
                Ok(q) => q,
                Err(err) => {
                    error!("Unable to bind {:?}. Reason: {:?}", value, err);
                    return Err(err);
                }
            };
        }

        self.execute(query).await?;

        Ok(())
    }

    async fn upsert(&mut self, upsert: &Upsert) -> anyhow::Result<()> {
        let query = build_upsert_query_sqlite(upsert);
        debug!(query, "sending");
        let mut query = sqlx::query(&query);

        for value in upsert.values.iter().chain(upsert.values.iter()) {
            query = match sqlite_bind(query, value) {
                Ok(q) => q,
                Err(err) => {
                    error!("Unable to bind {:?}. Reason: {:?}", value, err);
                    return Err(err);
                }
            };
        }

        self.execute(query).await?;

        Ok(())
    }

    #[cfg(test)]
    fn as_sqlite_conn(&mut self) -> Option<&mut SqliteConnection> {
        Some(self)
    }
}

#[async_trait]
impl DbConnection for PgConnection {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    async fn insert(&mut self, insert: &Insert) -> anyhow::Result<()> {
        let query = build_insert_query_postgres(insert);
        debug!(query, "sending");
        let mut query = sqlx::query(&query);

        for value in insert.values.iter() {
            query = match postgres_bind(query, value) {
                Ok(q) => q,
                Err(err) => {
                    error!("Unable to bind {:?}. Reason: {:?}", value, err);
                    return Err(err);
                }
            };
        }

        self.execute(query).await?;

        Ok(())
    }

    async fn upsert(&mut self, upsert: &Upsert) -> anyhow::Result<()> {
        let query = build_upsert_query_postgres(upsert);
        debug!(query, "sending");
        let mut query = sqlx::query(&query);

        for value in upsert.values.iter().chain(upsert.values.iter()) {
            query = match postgres_bind(query, value) {
                Ok(q) => q,
                Err(err) => {
                    error!("Unable to bind {:?}. Reason: {:?}", value, err);
                    return Err(err);
                }
            };
        }

        self.execute(query).await?;

        Ok(())
    }

    #[cfg(test)]
    fn as_postgres_conn(&mut self) -> Option<&mut PgConnection> {
        Some(self)
    }
}

fn build_insert_query_sqlite(Insert { table, values }: &Insert) -> String {
    let columns = values.iter().map(|v| v.column.as_str()).join(",");
    let values_clause = (1..=values.len()).map(|_| "?").join(",");
    format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
}

fn build_insert_query_postgres(Insert { table, values }: &Insert) -> String {
    let columns = values.iter().map(|v| v.column.as_str()).join(",");
    let values_clause = (1..=values.len()).map(|i| format!("${i}")).join(",");
    format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
}

fn build_upsert_query_sqlite(
    Upsert {
        table,
        values,
        uniq_idx,
    }: &Upsert,
) -> String {
    let columns = values.iter().map(|v| v.column.as_str()).join(",");
    let values_clause = (1..=values.len()).map(|_| "?").join(",");
    let set_clause = values.iter().map(|v| format!("{}=?", v.column)).join(",");
    format!("INSERT INTO {table} ({columns}) VALUES ({values_clause}) ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
}

fn build_upsert_query_postgres(
    Upsert {
        table,
        values,
        uniq_idx,
    }: &Upsert,
) -> String {
    let columns = values.iter().map(|v| v.column.as_str()).join(",");
    let values_clause = (1..=values.len()).map(|i| format!("${i}")).join(",");
    let set_clause = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let idx = i + values.len() + 1;
            format!("{}=${idx}", v.column)
        })
        .join(",");
    format!("INSERT INTO {table} ({columns}) VALUES ({values_clause}) ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
}

fn sqlite_bind<'a>(
    query: Query<'a, Sqlite, SqliteArguments<'a>>,
    value: &Value,
) -> anyhow::Result<Query<'a, Sqlite, SqliteArguments<'a>>> {
    let query = match value.type_ {
        Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
        Type::Char => query.bind(i8::from_str(&value.raw_value)?),
        Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
        Type::Int => query.bind(i32::from_str(&value.raw_value)?),
        Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
        Type::Float => query.bind(f32::from_str(&value.raw_value)?),
        Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
        Type::Text => query.bind(value.raw_value.clone()),
        Type::Bytes => query.bind(value.raw_value.as_bytes().to_vec()),
        Type::Numeric => query.bind(f64::from_str(&value.raw_value)?),
        Type::Timestamp => query.bind(chrono::NaiveDateTime::parse_from_str(
            &value.raw_value,
            NAIVE_DATE_TIME_FORMAT,
        )?),
        Type::Date => query.bind(chrono::NaiveDate::from_str(&value.raw_value)?),
        Type::Time => query.bind(chrono::NaiveTime::from_str(&value.raw_value)?),
        Type::Uuid => query.bind(uuid::Uuid::from_str(&value.raw_value)?),
        Type::Json => query.bind(serde_json::Value::from_str(&value.raw_value)?),
    };
    Ok(query)
}

fn postgres_bind<'a>(
    query: Query<'a, Postgres, PgArguments>,
    value: &Value,
) -> anyhow::Result<Query<'a, Postgres, PgArguments>> {
    let query = match value.type_ {
        Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
        Type::Char => query.bind(i8::from_str(&value.raw_value)?),
        Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
        Type::Int => query.bind(i32::from_str(&value.raw_value)?),
        Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
        Type::Float => query.bind(f32::from_str(&value.raw_value)?),
        Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
        Type::Text => query.bind(value.raw_value.clone()),
        Type::Bytes => query.bind(value.raw_value.as_bytes().to_vec()),
        Type::Numeric => query.bind(Decimal::from_str(&value.raw_value)?),
        Type::Timestamp => query.bind(chrono::NaiveDateTime::parse_from_str(
            &value.raw_value,
            NAIVE_DATE_TIME_FORMAT,
        )?),
        Type::Date => query.bind(chrono::NaiveDate::from_str(&value.raw_value)?),
        Type::Time => query.bind(chrono::NaiveTime::from_str(&value.raw_value)?),
        Type::Uuid => query.bind(uuid::Uuid::from_str(&value.raw_value)?),
        Type::Json => query.bind(serde_json::Value::from_str(&value.raw_value)?),
    };
    Ok(query)
}

pub struct Db {
    connection: Box<dyn DbConnection>,
}

impl Db {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let options = AnyConnectOptions::from_str(url)?;
        match options.kind() {
            AnyKind::Postgres => {
                let options = options
                    .as_postgres()
                    .ok_or_else(|| anyhow!("invalid postgres connect options"))?;
                let conn = PgConnection::connect_with(options).await?;
                Ok(Db {
                    connection: Box::new(conn),
                })
            }
            AnyKind::Sqlite => {
                let options = options
                    .as_sqlite()
                    .ok_or_else(|| anyhow!("invalid sqlite connect options"))?;
                let conn = SqliteConnection::connect_with(options).await?;
                Ok(Db {
                    connection: Box::new(conn),
                })
            }
        }
    }

    pub async fn execute(&mut self, operation: &Operation) -> anyhow::Result<()> {
        match &operation {
            Operation::Insert(insert) => self.insert(insert).await,
            Operation::Upsert(upsert) => self.upsert(upsert).await,
        }
    }

    async fn insert(&mut self, insert: &Insert) -> anyhow::Result<()> {
        self.connection.insert(insert).await
    }

    async fn upsert(&mut self, upsert: &Upsert) -> anyhow::Result<()> {
        self.connection.upsert(upsert).await
    }

    pub fn kind(&self) -> &'static str {
        self.connection.kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDateTime, Utc};
    use fluvio_connector_common::future::init_logger;
    use fluvio_model_sql::Type;
    use sqlx::{any::AnyRow, Executor, Row};
    use uuid::Uuid;

    const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS big_table (
        json_col TEXT,
        bool_col BOOLEAN,
        char_col INTEGER,
        smallint_col INTEGER,
        int_col INTEGER,
        big_int_col BIGINT,
        text_col TEXT,
        bytes_col BLOB,
        float_col REAL,
        double_col REAL,
        numeric_col REAL,
        timestamp_col TIMESTAMP,
        uuid_col TEXT PRIMARY KEY
    );";

    const SELECT: &str = "
    SELECT 
        json_col,
        bool_col,
        char_col,
        smallint_col,
        int_col,
        big_int_col,
        text_col,
        bytes_col,
        float_col,
        double_col,
        numeric_col,
        timestamp_col,
        uuid_col
    FROM big_table;
    ";

    fn make_insert() -> Insert {
        Insert {
            table: "big_table".to_string(),
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
                    column: "char_col".to_string(),
                    raw_value: "126".to_string(),
                    type_: Type::Char,
                },
                Value {
                    column: "smallint_col".to_string(),
                    raw_value: "12".to_string(),
                    type_: Type::SmallInt,
                },
                Value {
                    column: "int_col".to_string(),
                    raw_value: "40".to_string(),
                    type_: Type::Int,
                },
                Value {
                    column: "big_int_col".to_string(),
                    raw_value: "401".to_string(),
                    type_: Type::BigInt,
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
                    column: "numeric_col".to_string(),
                    raw_value: Decimal::TEN.to_string(),
                    type_: Type::Numeric,
                },
                Value {
                    column: "timestamp_col".to_string(),
                    raw_value: chrono::NaiveDateTime::MIN.to_string(),
                    type_: Type::Timestamp,
                },
                Value {
                    column: "uuid_col".to_string(),
                    raw_value: Uuid::from_str("f0841d15-133a-48a7-b48c-ce1ba72f8c94")
                        .unwrap()
                        .to_string(),
                    type_: Type::Uuid,
                },
            ],
        }
    }

    fn check_row(row: AnyRow) {
        let json_col: String = row.get(0);
        assert_eq!(json_col, "{\"json_key\":\"json_value\"}".to_string());
        let bool_col: bool = row.get(1);
        assert!(bool_col);
        let char_col: i32 = row.get(2);
        assert_eq!(char_col, 126);
        let small_int_col: i32 = row.get(3);
        assert_eq!(small_int_col, 12);
        let int_col: i32 = row.get(4);
        assert_eq!(int_col, 40);
        let big_int_col: i64 = row.get(5);
        assert_eq!(big_int_col, 401);
        let text_col: String = row.get(6);
        assert_eq!(text_col, "some text");
        let bytes_col: Vec<u8> = row.get(7);
        assert_eq!(bytes_col, b"some bytes");
        let float: f32 = row.get(8);
        assert_eq!(float, 3.123);
        let double: f64 = row.get(9);
        assert_eq!(double, 3.333333333);
        let numeric: f64 = row.get(10);
        assert_eq!(numeric, 10f64);
        let timestamp: NaiveDateTime = row.get(11);
        assert_eq!(timestamp, chrono::NaiveDateTime::MIN);
        let uuid: Vec<u8> = row.get(12);
        let uuid = Uuid::from_slice(&uuid).unwrap();
        assert_eq!(
            uuid,
            Uuid::from_str("f0841d15-133a-48a7-b48c-ce1ba72f8c94").unwrap()
        );
    }

    #[ignore]
    #[async_std::test]
    async fn test_insert_postgres() -> anyhow::Result<()> {
        init_logger();

        let url = "postgresql://myusername:mypassword@localhost:5432/myusername";

        //given
        let mut db = Db::connect(url).await?;

        db.connection
            .as_postgres_conn()
            .unwrap()
            .execute(CREATE_TABLE)
            .await?;

        let operation = Operation::Insert(make_insert());

        //when
        db.execute(&operation).await?;

        //then
        let row = db
            .connection
            .as_postgres_conn()
            .unwrap()
            .fetch_one(SELECT)
            .await?;
        check_row(row.into());

        Ok(())
    }

    #[ignore]
    #[async_std::test]
    async fn test_upsert_postgres() -> anyhow::Result<()> {
        init_logger();

        let url = "postgresql://myusername:mypassword@localhost:5432/myusername";

        let mut db = Db::connect(url).await?;

        db.connection
            .as_postgres_conn()
            .unwrap()
            .execute(CREATE_TABLE)
            .await?;

        let insert = make_insert();
        let upsert = Upsert {
            table: insert.table,
            values: insert.values,
            uniq_idx: "uuid_col".into(),
        };
        let operation = Operation::Upsert(upsert.clone());

        // second upsert should do nothing
        for _ in 0..2 {
            db.execute(&operation).await?;

            let row = db
                .connection
                .as_postgres_conn()
                .unwrap()
                .fetch_one(SELECT)
                .await?;
            check_row(row.into());
        }

        // update using upsert
        let mut upsert = upsert;
        upsert.values[4].raw_value = "41".into();
        let operation = Operation::Upsert(upsert);

        db.execute(&operation).await?;

        let row = db
            .connection
            .as_postgres_conn()
            .unwrap()
            .fetch_one(SELECT)
            .await?;
        let int_col: i32 = row.get(4);
        assert_eq!(int_col, 41);

        Ok(())
    }

    #[async_std::test]
    async fn test_insert_sqlite() -> anyhow::Result<()> {
        init_logger();

        let url = "sqlite::memory:";

        //given
        let mut db = Db::connect(url).await?;

        db.connection
            .as_sqlite_conn()
            .unwrap()
            .execute(CREATE_TABLE)
            .await?;

        let operation = Operation::Insert(make_insert());

        //when
        db.execute(&operation).await?;

        //then
        let row = db
            .connection
            .as_sqlite_conn()
            .unwrap()
            .fetch_one(SELECT)
            .await?;
        check_row(row.into());

        Ok(())
    }

    #[async_std::test]
    async fn test_upsert_sqlite() -> anyhow::Result<()> {
        init_logger();

        let url = "sqlite::memory:";

        let mut db = Db::connect(url).await?;

        db.connection
            .as_sqlite_conn()
            .unwrap()
            .execute(CREATE_TABLE)
            .await?;

        let insert = make_insert();
        let upsert = Upsert {
            table: insert.table,
            values: insert.values,
            uniq_idx: "uuid_col".into(),
        };
        let operation = Operation::Upsert(upsert.clone());

        // second upsert should do nothing
        for _ in 0..2 {
            db.execute(&operation).await?;

            let row = db
                .connection
                .as_sqlite_conn()
                .unwrap()
                .fetch_one(SELECT)
                .await?;
            check_row(row.into());
        }

        // update using upsert
        let mut upsert = upsert;
        upsert.values[4].raw_value = "41".into();
        let operation = Operation::Upsert(upsert);

        db.execute(&operation).await?;

        let row = db
            .connection
            .as_sqlite_conn()
            .unwrap()
            .fetch_one(SELECT)
            .await?;
        let int_col: i32 = row.get(4);
        assert_eq!(int_col, 41);

        Ok(())
    }

    #[test]
    fn test_date_time_format() {
        //given
        let timestamp = Utc::now().naive_local();
        let raw = timestamp.to_string();

        //when
        let parsed = NaiveDateTime::parse_from_str(&raw, NAIVE_DATE_TIME_FORMAT).unwrap();

        //then
        assert_eq!(timestamp, parsed);
    }

    #[test]
    fn test_insert_query_postgres() {
        //given
        let table = "test_table".to_owned();
        let values = vec![
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = build_insert_query_postgres(&Insert { table, values });

        //then
        assert_eq!(query, "INSERT INTO test_table (col1,col2) VALUES ($1,$2)");
    }

    #[test]
    fn test_upsert_query_postgres() {
        //given
        let table = "test_table".to_owned();
        let values = vec![
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = build_upsert_query_postgres(&Upsert {
            table,
            values,
            uniq_idx: "my_idx".into(),
        });

        //then
        assert_eq!(
            query,
            "INSERT INTO test_table (col1,col2) VALUES ($1,$2) ON CONFLICT(my_idx) DO UPDATE SET col1=$3,col2=$4"
        );
    }

    #[test]
    fn test_insert_query_sqlite() {
        //given
        let table = "test_table".to_owned();
        let values = vec![
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = build_insert_query_sqlite(&Insert { table, values });

        //then
        assert_eq!(query, "INSERT INTO test_table (col1,col2) VALUES (?,?)");
    }

    #[test]
    fn test_upsert_query_sqlite() {
        //given
        let table = "test_table".to_owned();
        let values = vec![
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = build_upsert_query_sqlite(&Upsert {
            table,
            values,
            uniq_idx: "my_idx".into(),
        });

        //then
        assert_eq!(
            query,
            "INSERT INTO test_table (col1,col2) VALUES (?,?) ON CONFLICT(my_idx) DO UPDATE SET col1=?,col2=?"
        );
    }
}
