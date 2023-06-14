use std::str::FromStr;

use anyhow::anyhow;
use fluvio_connector_common::tracing::{debug, error};
use fluvio_model_sql::{Insert as InsertData, Operation, Upsert as UpsertData, Value};
use sqlx::any::{AnyConnectOptions, AnyKind};
use sqlx::database::HasArguments;
use sqlx::{
    Connection, Database, Executor, IntoArguments, PgConnection, Postgres, Sqlite, SqliteConnection,
};

use fluvio_connector_common::tracing::{debug, error};
use fluvio_model_sql::{Operation, Type, Value};

use crate::bind::Bind;
use crate::insert::Insert;
use crate::upsert::Upsert;

pub enum Db {
    Postgres(Box<PgConnection>),
    Sqlite(Box<SqliteConnection>),
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
                Ok(Db::Postgres(conn.into()))
            }
            AnyKind::Sqlite => {
                let options = options
                    .as_sqlite()
                    .ok_or_else(|| anyhow!("invalid sqlite connect options"))?;
                let conn = SqliteConnection::connect_with(options).await?;
                Ok(Db::Sqlite(conn.into()))
            }
        }
    }

    pub async fn execute(&mut self, operation: &Operation) -> anyhow::Result<()> {
        match &operation {
            Operation::Insert(data) => self.insert(data).await,
            Operation::Upsert(data) => self.upsert(data).await,
        }
    }

    async fn insert(&mut self, data: &InsertData) -> anyhow::Result<()> {
        match self {
            Self::Postgres(conn) => {
                do_insert::<Postgres, &mut PgConnection, Self>(
                    conn.as_mut(),
                    &data.table,
                    &data.values,
                )
                .await
            }
            Self::Sqlite(conn) => {
                do_insert::<Sqlite, &mut SqliteConnection, Self>(
                    conn.as_mut(),
                    &data.table,
                    &data.values,
                )
                .await
            }
        }
    }

    async fn upsert(&mut self, data: &UpsertData) -> anyhow::Result<()> {
        match self {
            Self::Postgres(conn) => {
                do_upsert::<Postgres, &mut PgConnection, Self>(
                    conn.as_mut(),
                    &data.table,
                    &data.values,
                    &data.uniq_idx,
                )
                .await
            }
            Self::Sqlite(conn) => {
                do_upsert::<Sqlite, &mut SqliteConnection, Self>(
                    conn.as_mut(),
                    &data.table,
                    &data.values,
                    &data.uniq_idx,
                )
                .await
            }
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Db::Postgres(_) => "postgres",
            Db::Sqlite(_) => "sqlite",
        }
    }

    #[cfg(test)]
    pub fn as_sqlite_conn(&mut self) -> Option<&mut SqliteConnection> {
        match self {
            Self::Sqlite(conn) => Some(conn.as_mut()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn as_postgres_conn(&mut self) -> Option<&mut PgConnection> {
        match self {
            Self::Postgres(conn) => Some(conn.as_mut()),
            _ => None,
        }
    }
}

async fn do_insert<'c, DB, E, I>(conn: E, table: &str, values: &[Value]) -> anyhow::Result<()>
where
    DB: Database,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
    E: Executor<'c, Database = DB>,
    I: Insert<DB> + Bind<DB>,
{
    let sql = I::insert_query(table, values);
    debug!(sql, "sending");
    let mut query = sqlx::query(&sql);
    for value in values {
        query = match I::bind_value(query, value) {
            Ok(q) => q,
            Err(err) => {
                error!("Unable to bind {:?}. Reason: {:?}", value, err);
                return Err(err);
            }
        }
    }
    query.execute(conn).await?;
    Ok(())
}

async fn do_upsert<'c, DB, E, I>(
    conn: E,
    table: &str,
    values: &[Value],
    uniq_idx: &str,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
    E: Executor<'c, Database = DB>,
    I: Upsert<DB> + Bind<DB>,
{
    let sql = I::upsert_query(table, values, uniq_idx);
    debug!(sql, "sending");
    let mut query = sqlx::query(&sql);
    for value in values.iter().chain(values.iter()) {
        query = match I::bind_value(query, value) {
            Ok(q) => q,
            Err(err) => {
                error!("Unable to bind {:?}. Reason: {:?}", value, err);
                return Err(err);
            }
        }
    }
    query.execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bind::NAIVE_DATE_TIME_FORMAT;

    use super::*;
    use chrono::{NaiveDateTime, Utc};
    use fluvio_connector_common::future::init_logger;
    use fluvio_model_sql::Type;
    use rust_decimal::Decimal;
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

    fn make_insert() -> InsertData {
        InsertData {
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

        db.as_postgres_conn().unwrap().execute(CREATE_TABLE).await?;

        let operation = Operation::Insert(make_insert());

        //when
        db.execute(&operation).await?;

        //then
        let row = db.as_postgres_conn().unwrap().fetch_one(SELECT).await?;
        check_row(row.into());

        Ok(())
    }

    #[ignore]
    #[async_std::test]
    async fn test_upsert_postgres() -> anyhow::Result<()> {
        init_logger();

        let url = "postgresql://myusername:mypassword@localhost:5432/myusername";

        let mut db = Db::connect(url).await?;

        db.as_postgres_conn().unwrap().execute(CREATE_TABLE).await?;

        let insert = make_insert();
        let upsert = UpsertData {
            table: insert.table,
            values: insert.values,
            uniq_idx: "uuid_col".into(),
        };
        let operation = Operation::Upsert(upsert.clone());

        // second upsert should do nothing
        for _ in 0..2 {
            db.execute(&operation).await?;

            let row = db.as_postgres_conn().unwrap().fetch_one(SELECT).await?;
            check_row(row.into());
        }

        // update using upsert
        let mut upsert = upsert;
        upsert.values[4].raw_value = "41".into();
        let operation = Operation::Upsert(upsert);

        db.execute(&operation).await?;

        let row = db.as_postgres_conn().unwrap().fetch_one(SELECT).await?;
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

        db.as_sqlite_conn().unwrap().execute(CREATE_TABLE).await?;

        let operation = Operation::Insert(make_insert());

        //when
        db.execute(&operation).await?;

        //then
        let row = db.as_sqlite_conn().unwrap().fetch_one(SELECT).await?;
        check_row(row.into());

        Ok(())
    }

    #[async_std::test]
    async fn test_upsert_sqlite() -> anyhow::Result<()> {
        init_logger();

        let url = "sqlite::memory:";

        let mut db = Db::connect(url).await?;

        db.as_sqlite_conn().unwrap().execute(CREATE_TABLE).await?;

        let insert = make_insert();
        let upsert = UpsertData {
            table: insert.table,
            values: insert.values,
            uniq_idx: "uuid_col".into(),
        };
        let operation = Operation::Upsert(upsert.clone());

        // second upsert should do nothing
        for _ in 0..2 {
            db.execute(&operation).await?;

            let row = db.as_sqlite_conn().unwrap().fetch_one(SELECT).await?;
            check_row(row.into());
        }

        // update using upsert
        let mut upsert = upsert;
        upsert.values[4].raw_value = "41".into();
        let operation = Operation::Upsert(upsert);

        db.execute(&operation).await?;

        let row = db.as_sqlite_conn().unwrap().fetch_one(SELECT).await?;
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
}
