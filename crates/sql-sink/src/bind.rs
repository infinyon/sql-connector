use std::str::FromStr;

use rust_decimal::Decimal;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Database, Postgres, Sqlite};

use fluvio_model_sql::{Type, Value};

use crate::db::Db;

pub const NAIVE_DATE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

pub trait Bind<DB: Database> {
    fn bind_value<'a>(
        query: Query<'a, DB, <DB>::Arguments<'a>>,
        value: &'a Value,
    ) -> anyhow::Result<Query<'a, DB, <DB>::Arguments<'a>>>;
}

impl Bind<Postgres> for Db {
    fn bind_value<'a>(
        query: Query<'a, Postgres, PgArguments>,
        value: &'a Value,
    ) -> anyhow::Result<Query<'a, Postgres, PgArguments>> {
        let query = match value.type_ {
            Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
            Type::Char => query.bind(i8::from_str(&value.raw_value)?),
            Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
            Type::Int => query.bind(i32::from_str(&value.raw_value)?),
            Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
            Type::Float => query.bind(f32::from_str(&value.raw_value)?),
            Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
            Type::Text => query.bind(value.raw_value.as_str()),
            Type::Bytes => query.bind(value.raw_value.as_bytes()),
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
}

impl Bind<Sqlite> for Db {
    fn bind_value<'a>(
        query: Query<'a, Sqlite, SqliteArguments<'a>>,
        value: &'a Value,
    ) -> anyhow::Result<Query<'a, Sqlite, SqliteArguments<'a>>> {
        let query = match value.type_ {
            Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
            Type::Char => query.bind(i8::from_str(&value.raw_value)?),
            Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
            Type::Int => query.bind(i32::from_str(&value.raw_value)?),
            Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
            Type::Float => query.bind(f32::from_str(&value.raw_value)?),
            Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
            Type::Text => query.bind(value.raw_value.as_str()),
            Type::Bytes => query.bind(value.raw_value.as_bytes()),
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
}
