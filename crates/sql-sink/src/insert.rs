use itertools::Itertools;
use sqlx::{Database, Postgres, Sqlite};

use fluvio_model_sql::Value;

use crate::db::Db;

pub trait Insert<DB: Database> {
    fn insert_query(table: &str, values: &[Value]) -> String;
}

impl Insert<Postgres> for Db {
    fn insert_query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|i| format!("${i}")).join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
    }
}

impl Insert<Sqlite> for Db {
    fn insert_query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|_| "?").join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
    }
}
