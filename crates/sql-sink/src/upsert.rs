use fluvio_model_sql::Value;
use itertools::Itertools;
use sqlx::{Database, Postgres, Sqlite};

use crate::db::Db;

pub trait Upsert<DB: Database> {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String;
}

impl Upsert<Postgres> for Db {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String {
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
}

impl Upsert<Sqlite> for Db {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|_| "?").join(",");
        let set_clause = values.iter().map(|v| format!("{}=?", v.column)).join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause}) ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
    }
}
