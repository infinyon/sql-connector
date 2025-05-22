use fluvio_model_sql::Value;
use itertools::Itertools;
use sqlx::{Database, Postgres, Sqlite};

use crate::db::Db;

pub trait Upsert<DB: Database> {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String;
    fn upsert_batch_query(
        table: &str,
        columns: &[String],
        value_sets: &[Vec<Value>],
        uniq_idx: &str,
    ) -> String;
}

impl Upsert<Postgres> for Db {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|i| format!("${i}")).join(",");
        let set_clause = values
            .iter()
            .map(|v| format!("{}=EXCLUDED.{}", v.column, v.column))
            .join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause}) ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
    }

    fn upsert_batch_query(
        table: &str,
        columns: &[String],
        value_sets: &[Vec<Value>],
        uniq_idx: &str,
    ) -> String {
        let set_clause = columns
            .iter()
            .map(|col| format!("{}=EXCLUDED.{}", col, col))
            .join(",");
        let columns = columns.iter().join(",");
        let mut values = Vec::with_capacity(value_sets.len());
        for (idx, value_set) in value_sets.iter().enumerate() {
            values.push(format!(
                "({})",
                ((1 + idx * value_set.len())..=(idx * value_set.len() + value_set.len()))
                    .map(|i| format!("${i}"))
                    .join(",")
            ));
        }
        let values_clause = values.into_iter().join(",");

        format!("INSERT INTO {table} ({columns}) VALUES {values_clause} ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
    }
}

impl Upsert<Sqlite> for Db {
    fn upsert_query(table: &str, values: &[Value], uniq_idx: &str) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|_| "?").join(",");
        let set_clause = values
            .iter()
            .map(|v| format!("{}=excluded.{}", v.column, v.column))
            .join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause}) ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
    }

    fn upsert_batch_query(
        table: &str,
        columns: &[String],
        value_sets: &[Vec<Value>],
        uniq_idx: &str,
    ) -> String {
        let set_clause = columns
            .iter()
            .map(|col| format!("{}=excluded.{}", col, col))
            .join(",");
        let columns = columns.iter().join(",");
        let mut values = Vec::with_capacity(value_sets.len());
        for (idx, value_set) in value_sets.iter().enumerate() {
            values.push(format!(
                "({})",
                ((1 + idx * value_set.len())..=(idx * value_set.len() + value_set.len()))
                    .map(|_| "?")
                    .join(",")
            ));
        }
        let values_clause = values.into_iter().join(",");

        format!("INSERT INTO {table} ({columns}) VALUES {values_clause} ON CONFLICT({uniq_idx}) DO UPDATE SET {set_clause}")
    }
}
