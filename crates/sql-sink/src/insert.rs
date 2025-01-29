use itertools::Itertools;
use sqlx::{Database, Postgres, Sqlite};

use fluvio_model_sql::Value;

use crate::db::Db;

pub trait Insert<DB: Database> {
    fn insert_query(table: &str, values: &[Value]) -> String;
    fn insert_batch_query(table: &str, columns: &[String], value_sets: &[Vec<Value>]) -> String;
}

impl Insert<Postgres> for Db {
    fn insert_query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|i| format!("${i}")).join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
    }

    fn insert_batch_query(table: &str, columns: &[String], value_sets: &[Vec<Value>]) -> String {
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
        format!("INSERT INTO {table} ({columns}) VALUES {values_clause}")
    }
}

impl Insert<Sqlite> for Db {
    fn insert_query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|_| "?").join(",");
        format!("INSERT INTO {table} ({columns}) VALUES ({values_clause})")
    }

    fn insert_batch_query(table: &str, columns: &[String], value_sets: &[Vec<Value>]) -> String {
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
        format!("INSERT INTO {table} ({columns}) VALUES {values_clause}")
    }
}
