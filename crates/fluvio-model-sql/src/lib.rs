use serde::Deserialize;
use serde::Serialize;

/// Top-level list of supported operations in the SQL model.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Operation {
    Insert(Insert),
    Upsert(Upsert),
}

/// SQL Insert operation
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Insert {
    pub table: String,
    pub values: Vec<Value>,
}

/// SQL Upsert operation
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Upsert {
    pub table: String,
    pub values: Vec<Value>,
    pub uniq_idx: String,
}

/// Value with SQL column name and supported SQL type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Value {
    pub column: String,
    pub raw_value: String,
    #[serde(rename = "type")]
    pub type_: Type,
}

/// Supported SQL data types.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum Type {
    Bool,
    Char,

    SmallInt,
    Int,
    BigInt,

    Float,
    DoublePrecision,

    Text,
    Bytes,

    Numeric,

    Timestamp,
    Date,
    Time,

    Uuid,

    Json,
}
