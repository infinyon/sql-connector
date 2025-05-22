use serde::Deserialize;
use serde::Serialize;

/// Top-level list of supported operations in the SQL model.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Operation {
    UnInit,
    Insert(Insert),
    Upsert(Upsert),
    BatchInsert(BatchInsert),
    BatchUpsert(BatchUpsert),
}

impl Operation {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self::UnInit => 0,
            Self::Insert(_) => 1,
            Self::Upsert(_) => 1,
            Self::BatchInsert(batch_insert) => batch_insert.values.len(),
            Self::BatchUpsert(batch_upsert) => batch_upsert.values.len(),
        }
    }

    pub fn clear(&mut self) {
        *self = Self::UnInit;
    }

    pub fn push(&mut self, rhs: Operation) {
        match rhs {
            Self::Insert(rhs) => match self {
                Self::UnInit => *self = Self::Insert(rhs),
                Self::Insert(lhs) => {
                    let mut batch_insert: BatchInsert = lhs.clone().into();
                    batch_insert.values.push(rhs.values);
                    *self = Self::BatchInsert(batch_insert);
                }
                Self::BatchInsert(lhs) => lhs.values.push(rhs.values),
                _ => unreachable!("insert followed by an upsert Operation can not happen"),
            },
            Self::Upsert(rhs) => match self {
                Self::UnInit => *self = Self::Upsert(rhs),
                Self::Upsert(lhs) => {
                    let mut batch_upsert: BatchUpsert = lhs.clone().into();
                    batch_upsert.values.push(rhs.values);
                    *self = Self::BatchUpsert(batch_upsert);
                }
                Self::BatchUpsert(lhs) => lhs.values.push(rhs.values),
                _ => unreachable!("upsert followed by an insert Operation can not happen"),
            },
            _ => unreachable!("incoming operation other than Insert or Upsert can not happen"),
        }
    }
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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct BatchInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
}

impl From<Insert> for BatchInsert {
    fn from(value: Insert) -> Self {
        let table = value.table;
        let (columns, values) = value
            .values
            .into_iter()
            .fold((vec![], vec![]), |mut acc, x| {
                acc.0.push(x.column.clone());
                acc.1.push(x);

                acc
            });

        Self {
            table,
            columns,
            values: vec![values],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct BatchUpsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
    pub uniq_idx: String,
}

impl From<Upsert> for BatchUpsert {
    fn from(value: Upsert) -> Self {
        let table = value.table;
        let uniq_idx = value.uniq_idx;
        let (columns, values) = value
            .values
            .into_iter()
            .fold((vec![], vec![]), |mut acc, x| {
                acc.0.push(x.column.clone());
                acc.1.push(x);

                acc
            });

        Self {
            table,
            columns,
            values: vec![values],
            uniq_idx,
        }
    }
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
