use crate::mapping::{Mapping, Operation as MappingOperation};
use crate::pointer::pointer;
use eyre::eyre;
use fluvio_model_sql::{Insert, Operation, Type, Upsert, Value};
use fluvio_smartmodule::Result;

pub(crate) fn transform(record: serde_json::Value, mapping: &Mapping) -> Result<Operation> {
    let mut values = Vec::with_capacity(mapping.columns.len());
    for (name, column) in mapping.columns.iter() {
        let raw_value = match pointer(&record, column.json_key.as_str()) {
            None => match &column.value.default {
                None => {
                    if column.value.required {
                        return Err(eyre!("Missing required field: {}", column.json_key));
                    }
                    String::new()
                }
                Some(default) => default.clone(),
            },
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(found) => serde_json::to_string(found)?,
        };
        values.push(Value {
            column: name.clone(),
            raw_value,
            type_: Type::from(column.value.type_),
        });
    }

    let op = match mapping.operation {
        MappingOperation::Insert => Operation::Insert(Insert {
            table: mapping.table.clone(),
            values,
        }),
        MappingOperation::Upsert => {
            if mapping.unique_columns.is_empty() {
                return Err(eyre!("unique-columns can't be empty when doing upsert"));
            }

            Operation::Upsert(Upsert {
                table: mapping.table.clone(),
                values,
                uniq_idx: mapping.unique_columns.join(","),
            })
        }
    };

    Ok(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_transform_upsert_multiple_unique_columns() {
        // given
        let input = json!({
            "key": "value",
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "operation": "upsert",
            "unique-columns": ["my_col", "my_second_col"],
            "map-columns": {
                "body" : {
                    "json-key": "$",
                    "value": {
                        "type": "json"
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let operation = transform(input, &mapping).expect("transformation succeeded");

        // then
        assert_eq!(
            operation,
            Operation::Upsert(Upsert {
                table: "test_table".to_string(),
                uniq_idx: "my_col,my_second_col".into(),
                values: vec![Value {
                    column: "body".to_string(),
                    raw_value: "{\"key\":\"value\"}".to_string(),
                    type_: Type::Json
                }]
            })
        );
    }

    #[test]
    fn test_transform_upsert_single_unique_column() {
        // given
        let input = json!({
            "key": "value"
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "operation": "upsert",
            "unique-columns": ["my_col"],
            "map-columns": {
                "body" : {
                    "json-key": "$",
                    "value": {
                        "type": "json"
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let operation = transform(input, &mapping).expect("transformation succeeded");

        // then
        assert_eq!(
            operation,
            Operation::Upsert(Upsert {
                table: "test_table".to_string(),
                uniq_idx: "my_col".into(),
                values: vec![Value {
                    column: "body".to_string(),
                    raw_value: "{\"key\":\"value\"}".to_string(),
                    type_: Type::Json
                }]
            })
        );
    }

    #[test]
    fn test_pass_whole_object() {
        // given
        let input = json!({
            "key": "value"
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "map-columns": {
                "body" : {
                    "json-key": "$",
                    "value": {
                        "type": "json"
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let operation = transform(input, &mapping).expect("transformation succeeded");

        // then
        assert_eq!(
            operation,
            Operation::Insert(Insert {
                table: "test_table".to_string(),
                values: vec![Value {
                    column: "body".to_string(),
                    raw_value: "{\"key\":\"value\"}".to_string(),
                    type_: Type::Json
                }]
            })
        );
    }

    #[test]
    fn test_required_field_missed() {
        // given
        let input = json!({
            "key": "value"
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "map-columns": {
                "body" : {
                    "json-key": ".wrong_key",
                    "value": {
                        "type": "text",
                        "required": true
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let res = transform(input, &mapping);

        // then
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Missing required field: .wrong_key".to_string()
        );
    }

    #[test]
    fn test_default_value() {
        // given
        let input = json!({
            "key": "value"
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "map-columns": {
                "body" : {
                    "json-key": ".wrong_key",
                    "value": {
                        "type": "text",
                        "default": "some_value",
                        "required": true
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let operation = transform(input, &mapping).expect("transformation succeeded");

        // then
        assert_eq!(
            operation,
            Operation::Insert(Insert {
                table: "test_table".to_string(),
                values: vec![Value {
                    column: "body".to_string(),
                    raw_value: "some_value".to_string(),
                    type_: Type::Text
                }]
            })
        );
    }

    #[test]
    fn test_value_two_columns() {
        // given
        let input = json!({
            "key": {
                "inner": "value"
            }
        });

        let mapping: Mapping = serde_json::from_value(json!({
            "table" : "test_table",
            "map-columns": {
                "a_col1" : {
                    "json-key": ".key.inner",
                    "value": {
                        "type": "text"
                    }
                },
                "b_col2" : {
                    "json-key": ".key.inner",
                    "value": {
                        "type": "text"
                    }
                }
            }
        }))
        .expect("valid mapping");

        // when
        let operation = transform(input, &mapping).expect("transformation succeeded");

        // then
        assert!(
            matches!(operation, Operation::Insert(Insert{ table, values }) if table.eq("test_table") && values.len() == 2)
        );
    }
}
