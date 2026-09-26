//! [`rows_to_json`] (plan M1.2 Task 10 rule 7): the JSON form of a SQL
//! result, `{"columns": [{"name", "type"}], "rows": [[…]], "truncated"}`.

use base64::Engine;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use serde_json::{Map, Number, Value, json};

use crate::sql::SqlResult;

/// Rule 7: the columns (name and `format!("{}", data_type)`), the rows in
/// column order, and whether the rows were truncated.
pub fn rows_to_json(result: &SqlResult) -> Value {
    let columns: Vec<Value> = result
        .schema
        .fields()
        .iter()
        .map(|field| json!({"name": field.name(), "type": format!("{}", field.data_type())}))
        .collect();
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(Value::Array(
                batch
                    .columns()
                    .iter()
                    .map(|column| value_to_json(column.as_ref(), row))
                    .collect(),
            ));
        }
    }
    json!({"columns": columns, "rows": rows, "truncated": result.truncated})
}

/// A float as a JSON number; non-finite → `null`.
fn float(x: f64) -> Value {
    Number::from_f64(x).map_or(Value::Null, Value::Number)
}

/// An `f32` as its shortest decimal form (`0.1`, not `0.10000000149…`).
fn float32(x: f32) -> Value {
    if !x.is_finite() {
        return Value::Null;
    }
    x.to_string().parse::<f64>().map_or(Value::Null, float)
}

/// Arrow's display string of `array[row]`.
fn display(array: &dyn Array, row: usize) -> Value {
    match ArrayFormatter::try_new(array, &FormatOptions::default()) {
        Ok(formatter) => Value::String(formatter.value(row).to_string()),
        Err(_) => Value::Null,
    }
}

/// `YYYY-MM-DD` of `days` since the epoch.
fn date(days: i64) -> Option<String> {
    const UNIX_EPOCH_JULIAN_DAY: i64 = 2_440_588;
    let julian = i32::try_from(UNIX_EPOCH_JULIAN_DAY.checked_add(days)?).ok()?;
    let date = time::Date::from_julian_day(julian).ok()?;
    Some(format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    ))
}

/// RFC 3339 in UTC at `unit`'s precision.
fn timestamp(value: i64, unit: TimeUnit) -> Option<String> {
    let (per_second, digits) = match unit {
        TimeUnit::Second => (1, 0),
        TimeUnit::Millisecond => (1_000, 3),
        TimeUnit::Microsecond => (1_000_000, 6),
        TimeUnit::Nanosecond => (1_000_000_000, 9),
    };
    let seconds = value.div_euclid(per_second);
    let fraction = value.rem_euclid(per_second);
    let at = time::OffsetDateTime::from_unix_timestamp(seconds).ok()?;
    let fraction = if digits == 0 {
        String::new()
    } else {
        format!(".{fraction:0digits$}")
    };
    Some(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{fraction}Z",
        at.year(),
        u8::from(at.month()),
        at.day(),
        at.hour(),
        at.minute(),
        at.second()
    ))
}

/// `array[row]` as JSON (rule 7): the per-value half of [`rows_to_json`],
/// shared with Flight ingest (Task 13 rule 3). A null is `null`, and so is a
/// non-finite float.
pub fn value_to_json(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    let values = |values: ArrayRef| -> Value {
        Value::Array(
            (0..values.len())
                .map(|i| value_to_json(values.as_ref(), i))
                .collect(),
        )
    };
    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::Int8 => json!(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => json!(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => json!(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => json!(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => json!(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => json!(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => json!(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => json!(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float16 => float(f64::from(
            array.as_primitive::<Float16Type>().value(row).to_f32(),
        )),
        DataType::Float32 => float32(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => display(array, row),
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Value::String(array.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => Value::String(array.as_string_view().value(row).to_string()),
        DataType::Binary => base64(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => base64(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => base64(array.as_binary_view().value(row)),
        DataType::FixedSizeBinary(_) => base64(array.as_fixed_size_binary().value(row)),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            date(i64::from(days)).map_or_else(|| display(array, row), Value::String)
        }
        DataType::Date64 => {
            let ms = array.as_primitive::<Date64Type>().value(row);
            date(ms.div_euclid(86_400_000)).map_or_else(|| display(array, row), Value::String)
        }
        DataType::Timestamp(unit, _) => {
            let value = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row)
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            timestamp(value, *unit).map_or_else(|| display(array, row), Value::String)
        }
        DataType::List(_) => values(array.as_list::<i32>().value(row)),
        DataType::LargeList(_) => values(array.as_list::<i64>().value(row)),
        DataType::FixedSizeList(..) => values(array.as_fixed_size_list().value(row)),
        DataType::Struct(fields) => {
            let array = array.as_struct();
            let object: Map<String, Value> = fields
                .iter()
                .zip(array.columns())
                .map(|(field, column)| (field.name().clone(), value_to_json(column.as_ref(), row)))
                .collect();
            Value::Object(object)
        }
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            let (keys, values) = (entries.column(0), entries.column(1));
            Value::Array(
                (0..entries.len())
                    .map(|i| {
                        json!({
                            "key": value_to_json(keys.as_ref(), i),
                            "value": value_to_json(values.as_ref(), i),
                        })
                    })
                    .collect(),
            )
        }
        DataType::Dictionary(_, value_type) => match cast(&array.slice(row, 1), value_type) {
            Ok(value) => value_to_json(value.as_ref(), 0),
            Err(_) => display(array, row),
        },
        _ => display(array, row),
    }
}

fn base64(bytes: &[u8]) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
}
