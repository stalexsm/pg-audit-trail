use serde_json::Value;

use crate::decode::Cell;

/// OID типов PostgreSQL (`pg_type`), для которых выполняется типовая
/// конвертация текстового представления в JSON.
const OID_BOOL: u32 = 16;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;
const OID_JSON: u32 = 114;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_JSONB: u32 = 3802;

/// Текстовое значение pgoutput → JSON по типу колонки.
///
/// - bool → `true`/`false`, целые → числа, float4/float8 → числа;
/// - json/jsonb → вложенный JSON (по нему работают GIN-фильтры API);
/// - остальные типы (numeric, uuid, даты, тексты, массивы) остаются
///   строками в текстовом представлении PostgreSQL (для дат — ISO,
///   логическое декодирование фиксирует DateStyle);
///   numeric как строка — чтобы не терять точность.
///
/// Разбор каждого типа мягкий: неожиданное значение сохраняется строкой,
/// а не роняет инжест.
pub(crate) fn json_from_text(type_oid: u32, raw: &[u8]) -> Value {
    let text = String::from_utf8_lossy(raw);
    match type_oid {
        OID_BOOL => match &*text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => Value::String(text.into_owned()),
        },
        OID_INT2 | OID_INT4 | OID_INT8 => text
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(text.into_owned())),
        OID_FLOAT4 | OID_FLOAT8 => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.into_owned())),
        OID_JSON | OID_JSONB => {
            serde_json::from_str(&text).unwrap_or_else(|_| Value::String(text.into_owned()))
        }
        _ => Value::String(text.into_owned()),
    }
}

/// Значение ячейки кортежа → JSON.
pub(crate) fn json_from_cell(type_oid: u32, cell: Cell<'_>) -> Value {
    match cell {
        Cell::Null => Value::Null,
        // Декодер пропускает неизменённые TOAST-колонки до конвертации,
        // сюда такие ячейки доходить не должны.
        Cell::UnchangedToast => Value::Null,
        Cell::Text(raw) => json_from_text(type_oid, raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_decode() {
        assert_eq!(json_from_text(OID_BOOL, b"t"), Value::Bool(true));
        assert_eq!(json_from_text(OID_BOOL, b"f"), Value::Bool(false));
        assert_eq!(
            json_from_text(OID_BOOL, "д".as_bytes()),
            Value::String("д".into())
        );
    }

    #[test]
    fn integers_decode_as_numbers() {
        assert_eq!(json_from_text(OID_INT8, b"42"), Value::from(42_i64));
        assert_eq!(json_from_text(OID_INT4, b"-7"), Value::from(-7_i64));
        assert_eq!(json_from_text(OID_INT2, b"300"), Value::from(300_i64));
    }

    #[test]
    fn floats_decode_as_numbers_and_fall_back_to_string() {
        assert_eq!(json_from_text(OID_FLOAT8, b"1.5"), Value::from(1.5_f64));
        assert_eq!(
            json_from_text(OID_FLOAT4, b"nan"),
            Value::String("nan".into())
        );
    }

    #[test]
    fn json_types_are_parsed_nested() {
        assert_eq!(
            json_from_text(OID_JSON, b"{\"a\":1}"),
            serde_json::json!({"a": 1})
        );
        assert_eq!(
            json_from_text(OID_JSONB, b"[1,2]"),
            serde_json::json!([1, 2])
        );
        // Битый JSON не роняет инжест.
        assert_eq!(
            json_from_text(OID_JSONB, b"{oops"),
            Value::String("{oops".into())
        );
    }

    #[test]
    fn other_types_stay_as_text() {
        assert_eq!(
            json_from_text(1700, b"10.50"),
            Value::String("10.50".into())
        );
        assert_eq!(
            json_from_text(25, "привет".as_bytes()),
            Value::String("привет".into())
        );
        assert_eq!(
            json_from_text(1184, b"2026-10-21 09:00:00+00"),
            Value::String("2026-10-21 09:00:00+00".into())
        );
    }

    #[test]
    fn null_cell_is_json_null() {
        assert_eq!(json_from_cell(OID_INT8, Cell::Null), Value::Null);
        assert_eq!(
            json_from_cell(OID_INT8, Cell::Text(b"42")),
            Value::from(42_i64)
        );
    }
}
