use std::fmt;

/// Ошибка декодирования pgoutput.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputError(pub String);

impl fmt::Display for PgOutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ошибка парсера pgoutput: {}", self.0)
    }
}

impl std::error::Error for PgOutputError {}

impl From<&str> for PgOutputError {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for PgOutputError {
    fn from(s: String) -> Self {
        Self(s)
    }
}
