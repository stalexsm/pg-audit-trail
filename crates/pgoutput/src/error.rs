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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_prefixes_message() {
        let err = PgOutputError("ветка не декодирована".to_owned());
        assert_eq!(
            err.to_string(),
            "ошибка парсера pgoutput: ветка не декодирована"
        );
    }

    #[test]
    fn builds_from_str_and_string() {
        assert_eq!(
            PgOutputError::from("из среза"),
            PgOutputError("из среза".to_owned())
        );
        assert_eq!(
            PgOutputError::from(String::from("из строки")),
            PgOutputError("из строки".to_owned())
        );
    }

    #[test]
    fn usable_as_std_error() {
        fn takes_error(_: &dyn std::error::Error) {}
        takes_error(&PgOutputError("динамический объект ошибки".to_owned()));
    }
}
