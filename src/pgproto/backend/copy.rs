use super::result::CopyStart;
use super::Backend;
use crate::pgproto::error::{PgError, PgResult};
use bytes::Bytes;
use smol_str::format_smolstr;
use sql::{CopyFormat, CopyInput, CopyStatement as ParsedCopyStatement};

#[derive(Debug, Clone)]
pub struct CopySpec {
    table_name: String,
    columns: Vec<String>,
    delimiter: u8,
    null_marker: Vec<u8>,
    header: bool,
}

impl CopySpec {
    pub fn try_from_statement(statement: ParsedCopyStatement) -> PgResult<Self> {
        fn unsupported_copy(reason: impl std::fmt::Display) -> PgError {
            PgError::FeatureNotSupported(format_smolstr!("{reason}"))
        }

        let sql::CopyStatement::From(copy_from) = statement else {
            return Err(unsupported_copy("COPY TO is not supported"));
        };

        let CopyInput::Stdin = copy_from.input;

        if copy_from.options.format != CopyFormat::Text {
            return Err(unsupported_copy(format_smolstr!(
                "COPY format {} is not supported",
                copy_format_name(copy_from.options.format)
            )));
        }

        let delimiter = match copy_from.options.delimiter.as_deref() {
            Some(delimiter) => parse_copy_delimiter(delimiter)?,
            None => b'\t',
        };
        let null_marker = copy_from
            .options
            .null_string
            .unwrap_or_else(|| String::from("\\N"));

        Ok(Self {
            table_name: copy_from.table.table_name,
            columns: copy_from.table.columns,
            delimiter,
            null_marker: null_marker.into_bytes(),
            header: copy_from.options.header,
        })
    }
}

pub fn start_copy(_backend: &Backend, _spec: CopySpec) -> PgResult<CopyStart> {
    Err(PgError::FeatureNotSupported(format_smolstr!(
        "COPY FROM STDIN execution is not available yet"
    )))
}

pub fn on_copy_data(_backend: &Backend, _data: Bytes) -> PgResult<()> {
    Err(PgError::ProtocolViolation(format_smolstr!(
        "COPY session is missing"
    )))
}

pub fn on_copy_done(_backend: &Backend) -> PgResult<usize> {
    Err(PgError::ProtocolViolation(format_smolstr!(
        "COPY session is missing"
    )))
}

pub fn abort_copy(_backend: &Backend) {}

fn parse_copy_delimiter(delimiter: &str) -> PgResult<u8> {
    let mut bytes = delimiter.as_bytes().iter().copied();
    let Some(byte) = bytes.next() else {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY delimiter must be a single-byte character"
        )));
    };
    if bytes.next().is_some() {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY delimiter must be a single-byte character"
        )));
    }
    Ok(byte)
}

fn copy_format_name(format: CopyFormat) -> &'static str {
    match format {
        CopyFormat::Text => "text",
        CopyFormat::Csv => "csv",
        CopyFormat::Binary => "binary",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql::PreparedCommand;

    fn parse_spec(sql: &str) -> PgResult<CopySpec> {
        let router = crate::sql::router::RouterRuntime::new();
        let PreparedCommand::Copy(statement) =
            PreparedCommand::parse(&router, sql, &[]).map_err(PgError::other)?
        else {
            panic!("COPY statement");
        };
        CopySpec::try_from_statement(statement)
    }

    #[test]
    fn parses_supported_copy_spec() {
        let spec = parse_spec(
            r#"COPY "t" ("id", "value") FROM STDIN WITH (DELIMITER '|', NULL 'nil', HEADER true)"#,
        )
        .expect("copy spec");
        assert_eq!(spec.table_name, r#""t""#);
        assert_eq!(spec.columns, vec![r#""id""#, r#""value""#]);
        assert_eq!(spec.delimiter, b'|');
        assert_eq!(spec.null_marker, b"nil");
        assert!(spec.header);
    }
}
