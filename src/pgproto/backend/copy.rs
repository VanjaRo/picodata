use super::result::CopyStart;
use super::Backend;
use crate::catalog::pico_bucket::DEFAULT_BUCKET_ID_COLUMN_NAME;
use crate::pgproto::client::ClientId;
use crate::pgproto::error::{PedanticError, PgError, PgErrorCode, PgResult};
use crate::schema::{Distribution, TableDef, ADMIN_ID};
use crate::storage::{Catalog, ToEntryIter};
use bytes::Bytes;
use smol_str::{format_smolstr, SmolStr};
use sql::{CopyFormat, CopyStatement as ParsedCopyStatement};
use std::cell::RefCell;
use std::collections::BTreeMap;
use tarantool::session::with_su;

thread_local! {
    static COPY_SESSIONS: RefCell<BTreeMap<ClientId, CopySession>> = RefCell::new(BTreeMap::new());
}

#[derive(Debug, Clone)]
pub struct CopySpec {
    schema_name: Option<SmolStr>,
    table_name: SmolStr,
    columns: Vec<SmolStr>,
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

        // TODO: extend COPY decoding beyond text once the MVP protocol and execution
        // contract is settled. CSV and binary should plug into the same resolved spec
        // and session flow instead of growing ad hoc branches here.
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
            schema_name: copy_from.table.schema_name,
            table_name: copy_from.table.table_name,
            columns: copy_from.table.columns,
            delimiter,
            null_marker: null_marker.into_bytes(),
            header: copy_from.options.header,
        })
    }
}

#[derive(Debug, Clone)]
struct ResolvedCopySpec {
    table_name: SmolStr,
    table_sql: String,
    columns_sql: Vec<String>,
    field_count: usize,
    delimiter: u8,
    null_marker: Vec<u8>,
    header: bool,
    schema_version: u64,
}

impl ResolvedCopySpec {
    fn build_insert_sql(&self, row_count: usize) -> String {
        let mut values = String::new();
        let mut parameter_idx = 1usize;

        for row_idx in 0..row_count {
            if row_idx > 0 {
                values.push_str(", ");
            }
            values.push('(');
            for field_idx in 0..self.field_count {
                if field_idx > 0 {
                    values.push_str(", ");
                }
                values.push('$');
                values.push_str(&parameter_idx.to_string());
                parameter_idx += 1;
            }
            values.push(')');
        }

        let columns = if self.columns_sql.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.columns_sql.join(", "))
        };

        format!(
            "INSERT INTO {}{} VALUES {}",
            self.table_sql, columns, values
        )
    }
}

struct CopySession {
    spec: ResolvedCopySpec,
    skipped_header: bool,
    record_reader: TextRecordReader,
    row_parser: TextRowParser,
    row_buffer: Vec<Vec<Option<Bytes>>>,
}

impl CopySession {
    fn new(spec: ResolvedCopySpec) -> Self {
        let row_parser =
            TextRowParser::new(spec.delimiter, spec.null_marker.clone(), spec.field_count);
        Self {
            spec,
            skipped_header: false,
            record_reader: TextRecordReader::new(),
            row_parser,
            row_buffer: Vec::new(),
        }
    }

    fn on_copy_data(&mut self, data: Bytes) -> PgResult<()> {
        self.record_reader.push(data.as_ref());

        while let Some(record) = self.record_reader.next_record()? {
            self.process_record(&record)?;
        }

        Ok(())
    }

    fn on_copy_done(mut self, backend: &Backend) -> PgResult<usize> {
        if let Some(record) = self.record_reader.finish_record()? {
            self.process_record(&record)?;
        }
        self.ensure_schema_unchanged()?;
        self.apply_rows(backend)
    }

    fn process_record(&mut self, record: &[u8]) -> PgResult<()> {
        if self.spec.header && !self.skipped_header {
            self.skipped_header = true;
            return Ok(());
        }

        self.row_buffer.push(self.row_parser.parse_record(record)?);
        Ok(())
    }

    fn ensure_schema_unchanged(&self) -> PgResult<()> {
        let current = load_table_schema_version(&self.spec.table_name)?;
        if current != self.spec.schema_version {
            return Err(PgError::other(format!(
                "COPY target table schema changed during execution: {}",
                self.spec.table_name
            )));
        }
        Ok(())
    }

    fn apply_rows(&self, backend: &Backend) -> PgResult<usize> {
        if self.row_buffer.is_empty() {
            return Ok(0);
        }

        // TODO: replace the buffered single-statement INSERT synthesis with a dedicated
        // COPY apply path once COPY grows beyond the current strict single-node MVP.
        let sql = self.spec.build_insert_sql(self.row_buffer.len());

        let mut params = Vec::with_capacity(self.row_buffer.len() * self.spec.field_count);
        for row in &self.row_buffer {
            params.extend(row.iter().cloned());
        }

        let bound = backend.bind_sql_statement(&sql, params)?;
        let router = crate::sql::router::RouterRuntime::new();
        super::storage::execute_bound_dml(&router, bound)
    }
}

#[derive(Default)]
struct TextRecordReader {
    pending: Vec<u8>,
    eol_style: EolStyle,
}

impl TextRecordReader {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    fn next_record(&mut self) -> PgResult<Option<Vec<u8>>> {
        let Some((line_end, line_ending_len, eol_style)) = find_record_end(&self.pending) else {
            return Ok(None);
        };
        self.register_eol(eol_style)?;
        let record = self.pending[..line_end].to_vec();
        self.pending.drain(0..line_end + line_ending_len);
        Ok(Some(record))
    }

    fn finish_record(&mut self) -> PgResult<Option<Vec<u8>>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let mut record = std::mem::take(&mut self.pending);
        if ends_with_unescaped_cr(&record) {
            self.register_eol(EolStyle::Cr)?;
            record.pop();
        }
        Ok(Some(record))
    }

    fn register_eol(&mut self, eol_style: EolStyle) -> PgResult<()> {
        if matches!(self.eol_style, EolStyle::Unknown) {
            self.eol_style = eol_style;
            return Ok(());
        }

        if self.eol_style == eol_style {
            return Ok(());
        }

        Err(PedanticError::new(
            PgErrorCode::InvalidTextRepresentation,
            format!(
                "COPY data has mixed line endings: expected {} but found {}",
                self.eol_style.name(),
                eol_style.name()
            ),
        )
        .into())
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum EolStyle {
    #[default]
    Unknown,
    Lf,
    Cr,
    CrLf,
}

impl EolStyle {
    fn name(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Lf => "LF",
            Self::Cr => "CR",
            Self::CrLf => "CRLF",
        }
    }
}

struct TextRowParser {
    delimiter: u8,
    null_marker: Vec<u8>,
    field_count: usize,
}

impl TextRowParser {
    fn new(delimiter: u8, null_marker: Vec<u8>, field_count: usize) -> Self {
        Self {
            delimiter,
            null_marker,
            field_count,
        }
    }

    fn parse_record(&self, record: &[u8]) -> PgResult<Vec<Option<Bytes>>> {
        let fields = self.split_raw_fields(record);
        if fields.len() != self.field_count {
            return Err(PedanticError::new(
                PgErrorCode::InvalidTextRepresentation,
                format!(
                    "COPY row has {} columns but expected {}",
                    fields.len(),
                    self.field_count
                ),
            )
            .into());
        }

        fields
            .into_iter()
            .map(|raw| {
                if raw == self.null_marker {
                    Ok(None)
                } else {
                    self.decode_text_field(&raw).map(|v| Some(Bytes::from(v)))
                }
            })
            .collect()
    }

    fn split_raw_fields(&self, record: &[u8]) -> Vec<Vec<u8>> {
        let mut fields = Vec::new();
        let mut current = Vec::new();
        let mut escaped = false;

        for byte in record {
            if !escaped && *byte == self.delimiter {
                fields.push(current);
                current = Vec::new();
                continue;
            }

            current.push(*byte);
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            }
        }

        fields.push(current);
        fields
    }

    fn decode_text_field(&self, raw: &[u8]) -> PgResult<Vec<u8>> {
        let mut decoded = Vec::with_capacity(raw.len());
        let mut idx = 0usize;

        while idx < raw.len() {
            let byte = raw[idx];
            if byte != b'\\' {
                decoded.push(byte);
                idx += 1;
                continue;
            }

            idx += 1;
            if idx == raw.len() {
                return Err(PedanticError::new(
                    PgErrorCode::InvalidTextRepresentation,
                    "COPY data ended inside an escape sequence",
                )
                .into());
            }

            let escaped = raw[idx];
            let decoded_byte = match escaped {
                b'b' => {
                    idx += 1;
                    b'\x08'
                }
                b'f' => {
                    idx += 1;
                    b'\x0c'
                }
                b'n' => {
                    idx += 1;
                    b'\n'
                }
                b'r' => {
                    idx += 1;
                    b'\r'
                }
                b't' => {
                    idx += 1;
                    b'\t'
                }
                b'v' => {
                    idx += 1;
                    b'\x0b'
                }
                b'\\' => {
                    idx += 1;
                    b'\\'
                }
                b'x' => {
                    let Some(first) = raw.get(idx + 1).copied() else {
                        return Err(PedanticError::new(
                            PgErrorCode::InvalidTextRepresentation,
                            "COPY data ended inside a hexadecimal escape sequence",
                        )
                        .into());
                    };
                    let Some(mut value) = hex_value(first) else {
                        return Err(PedanticError::new(
                            PgErrorCode::InvalidTextRepresentation,
                            format!(
                                "invalid COPY hexadecimal escape sequence: \\x{}",
                                char::from(first)
                            ),
                        )
                        .into());
                    };

                    idx += 2;
                    if let Some(second) = raw.get(idx).copied().and_then(hex_value) {
                        value = value * 16 + second;
                        idx += 1;
                    }
                    value
                }
                b'0'..=b'7' => {
                    let mut value = escaped - b'0';
                    idx += 1;
                    for _ in 0..2 {
                        let Some(next) = raw.get(idx).copied() else {
                            break;
                        };
                        if !(b'0'..=b'7').contains(&next) {
                            break;
                        }
                        value = value.saturating_mul(8).saturating_add(next - b'0');
                        idx += 1;
                    }
                    value
                }
                byte if byte == self.delimiter => {
                    idx += 1;
                    self.delimiter
                }
                other => {
                    idx += 1;
                    other
                }
            };

            decoded.push(decoded_byte);
        }

        Ok(decoded)
    }
}

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

fn find_record_end(data: &[u8]) -> Option<(usize, usize, EolStyle)> {
    let mut idx = 0usize;
    let mut escaped = false;
    while idx < data.len() {
        if escaped {
            escaped = false;
            idx += 1;
            continue;
        }

        match data[idx] {
            b'\\' => {
                escaped = true;
            }
            b'\n' => {
                return Some((idx, 1, EolStyle::Lf));
            }
            b'\r' => {
                if idx + 1 < data.len() {
                    let eol_style = if data[idx + 1] == b'\n' {
                        EolStyle::CrLf
                    } else {
                        EolStyle::Cr
                    };
                    let line_ending_len = if matches!(eol_style, EolStyle::CrLf) {
                        2
                    } else {
                        1
                    };
                    return Some((idx, line_ending_len, eol_style));
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn ends_with_unescaped_cr(data: &[u8]) -> bool {
    if data.last() != Some(&b'\r') {
        return false;
    }

    let mut escaped = false;
    for (idx, byte) in data.iter().enumerate() {
        if idx == data.len() - 1 {
            return !escaped;
        }

        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        }
    }

    false
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn ensure_supported_schema(schema_name: Option<&SmolStr>) -> PgResult<()> {
    if let Some(schema_name) = schema_name {
        if schema_name != "public" {
            return Err(PgError::FeatureNotSupported(format_smolstr!(
                "COPY FROM STDIN currently supports only the public schema"
            )));
        }
    }
    Ok(())
}

fn sql_quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn default_copy_columns(table_def: &TableDef) -> Vec<String> {
    let skip_implicit_bucket_id = matches!(
        table_def.distribution,
        Distribution::ShardedImplicitly { .. }
    );

    table_def
        .format
        .iter()
        .filter(|field| {
            !(skip_implicit_bucket_id && field.name.as_str() == DEFAULT_BUCKET_ID_COLUMN_NAME)
        })
        .map(|field| sql_quote_identifier(&field.name))
        .collect()
}

fn resolve_copy_spec(spec: CopySpec) -> PgResult<ResolvedCopySpec> {
    let storage = Catalog::try_get(false).expect("storage should be initialized");
    ensure_supported_schema(spec.schema_name.as_ref())?;
    let table_name = spec.table_name.clone();
    let table_def = with_su(ADMIN_ID, || storage.pico_table.by_name(&table_name))??
        .ok_or_else(|| PgError::other(format!("table does not exist: {}", spec.table_name)))?;

    let columns_sql = if spec.columns.is_empty() {
        default_copy_columns(&table_def)
    } else {
        let mut columns = Vec::with_capacity(spec.columns.len());
        for column in &spec.columns {
            let field = table_def
                .format
                .iter()
                .find(|field| field.name == *column)
                .ok_or_else(|| PgError::other(format!("column does not exist: {column}")))?;
            columns.push(sql_quote_identifier(&field.name));
        }
        columns
    };

    let table_name_sql = sql_quote_identifier(&table_def.name);

    Ok(ResolvedCopySpec {
        table_name: table_def.name,
        table_sql: table_name_sql,
        field_count: columns_sql.len(),
        columns_sql,
        delimiter: spec.delimiter,
        null_marker: spec.null_marker,
        header: spec.header,
        schema_version: table_def.schema_version,
    })
}

fn load_table_schema_version(table_name: &SmolStr) -> PgResult<u64> {
    let storage = Catalog::try_get(false).expect("storage should be initialized");
    let table_def = with_su(ADMIN_ID, || storage.pico_table.by_name(table_name))??
        .ok_or_else(|| PgError::other(format!("table does not exist: {table_name}")))?;
    Ok(table_def.schema_version)
}

fn ensure_single_node_topology() -> PgResult<()> {
    let node = crate::traft::node::global()?;
    let replicaset_count = node.storage.replicasets.iter()?.count();
    let instance_count = node.storage.instances.iter()?.count();
    if replicaset_count != 1 || instance_count != 1 {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY FROM STDIN is available only for single-node clusters",
        )));
    }
    Ok(())
}

pub fn start_copy(client_id: ClientId, spec: CopySpec) -> PgResult<CopyStart> {
    ensure_single_node_topology()?;
    let resolved = resolve_copy_spec(spec)?;
    let start = CopyStart {
        column_count: resolved.field_count,
    };

    COPY_SESSIONS.with(|storage| {
        let prev = storage
            .borrow_mut()
            .insert(client_id, CopySession::new(resolved));
        if prev.is_some() {
            return Err(PgError::other("COPY session already exists"));
        }
        Ok(start)
    })
}

pub fn on_copy_data(backend: &Backend, data: Bytes) -> PgResult<()> {
    COPY_SESSIONS.with(|storage| {
        let mut storage = storage.borrow_mut();
        let session = storage.get_mut(&backend.client_id()).ok_or_else(|| {
            PgError::ProtocolViolation(format_smolstr!("COPY session is missing"))
        })?;
        session.on_copy_data(data)
    })
}

pub fn on_copy_done(backend: &Backend) -> PgResult<usize> {
    COPY_SESSIONS.with(|storage| {
        let session = storage
            .borrow_mut()
            .remove(&backend.client_id())
            .ok_or_else(|| {
                PgError::ProtocolViolation(format_smolstr!("COPY session is missing"))
            })?;
        session.on_copy_done(backend)
    })
}

pub fn abort_copy(backend: &Backend) {
    COPY_SESSIONS.with(|storage| {
        storage.borrow_mut().remove(&backend.client_id());
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_spec(sql: &str) -> PgResult<CopySpec> {
        let router = crate::sql::router::RouterRuntime::new();
        let sql::Command::Copy(statement) =
            sql::parse_command(&router, sql, &[]).map_err(PgError::other)?
        else {
            panic!("COPY statement");
        };
        CopySpec::try_from_statement(statement)
    }

    fn decode_row_parser(sql: &str) -> TextRowParser {
        let spec = parse_spec(sql).expect("spec");
        TextRowParser::new(spec.delimiter, spec.null_marker, 1)
    }

    #[test]
    fn parses_supported_copy_spec() {
        let spec = parse_spec(
            r#"COPY "t" ("id", "value") FROM STDIN WITH (DELIMITER '|', NULL 'nil', HEADER true)"#,
        )
        .expect("copy spec");
        assert_eq!(spec.table_name, "t");
        assert_eq!(spec.columns, vec!["id", "value"]);
        assert_eq!(spec.delimiter, b'|');
        assert_eq!(spec.null_marker, b"nil");
        assert!(spec.header);
    }

    #[test]
    fn decodes_postgres_text_escapes() {
        let parser = decode_row_parser(r#"COPY "t" FROM STDIN"#);
        assert_eq!(
            parser
                .decode_text_field(br#"hello\\world\tok\n\141\x42"#)
                .expect("decode"),
            b"hello\\world\tok\naB"
        );
    }

    #[test]
    fn rejects_trailing_escape() {
        let parser = decode_row_parser(r#"COPY "t" FROM STDIN"#);
        let err = parser
            .decode_text_field(br#"broken\"#)
            .expect_err("must fail");
        assert!(err.to_string().contains("ended inside an escape sequence"));
    }

    #[test]
    fn text_record_reader_keeps_partial_record_across_messages() {
        let mut reader = TextRecordReader::new();
        reader.push(b"1\tal");
        assert!(reader.next_record().expect("partial").is_none());

        reader.push(b"pha\n2\tbeta\n");
        assert_eq!(
            reader.next_record().expect("first record").expect("first"),
            b"1\talpha"
        );
        assert_eq!(
            reader
                .next_record()
                .expect("second record")
                .expect("second"),
            b"2\tbeta"
        );
        assert!(reader.next_record().expect("drained").is_none());
    }

    #[test]
    fn text_record_reader_finishes_tail_record_without_newline() {
        let mut reader = TextRecordReader::new();
        reader.push(b"1\talpha\r");
        assert!(reader.next_record().expect("partial").is_none());
        assert_eq!(
            reader.finish_record().expect("tail record").expect("tail"),
            b"1\talpha"
        );
        assert!(reader.finish_record().expect("drained").is_none());
    }

    #[test]
    fn text_record_reader_handles_crlf_split_across_messages() {
        let mut reader = TextRecordReader::new();
        reader.push(b"1\talpha\r");
        assert!(reader.next_record().expect("partial").is_none());

        reader.push(b"\n2\tbeta\r\n");
        assert_eq!(
            reader.next_record().expect("first record").expect("first"),
            b"1\talpha"
        );
        assert_eq!(
            reader
                .next_record()
                .expect("second record")
                .expect("second"),
            b"2\tbeta"
        );
        assert!(reader.next_record().expect("drained").is_none());
    }

    #[test]
    fn text_record_reader_treats_backslash_newline_as_data() {
        let mut reader = TextRecordReader::new();
        reader.push(b"1\thello\\\nworld\n2\tbeta\n");
        assert_eq!(
            reader.next_record().expect("first record").expect("first"),
            b"1\thello\\\nworld"
        );
        assert_eq!(
            reader
                .next_record()
                .expect("second record")
                .expect("second"),
            b"2\tbeta"
        );
        assert!(reader.next_record().expect("drained").is_none());
    }

    #[test]
    fn text_record_reader_rejects_mixed_line_endings() {
        let mut reader = TextRecordReader::new();
        reader.push(b"1\talpha\n2\tbeta\r\n");
        assert_eq!(
            reader.next_record().expect("first record").expect("first"),
            b"1\talpha"
        );
        let err = reader
            .next_record()
            .expect_err("mixed line endings must fail");
        assert!(err.to_string().contains("mixed line endings"));
    }
}
