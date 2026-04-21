use crate::audit;
use crate::cas;
use crate::config::{DEFAULT_SQL_LOG, DYNAMIC_CONFIG};
use crate::pgproto::error::{PedanticError, PgError, PgErrorCode, PgResult};
use crate::pgproto::value::{FieldFormat, PgValue};
use crate::sql::copy::{prepare_copy_target, CopyTargetError, PreparedCopyTarget};
use crate::sql::storage::StorageRuntime;
use bytes::{Bytes, BytesMut};
use postgres_types::Oid;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts};
use smol_str::{format_smolstr, SmolStr};
use sql::executor::vtable::VTableTuple;
use sql::ir::types::UnrestrictedType as SbroadType;
use sql::ir::value::Value as SbroadValue;
use sql::{CopyFormat, CopyStatement as ParsedCopyStatement};
use std::{borrow::Cow, sync::LazyLock, time::Instant};
use tarantool::error::{IntoBoxError, TarantoolErrorCode};

const DEFAULT_COPY_BATCH_SIZE: usize = 1024;
const DEFAULT_COPY_BATCH_BYTES: usize = 1 << 20;
const DEFAULT_COPY_RECORD_BYTES: usize = DEFAULT_COPY_BATCH_BYTES;

pub(crate) static PGPROTO_COPY_SESSIONS_STARTED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_sessions_started_total",
        "Total number of pgproto COPY FROM STDIN sessions started",
    ))
    .expect("Failed to create pico_pgproto_copy_sessions_started_total counter")
});

pub(crate) static PGPROTO_COPY_BYTES_RECEIVED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_bytes_received_total",
        "Total number of pgproto COPY FROM STDIN payload bytes received",
    ))
    .expect("Failed to create pico_pgproto_copy_bytes_received_total counter")
});

pub(crate) static PGPROTO_COPY_ROWS_INSERTED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_pgproto_copy_rows_inserted_total",
        "Total number of rows inserted by pgproto COPY FROM STDIN",
    ))
    .expect("Failed to create pico_pgproto_copy_rows_inserted_total counter")
});

pub(crate) static PGPROTO_COPY_BATCHES_FLUSHED_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new(
                "pico_pgproto_copy_batches_flushed_total",
                "Total number of pgproto COPY FROM STDIN batches flushed",
            ),
            &["reason"],
        )
        .expect("Failed to create pico_pgproto_copy_batches_flushed_total counter")
    });

pub(crate) static PGPROTO_COPY_BATCH_FLUSH_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_pgproto_copy_batch_flush_duration",
        "Histogram of pgproto COPY FROM STDIN batch flush durations (in seconds)",
    ))
    .expect("Failed to create pico_pgproto_copy_batch_flush_duration histogram")
});

pub(crate) static PGPROTO_COPY_RECORD_LIMIT_ERRORS_TOTAL: LazyLock<IntCounter> =
    LazyLock::new(|| {
        IntCounter::with_opts(Opts::new(
            "pico_pgproto_copy_record_limit_errors_total",
            "Total number of pgproto COPY FROM STDIN record-limit violations",
        ))
        .expect("Failed to create pico_pgproto_copy_record_limit_errors_total counter")
    });

fn bad_copy_format(reason: impl Into<Box<crate::pgproto::error::DynError>>) -> PgError {
    PedanticError::new(PgErrorCode::BadCopyFileFormat, reason).into()
}

fn record_limit_error(record_byte_limit: usize) -> PgError {
    PGPROTO_COPY_RECORD_LIMIT_ERRORS_TOTAL.inc();
    bad_copy_format(format!(
        "COPY row exceeds maximum size of {} bytes",
        record_byte_limit
    ))
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
pub struct PreparedCopy {
    spec: CopySpec,
    query_text: SmolStr,
    audit_enabled: bool,
    sql_log_enabled: bool,
}

impl PreparedCopy {
    pub fn try_from_statement(statement: ParsedCopyStatement, query_text: &str) -> PgResult<Self> {
        let spec = CopySpec::try_from_statement(statement)?;
        let audit_enabled = audit::policy::is_dml_audit_enabled_for_current_user()?;
        let sql_log_enabled = DYNAMIC_CONFIG
            .sql_log
            .try_current_value()
            .unwrap_or(DEFAULT_SQL_LOG);

        Ok(Self {
            spec,
            query_text: query_text.into(),
            audit_enabled,
            sql_log_enabled,
        })
    }

    pub fn query_for_audit(&self) -> Option<&str> {
        self.audit_enabled.then_some(self.query_text.as_str())
    }

    pub fn query_for_logging(&self) -> Option<&str> {
        self.sql_log_enabled.then_some(self.query_text.as_str())
    }

    pub fn spec(&self) -> &CopySpec {
        &self.spec
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CopyStart {
    pub column_count: usize,
}

pub(crate) struct CopySession {
    table_name: SmolStr,
    target: PreparedCopyTarget,
    field_oids: Vec<Oid>,
    runtime: StorageRuntime,
    header: bool,
    batch_size: usize,
    batch_byte_limit: usize,
    record_byte_limit: usize,
    skipped_header: bool,
    record_reader: TextRecordReader,
    row_parser: TextRowParser,
    batch_tuples: Vec<Vec<u8>>,
    batch_bytes: usize,
    inserted_rows: usize,
}

impl CopySession {
    fn new(
        table_name: SmolStr,
        target: PreparedCopyTarget,
        field_oids: Vec<Oid>,
        runtime: StorageRuntime,
        delimiter: u8,
        null_marker: Vec<u8>,
        header: bool,
        batch_size: usize,
        batch_byte_limit: usize,
        record_byte_limit: usize,
    ) -> Self {
        let row_parser = TextRowParser::new(delimiter, null_marker, field_oids.len());
        Self {
            table_name,
            target,
            field_oids,
            runtime,
            header,
            batch_size,
            batch_byte_limit,
            record_byte_limit,
            skipped_header: false,
            record_reader: TextRecordReader::new(),
            row_parser,
            batch_tuples: Vec::new(),
            batch_bytes: 0,
            inserted_rows: 0,
        }
    }

    pub(crate) fn on_copy_data(&mut self, data: Bytes) -> PgResult<()> {
        PGPROTO_COPY_BYTES_RECEIVED_TOTAL.inc_by(data.len() as u64);
        self.ensure_incoming_record_limit(data.as_ref())?;
        self.record_reader.push(data.as_ref());

        while let Some(record) = self.record_reader.next_record()? {
            self.process_record(&record)?;
        }

        self.ensure_pending_record_limit()?;

        Ok(())
    }

    pub(crate) fn on_copy_done(mut self) -> PgResult<usize> {
        self.ensure_pending_record_limit()?;
        if let Some(record) = self.record_reader.finish_record()? {
            self.process_record(&record)?;
        }
        self.flush_batch("copy_done")?;
        Ok(self.inserted_rows)
    }

    fn process_record(&mut self, record: &[u8]) -> PgResult<()> {
        self.ensure_record_size(record.len())?;

        if self.header && !self.skipped_header {
            self.skipped_header = true;
            return Ok(());
        }

        let values = self.decode_record(record)?;
        let tuple = self
            .target
            .encode_row(&self.runtime, &values)
            .map_err(PgError::from)?;
        if !self.batch_tuples.is_empty()
            && self.batch_bytes.saturating_add(tuple.len()) > self.batch_byte_limit
        {
            self.flush_batch("batch_bytes")?;
        }

        self.batch_bytes = self.batch_bytes.saturating_add(tuple.len());
        self.batch_tuples.push(tuple);

        if self.batch_tuples.len() >= self.batch_size {
            self.flush_batch("batch_size")?;
        } else if self.batch_bytes >= self.batch_byte_limit {
            self.flush_batch("batch_bytes")?;
        }

        Ok(())
    }

    fn decode_record(&self, record: &[u8]) -> PgResult<VTableTuple> {
        let decoded_fields = self.row_parser.parse_record(record)?;
        decoded_fields
            .into_iter()
            .zip(self.field_oids.iter().copied())
            .map(|(field, oid)| {
                let pg_value = PgValue::decode(field.as_deref(), oid, FieldFormat::Text)?;
                let sbroad_value: SbroadValue = pg_value.try_into()?;
                Ok(sbroad_value)
            })
            .collect()
    }

    fn flush_batch(&mut self, reason: &'static str) -> PgResult<()> {
        if self.batch_tuples.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let flush_result = self
            .target
            .flush_batch(&self.runtime, &self.batch_tuples)
            .map_err(|error| map_copy_target_flush_error(&self.table_name, error));
        PGPROTO_COPY_BATCH_FLUSH_DURATION.observe(started_at.elapsed().as_secs_f64());
        let inserted = flush_result?;
        PGPROTO_COPY_BATCHES_FLUSHED_TOTAL
            .with_label_values(&[reason])
            .inc();
        PGPROTO_COPY_ROWS_INSERTED_TOTAL.inc_by(inserted as u64);
        self.batch_tuples.clear();
        self.batch_bytes = 0;
        self.inserted_rows += inserted;
        Ok(())
    }

    fn ensure_pending_record_limit(&self) -> PgResult<()> {
        let pending = pending_record_len(self.record_reader.pending.as_ref());
        self.ensure_record_size(pending)
    }

    fn ensure_incoming_record_limit(&self, incoming: &[u8]) -> PgResult<()> {
        if !incoming_exceeds_record_limit(
            self.record_reader.pending.as_ref(),
            self.record_reader.scan_escaped,
            incoming,
            self.record_byte_limit,
        ) {
            return Ok(());
        }

        Err(record_limit_error(self.record_byte_limit))
    }

    fn ensure_record_size(&self, record_len: usize) -> PgResult<()> {
        if record_len <= self.record_byte_limit {
            return Ok(());
        }

        Err(record_limit_error(self.record_byte_limit))
    }
}

#[derive(Default)]
struct TextRecordReader {
    pending: BytesMut,
    eol_style: EolStyle,
    scan_offset: usize,
    scan_escaped: bool,
}

impl TextRecordReader {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    fn next_record(&mut self) -> PgResult<Option<Bytes>> {
        let Some((line_end, line_ending_len, eol_style)) = self.find_record_end() else {
            return Ok(None);
        };
        self.register_eol(eol_style)?;
        let mut record = self.pending.split_to(line_end + line_ending_len);
        record.truncate(line_end);
        self.reset_scan();
        Ok(Some(record.freeze()))
    }

    fn finish_record(&mut self) -> PgResult<Option<Bytes>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let mut record = self.pending.split();
        if ends_with_unescaped_cr(record.as_ref()) {
            self.register_eol(EolStyle::Cr)?;
            record.truncate(record.len() - 1);
        }
        self.reset_scan();
        Ok(Some(record.freeze()))
    }

    fn register_eol(&mut self, eol_style: EolStyle) -> PgResult<()> {
        if matches!(self.eol_style, EolStyle::Unknown) {
            self.eol_style = eol_style;
            return Ok(());
        }

        if self.eol_style == eol_style {
            return Ok(());
        }

        Err(bad_copy_format(format!(
            "COPY data has mixed line endings: expected {} but found {}",
            self.eol_style.name(),
            eol_style.name()
        )))
    }

    fn find_record_end(&mut self) -> Option<(usize, usize, EolStyle)> {
        let mut idx = self.scan_offset;
        let mut escaped = self.scan_escaped;

        while idx < self.pending.len() {
            if escaped {
                escaped = false;
                idx += 1;
                continue;
            }

            match self.pending[idx] {
                b'\\' => {
                    escaped = true;
                    idx += 1;
                }
                b'\n' => {
                    self.reset_scan();
                    return Some((idx, 1, EolStyle::Lf));
                }
                b'\r' => {
                    if idx + 1 < self.pending.len() {
                        let eol_style = if self.pending[idx + 1] == b'\n' {
                            EolStyle::CrLf
                        } else {
                            EolStyle::Cr
                        };
                        let line_ending_len = if matches!(eol_style, EolStyle::CrLf) {
                            2
                        } else {
                            1
                        };
                        self.reset_scan();
                        return Some((idx, line_ending_len, eol_style));
                    }

                    self.scan_offset = idx;
                    self.scan_escaped = false;
                    return None;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        self.scan_offset = idx;
        self.scan_escaped = escaped;
        None
    }

    fn reset_scan(&mut self) {
        self.scan_offset = 0;
        self.scan_escaped = false;
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

    fn parse_record<'a>(&self, record: &'a [u8]) -> PgResult<Vec<Option<Cow<'a, [u8]>>>> {
        let fields = self.split_fields(record)?;
        if fields.len() != self.field_count {
            return Err(PedanticError::new(
                PgErrorCode::BadCopyFileFormat,
                format!(
                    "COPY row has {} columns but expected {}",
                    fields.len(),
                    self.field_count
                ),
            )
            .into());
        }

        Ok(fields)
    }

    fn split_fields<'a>(&self, record: &'a [u8]) -> PgResult<Vec<Option<Cow<'a, [u8]>>>> {
        let mut fields = Vec::with_capacity(self.field_count);
        let mut field_start = 0usize;
        let mut escaped = false;
        let mut has_escape = false;

        for (idx, byte) in record.iter().copied().enumerate() {
            if !escaped && byte == self.delimiter {
                self.push_field(record, field_start, idx, has_escape, &mut fields)?;
                field_start = idx + 1;
                has_escape = false;
                continue;
            }

            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
                has_escape = true;
            }
        }

        self.push_field(record, field_start, record.len(), has_escape, &mut fields)?;
        Ok(fields)
    }

    fn push_field<'a>(
        &self,
        record: &'a [u8],
        start: usize,
        end: usize,
        has_escape: bool,
        fields: &mut Vec<Option<Cow<'a, [u8]>>>,
    ) -> PgResult<()> {
        let raw = &record[start..end];
        if raw == self.null_marker.as_slice() {
            fields.push(None);
        } else if has_escape {
            fields.push(Some(Cow::Owned(self.decode_text_field(raw)?)));
        } else {
            fields.push(Some(Cow::Borrowed(raw)));
        }
        Ok(())
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
                    PgErrorCode::BadCopyFileFormat,
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
                            PgErrorCode::BadCopyFileFormat,
                            "COPY data ended inside a hexadecimal escape sequence",
                        )
                        .into());
                    };
                    let Some(mut value) = hex_value(first) else {
                        return Err(PedanticError::new(
                            PgErrorCode::BadCopyFileFormat,
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

fn pending_record_len(data: &[u8]) -> usize {
    if ends_with_unescaped_cr(data) {
        data.len().saturating_sub(1)
    } else {
        data.len()
    }
}

fn incoming_exceeds_record_limit(
    pending: &[u8],
    pending_escaped: bool,
    incoming: &[u8],
    record_byte_limit: usize,
) -> bool {
    if pending.len().saturating_add(incoming.len()) <= record_byte_limit {
        return false;
    }

    let mut record_len = pending.len();
    let mut escaped = pending_escaped;
    let mut awaiting_lf_after_cr = false;

    if ends_with_unescaped_cr(pending) {
        record_len = 0;
        escaped = false;
        awaiting_lf_after_cr = true;
    }

    if record_len > record_byte_limit {
        return true;
    }

    for byte in incoming.iter().copied() {
        if awaiting_lf_after_cr {
            awaiting_lf_after_cr = false;
            if byte == b'\n' {
                continue;
            }
        }

        if escaped {
            escaped = false;
            record_len = record_len.saturating_add(1);
        } else {
            match byte {
                b'\\' => {
                    escaped = true;
                    record_len = record_len.saturating_add(1);
                }
                b'\n' => {
                    record_len = 0;
                }
                b'\r' => {
                    record_len = 0;
                    awaiting_lf_after_cr = true;
                }
                _ => {
                    record_len = record_len.saturating_add(1);
                }
            }
        }

        if record_len > record_byte_limit {
            return true;
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

fn resolve_copy_batch_size(batch_size: Option<usize>) -> PgResult<usize> {
    let batch_size = batch_size.unwrap_or(DEFAULT_COPY_BATCH_SIZE);
    if batch_size == 0 {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY batch_size must be greater than zero"
        )));
    }
    Ok(batch_size)
}

fn copy_field_oid(field_type: &SbroadType) -> Oid {
    super::storage::sbroad_type_to_pg(field_type).oid()
}

fn map_copy_target_flush_error(table_name: &SmolStr, error: CopyTargetError) -> PgError {
    match error {
        CopyTargetError::Storage(sql::errors::SbroadError::OutdatedStorageSchema) => {
            PedanticError::new(
                PgErrorCode::ObjectNotInPrerequisiteState,
                format!("target table schema changed during execution: {table_name}"),
            )
            .into()
        }
        other => map_copy_target_error(other),
    }
}

fn map_copy_target_error(error: CopyTargetError) -> PgError {
    match error {
        CopyTargetError::TableDoesNotExist { table } => PedanticError::new(
            PgErrorCode::UndefinedTable,
            format!("table does not exist: {table}"),
        )
        .into(),
        CopyTargetError::DuplicateColumn { column } => PedanticError::new(
            PgErrorCode::DuplicateColumn,
            format!("column \"{column}\" specified more than once"),
        )
        .into(),
        CopyTargetError::ColumnDoesNotExist { column } => PedanticError::new(
            PgErrorCode::UndefinedColumn,
            format!("column does not exist: {column}"),
        )
        .into(),
        CopyTargetError::SystemColumnInsertNotAllowed { column } => PedanticError::new(
            PgErrorCode::InvalidColumnReference,
            format!("system column \"{column}\" cannot be inserted"),
        )
        .into(),
        CopyTargetError::MissingRequiredColumn { column } => PedanticError::new(
            PgErrorCode::NotNullViolation,
            format!("NonNull column \"{column}\" must be specified"),
        )
        .into(),
        CopyTargetError::Internal(message) => {
            PedanticError::new(PgErrorCode::InternalError, message.to_string()).into()
        }
        CopyTargetError::FeatureNotSupported(message) => PgError::FeatureNotSupported(message),
        CopyTargetError::Picodata(crate::traft::error::Error::Cas(
            cas::Error::TableNotOperable { table },
        )) => PedanticError::new(
            PgErrorCode::ObjectNotInPrerequisiteState,
            format!("table {table} cannot be modified now as DDL operation is in progress"),
        )
        .into(),
        CopyTargetError::Picodata(error) => error.into(),
        CopyTargetError::Storage(error) => error.into(),
        CopyTargetError::Tarantool(error)
            if error.error_code() == TarantoolErrorCode::AccessDenied as u32 =>
        {
            PedanticError::new(PgErrorCode::InsufficientPrivilege, error.to_string()).into()
        }
        CopyTargetError::Tarantool(error) => error.into(),
    }
}

pub(crate) fn start_copy(spec: CopySpec) -> PgResult<(CopyStart, CopySession)> {
    let target = prepare_copy_target(
        spec.schema_name.as_ref(),
        &spec.table_name,
        &spec.columns,
        ConflictPolicy::DoFail,
    )
        .map_err(map_copy_target_error)?;
    let field_oids = target
        .field_types()
        .iter()
        .map(copy_field_oid)
        .collect::<Vec<_>>();
    let runtime = StorageRuntime::new();
    let start = CopyStart {
        column_count: field_oids.len(),
    };

    let session = CopySession::new(
        target.table_name().clone(),
        target,
        field_oids,
        runtime,
        spec.delimiter,
        spec.null_marker,
        spec.header,
        DEFAULT_COPY_BATCH_SIZE,
        DEFAULT_COPY_BATCH_BYTES,
        DEFAULT_COPY_RECORD_BYTES,
    );
    PGPROTO_COPY_SESSIONS_STARTED_TOTAL.inc();

    Ok((start, session))
}
