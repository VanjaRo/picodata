use crate::pgproto::backend::{result::CopyStart, Backend};
use crate::pgproto::error::{PgError, PgResult};
use crate::pgproto::messages;
use crate::pgproto::stream::{FeMessage, PgStream};
use smol_str::format_smolstr;
use std::io::{Read, Write};

pub enum CopyInMode {
    SimpleQuery,
    ExtendedQuery,
}

pub enum CopyInMessageOutcome {
    Continue,
    Done { inserted_rows: usize },
    ClientFailed { reason: String },
    Terminate,
}

pub fn is_copy_in_message(message: &FeMessage) -> bool {
    matches!(
        message,
        FeMessage::CopyData(_)
            | FeMessage::CopyDone(_)
            | FeMessage::CopyFail(_)
            | FeMessage::Flush(_)
            | FeMessage::Sync(_)
    )
}

pub fn send_copy_in_response(
    stream: &mut PgStream<impl Read + Write>,
    start: &CopyStart,
) -> PgResult<()> {
    stream.write_message(messages::copy_in_response_text(start.column_count))?;
    Ok(())
}

pub fn process_copy_in_message(
    backend: &Backend,
    message: FeMessage,
) -> PgResult<CopyInMessageOutcome> {
    match message {
        FeMessage::CopyData(copy_data) => {
            backend.on_copy_data(copy_data.data)?;
            Ok(CopyInMessageOutcome::Continue)
        }
        FeMessage::CopyDone(_) => {
            let inserted_rows = backend.on_copy_done()?;
            Ok(CopyInMessageOutcome::Done { inserted_rows })
        }
        FeMessage::CopyFail(copy_fail) => {
            backend.abort_copy();
            Ok(CopyInMessageOutcome::ClientFailed {
                reason: copy_fail.message,
            })
        }
        FeMessage::Flush(_) | FeMessage::Sync(_) => Ok(CopyInMessageOutcome::Continue),
        FeMessage::Terminate(_) => {
            backend.abort_copy();
            Ok(CopyInMessageOutcome::Terminate)
        }
        other => Err(PgError::ProtocolViolation(format_smolstr!(
            "unexpected frontend message during COPY FROM STDIN: {other:?}"
        ))),
    }
}
