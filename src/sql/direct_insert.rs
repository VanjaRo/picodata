use crate::tlog;
use smol_str::format_smolstr;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{write_insert_args, TupleBuilderCommand, TupleBuilderPattern};
use sql::executor::engine::{QueryCache, Vshard};
use sql::executor::vtable::VTableTuple;
use sql::ir::transformation::redistribution::{MotionKey, Target};
use sql_protocol::dml::insert::ConflictPolicy;
use std::fmt::Debug;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::space::Space;
use tarantool::transaction::transaction;
use tarantool::tuple::RawBytes;

#[derive(Debug)]
pub(crate) struct DirectInsertTarget {
    table_id: u32,
    schema_version: u64,
    builder: TupleBuilderPattern,
}

impl DirectInsertTarget {
    pub(crate) fn new(table_id: u32, schema_version: u64, builder: TupleBuilderPattern) -> Self {
        Self {
            table_id,
            schema_version,
            builder,
        }
    }

    pub(crate) fn encode_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<Vec<u8>, SbroadError> {
        let bucket_id = match find_insert_motion_key(&self.builder) {
            Some(motion_key) => Some(determine_insert_bucket_id(runtime, values, motion_key)?),
            None => None,
        };

        let mut encoded = Vec::new();
        rmp::encode::write_array_len(&mut encoded, self.builder.len() as u32).map_err(|e| {
            SbroadError::FailedTo(
                Action::Encode,
                Some(Entity::MsgPack),
                format_smolstr!("{e}"),
            )
        })?;
        write_insert_args(values, &self.builder, bucket_id.as_ref(), &mut encoded)?;
        Ok(encoded)
    }

    pub(crate) fn flush_batch<R: QueryCache>(
        &self,
        runtime: &R,
        tuples: &[Vec<u8>],
    ) -> Result<usize, SbroadError> {
        if tuples.is_empty() {
            return Ok(0);
        }

        let space = ensure_target_space(runtime, self.table_id, self.schema_version)?;
        let inserted = transaction(|| -> Result<usize, SbroadError> {
            let mut inserted = 0usize;
            for tuple in tuples.iter() {
                if insert_encoded_tuple(&space, tuple, ConflictPolicy::DoFail)? {
                    inserted = inserted.saturating_add(1);
                }
            }
            Ok(inserted)
        })?;
        Ok(inserted)
    }
}

pub(crate) fn ensure_target_space<R: QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
) -> Result<Space, SbroadError> {
    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: `table_id` already exists. Checked by `get_table_version_by_id`.
    Ok(unsafe { Space::from_id_unchecked(table_id) })
}

pub(crate) fn find_insert_motion_key(builder: &TupleBuilderPattern) -> Option<&MotionKey> {
    builder.iter().find_map(|command| match command {
        TupleBuilderCommand::CalculateBucketId(motion_key) if !motion_key.targets.is_empty() => {
            Some(motion_key)
        }
        _ => None,
    })
}

pub(crate) fn determine_insert_bucket_id<R: Vshard>(
    runtime: &R,
    vt_tuple: &VTableTuple,
    motion_key: &MotionKey,
) -> Result<u64, SbroadError> {
    let mut shard_key_tuple = Vec::with_capacity(motion_key.targets.len());
    for target in &motion_key.targets {
        match target {
            Target::Reference(col_idx) => {
                let value = vt_tuple.get(*col_idx).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::DistributionKey,
                        format_smolstr!(
                            "failed to find a distribution key column {col_idx} in the tuple {vt_tuple:?}."
                        ),
                    )
                })?;
                shard_key_tuple.push(value);
            }
            Target::Value(value) => shard_key_tuple.push(value),
        }
    }

    runtime.determine_bucket_id(&shard_key_tuple)
}

pub(crate) fn apply_insert_with_conflict(
    insert_result: Result<(), Error>,
    conflict_strategy: ConflictPolicy,
    insert_tuple: &impl Debug,
    replace_on_conflict: impl FnOnce() -> Result<(), SbroadError>,
) -> Result<bool, SbroadError> {
    match insert_result {
        Ok(()) => Ok(true),
        Err(Error::Tarantool(tnt_err))
            if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 =>
        {
            match conflict_strategy {
                ConflictPolicy::DoNothing => {
                    tlog!(
                        Debug,
                        "failed to insert tuple: {insert_tuple:?}. Skipping according to conflict strategy",
                    );
                    Ok(false)
                }
                ConflictPolicy::DoReplace => {
                    tlog!(
                        Debug,
                        "failed to insert tuple: {insert_tuple:?}. Trying to replace according to conflict strategy"
                    );
                    replace_on_conflict()?;
                    Ok(true)
                }
                ConflictPolicy::DoFail => Err(SbroadError::FailedTo(
                    Action::Insert,
                    Some(Entity::Space),
                    format_smolstr!("{tnt_err}"),
                )),
            }
        }
        Err(e) => Err(SbroadError::FailedTo(
            Action::Insert,
            Some(Entity::Space),
            format_smolstr!("{e}"),
        )),
    }
}

pub(crate) fn insert_encoded_tuple(
    space: &Space,
    tuple_data: &[u8],
    conflict_strategy: ConflictPolicy,
) -> Result<bool, SbroadError> {
    let insert_tuple = RawBytes::new(tuple_data);
    let insert_result = space.insert(insert_tuple).map(|_| ());
    apply_insert_with_conflict(insert_result, conflict_strategy, &insert_tuple, || {
        space
            .replace(RawBytes::new(tuple_data))
            .map(|_| ())
            .map_err(|e| {
                SbroadError::FailedTo(
                    Action::ReplaceOnConflict,
                    Some(Entity::Space),
                    format_smolstr!("{e}"),
                )
            })
    })
}
