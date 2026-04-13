use crate::tlog;
use smol_str::format_smolstr;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{write_insert_args, TupleBuilderCommand, TupleBuilderPattern};
use sql::executor::engine::{QueryCache, Vshard};
use sql::executor::vtable::{VTableTuple, VirtualTable};
use sql::ir::helpers::RepeatableState;
use sql::ir::transformation::redistribution::{MotionKey, Target};
use sql_protocol::dml::insert::ConflictPolicy;
use std::collections::HashMap;
use std::fmt::Debug;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::space::Space;
use tarantool::transaction::transaction;
use tarantool::tuple::RawBytes;

#[derive(Debug)]
pub(crate) struct PreparedDirectInsert {
    table_id: u32,
    schema_version: u64,
    builder: TupleBuilderPattern,
    conflict_policy: ConflictPolicy,
    motion_key: Option<MotionKey>,
}

impl PreparedDirectInsert {
    pub(crate) fn new(
        table_id: u32,
        schema_version: u64,
        builder: TupleBuilderPattern,
        conflict_policy: ConflictPolicy,
    ) -> Self {
        let motion_key = find_insert_motion_key(&builder).cloned();
        Self {
            table_id,
            schema_version,
            builder,
            conflict_policy,
            motion_key,
        }
    }

    pub(crate) fn insert_encoded_slices<'a, R, I>(
        &self,
        runtime: &R,
        tuples: I,
    ) -> Result<usize, SbroadError>
    where
        R: QueryCache,
        I: IntoIterator<Item = &'a [u8]>,
    {
        let space = self.ensure_target_space(runtime)?;
        Ok(transaction(|| -> Result<usize, SbroadError> {
            let mut inserted = 0usize;
            for tuple in tuples {
                if insert_encoded_tuple(&space, tuple, self.conflict_policy)? {
                    inserted = inserted.saturating_add(1);
                }
            }
            Ok(inserted)
        })?)
    }

    pub(crate) fn insert_vtable<R: Vshard + QueryCache>(
        &self,
        runtime: &R,
        vtable: &VirtualTable,
    ) -> Result<u64, SbroadError> {
        let space = self.ensure_target_space(runtime)?;
        insert_vtable_impl(
            runtime,
            &space,
            self.conflict_policy,
            &self.builder,
            self.motion_key.as_ref(),
            vtable,
        )
    }

    fn ensure_target_space<R: QueryCache>(&self, runtime: &R) -> Result<Space, SbroadError> {
        ensure_target_space(runtime, self.table_id, self.schema_version)
    }

    pub(crate) fn bucket_id_for_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<Option<u64>, SbroadError> {
        self.motion_key
            .as_ref()
            .map(|motion_key| determine_insert_bucket_id(runtime, values, motion_key))
            .transpose()
    }

    pub(crate) fn encode_row_with_bucket(
        &self,
        values: &VTableTuple,
        bucket_id: Option<&u64>,
    ) -> Result<Vec<u8>, SbroadError> {
        encode_row_with_builder(&self.builder, values, bucket_id)
    }
}

pub(crate) fn insert_vtable<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    schema_version: u64,
    conflict_policy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let space = ensure_target_space(runtime, table_id, schema_version)?;
    insert_vtable_impl(
        runtime,
        &space,
        conflict_policy,
        builder,
        find_insert_motion_key(builder),
        vtable,
    )
}

fn insert_vtable_impl<R: Vshard>(
    runtime: &R,
    space: &Space,
    conflict_policy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    motion_key: Option<&MotionKey>,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let computed_bucket_index = build_bucket_index(runtime, motion_key, vtable)?;
    let bucket_index = computed_bucket_index
        .as_ref()
        .unwrap_or_else(|| vtable.get_bucket_index());
    let mut row_count = 0u64;

    transaction(|| -> Result<(), SbroadError> {
        let mut insert_one = |tuple_data: Vec<u8>| -> Result<(), SbroadError> {
            if insert_encoded_tuple(space, &tuple_data, conflict_policy)? {
                row_count += 1;
            }
            Ok(())
        };

        if bucket_index.is_empty() {
            for vt_tuple in vtable.get_tuples() {
                insert_one(encode_row_with_builder(builder, vt_tuple, None)?)?;
            }
            return Ok(());
        }

        for (bucket_id, positions) in bucket_index {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                insert_one(encode_row_with_builder(builder, vt_tuple, Some(bucket_id))?)?;
            }
        }

        Ok(())
    })?;

    Ok(row_count)
}

fn build_bucket_index<R: Vshard>(
    runtime: &R,
    motion_key: Option<&MotionKey>,
    vtable: &VirtualTable,
) -> Result<Option<HashMap<u64, Vec<usize>, RepeatableState>>, SbroadError> {
    if !vtable.get_bucket_index().is_empty() {
        return Ok(None);
    }

    let Some(motion_key) = motion_key else {
        return Ok(None);
    };

    let mut bucket_index: HashMap<u64, Vec<usize>, RepeatableState> =
        HashMap::with_hasher(RepeatableState);
    for (pos, vt_tuple) in vtable.get_tuples().iter().enumerate() {
        let bucket_id = determine_insert_bucket_id(runtime, vt_tuple, motion_key)?;
        bucket_index.entry(bucket_id).or_default().push(pos);
    }
    Ok(Some(bucket_index))
}

fn encode_row_with_builder(
    builder: &TupleBuilderPattern,
    values: &VTableTuple,
    bucket_id: Option<&u64>,
) -> Result<Vec<u8>, SbroadError> {
    let mut encoded = Vec::new();
    rmp::encode::write_array_len(&mut encoded, builder.len() as u32).map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::MsgPack),
            format_smolstr!("{e}"),
        )
    })?;
    write_insert_args(values, builder, bucket_id, &mut encoded)?;
    Ok(encoded)
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
