#[path = "target_error.rs"]
mod error;
#[path = "target_prepare.rs"]
mod prepare;

use super::pending::{CopyDestination, DestinationBatch, PendingCopyRow, PendingCopyRows};
use super::routing::CopyRouting;
use crate::cas;
use crate::schema::ADMIN_ID;
use crate::sql::direct_insert::PreparedDirectInsert;
use crate::sql::local_ref::with_local_bucket_ref;
use crate::sql::router::DEFAULT_QUERY_TIMEOUT;
use crate::storage::Catalog;
use smol_str::SmolStr;
use sql::executor::engine::protocol::InsertCoreData;
use sql::executor::engine::Vshard;
use sql::executor::vtable::VTableTuple;
use sql::ir::types::UnrestrictedType as SbroadType;
use sql_protocol::dml::insert::ConflictPolicy;
use std::collections::HashMap;
use tarantool::session::with_su;

pub(crate) use error::CopyTargetError;
pub(crate) use prepare::prepare_copy_target;

#[derive(Debug)]
pub(crate) struct PreparedCopyTarget {
    table_id: u32,
    table_name: SmolStr,
    field_types: Vec<SbroadType>,
    conflict_policy: ConflictPolicy,
    insert: PreparedDirectInsert,
    routing: CopyRouting,
}

impl PreparedCopyTarget {
    pub(crate) fn table_name(&self) -> &SmolStr {
        &self.table_name
    }

    pub(crate) fn field_types(&self) -> &[SbroadType] {
        &self.field_types
    }

    pub(crate) fn prepare_pending_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<PendingCopyRow, CopyTargetError> {
        let bucket_id = self.insert.bucket_id_for_row(runtime, values)?;
        let encoded_row = self
            .insert
            .encode_row_with_bucket(values, bucket_id.as_ref())?;
        let destination = self.destination_for_bucket(bucket_id)?;
        Ok(PendingCopyRow::new(destination, encoded_row))
    }

    pub(crate) fn flush_pending_destination(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        pending: &mut PendingCopyRows,
        destination: &CopyDestination,
    ) -> Result<usize, CopyTargetError> {
        if !pending.contains_destination(destination) {
            return Ok(0);
        }
        self.ensure_operable()?;
        self.ensure_routing_current()?;
        let row_count = {
            let Some(batch) = pending.destination(destination) else {
                return Ok(0);
            };
            self.flush_destination_batch(runtime, destination, batch)?
        };
        pending.take_destination(destination);
        Ok(row_count)
    }

    pub(crate) fn flush_pending_rows(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        pending: &mut PendingCopyRows,
    ) -> Result<usize, CopyTargetError> {
        if pending.is_empty() {
            return Ok(0);
        }
        self.ensure_operable()?;
        self.ensure_routing_current()?;

        let mut row_count = 0usize;
        let mut remote_batches = HashMap::new();
        for (destination, batch) in pending.destinations() {
            match destination {
                CopyDestination::Local => {
                    row_count =
                        row_count.saturating_add(self.insert_destination_batch(runtime, batch)?);
                }
                CopyDestination::Replicaset(replicaset_uuid) => {
                    remote_batches
                        .insert(replicaset_uuid.to_string(), batch.encoded_rows.as_slice());
                }
            }
        }

        if !remote_batches.is_empty() {
            row_count = row_count.saturating_add(self.dispatch_remote_batches(remote_batches)?);
        }

        pending.clear();
        Ok(row_count)
    }

    pub(super) fn destination_for_bucket(
        &self,
        bucket_id: Option<u64>,
    ) -> Result<CopyDestination, CopyTargetError> {
        match &self.routing {
            CopyRouting::Local => Ok(CopyDestination::Local),
            CopyRouting::Sharded(routing) => {
                let bucket_id = bucket_id.ok_or(CopyTargetError::MissingBucketId)?;
                routing.ensure_current()?;
                routing.destination_for_bucket(bucket_id).ok_or_else(|| {
                    CopyTargetError::MissingBucketRoute {
                        tier_name: routing.tier_name.clone(),
                        bucket_id,
                    }
                })
            }
        }
    }

    fn ensure_operable(&self) -> Result<(), CopyTargetError> {
        let storage = Catalog::try_get(false).expect("storage should be initialized");
        with_su(ADMIN_ID, || {
            cas::check_table_operable(storage, self.table_id)
        })??;
        Ok(())
    }

    fn ensure_routing_current(&self) -> Result<(), CopyTargetError> {
        if let CopyRouting::Sharded(routing) = &self.routing {
            routing.ensure_current()?;
        }
        Ok(())
    }

    fn flush_destination_batch(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        destination: &CopyDestination,
        batch: &DestinationBatch,
    ) -> Result<usize, CopyTargetError> {
        if batch.is_empty() {
            return Ok(0);
        }

        match destination {
            CopyDestination::Local => self.insert_destination_batch(runtime, batch),
            CopyDestination::Replicaset(replicaset_uuid) => {
                let mut remote_batches = HashMap::new();
                remote_batches.insert(replicaset_uuid.to_string(), batch.encoded_rows.as_slice());
                self.dispatch_remote_batches(remote_batches)
            }
        }
    }

    fn insert_destination_batch(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        batch: &DestinationBatch,
    ) -> Result<usize, CopyTargetError> {
        with_local_bucket_ref(self.local_insert_timeout(), "leader", || {
            self.insert
                .insert_encoded_slices(runtime, batch.encoded_rows.iter().map(Vec::as_slice))
        })
        .map_err(CopyTargetError::from)
    }

    fn local_insert_timeout(&self) -> u64 {
        match &self.routing {
            CopyRouting::Local => DEFAULT_QUERY_TIMEOUT,
            CopyRouting::Sharded(routing) => routing.dispatch_timeout,
        }
    }

    fn dispatch_remote_batches<'a>(
        &self,
        remote_batches: HashMap<String, &'a [Vec<u8>]>,
    ) -> Result<usize, CopyTargetError> {
        let CopyRouting::Sharded(routing) = &self.routing else {
            return Err(CopyTargetError::internal(
                "remote COPY batch requires sharded routing",
            ));
        };
        let remote_row_count = crate::sql::dispatch::dispatch_encoded_insert_batches(
            InsertCoreData {
                request_id: uuid::Uuid::new_v4().to_string().into(),
                space_id: self.table_id,
                space_version: routing.schema_version,
                conflict_policy: self.conflict_policy,
            },
            remote_batches,
            Some(routing.tier_name.as_str()),
            routing.dispatch_timeout,
        )?;
        Ok(remote_row_count as usize)
    }

    #[cfg(test)]
    pub(super) fn for_test_with_routing(routing: CopyRouting) -> Self {
        Self {
            table_id: 1,
            table_name: "test".into(),
            field_types: Vec::new(),
            conflict_policy: ConflictPolicy::DoFail,
            insert: PreparedDirectInsert::new(1, 1, Vec::new(), ConflictPolicy::DoFail),
            routing,
        }
    }
}
