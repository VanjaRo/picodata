use crate::cas;
use crate::schema::{Distribution, TableDef, ADMIN_ID};
use crate::sql::direct_insert::PreparedDirectInsert;
use crate::storage::Catalog;
use smol_str::{format_smolstr, SmolStr};
use sql::executor::engine::helpers::TupleBuilderCommand;
use sql::executor::engine::Vshard;
use sql::executor::vtable::VTableTuple;
use sql::ir::relation::Column;
use sql::ir::transformation::redistribution::{MotionKey, Target};
use sql::ir::types::UnrestrictedType as SbroadType;
use sql_protocol::dml::insert::ConflictPolicy;
use tarantool::access_control::{box_access_check_space, PrivType};
use tarantool::session::with_su;
use tarantool::space::Field;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct PreparedCopyTarget {
    table_id: u32,
    table_name: SmolStr,
    field_types: Vec<SbroadType>,
    insert: PreparedDirectInsert,
}

impl PreparedCopyTarget {
    pub(crate) fn table_name(&self) -> &SmolStr {
        &self.table_name
    }

    pub(crate) fn field_types(&self) -> &[SbroadType] {
        &self.field_types
    }

    pub(crate) fn flush_batch(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        tuples: &[Vec<u8>],
    ) -> Result<usize, CopyTargetError> {
        if tuples.is_empty() {
            return Ok(0);
        }

        self.ensure_operable()?;
        self.insert
            .insert_encoded_batch(runtime, tuples)
            .map_err(CopyTargetError::from)
    }

    pub(crate) fn encode_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<Vec<u8>, sql::errors::SbroadError> {
        self.insert.encode_row(runtime, values)
    }

    fn ensure_operable(&self) -> Result<(), CopyTargetError> {
        let storage = Catalog::try_get(false).expect("storage should be initialized");
        with_su(ADMIN_ID, || {
            cas::check_table_operable(storage, self.table_id)
        })??;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum CopyTargetError {
    #[error("table does not exist: {table}")]
    TableDoesNotExist { table: SmolStr },

    #[error("column \"{column}\" specified more than once")]
    DuplicateColumn { column: SmolStr },

    #[error("column does not exist: {column}")]
    ColumnDoesNotExist { column: SmolStr },

    #[error("system column \"{column}\" cannot be inserted")]
    SystemColumnInsertNotAllowed { column: SmolStr },

    #[error("NonNull column \"{column}\" must be specified")]
    MissingRequiredColumn { column: SmolStr },

    #[error("{0}")]
    Internal(SmolStr),

    #[error("feature is not supported: {0}")]
    FeatureNotSupported(SmolStr),

    #[error(transparent)]
    Picodata(#[from] crate::traft::error::Error),

    #[error(transparent)]
    Storage(#[from] sql::errors::SbroadError),

    #[error(transparent)]
    Tarantool(#[from] tarantool::error::Error),
}

impl CopyTargetError {
    fn table_does_not_exist(table: &SmolStr) -> Self {
        Self::TableDoesNotExist {
            table: table.clone(),
        }
    }

    fn duplicate_column(column: &SmolStr) -> Self {
        Self::DuplicateColumn {
            column: column.clone(),
        }
    }

    fn column_does_not_exist(column: &SmolStr) -> Self {
        Self::ColumnDoesNotExist {
            column: column.clone(),
        }
    }

    fn system_column_insert_not_allowed(column: impl AsRef<str>) -> Self {
        Self::SystemColumnInsertNotAllowed {
            column: column.as_ref().into(),
        }
    }

    fn missing_required_column(column: impl AsRef<str>) -> Self {
        Self::MissingRequiredColumn {
            column: column.as_ref().into(),
        }
    }

    fn internal(message: impl Into<SmolStr>) -> Self {
        Self::Internal(message.into())
    }
}

pub(crate) fn prepare_copy_target(
    schema_name: Option<&SmolStr>,
    table_name: &SmolStr,
    selected_columns: &[SmolStr],
) -> Result<PreparedCopyTarget, CopyTargetError> {
    ensure_supported_schema(schema_name)?;

    let storage = Catalog::try_get(false).expect("storage should be initialized");
    let table_def = with_su(ADMIN_ID, || storage.pico_table.by_name(table_name))??
        .ok_or_else(|| CopyTargetError::table_does_not_exist(table_name))?;

    box_access_check_space(table_def.id, PrivType::Write)?;
    with_su(ADMIN_ID, || {
        cas::check_table_operable(storage, table_def.id)
    })??;

    let selected_fields = resolve_copy_fields(selected_columns, &table_def)?;
    build_copy_target(table_def.name.clone(), &table_def, &selected_fields)
}

fn ensure_supported_schema(schema_name: Option<&SmolStr>) -> Result<(), CopyTargetError> {
    if let Some(schema_name) = schema_name {
        if schema_name != "public" {
            return Err(CopyTargetError::FeatureNotSupported(format_smolstr!(
                "COPY FROM STDIN currently supports only the public schema"
            )));
        }
    }
    Ok(())
}

fn build_copy_target(
    table_name: SmolStr,
    table_def: &TableDef,
    selected_fields: &[(usize, &Field)],
) -> Result<PreparedCopyTarget, CopyTargetError> {
    let mut field_types = Vec::with_capacity(selected_fields.len());
    let mut input_positions = vec![None; table_def.format.len()];
    for (input_position, (table_position, field)) in selected_fields.iter().enumerate() {
        input_positions[*table_position] = Some(input_position);
        field_types.push(copy_field_type(field)?);
    }

    let motion_key = match &table_def.distribution {
        Distribution::ShardedImplicitly { sharding_key, .. } => Some(build_copy_motion_key(
            table_def,
            &input_positions,
            sharding_key,
        )?),
        Distribution::Global | Distribution::ShardedByField { .. } => None,
    };

    let mut builder = Vec::with_capacity(table_def.format.len());
    for (table_position, field) in table_def.format.iter().enumerate() {
        if is_implicit_bucket_id_field(table_def, &field.name) {
            builder.push(TupleBuilderCommand::CalculateBucketId(
                motion_key
                    .clone()
                    .ok_or_else(|| CopyTargetError::internal("insert motion key is missing"))?,
            ));
            continue;
        }

        match input_positions[table_position] {
            Some(input_position) => builder.push(TupleBuilderCommand::TakePosition(input_position)),
            None => builder.push(TupleBuilderCommand::SetValue(Column::default_value())),
        }
    }

    Ok(PreparedCopyTarget {
        table_id: table_def.id,
        table_name,
        field_types,
        insert: PreparedDirectInsert::new(
            table_def.id,
            table_def.schema_version,
            builder,
            ConflictPolicy::DoFail,
        ),
    })
}

fn resolve_copy_fields<'a>(
    selected_columns: &[SmolStr],
    table_def: &'a TableDef,
) -> Result<Vec<(usize, &'a Field)>, CopyTargetError> {
    if selected_columns.is_empty() {
        return Ok(default_copy_fields(table_def));
    }

    let mut seen_columns = std::collections::BTreeSet::new();
    let mut fields = Vec::with_capacity(selected_columns.len());
    for column in selected_columns {
        if !seen_columns.insert(column.as_str()) {
            return Err(CopyTargetError::duplicate_column(column));
        }

        if is_implicit_bucket_id_field(table_def, column.as_str()) {
            return Err(CopyTargetError::system_column_insert_not_allowed(column));
        }

        let (position, field) = table_def
            .format
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == *column)
            .ok_or_else(|| CopyTargetError::column_does_not_exist(column))?;
        fields.push((position, field));
    }

    validate_required_copy_fields(table_def, &fields)?;
    Ok(fields)
}

fn default_copy_fields(table_def: &TableDef) -> Vec<(usize, &Field)> {
    table_def
        .format
        .iter()
        .enumerate()
        .filter(|(_, field)| !is_implicit_bucket_id_field(table_def, &field.name))
        .collect()
}

fn validate_required_copy_fields(
    table_def: &TableDef,
    selected_fields: &[(usize, &Field)],
) -> Result<(), CopyTargetError> {
    let selected_names = selected_fields
        .iter()
        .map(|(_, field)| field.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    for field in &table_def.format {
        if is_implicit_bucket_id_field(table_def, field.name.as_str()) {
            continue;
        }

        if !field.is_nullable && !selected_names.contains(field.name.as_str()) {
            return Err(CopyTargetError::missing_required_column(&field.name));
        }
    }

    Ok(())
}

fn is_implicit_bucket_id_field(table_def: &TableDef, field_name: &str) -> bool {
    matches!(
        table_def.distribution,
        Distribution::ShardedImplicitly { .. }
    ) && field_name == crate::catalog::pico_bucket::DEFAULT_BUCKET_ID_COLUMN_NAME
}

fn copy_field_type(field: &Field) -> Result<SbroadType, CopyTargetError> {
    SbroadType::try_from(field.field_type).map_err(|e| {
        CopyTargetError::FeatureNotSupported(format_smolstr!(
            "unsupported column type {}: {e}",
            field.field_type.as_str()
        ))
    })
}

fn build_copy_motion_key(
    table_def: &TableDef,
    input_positions: &[Option<usize>],
    sharding_key: &[SmolStr],
) -> Result<MotionKey, CopyTargetError> {
    let mut targets = Vec::with_capacity(sharding_key.len());
    for key_column in sharding_key {
        let table_position = table_def
            .format
            .iter()
            .position(|field| field.name == *key_column)
            .ok_or_else(|| {
                CopyTargetError::internal(format_smolstr!("column does not exist: {key_column}"))
            })?;
        let target = match input_positions.get(table_position).copied().flatten() {
            Some(input_position) => Target::Reference(input_position),
            None => Target::Value(Column::default_value()),
        };
        targets.push(target);
    }
    Ok(MotionKey { targets })
}
