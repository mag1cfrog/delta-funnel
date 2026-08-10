//! NativeAsync Parquet data-file reader.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Int64Array, ListArray, MapArray, StructArray, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, Fields, SchemaRef},
    record_batch::RecordBatch,
};
use futures_util::{StreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::{
    PARQUET_FIELD_ID_META_KEY, ProjectionMask, RowNumber,
    arrow_reader::ArrowReaderOptions,
    async_reader::{
        ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
    },
};
use parquet::schema::types::{SchemaDescriptor, TypePtr};
use snafu::ResultExt;

const ORIGINAL_ROW_INDEX_COLUMN: &str = "__delta_arrow_reader_original_row_index";

use crate::{
    DeltaReadMetrics, DeltaReaderError, DeltaReaderExecutionOptions,
    deletion_vector::{
        DeletionVectorSelection, load_deletion_vector_selection_from_engine_context,
    },
    error::{CancelledSnafu, DataFileReadSnafu, PhysicalToLogicalTransformSnafu},
    kernel::{
        DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
        KernelScanSchemas,
    },
    metered_object_store::MeteredParquetObjectStore,
    native_async_row_group_pruning::native_async_pruned_row_groups,
    planning::{DeltaScanFileTask, DeltaScanPlan},
    scheduling::{FileBatchStream, FileExecutor, FileReadPermit, ScanCancellation},
};

pub(crate) struct NativeAsyncFileReader {
    engine_context: Arc<DeltaKernelEngineContext>,
    store: Arc<dyn ObjectStore>,
    execution_options: DeltaReaderExecutionOptions,
    metrics: DeltaReadMetrics,
}

#[derive(Debug)]
pub(crate) struct NativeAsyncParquetObject {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) file_size: u64,
}

struct NativeAsyncParquetStream {
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    schema_match: NativeAsyncSchemaMatch,
    include_original_row_index: bool,
}

pub(crate) struct NativeAsyncFileReadStream {
    parquet: NativeAsyncParquetStream,
    engine_context: Arc<DeltaKernelEngineContext>,
    kernel_schemas: KernelScanSchemas,
    logical_schema: SchemaRef,
    transform: KernelPhysicalToLogicalTransform,
    deletion_vector: Option<DeletionVectorSelection>,
    cancellation: ScanCancellation,
    _permit: FileReadPermit,
}

struct NativeAsyncFileReadRequest {
    task: DeltaScanFileTask,
    physical_schema: SchemaRef,
    logical_schema: SchemaRef,
    kernel_schemas: KernelScanSchemas,
    physical_predicate: Option<DeltaKernelPredicate>,
    output_batch_size: Option<usize>,
    permit: FileReadPermit,
    cancellation: ScanCancellation,
}

#[derive(Clone)]
struct NativeAsyncSchemaMatch {
    provider_schema: SchemaRef,
    projected_roots: Vec<usize>,
    provider_columns: Vec<NativeAsyncProviderColumn>,
    needs_batch_reshape: bool,
}

#[derive(Clone)]
enum NativeAsyncProviderColumn {
    ProjectedStreamColumn {
        stream_index: usize,
        field_plan: NativeAsyncFieldPlan,
    },
    Null,
}

#[derive(Clone)]
enum NativeAsyncFieldPlan {
    Identity,
    Cast {
        target_type: DataType,
    },
    Struct {
        children: Vec<NativeAsyncStructChild>,
    },
    List {
        element_plan: Box<NativeAsyncFieldPlan>,
    },
    Map {
        key_plan: Box<NativeAsyncFieldPlan>,
        value_plan: Box<NativeAsyncFieldPlan>,
    },
}

impl NativeAsyncFieldPlan {
    fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }
}

#[derive(Clone)]
enum NativeAsyncStructChild {
    ProjectedChild {
        child_index: usize,
        field_plan: NativeAsyncFieldPlan,
    },
    Null,
}

#[derive(Clone)]
struct NativeAsyncRootMatch {
    parquet_root_index: usize,
    field_plan: NativeAsyncFieldPlan,
}

impl NativeAsyncFileReader {
    pub(crate) fn new(
        engine_context: Arc<DeltaKernelEngineContext>,
        execution_options: DeltaReaderExecutionOptions,
        metrics: DeltaReadMetrics,
    ) -> Self {
        let store = Arc::new(MeteredParquetObjectStore::new(
            engine_context.object_store(),
            metrics.clone(),
        ));
        Self {
            engine_context,
            store,
            execution_options,
            metrics,
        }
    }

    pub(crate) async fn parquet_object_for_task(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let object = self.resolve_parquet_object(task)?;
        self.buffer_small_parquet_object(object).await
    }

    async fn open_parquet_stream(
        &self,
        task: &DeltaScanFileTask,
        provider_schema: SchemaRef,
        output_batch_size: Option<usize>,
        physical_predicate: Option<&DeltaKernelPredicate>,
        include_original_row_index: bool,
    ) -> Result<NativeAsyncParquetStream, DeltaReaderError> {
        let object = self.parquet_object_for_task(task).await?;
        let reader =
            ParquetObjectReader::new(object.store, object.path).with_file_size(object.file_size);
        let reader = match self.execution_options.parquet_metadata_size_hint() {
            Some(hint) => reader.with_footer_size_hint(hint),
            None => reader,
        };
        let reader_options = native_async_arrow_reader_options(include_original_row_index)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_row_index_setup_failed",
            })?;
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_options)
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_read_setup_failed",
            })?;
        let schema_match = build_native_async_schema_match(
            builder.parquet_schema(),
            builder.schema(),
            provider_schema,
        )
        .map_err(|error| data_file_error("parquet_schema_match_failed", error))?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_match.projected_roots());
        let row_groups = native_async_pruned_row_groups(builder.metadata(), physical_predicate);
        let builder = match row_groups {
            Some(row_groups) => builder.with_row_groups(row_groups),
            None => builder,
        };
        let builder = match output_batch_size {
            Some(batch_size) => builder.with_batch_size(batch_size),
            None => builder,
        };

        let stream = builder
            .with_projection(projection)
            .build()
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_read_setup_failed",
            })?;

        Ok(NativeAsyncParquetStream {
            stream,
            schema_match,
            include_original_row_index,
        })
    }

    async fn open_logical_file_stream(
        self: &Arc<Self>,
        request: NativeAsyncFileReadRequest,
    ) -> Result<NativeAsyncFileReadStream, DeltaReaderError> {
        let include_original_row_index = request.task.deletion_vector.is_present();
        let parquet = tokio::select! {
            biased;
            () = request.cancellation.cancelled() => return Err(cancelled_error()),
            result = self.open_parquet_stream(
                &request.task,
                request.physical_schema,
                request.output_batch_size,
                request.physical_predicate.as_ref(),
                include_original_row_index,
            ) => result?,
        };
        let deletion_vector = tokio::select! {
            biased;
            () = request.cancellation.cancelled() => return Err(cancelled_error()),
            result = load_deletion_vector_selection_from_engine_context(
                Arc::clone(&self.engine_context),
                request.task.deletion_vector.clone(),
                &self.metrics,
            ) => result?,
        };

        Ok(NativeAsyncFileReadStream {
            parquet,
            engine_context: Arc::clone(&self.engine_context),
            kernel_schemas: request.kernel_schemas,
            logical_schema: request.logical_schema,
            transform: request.task.transform,
            deletion_vector,
            cancellation: request.cancellation,
            _permit: request.permit,
        })
    }

    fn resolve_parquet_object(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let location = self
            .engine_context
            .table_url()
            .join(&task.path)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let path = Path::from_url_path(location.path())
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let file_size = task.estimated_bytes.ok_or_else(|| {
            data_file_error(
                "data_file_size_missing",
                delta_kernel::Error::generic("file size is required for NativeAsync reads"),
            )
        })?;

        Ok(NativeAsyncParquetObject {
            store: Arc::clone(&self.store),
            path,
            file_size,
        })
    }

    async fn buffer_small_parquet_object(
        &self,
        mut object: NativeAsyncParquetObject,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let should_buffer = self
            .execution_options
            .parquet_full_file_read_threshold()
            .and_then(|threshold| u64::try_from(threshold).ok())
            .is_some_and(|threshold| object.file_size <= threshold);
        if !should_buffer {
            return Ok(object);
        }

        let bytes = object
            .store
            .get(&object.path)
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?
            .bytes()
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&object.path, bytes.into())
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_file_buffer_initialization_failed",
            })?;
        object.store = store;
        Ok(object)
    }
}

pub(crate) fn native_async_file_executor(
    plan: &Arc<DeltaScanPlan>,
    output_batch_size: Option<usize>,
) -> FileExecutor<DeltaScanFileTask, FileBatchStream> {
    let reader = Arc::new(NativeAsyncFileReader::new(
        Arc::clone(&plan.engine_context),
        plan.execution_options,
        plan.metrics.clone(),
    ));
    let physical_schema = Arc::clone(&plan.physical_schema);
    let logical_schema = Arc::clone(&plan.logical_schema);
    let kernel_schemas = plan.kernel_schemas.clone();
    let physical_predicate = plan.physical_predicate.clone();

    Arc::new(move |task, permit, cancellation| {
        if let Some(bytes) = task.estimated_bytes {
            reader.metrics.record_parquet_data_file_opened_bytes(bytes);
        }
        let reader = Arc::clone(&reader);
        let physical_schema = Arc::clone(&physical_schema);
        let logical_schema = Arc::clone(&logical_schema);
        let kernel_schemas = kernel_schemas.clone();
        let physical_predicate = physical_predicate.clone();
        Box::pin(async move {
            let file = reader
                .open_logical_file_stream(NativeAsyncFileReadRequest {
                    task,
                    physical_schema,
                    logical_schema,
                    kernel_schemas,
                    physical_predicate,
                    output_batch_size,
                    permit,
                    cancellation,
                })
                .await?;
            let batches = stream::try_unfold(file, |mut file| async move {
                file.next_batch()
                    .await
                    .map(|batch| batch.map(|batch| (batch, file)))
            });
            Ok(Box::pin(batches) as FileBatchStream)
        })
    })
}

impl NativeAsyncParquetStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        self.next_batch_with_original_row_indexes()
            .await
            .map(|batch| batch.map(|(batch, _)| batch))
    }

    async fn next_batch_with_original_row_indexes(
        &mut self,
    ) -> Result<Option<(RecordBatch, Option<Int64Array>)>, DeltaReaderError> {
        let Some(batch) = self.stream.next().await else {
            return Ok(None);
        };
        let batch = batch.boxed().context(DataFileReadSnafu {
            reason: "parquet_batch_read_failed",
        })?;
        let row_indexes = if self.include_original_row_index {
            let index = batch
                .schema()
                .index_of(ORIGINAL_ROW_INDEX_COLUMN)
                .map_err(|error| data_file_error("parquet_row_index_missing", error))?;
            Some(
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        data_file_error(
                            "parquet_row_index_type_mismatch",
                            delta_kernel::Error::generic("original row index is not Int64"),
                        )
                    })?
                    .clone(),
            )
        } else {
            None
        };
        let batch = if self.include_original_row_index || self.schema_match.needs_batch_reshape {
            self.schema_match
                .reshape_batch_to_provider_schema(batch)
                .map_err(|error| data_file_error("parquet_batch_reshape_failed", error))?
        } else {
            batch
        };

        Ok(Some((batch, row_indexes)))
    }
}

impl NativeAsyncFileReadStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        let next = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(cancelled_error()),
            result = self.parquet.next_batch_with_original_row_indexes() => result?,
        };
        let Some((physical_batch, original_row_indexes)) = next else {
            if let Some(deletion_vector) = self.deletion_vector.as_mut() {
                deletion_vector.finish()?;
            }
            return Ok(None);
        };
        let logical_batch = self
            .transform
            .apply(
                self.engine_context.as_ref(),
                &self.kernel_schemas,
                physical_batch,
            )
            .boxed()
            .context(PhysicalToLogicalTransformSnafu {
                reason: "physical_to_logical_transform_failed",
            })?;
        if logical_batch.schema().as_ref() != self.logical_schema.as_ref() {
            return Err(data_file_error(
                "backend_logical_schema_mismatch",
                delta_kernel::Error::generic(
                    "NativeAsync output does not match the planned logical schema",
                ),
            ));
        }
        let logical_batch = match self.deletion_vector.as_mut() {
            Some(deletion_vector) => deletion_vector
                .mask_original_row_indexes(logical_batch, original_row_indexes.as_ref())?,
            None => logical_batch,
        };
        Ok(Some(logical_batch))
    }
}

fn native_async_arrow_reader_options(
    include_original_row_index: bool,
) -> parquet::errors::Result<ArrowReaderOptions> {
    if !include_original_row_index {
        return Ok(ArrowReaderOptions::new());
    }
    let row_number_field = Arc::new(
        Field::new(ORIGINAL_ROW_INDEX_COLUMN, DataType::Int64, false)
            .with_extension_type(RowNumber),
    );
    ArrowReaderOptions::new().with_virtual_columns(vec![row_number_field])
}

fn cancelled_error() -> DeltaReaderError {
    CancelledSnafu {
        reason: "scan_execution_cancelled",
    }
    .build()
}

impl NativeAsyncSchemaMatch {
    fn projected_roots(&self) -> impl Iterator<Item = usize> + '_ {
        self.projected_roots.iter().copied()
    }

    fn reshape_batch_to_provider_schema(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, delta_kernel::Error> {
        let columns = self
            .provider_columns
            .iter()
            .zip(self.provider_schema.fields())
            .map(|(column, field)| match column {
                NativeAsyncProviderColumn::ProjectedStreamColumn {
                    stream_index,
                    field_plan,
                } => reshape_array_to_provider_field(
                    Arc::clone(batch.column(*stream_index)),
                    field,
                    field_plan,
                ),
                NativeAsyncProviderColumn::Null => {
                    Ok(new_null_array(field.data_type(), batch.num_rows()))
                }
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;

        RecordBatch::try_new(Arc::clone(&self.provider_schema), columns)
            .map_err(delta_kernel::Error::from)
    }
}

fn build_native_async_schema_match(
    parquet_schema: &SchemaDescriptor,
    parquet_arrow_schema: &SchemaRef,
    provider_schema: SchemaRef,
) -> Result<NativeAsyncSchemaMatch, delta_kernel::Error> {
    let root_matches = provider_schema
        .fields()
        .iter()
        .map(|provider_field| {
            match_provider_field_to_parquet_root(
                provider_field,
                parquet_schema.root_schema().get_fields(),
                parquet_arrow_schema,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut projected_roots = root_matches
        .iter()
        .filter_map(|root_match| {
            root_match
                .as_ref()
                .map(|root_match| root_match.parquet_root_index)
        })
        .collect::<Vec<_>>();
    projected_roots.sort_unstable();
    projected_roots.dedup();
    let provider_columns = root_matches
        .iter()
        .zip(provider_schema.fields())
        .map(|(root_match, provider_field)| match root_match {
            Some(root_match) => projected_roots
                .iter()
                .position(|root| *root == root_match.parquet_root_index)
                .map(
                    |stream_index| NativeAsyncProviderColumn::ProjectedStreamColumn {
                        stream_index,
                        field_plan: root_match.field_plan.clone(),
                    },
                )
                .ok_or_else(|| {
                    delta_kernel::Error::generic("matched Parquet root was not projected")
                }),
            None if provider_field.is_nullable() => Ok(NativeAsyncProviderColumn::Null),
            None => Err(delta_kernel::Error::generic(format!(
                "non-nullable provider field '{}' is missing from the Parquet file",
                provider_field.name()
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let needs_batch_reshape = provider_columns
        .iter()
        .zip(provider_schema.fields())
        .enumerate()
        .any(|(provider_index, (column, provider_field))| match column {
            NativeAsyncProviderColumn::ProjectedStreamColumn {
                stream_index,
                field_plan,
            } => {
                *stream_index != provider_index
                    || !field_plan.is_identity()
                    || projected_roots
                        .get(*stream_index)
                        .and_then(|root| parquet_arrow_schema.fields().get(*root))
                        .is_none_or(|file_field| file_field.name() != provider_field.name())
            }
            NativeAsyncProviderColumn::Null => true,
        });

    Ok(NativeAsyncSchemaMatch {
        provider_schema,
        projected_roots,
        provider_columns,
        needs_batch_reshape,
    })
}

fn match_provider_field_to_parquet_root(
    provider_field: &Field,
    parquet_roots: &[TypePtr],
    parquet_arrow_schema: &SchemaRef,
) -> Result<Option<NativeAsyncRootMatch>, delta_kernel::Error> {
    if let Some(field_id) = arrow_field_id(provider_field)? {
        let matches = parquet_roots
            .iter()
            .enumerate()
            .filter_map(|(index, root)| (parquet_field_id(root) == Some(field_id)).then_some(index))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => {
                return Ok(Some(NativeAsyncRootMatch {
                    parquet_root_index: *index,
                    field_plan: build_matched_field_plan(
                        provider_field,
                        parquet_arrow_schema.field(*index),
                        parquet_roots[*index].as_ref(),
                        provider_field.name(),
                    )?,
                }));
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched provider field id {field_id}"
                )));
            }
        }
    }

    let Some((index, file_field)) = parquet_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, file_field)| file_field.name() == provider_field.name())
    else {
        return Ok(None);
    };

    Ok(Some(NativeAsyncRootMatch {
        parquet_root_index: index,
        field_plan: build_matched_field_plan(
            provider_field,
            file_field,
            parquet_roots[index].as_ref(),
            provider_field.name(),
        )?,
    }))
}

fn build_matched_field_plan(
    provider_field: &Field,
    file_field: &Field,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    match (provider_field.data_type(), file_field.data_type()) {
        (DataType::Struct(provider_fields), DataType::Struct(file_fields)) => {
            build_matched_struct_field_plan(
                provider_field,
                provider_fields,
                file_field,
                file_fields,
                parquet_field,
                path,
            )
        }
        (DataType::List(provider_element), DataType::List(file_element)) => {
            build_matched_list_field_plan(
                provider_field,
                provider_element,
                file_field,
                file_element,
                parquet_field,
                path,
            )
        }
        (DataType::Map(provider_entries, provider_ordered), DataType::Map(file_entries, _)) => {
            build_matched_map_field_plan(
                provider_entries,
                *provider_ordered,
                file_field,
                file_entries,
                parquet_field,
                path,
            )
        }
        _ => native_async_leaf_cast_plan(provider_field.data_type(), file_field.data_type())
            .map(|target_type| match target_type {
                Some(target_type) => NativeAsyncFieldPlan::Cast { target_type },
                None => NativeAsyncFieldPlan::Identity,
            })
            .map_err(|()| {
                incompatible_parquet_type(path, provider_field.data_type(), file_field.data_type())
            }),
    }
}

fn build_matched_map_field_plan(
    provider_entries: &Arc<Field>,
    provider_ordered: bool,
    file_field: &Field,
    file_entries: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let (provider_key, provider_value) = map_entry_fields(provider_entries, path)?;
    let (file_key, file_value) = map_entry_fields(file_entries, path)?;
    let key_plan = build_matched_field_plan(
        provider_key,
        file_key,
        parquet_map_entry_field(parquet_field, path, 0)?,
        &format!("{path}.key"),
    )?;
    let value_plan = build_matched_field_plan(
        provider_value,
        file_value,
        parquet_map_entry_field(parquet_field, path, 1)?,
        &format!("{path}.value"),
    )?;
    let provider_type = DataType::Map(Arc::clone(provider_entries), provider_ordered);
    if file_field.data_type() != &provider_type
        || !key_plan.is_identity()
        || !value_plan.is_identity()
    {
        Ok(NativeAsyncFieldPlan::Map {
            key_plan: Box::new(key_plan),
            value_plan: Box::new(value_plan),
        })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn map_entry_fields<'a>(
    entries: &'a Field,
    path: &str,
) -> Result<(&'a Field, &'a Field), delta_kernel::Error> {
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected map entries struct but has type {}",
            entries.data_type()
        )));
    };
    if fields.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected map entries to contain key and value fields but found {}",
            fields.len()
        )));
    }
    Ok((fields[0].as_ref(), fields[1].as_ref()))
}

fn parquet_map_entry_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
    entry_index: usize,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet map entry metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected one Parquet map entry child but found {}",
            parquet_children.len()
        )));
    }
    let entry_children = repeated_child.get_fields();
    if entry_children.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet map entry to contain two fields but found {}",
            entry_children.len()
        )));
    }
    entry_children
        .get(entry_index)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            delta_kernel::Error::generic(format!(
                "provider field '{path}' expected Parquet map entry key and value fields"
            ))
        })
}

fn build_matched_list_field_plan(
    provider_field: &Field,
    provider_element: &Arc<Field>,
    file_field: &Field,
    file_element: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let element_path = format!("{path}.element");
    let element_plan = build_matched_field_plan(
        provider_element,
        file_element,
        parquet_list_element_field(parquet_field, path)?,
        &element_path,
    )?;
    if matches!(element_plan, NativeAsyncFieldPlan::Cast { .. }) {
        return Err(incompatible_parquet_type(
            &element_path,
            provider_element.data_type(),
            file_element.data_type(),
        ));
    }
    if file_field.data_type() != provider_field.data_type() || !element_plan.is_identity() {
        Ok(NativeAsyncFieldPlan::List {
            element_plan: Box::new(element_plan),
        })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn parquet_list_element_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet list element metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected one Parquet list child but found {}",
            parquet_children.len()
        )));
    }
    let repeated_child_fields = repeated_child.get_fields();
    if repeated_child_fields.len() == 1 {
        Ok(repeated_child_fields[0].as_ref())
    } else {
        Ok(repeated_child.as_ref())
    }
}

fn build_matched_struct_field_plan(
    provider_field: &Field,
    provider_fields: &Fields,
    file_field: &Field,
    file_fields: &Fields,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    if parquet_children.len() != file_fields.len() {
        return Err(delta_kernel::Error::generic(format!(
            "provider field '{path}' expected Parquet struct field metadata to match Arrow child count"
        )));
    }
    let children = provider_fields
        .iter()
        .map(|provider_child| {
            match_provider_struct_child(
                provider_child,
                file_fields,
                parquet_children,
                &format!("{path}.{}", provider_child.name()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let needs_reshape = file_field.data_type() != provider_field.data_type()
        || children.iter().zip(provider_fields.iter()).enumerate().any(
            |(provider_index, (child, provider_child))| match child {
                NativeAsyncStructChild::ProjectedChild {
                    child_index,
                    field_plan,
                } => {
                    *child_index != provider_index
                        || !field_plan.is_identity()
                        || file_fields
                            .get(*child_index)
                            .is_none_or(|file_child| file_child.name() != provider_child.name())
                }
                NativeAsyncStructChild::Null => true,
            },
        );
    if needs_reshape {
        Ok(NativeAsyncFieldPlan::Struct { children })
    } else {
        Ok(NativeAsyncFieldPlan::Identity)
    }
}

fn match_provider_struct_child(
    provider_child: &Field,
    file_fields: &Fields,
    parquet_children: &[TypePtr],
    path: &str,
) -> Result<NativeAsyncStructChild, delta_kernel::Error> {
    if let Some(field_id) = arrow_field_id(provider_child)? {
        let matches = parquet_children
            .iter()
            .enumerate()
            .filter_map(|(index, child)| {
                (parquet_field_id(child) == Some(field_id)).then_some(index)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => {
                let file_child = file_fields.get(*index).ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "provider field '{path}' matched Parquet field id {field_id} without Arrow metadata"
                    ))
                })?;
                return Ok(NativeAsyncStructChild::ProjectedChild {
                    child_index: *index,
                    field_plan: build_matched_field_plan(
                        provider_child,
                        file_child,
                        parquet_children[*index].as_ref(),
                        path,
                    )?,
                });
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched provider field id {field_id} at '{path}'"
                )));
            }
        }
    }
    let Some((index, file_child)) = file_fields
        .iter()
        .enumerate()
        .find(|(_, file_child)| file_child.name() == provider_child.name())
    else {
        return if provider_child.is_nullable() {
            Ok(NativeAsyncStructChild::Null)
        } else {
            Err(delta_kernel::Error::generic(format!(
                "non-nullable provider field '{path}' is missing from the Parquet file"
            )))
        };
    };
    Ok(NativeAsyncStructChild::ProjectedChild {
        child_index: index,
        field_plan: build_matched_field_plan(
            provider_child,
            file_child,
            parquet_children[index].as_ref(),
            path,
        )?,
    })
}

fn incompatible_parquet_type(
    path: &str,
    provider_type: &DataType,
    file_type: &DataType,
) -> delta_kernel::Error {
    delta_kernel::Error::generic(format!(
        "provider field '{path}' expected Parquet type {provider_type} but found {file_type}"
    ))
}

fn native_async_leaf_cast_plan(
    provider_type: &DataType,
    file_type: &DataType,
) -> Result<Option<DataType>, ()> {
    use DataType::{Date32, Decimal128, Float32, Float64, Int8, Int16, Int32, Int64, Timestamp};

    if file_type.equals_datatype(provider_type) {
        return Ok(None);
    }
    match (file_type, provider_type) {
        (Timestamp(_, _), Timestamp(_, _)) => Ok(Some(provider_type.clone())),
        (Int8, Int16 | Int32 | Int64 | Float64) => Ok(Some(provider_type.clone())),
        (Int16, Int32 | Int64 | Float64) => Ok(Some(provider_type.clone())),
        (Int32, Int64 | Float64) => Ok(Some(provider_type.clone())),
        (Float32, Float64) => Ok(Some(provider_type.clone())),
        (source_type, Decimal128(precision, scale))
            if native_async_can_upcast_to_decimal(source_type, *precision, *scale) =>
        {
            Ok(Some(provider_type.clone()))
        }
        (Date32, Timestamp(_, None)) => Ok(Some(provider_type.clone())),
        (Int32, Date32) => Ok(Some(provider_type.clone())),
        (Int64, Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)) => {
            Ok(Some(provider_type.clone()))
        }
        _ => Err(()),
    }
}

fn native_async_can_upcast_to_decimal(
    source_type: &DataType,
    target_precision: u8,
    target_scale: i8,
) -> bool {
    use DataType::{Decimal128, Int8, Int16, Int32, Int64};

    let (source_precision, source_scale) = match source_type {
        Decimal128(precision, scale) => (*precision, *scale),
        Int8 => (3, 0),
        Int16 => (5, 0),
        Int32 => (10, 0),
        Int64 => (20, 0),
        _ => return false,
    };
    target_precision >= source_precision
        && target_scale >= source_scale
        && target_precision - source_precision >= (target_scale - source_scale) as u8
}

fn arrow_field_id(field: &Field) -> Result<Option<i32>, delta_kernel::Error> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|field_id| {
            field_id.parse::<i32>().map_err(|error| {
                delta_kernel::Error::generic(format!(
                    "invalid provider field id metadata on '{}': {error}",
                    field.name()
                ))
            })
        })
        .transpose()
}

fn parquet_field_id(parquet_field: &TypePtr) -> Option<i32> {
    let basic_info = parquet_field.get_basic_info();
    basic_info.has_id().then(|| basic_info.id())
}

fn reshape_array_to_provider_field(
    array: ArrayRef,
    provider_field: &Field,
    field_plan: &NativeAsyncFieldPlan,
) -> Result<ArrayRef, delta_kernel::Error> {
    match field_plan {
        NativeAsyncFieldPlan::Identity => Ok(array),
        NativeAsyncFieldPlan::Cast { target_type } => {
            cast(array.as_ref(), target_type).map_err(delta_kernel::Error::from)
        }
        NativeAsyncFieldPlan::Struct { children } => {
            let DataType::Struct(provider_fields) = provider_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected struct reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "provider field '{}' expected Parquet struct array but found {}",
                        provider_field.name(),
                        array.data_type()
                    ))
                })?;
            let columns = children
                .iter()
                .zip(provider_fields.iter())
                .map(|(child, provider_child)| match child {
                    NativeAsyncStructChild::ProjectedChild {
                        child_index,
                        field_plan,
                    } => reshape_array_to_provider_field(
                        Arc::clone(struct_array.column(*child_index)),
                        provider_child,
                        field_plan,
                    ),
                    NativeAsyncStructChild::Null => Ok(new_null_array(
                        provider_child.data_type(),
                        struct_array.len(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::new(
                provider_fields.clone(),
                columns,
                struct_array.nulls().cloned(),
            )))
        }
        NativeAsyncFieldPlan::List { element_plan } => {
            let DataType::List(provider_element) = provider_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected list reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "provider field '{}' expected Parquet list array but found {}",
                    provider_field.name(),
                    array.data_type()
                ))
            })?;
            let values = reshape_array_to_provider_field(
                Arc::clone(list_array.values()),
                provider_element,
                element_plan,
            )?;
            ListArray::try_new(
                Arc::clone(provider_element),
                list_array.offsets().clone(),
                values,
                list_array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
        NativeAsyncFieldPlan::Map {
            key_plan,
            value_plan,
        } => {
            let DataType::Map(provider_entries, provider_ordered) = provider_field.data_type()
            else {
                return Err(delta_kernel::Error::generic(format!(
                    "provider field '{}' expected map reshape plan but has type {}",
                    provider_field.name(),
                    provider_field.data_type()
                )));
            };
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "provider field '{}' expected Parquet map array but found {}",
                    provider_field.name(),
                    array.data_type()
                ))
            })?;
            let (provider_key, provider_value) =
                map_entry_fields(provider_entries, provider_field.name())?;
            let keys = reshape_array_to_provider_field(
                Arc::clone(map_array.keys()),
                provider_key,
                key_plan,
            )?;
            let values = reshape_array_to_provider_field(
                Arc::clone(map_array.values()),
                provider_value,
                value_plan,
            )?;
            let entries = StructArray::new(
                vec![
                    Arc::new(provider_key.clone()),
                    Arc::new(provider_value.clone()),
                ]
                .into(),
                vec![keys, values],
                map_array.entries().nulls().cloned(),
            );
            MapArray::try_new(
                Arc::clone(provider_entries),
                map_array.offsets().clone(),
                entries,
                map_array.nulls().cloned(),
                *provider_ordered,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
    }
}

fn data_file_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    Err::<(), _>(source)
        .boxed()
        .context(DataFileReadSnafu { reason })
        .expect_err("constructed data-file error")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path as FsPath, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{
            Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
            TimestampMicrosecondArray, TimestampNanosecondArray,
        },
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use delta_kernel::scan::state::{DvInfo, ScanFile};
    use delta_kernel::{
        actions::deletion_vector_writer::{KernelDeletionVector, StreamingDeletionVectorWriter},
        expressions::{ColumnName, Expression, Predicate, Scalar},
    };
    use futures_util::StreamExt;
    use object_store::ObjectStoreExt;
    use parquet::arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, ProjectionMask,
        arrow_reader::ParquetRecordBatchReaderBuilder,
    };
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::{NativeAsyncFileReader, data_file_error, native_async_file_executor};
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions,
        DeltaSnapshotSelection, DeltaStorageOptions,
        kernel::{DeltaKernelEngineContext, DeltaKernelPredicate, KernelScanFileMetadata},
        metrics::DeltaReadMetricsConfig,
        planning::{DeltaScanFileTask, DeltaScanPartitionTargetOptions, plan_scan},
        scheduling::{DeltaScanExecution, FileAdmission},
        snapshot::load_delta_table_snapshot_blocking,
    };

    const DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
    const DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = std::env::temp_dir().join(format!(
                "delta-arrow-reader-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &FsPath {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn metrics() -> DeltaReadMetrics {
        DeltaReadMetrics::new(DeltaReadMetricsConfig {
            snapshot_version: 1,
            reader_backend: DeltaReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 1,
            files_filtered_during_planning: Some(0),
            estimated_rows: Some(1),
            estimated_bytes: Some(1),
        })
    }

    fn reader(
        root: &TestDir,
        options: DeltaReaderExecutionOptions,
        metrics: DeltaReadMetrics,
    ) -> Result<NativeAsyncFileReader, Box<dyn std::error::Error>> {
        let table_url = url::Url::from_directory_path(root.path())
            .map_err(|()| "temporary table path cannot become a file URL")?;
        let engine_context = Arc::new(DeltaKernelEngineContext::build(
            table_url,
            &DeltaStorageOptions::default(),
        )?);
        Ok(NativeAsyncFileReader::new(engine_context, options, metrics))
    }

    fn task(path: &str, file_size: Option<u64>) -> Result<DeltaScanFileTask, DeltaReaderError> {
        let size = file_size
            .map(i64::try_from)
            .transpose()
            .map_err(|error| data_file_error("test_file_size_overflow", error))?
            .unwrap_or(1);
        let mut task =
            DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(ScanFile {
                path: path.to_owned(),
                size,
                modification_time: 0,
                stats: None,
                dv_info: DvInfo::default(),
                transform: None,
                partition_values: HashMap::new(),
            }))?;
        task.estimated_bytes = file_size;
        Ok(task)
    }

    fn parquet_bytes_for(
        schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
        writer.write(&batch)?;
        Ok(writer.into_inner()?)
    }

    fn parquet_bytes_with_properties(
        schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        properties: WriterProperties,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
        writer.write(&batch)?;
        Ok(writer.into_inner()?)
    }

    fn parquet_bytes() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        parquet_bytes_for(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
    }

    fn write_partitioned_dv_table(
        root: &TestDir,
        parquet_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        fs::write(root.path().join("part.parquet"), parquet_bytes)?;
        let mut dv_bytes = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut dv_bytes);
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes([4]);
        let result = writer.write_deletion_vector(deletion_vector)?;
        writer.finalize()?;
        fs::write(root.path().join(DV_FILE), dv_bytes)?;

        let log = root.path().join("_delta_log");
        fs::create_dir_all(&log)?;
        let protocol = serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["deletionVectors"]
            }
        });
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "region", "type": "string", "nullable": true, "metadata": {}}
            ]
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "native-async-pipeline-test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["region"],
                "configuration": {},
                "createdTime": 1587968585495_i64
            }
        });
        let stats = serde_json::json!({
            "numRecords": 6,
            "minValues": {"id": 1},
            "maxValues": {"id": 6},
            "nullCount": {"id": 0}
        });
        let add = serde_json::json!({
            "add": {
                "path": "part.parquet",
                "partitionValues": {"region": "west"},
                "size": parquet_bytes.len(),
                "modificationTime": 1587968586000_i64,
                "dataChange": true,
                "stats": stats.to_string(),
                "deletionVector": {
                    "storageType": "u",
                    "pathOrInlineDv": DV_ID,
                    "offset": result.offset,
                    "sizeInBytes": result.size_in_bytes,
                    "cardinality": result.cardinality
                }
            }
        });
        fs::write(
            log.join("00000000000000000000.json"),
            format!("{protocol}\n{metadata}\n{add}\n"),
        )?;
        Ok(())
    }

    fn pipeline_plan(
        root: &TestDir,
        full_file_threshold: Option<usize>,
    ) -> Result<Arc<crate::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        let snapshot = load_delta_table_snapshot_blocking(
            &root.path().to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let options = DeltaReaderExecutionOptions::new()
            .with_parquet_full_file_read_threshold(full_file_threshold)?;
        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::gt(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(3)),
        ));
        Ok(Arc::new(plan_scan(
            &snapshot,
            Some(&["id".to_owned()]),
            &["region".to_owned()],
            Some(predicate),
            true,
            options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(1),
                caller_target_partitions: None,
            },
        )?))
    }

    async fn execute_pipeline_plan(
        plan: Arc<crate::planning::DeltaScanPlan>,
    ) -> Result<Vec<RecordBatch>, DeltaReaderError> {
        let execution = DeltaScanExecution::new(Arc::clone(&plan));
        let executor = native_async_file_executor(&plan, Some(2));
        let mut stream =
            execution.partition_stream(0, Arc::new(|_| Ok(FileAdmission::Admit)), executor)?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]))
    }

    fn field_id_metadata(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())])
    }

    fn struct_field(name: &str, fields: Vec<Field>, nullable: bool) -> Field {
        Field::new(name, DataType::Struct(fields.into()), nullable)
    }

    fn timestamp_us_utc_field(name: &str, nullable: bool) -> Field {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            nullable,
        )
    }

    fn struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> ArrayRef {
        struct_array_with_nulls(fields, columns, None)
    }

    fn struct_array_with_nulls(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        Arc::new(StructArray::new(
            fields.into_iter().map(Arc::new).collect::<Vec<_>>().into(),
            columns,
            nulls,
        ))
    }

    fn list_array(
        element: Field,
        offsets: Vec<i32>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        Ok(Arc::new(ListArray::try_new(
            Arc::new(element),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            nulls,
        )?))
    }

    fn map_field(name: &str, key: Field, value: Field, nullable: bool) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key, value].into()),
                    false,
                )),
                false,
            ),
            nullable,
        )
    }

    fn map_array(
        key: Field,
        value: Field,
        offsets: Vec<i32>,
        keys: ArrayRef,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        let entries = vec![key.clone(), value.clone()].into();
        Ok(Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            StructArray::new(
                vec![Arc::new(key), Arc::new(value)].into(),
                vec![keys, values],
                None,
            ),
            nulls,
            false,
        )?))
    }

    fn project_parquet_batch_to_provider_schema(
        name: &str,
        file_schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        provider_schema: Arc<Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let root = TestDir::new(name)?;
        let file_path = root.path().join("part.parquet");
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), columns)?;
        let mut writer = ArrowWriter::try_new(fs::File::create(&file_path)?, file_schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(file_path)?)?;
        let schema_match = super::build_native_async_schema_match(
            builder.parquet_schema(),
            builder.schema(),
            provider_schema,
        )?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_match.projected_roots());
        let projected = builder
            .with_projection(projection)
            .build()?
            .next()
            .transpose()?
            .ok_or("expected one projected Parquet batch")?;
        Ok(schema_match.reshape_batch_to_provider_schema(projected)?)
    }

    #[tokio::test]
    async fn resolves_table_relative_paths_and_requires_file_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-object-resolution")?;
        fs::write(root.path().join("part.parquet"), b"data")?;
        let metrics = metrics();
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics.clone())?;

        let object = reader
            .parquet_object_for_task(&task("part.parquet", Some(4))?)
            .await?;
        assert!(object.path.as_ref().ends_with("part.parquet"));
        assert_eq!(object.file_size, 4);
        assert_eq!(
            object.store.get_range(&object.path, 1..3).await?.as_ref(),
            b"at"
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_range_get_operations,
            Some(1)
        );

        let error = reader
            .parquet_object_for_task(&task("secret-file.parquet", None)?)
            .await
            .expect_err("missing size must fail");
        assert_eq!(error.as_str(), "data_file_read");
        assert!(!error.to_string().contains("secret-file"));
        Ok(())
    }

    #[tokio::test]
    async fn threshold_buffers_only_eligible_files_with_one_metered_full_get()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"0123456789abcdef";

        for (name, threshold, expect_buffered) in [
            ("disabled", None, false),
            ("below", Some(bytes.len() - 1), false),
            ("exact", Some(bytes.len()), true),
            ("above", Some(bytes.len() + 1), true),
        ] {
            let root = TestDir::new(name)?;
            fs::write(root.path().join("part.parquet"), bytes)?;
            let metrics = metrics();
            let options = DeltaReaderExecutionOptions::new()
                .with_parquet_full_file_read_threshold(threshold)?;
            let reader = reader(&root, options, metrics.clone())?;
            let object = reader
                .parquet_object_for_task(&task("part.parquet", Some(u64::try_from(bytes.len())?))?)
                .await?;
            let snapshot = metrics.snapshot();
            assert_eq!(
                snapshot.parquet_data_file_full_get_operations,
                Some(u64::from(expect_buffered)),
                "{name}"
            );
            assert_eq!(
                snapshot.parquet_data_file_bytes_received,
                Some(if expect_buffered {
                    u64::try_from(bytes.len())?
                } else {
                    0
                }),
                "{name}"
            );

            assert_eq!(
                object.store.get_range(&object.path, 2..7).await?.as_ref(),
                b"23456",
                "{name}"
            );
            assert_eq!(
                metrics.snapshot().parquet_data_file_range_get_operations,
                Some(u64::from(!expect_buffered)),
                "{name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn opens_projected_parquet_stream_with_configured_batch_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-projected-stream")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let provider_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let mut stream = reader
            .open_parquet_stream(&task, provider_schema, Some(2), None, false)
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            batches.push(batch);
        }

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().field(0).name() == "name")
        );
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected projected StringArray")?;
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(1));
        Ok(())
    }

    #[tokio::test]
    async fn reads_full_ordered_and_empty_physical_projections()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-projection-shapes")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;

        for (schema, names, columns) in [
            (
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec!["id", "name"],
                2,
            ),
            (
                Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("id", DataType::Int32, false),
                ])),
                vec!["name", "id"],
                2,
            ),
            (Arc::new(Schema::empty()), Vec::new(), 0),
        ] {
            let mut stream = reader
                .open_parquet_stream(&task, schema, None, None, false)
                .await?;
            let mut rows = 0;
            while let Some(batch) = stream.next_batch().await? {
                rows += batch.num_rows();
                assert_eq!(batch.num_columns(), columns);
                assert_eq!(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>(),
                    names
                );
            }
            assert_eq!(rows, 3);
        }
        Ok(())
    }

    #[tokio::test]
    async fn footer_hint_controls_metadata_request_count_and_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-footer-hint")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let file_size = u64::try_from(bytes.len())?;
        let mut snapshots = Vec::new();

        for hint in [Some(65_536), None, Some(9)] {
            let metrics = metrics();
            let options =
                DeltaReaderExecutionOptions::new().with_parquet_metadata_size_hint(hint)?;
            let reader = reader(&root, options, metrics.clone())?;
            let task = task("part.parquet", Some(file_size))?;
            let provider_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let _stream = reader
                .open_parquet_stream(&task, provider_schema, None, None, false)
                .await?;
            snapshots.push(metrics.snapshot());
        }

        assert_eq!(snapshots[0].parquet_data_file_range_get_operations, Some(1));
        assert_eq!(
            snapshots[0].parquet_data_file_bytes_received,
            Some(file_size)
        );
        assert_eq!(snapshots[1].parquet_data_file_range_get_operations, Some(2));
        assert_eq!(snapshots[2].parquet_data_file_range_get_operations, Some(2));
        assert_eq!(
            snapshots[2].parquet_data_file_bytes_received,
            snapshots[1]
                .parquet_data_file_bytes_received
                .map(|bytes| bytes + 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn row_group_pruning_is_conservative_and_preserves_rows_when_disabled()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-row-group-pruning")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let columns = || vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef];
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let bytes = parquet_bytes_with_properties(Arc::clone(&schema), columns(), properties)?;
        fs::write(root.path().join("with-stats.parquet"), &bytes)?;
        let no_stats_properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let no_stats_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns(), no_stats_properties)?;
        fs::write(root.path().join("without-stats.parquet"), &no_stats_bytes)?;
        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::gt(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(3)),
        ));
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics())?;

        for (path, file_size, predicate, expected) in [
            (
                "with-stats.parquet",
                bytes.len(),
                Some(&predicate),
                vec![4, 5, 6],
            ),
            (
                "with-stats.parquet",
                bytes.len(),
                None,
                vec![1, 2, 3, 4, 5, 6],
            ),
            (
                "without-stats.parquet",
                no_stats_bytes.len(),
                Some(&predicate),
                vec![1, 2, 3, 4, 5, 6],
            ),
        ] {
            let task = task(path, Some(u64::try_from(file_size)?))?;
            let mut stream = reader
                .open_parquet_stream(&task, Arc::clone(&schema), None, predicate, false)
                .await?;
            let mut ids = Vec::new();
            while let Some(batch) = stream.next_batch().await? {
                ids.extend_from_slice(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("expected Int32Array")?
                        .values(),
                );
            }
            assert_eq!(ids, expected, "{path}");
        }

        let task = task("with-stats.parquet", Some(u64::try_from(bytes.len())?))?;
        let mut stream = reader
            .open_parquet_stream(&task, Arc::clone(&schema), None, Some(&predicate), true)
            .await?;
        let mut original_indexes = Vec::new();
        while let Some((_batch, indexes)) = stream.next_batch_with_original_row_indexes().await? {
            original_indexes.extend(
                indexes
                    .ok_or("expected original row indexes")?
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(original_indexes, [3, 4, 5]);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_pipeline_applies_transform_then_dv_and_preserves_hidden_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-scheduler-pipeline")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            properties,
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;

        let direct_plan = pipeline_plan(&root, None)?;
        let direct_metrics = direct_plan.metrics.clone();
        let direct = execute_pipeline_plan(direct_plan).await?;
        let buffered_plan = pipeline_plan(&root, Some(parquet_bytes.len()))?;
        let buffered_metrics = buffered_plan.metrics.clone();
        let buffered = execute_pipeline_plan(buffered_plan).await?;

        for batches in [&direct, &buffered] {
            let ids = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("planned id Int32Array")
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, [4, 6]);
            assert!(batches.iter().all(|batch| {
                batch.schema().fields()[0].name() == "id"
                    && batch.schema().fields()[1].name() == "region"
                    && batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .is_some_and(|regions| {
                            (0..regions.len()).all(|index| regions.value(index) == "west")
                        })
            }));
        }
        assert_eq!(direct, buffered);

        let direct_metrics = direct_metrics.snapshot();
        assert_eq!(direct_metrics.files_started, 1);
        assert_eq!(direct_metrics.files_completed, 1);
        assert_eq!(
            direct_metrics.parquet_data_file_opened_bytes,
            Some(u64::try_from(parquet_bytes.len())?)
        );
        assert_eq!(direct_metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(direct_metrics.deletion_vectors_applied, 1);
        assert_eq!(direct_metrics.deletion_vector_rows_deleted, 1);
        assert_eq!(
            direct_metrics.parquet_data_file_full_get_operations,
            Some(0)
        );
        assert!(
            direct_metrics
                .parquet_data_file_range_get_operations
                .is_some_and(|count| count > 0)
        );
        let buffered_metrics = buffered_metrics.snapshot();
        assert_eq!(
            buffered_metrics.parquet_data_file_full_get_operations,
            Some(1)
        );
        assert_eq!(
            buffered_metrics.parquet_data_file_range_get_operations,
            Some(0)
        );
        assert_eq!(
            buffered_metrics.parquet_data_file_bytes_received,
            Some(u64::try_from(parquet_bytes.len())?)
        );
        assert_eq!(
            buffered_metrics.parquet_data_file_opened_bytes,
            direct_metrics.parquet_data_file_opened_bytes
        );
        Ok(())
    }

    #[test]
    fn native_async_leaf_cast_plan_matches_timestamp_compatibility()
    -> Result<(), Box<dyn std::error::Error>> {
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        assert_eq!(
            super::native_async_leaf_cast_plan(
                &target,
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            ),
            Ok(Some(target.clone()))
        );
        assert_eq!(
            super::native_async_leaf_cast_plan(
                &target,
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            ),
            Ok(Some(target))
        );

        Ok(())
    }

    #[test]
    fn native_async_leaf_cast_plan_rejects_incompatible_primitive_types()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            super::native_async_leaf_cast_plan(&DataType::Int32, &DataType::Utf8),
            Err(())
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_casts_top_level_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_schema = Arc::new(Schema::new(vec![timestamp_us_utc_field("event_ts", true)]));
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = project_parquet_batch_to_provider_schema(
            "top-level-timestamp-leaf-cast",
            file_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_067_200_000_000_000),
                None,
            ])) as ArrayRef],
            provider_schema,
        )?;
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn native_async_reshape_casts_top_level_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_field = timestamp_us_utc_field("event_ts", true);
        let array = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_704_067_200_000_000_000),
            None,
        ])) as ArrayRef;

        let reshaped = super::reshape_array_to_provider_field(
            array,
            &provider_field,
            &super::NativeAsyncFieldPlan::Cast {
                target_type: provider_field.data_type().clone(),
            },
        )?;
        let timestamps = reshaped
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn native_async_reshape_casts_nested_struct_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_child = timestamp_us_utc_field("event_ts", true);
        let provider_field = struct_field("payload", vec![provider_child.clone()], true);
        let file_child = Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        );
        let array = struct_array(
            vec![file_child],
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_153_600_000_000_000),
                None,
            ])) as ArrayRef],
        );

        let reshaped = super::reshape_array_to_provider_field(
            array,
            &provider_field,
            &super::NativeAsyncFieldPlan::Struct {
                children: vec![super::NativeAsyncStructChild::ProjectedChild {
                    child_index: 0,
                    field_plan: super::NativeAsyncFieldPlan::Cast {
                        target_type: provider_child.data_type().clone(),
                    },
                }],
            },
        )?;
        let payload = reshaped
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected StructArray")?;
        let timestamps = payload
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(payload.fields()[0].name(), "event_ts");
        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_153_600_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn native_async_schema_match_casts_list_struct_leaf() -> Result<(), Box<dyn std::error::Error>>
    {
        let provider_element = Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("city", DataType::Utf8, true),
                    Field::new("zip", DataType::Int64, true),
                ]
                .into(),
            ),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("zip", DataType::Int32, true),
        ];
        let file_element = Field::new(
            "element",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(94110),
                    Some(10001),
                    Some(60601),
                ])) as ArrayRef,
            ],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 3],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "list-struct-leaf-cast-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected StructArray list values")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected Int64Array zip values")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 3]);
        assert!(addresses.is_valid(0));
        assert!(addresses.is_null(1));
        assert!(addresses.is_valid(2));
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.values(), &[94110, 10001, 60601]);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_list_primitive_leaf_cast()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_schema = Arc::new(Schema::new(vec![
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
                true,
            ),
            Field::new("id", DataType::Int32, false),
        ]));
        let file_element = Field::new("element", DataType::Int32, true);
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("tags", DataType::List(Arc::new(file_element.clone())), true),
        ]));
        let tags = list_array(
            file_element,
            vec![0, 2, 2, 3],
            Arc::new(Int32Array::from(vec![Some(7), Some(11), None])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let error = match project_parquet_batch_to_provider_schema(
            "list-primitive-leaf-cast-schema-match",
            file_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as ArrayRef,
                tags,
            ],
            provider_schema,
        ) {
            Ok(_) => return Err("primitive list element cast must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("tags.element"), "{error}");
        assert!(error.contains("expected Parquet type Int64"), "{error}");
        assert!(error.contains("found Int32"), "{error}");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_casts_map_key_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
            true,
        )]));
        let file_key = Field::new("key", DataType::Int32, false);
        let file_value = Field::new("value", DataType::Utf8, true);
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![file_key.clone(), file_value.clone()].into()),
                    false,
                )),
                false,
            ),
            true,
        )]));
        let attributes = map_array(
            file_key,
            file_value,
            vec![0, 2, 2, 3],
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-key-leaf-cast-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected Int64Array map keys")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected StringArray map values")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.values(), &[10, 20, 30]);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "mailing");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_recurses_by_nested_field_id_before_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("stale_age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array_with_nulls(
            file_profile_fields,
            vec![
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, false])),
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-field-id-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let names = profile
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;
        let ages = profile
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;

        assert_eq!(profile.fields()[0].name(), "first_name");
        assert_eq!(profile.fields()[1].name(), "age");
        assert!(profile.is_valid(0));
        assert!(profile.is_null(1));
        assert_eq!(names.value(0), "alice");
        assert_eq!(ages.value(0), 34);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_list_struct_elements_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 3],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "list-struct-field-id-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 3]);
        assert!(addresses.is_valid(0));
        assert!(addresses.is_null(1));
        assert!(addresses.is_valid(2));
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_recurses_by_local_nested_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("first_name", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
            ],
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-name-fallback-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let ages = profile
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;
        let names = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;

        assert_eq!(profile.fields()[0].name(), "age");
        assert_eq!(profile.fields()[1].name(), "first_name");
        assert_eq!(ages.values(), &[34, 41]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("loyalty_tier", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );

        let batch = project_parquet_batch_to_provider_schema(
            "nested-missing-nullable-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let loyalty_tiers = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected loyalty_tier StringArray")?;

        assert_eq!(profile.fields()[1].name(), "loyalty_tier");
        assert_eq!(loyalty_tiers.len(), 2);
        assert_eq!(loyalty_tiers.null_count(), 2);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 5],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "list-struct-missing-nullable-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 5]);
        assert!(addresses.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_address_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(provider_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let addresses = list_array(file_element, vec![0, 2], values, None)?;
        let error = match project_parquet_batch_to_provider_schema(
            "list-struct-missing-required-schema-match",
            file_schema,
            vec![addresses],
            provider_schema,
        ) {
            Ok(_) => return Err("missing non-nullable list struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable provider field"), "{error}");
        assert!(
            error.contains("addresses.element.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_map_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(provider_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let values = Arc::new(StringArray::from(vec![
            Some("home"),
            Some("work"),
            Some("other"),
        ])) as ArrayRef;
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            keys,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map value StringArray")?;
        let cities = keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.fields()[0].name(), "city");
        assert_eq!(keys.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "other");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_map_key_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_key_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(provider_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 5],
            keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
                Some("billing"),
                Some("shipping"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-key-struct-missing-nullable-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let countries = keys
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 5]);
        assert!(attributes.is_null(1));
        assert_eq!(keys.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_map_key_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_key_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(provider_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2],
            keys,
            Arc::new(StringArray::from(vec![Some("home"), Some("work")])) as ArrayRef,
            None,
        )?;
        let error = match project_parquet_batch_to_provider_schema(
            "map-key-struct-missing-required-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        ) {
            Ok(_) => return Err("missing non-nullable map key struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable provider field"), "{error}");
        assert!(error.contains("attributes.key.required_country"), "{error}");
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_map_list_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_element_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_element = Field::new(
            "item",
            DataType::Struct(provider_element_fields.into()),
            true,
        );
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::List(Arc::new(provider_element)), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_element_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_element_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::List(Arc::new(file_element.clone())),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let key_elements = struct_array(
            file_element_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let keys = list_array(file_element, vec![0, 2, 2, 3], key_elements, None)?;
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("stale_zip", DataType::Int32, true)
                                .with_metadata(field_id_metadata(10)),
                            Field::new("stale_city", DataType::Utf8, true)
                                .with_metadata(field_id_metadata(11)),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-list-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected map key ListArray")?;
        let key_elements = keys
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected key element StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map value StringArray")?;
        let cities = key_elements
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = key_elements
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(key_elements.fields()[0].name(), "city");
        assert_eq!(key_elements.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "other");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_nested_map_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_inner_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_inner_key = Field::new(
            "keys",
            DataType::Struct(provider_inner_key_fields.into()),
            false,
        );
        let provider_outer_key = map_field(
            "keys",
            provider_inner_key,
            Field::new("values", DataType::Int32, true),
            false,
        );
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            provider_outer_key,
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_inner_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_inner_key = Field::new(
            "keys",
            DataType::Struct(file_inner_key_fields.clone().into()),
            false,
        );
        let file_outer_key = map_field(
            "keys",
            file_inner_key.clone(),
            Field::new("values", DataType::Int32, true),
            false,
        );
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            file_outer_key.clone(),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let inner_keys = struct_array(
            file_inner_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let outer_keys = map_array(
            file_inner_key,
            Field::new("values", DataType::Int32, true),
            vec![0, 2, 2, 3],
            inner_keys,
            Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
            None,
        )?;
        let attributes = map_array(
            file_outer_key,
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            outer_keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "nested-map-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let outer_keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected outer key MapArray")?;
        let inner_keys = outer_keys
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected inner key StructArray")?;
        let outer_values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected outer value StringArray")?;
        let inner_values = outer_keys
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected inner value Int32Array")?;
        let cities = inner_keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = inner_keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(outer_keys.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_null(1));
        assert_eq!(inner_keys.fields()[0].name(), "city");
        assert_eq!(inner_keys.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(inner_values.value(0), 7);
        assert_eq!(inner_values.value(2), 9);
        assert_eq!(outer_values.value(0), "home");
        assert_eq!(outer_values.value(2), "other");

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_map_key_and_value_structs_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_value_fields = vec![
            Field::new("label", DataType::Utf8, true).with_metadata(field_id_metadata(21)),
            Field::new("score", DataType::Int32, true).with_metadata(field_id_metadata(20)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(provider_key_fields.into()), false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_value_fields = vec![
            Field::new("stale_score", DataType::Int32, true).with_metadata(field_id_metadata(20)),
            Field::new("stale_label", DataType::Utf8, true).with_metadata(field_id_metadata(21)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let values = struct_array(
            file_value_fields,
            vec![
                Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("home"),
                    Some("work"),
                    Some("other"),
                ])) as ArrayRef,
            ],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "values",
                DataType::Struct(
                    vec![
                        Field::new("stale_score", DataType::Int32, true)
                            .with_metadata(field_id_metadata(20)),
                        Field::new("stale_label", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(21)),
                    ]
                    .into(),
                ),
                true,
            ),
            vec![0, 2, 2, 3],
            keys,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-key-and-value-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let cities = keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;
        let labels = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected label StringArray")?;
        let scores = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected score Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_null(1));
        assert_eq!(keys.fields()[0].name(), "city");
        assert_eq!(keys.fields()[1].name(), "zip");
        assert_eq!(values.fields()[0].name(), "label");
        assert_eq!(values.fields()[1].name(), "score");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(labels.value(0), "home");
        assert_eq!(scores.value(0), 7);
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(2), 60601);
        assert_eq!(labels.value(2), "other");
        assert_eq!(scores.value(2), 9);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_reshapes_map_value_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                true,
            ),
            vec![0, 2, 2, 3],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-value-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map key StringArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.value(0), "home");
        assert_eq!(keys.value(2), "other");
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_null_fills_missing_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2, 2, 5],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
                Some("billing"),
                Some("shipping"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_provider_schema(
            "map-value-struct-missing-nullable-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 5]);
        assert!(attributes.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let provider_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(provider_value_fields.into()),
                true,
            ),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2],
            Arc::new(StringArray::from(vec![Some("home"), Some("work")])) as ArrayRef,
            values,
            None,
        )?;
        let error = match project_parquet_batch_to_provider_schema(
            "map-value-struct-missing-required-schema-match",
            file_schema,
            vec![attributes],
            provider_schema,
        ) {
            Ok(_) => return Err("missing non-nullable map value struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable provider field"), "{error}");
        assert!(
            error.contains("attributes.value.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn native_async_schema_match_rejects_missing_non_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("required_code", DataType::Utf8, false),
        ];
        let provider_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            provider_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );
        let error = match project_parquet_batch_to_provider_schema(
            "nested-missing-required-schema-match",
            file_schema,
            vec![profile],
            provider_schema,
        ) {
            Ok(_) => return Err("missing nested required child must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("non-nullable provider field"), "{display}");
        assert!(display.contains("profile.required_code"), "{display}");
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn matches_top_level_fields_by_id_reorders_casts_and_null_fills()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-top-level-schema-match")?;
        let file_schema = Arc::new(Schema::new(vec![
            field_with_id("stale_name", DataType::Utf8, true, 2),
            field_with_id("stale_id", DataType::Int32, false, 1),
        ]));
        let bytes = parquet_bytes_for(
            file_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let provider_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int64, false, 1),
            field_with_id("name", DataType::Utf8, true, 2),
            Field::new("added", DataType::Utf8, true),
        ]));
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let mut stream = reader
            .open_parquet_stream(&task, provider_schema, None, None, false)
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "added"]
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected cast Int64Array")?
                .values(),
            &[1, 2, 3]
        );
        assert_eq!(batch.column(2).null_count(), 3);
        assert!(stream.next_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn casts_top_level_timestamp_and_rejects_incompatible_or_missing_required_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("native-top-level-casts")?;
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let bytes = parquet_bytes_for(
            file_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_067_200_000_000_000),
                None,
            ]))],
        )?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaReaderExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let timestamp_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let mut stream = reader
            .open_parquet_stream(&task, timestamp_schema, None, None, false)
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;
        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        for provider_schema in [
            Arc::new(Schema::new(vec![Field::new(
                "event_ts",
                DataType::Utf8,
                true,
            )])),
            Arc::new(Schema::new(vec![Field::new(
                "required",
                DataType::Int32,
                false,
            )])),
        ] {
            let error = match reader
                .open_parquet_stream(&task, provider_schema, None, None, false)
                .await
            {
                Ok(_) => return Err("unsupported schema must fail".into()),
                Err(error) => error,
            };
            assert_eq!(error.as_str(), "data_file_read");
            assert_eq!(
                error.to_string(),
                "delta reader error: phase=data_file_read error=data_file_read reason=parquet_schema_match_failed"
            );
        }
        Ok(())
    }
}
