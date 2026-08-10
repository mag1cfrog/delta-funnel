//! NativeAsync Parquet data-file reader.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::{
    PARQUET_FIELD_ID_META_KEY, ProjectionMask,
    async_reader::{
        ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
    },
};
use parquet::schema::types::{SchemaDescriptor, TypePtr};
use snafu::ResultExt;

use crate::{
    DeltaReadMetrics, DeltaReaderError, DeltaReaderExecutionOptions, error::DataFileReadSnafu,
    kernel::DeltaKernelEngineContext, metered_object_store::MeteredParquetObjectStore,
    planning::DeltaScanFileTask,
};

pub(crate) struct NativeAsyncFileReader {
    engine_context: Arc<DeltaKernelEngineContext>,
    store: Arc<dyn ObjectStore>,
    execution_options: DeltaReaderExecutionOptions,
}

#[derive(Debug)]
pub(crate) struct NativeAsyncParquetObject {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) file_size: u64,
}

pub(crate) struct NativeAsyncFileReadStream {
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    schema_match: NativeAsyncSchemaMatch,
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
    Cast { target_type: DataType },
}

impl NativeAsyncFieldPlan {
    fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }
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
            metrics,
        ));
        Self {
            engine_context,
            store,
            execution_options,
        }
    }

    pub(crate) async fn parquet_object_for_task(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<NativeAsyncParquetObject, DeltaReaderError> {
        let object = self.resolve_parquet_object(task)?;
        self.buffer_small_parquet_object(object).await
    }

    async fn open_file_stream(
        &self,
        task: &DeltaScanFileTask,
        provider_schema: SchemaRef,
        output_batch_size: Option<usize>,
    ) -> Result<NativeAsyncFileReadStream, DeltaReaderError> {
        let object = self.parquet_object_for_task(task).await?;
        let reader =
            ParquetObjectReader::new(object.store, object.path).with_file_size(object.file_size);
        let reader = match self.execution_options.parquet_metadata_size_hint() {
            Some(hint) => reader.with_footer_size_hint(hint),
            None => reader,
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
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

        Ok(NativeAsyncFileReadStream {
            stream,
            schema_match,
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

impl NativeAsyncFileReadStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        let Some(batch) = self.stream.next().await else {
            return Ok(None);
        };
        let batch = batch.boxed().context(DataFileReadSnafu {
            reason: "parquet_batch_read_failed",
        })?;
        if !self.schema_match.needs_batch_reshape {
            return Ok(Some(batch));
        }

        self.schema_match
            .reshape_batch_to_provider_schema(batch)
            .map(Some)
            .map_err(|error| data_file_error("parquet_batch_reshape_failed", error))
    }
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
        field_plan: build_matched_field_plan(provider_field, file_field, provider_field.name())?,
    }))
}

fn build_matched_field_plan(
    provider_field: &Field,
    file_field: &Field,
    path: &str,
) -> Result<NativeAsyncFieldPlan, delta_kernel::Error> {
    if file_field
        .data_type()
        .equals_datatype(provider_field.data_type())
    {
        return Ok(NativeAsyncFieldPlan::Identity);
    }
    if matches!(
        provider_field.data_type(),
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
    ) || matches!(
        file_field.data_type(),
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
    ) {
        return Err(incompatible_parquet_type(
            path,
            provider_field.data_type(),
            file_field.data_type(),
        ));
    }

    native_async_leaf_cast_plan(provider_field.data_type(), file_field.data_type())
        .map(|target_type| match target_type {
            Some(target_type) => NativeAsyncFieldPlan::Cast { target_type },
            None => NativeAsyncFieldPlan::Identity,
        })
        .map_err(|()| {
            incompatible_parquet_type(path, provider_field.data_type(), file_field.data_type())
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
    _provider_field: &Field,
    field_plan: &NativeAsyncFieldPlan,
) -> Result<ArrayRef, delta_kernel::Error> {
    match field_plan {
        NativeAsyncFieldPlan::Identity => Ok(array),
        NativeAsyncFieldPlan::Cast { target_type } => {
            cast(array.as_ref(), target_type).map_err(delta_kernel::Error::from)
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
            Array, ArrayRef, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
            TimestampNanosecondArray,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use delta_kernel::scan::state::{DvInfo, ScanFile};
    use object_store::ObjectStoreExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{NativeAsyncFileReader, data_file_error};
    use crate::{
        DeltaReadMetrics, DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions,
        DeltaStorageOptions,
        kernel::{DeltaKernelEngineContext, KernelScanFileMetadata},
        metrics::DeltaReadMetricsConfig,
        planning::DeltaScanFileTask,
    };

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

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]))
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
            .open_file_stream(&task, provider_schema, Some(2))
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
                .open_file_stream(&task, provider_schema, None)
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
            .open_file_stream(&task, provider_schema, None)
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
            .open_file_stream(&task, timestamp_schema, None)
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
            let error = match reader.open_file_stream(&task, provider_schema, None).await {
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
