from typing import Literal, Mapping, Sequence, TypeAlias, overload

__version__: str

LoadMode: TypeAlias = Literal["append_existing", "create_and_load", "replace"]
WriteAllCacheMode: TypeAlias = Literal["auto", "disabled"]
Report: TypeAlias = dict[str, object]
Options: TypeAlias = Mapping[str, object]


class DeltaFunnelError(Exception):
    phase: str
    kind: str
    message: str
    context: object | None


class Session:
    def __init__(
        self,
        *,
        default_mssql_connection_string: str | None = None,
        target_partitions: int | None = None,
        output_batch_size: int | None = None,
        provider_scan_options: Options | None = None,
        validation_options: Options | None = None,
        schema_options: Options | None = None,
    ) -> None: ...

    @overload
    def delta_lake(
        self,
        source_uri: str,
        *,
        version: int | None = None,
        storage_options: Mapping[str, str] | None = None,
        name: str,
    ) -> Table: ...

    @overload
    def delta_lake(
        self,
        source_uri: str,
        *,
        version: int | None = None,
        storage_options: Mapping[str, str] | None = None,
        name: None = None,
    ) -> PendingDeltaSource: ...

    def table_from_sql(self, sql: str) -> Table: ...

    def write_all(
        self,
        outputs: Sequence[MssqlOutputSpec],
        *,
        options: Options | None = None,
        dry_run: bool | None = None,
    ) -> Report: ...


class PendingDeltaSource:
    def alias(self, name: str) -> Table: ...


class Table:
    def alias(self, name: str) -> Table: ...

    def to_mssql(
        self,
        *,
        schema: str,
        table: str,
        load_mode: LoadMode,
        name: str | None = None,
        connection_string: str | None = None,
    ) -> MssqlOutputSpec: ...

    def write_to_mssql(
        self,
        *,
        schema: str,
        table: str,
        load_mode: LoadMode,
        dry_run: bool | None = None,
        name: str | None = None,
        connection_string: str | None = None,
    ) -> Report: ...


class MssqlOutputSpec: ...
