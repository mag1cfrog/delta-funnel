# Concepts

Delta Funnel has a small workflow model. These terms appear across the Rust and
Python APIs.

## Session

A session owns registered sources, derived SQL tables, default options, and the
default SQL Server connection. In Python, create one with `Session(...)`.

## Source

A source is an input table, such as a Delta Lake table loaded from a local path
or object-store URI. A named source is registered in the SQL catalog and can be
referenced by queries.

## Table

A table is a lazy query object. It may represent a Delta source or SQL derived
from other registered tables. Creating a table does not by itself execute rows.
Previewing, showing, writing, or dry-running a table are terminal actions that
may plan and read data.

## Output

An output describes where a table should be written in SQL Server. Use
`Table.to_mssql(...)` to create an output spec for `Session.write_all(...)`, or
use `Table.write_to_mssql(...)` for a single output.

See [Multiple outputs and shared caching](advanced/multiple-outputs.md) for the
multi-output workflow.

## Report

A report describes what Delta Funnel planned or executed. Dry-run reports
describe the plan without writing rows. Execute reports describe the write
workflow and validation facts.

Reports are structured data, not log text. Sensitive values such as connection
strings and credentials should not appear in them.
