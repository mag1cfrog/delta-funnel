"""Build the deterministic local Delta table used by progress smoke tests."""

import base64
import json
from pathlib import Path


def delta_table_uri(root):
    """Create a local Delta table with two real Parquet files."""

    table = Path(root, "orders")
    log = table / "_delta_log"
    log.mkdir(parents=True)
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}}
        ],
    }
    protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
    metadata = {
        "metaData": {
            "id": "delta-funnel-progress-smoke",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495,
        }
    }
    parquet = base64.b64decode(
        Path(__file__).with_name("progress_smoke.parquet.b64").read_text()
    )
    data_files = ["part-00000.parquet", "part-00001.parquet"]
    for data_file in data_files:
        (table / data_file).write_bytes(parquet)
    adds = [
        {
            "add": {
                "path": data_file,
                "partitionValues": {},
                "size": len(parquet),
                "modificationTime": 1587968586000,
                "dataChange": True,
            }
        }
        for data_file in data_files
    ]
    (log / "00000000000000000000.json").write_text(
        f"{json.dumps(protocol)}\n{json.dumps(metadata)}\n"
    )
    (log / "00000000000000000001.json").write_text(
        "".join(f"{json.dumps(add)}\n" for add in adds)
    )
    return table.resolve().as_uri()
