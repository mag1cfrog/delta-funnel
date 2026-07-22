"""Produce one deterministic stable trace while Perfetto records the same preview."""

import argparse
import sys
import tempfile
from pathlib import Path

import deltafunnel


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stable_trace", type=Path)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "crates/delta-funnel-python/tests"))
    from progress_fixture import delta_table_uri

    if not deltafunnel.init_perfetto_diagnostics(wait_timeout_seconds=10.0):
        raise RuntimeError("Perfetto diagnostics require a fresh Python process")

    with tempfile.TemporaryDirectory(prefix="semantic-parity-") as temp_dir:
        source_uri = delta_table_uri(temp_dir)
        table = deltafunnel.Session().delta_lake(source_uri, name="orders")
        preview = table.preview(limit=2, progress=False, profile=True)
        if "| id |" not in preview.text:
            raise RuntimeError("deterministic Delta preview did not return the fixture row")
        preview.export_trace(args.stable_trace)


if __name__ == "__main__":
    main()
