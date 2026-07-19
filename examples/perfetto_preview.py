"""Run a generated-data preview for local Perfetto diagnostics."""

import sys

import deltafunnel


def main() -> int:
    try:
        installed = deltafunnel.init_perfetto_diagnostics(wait_timeout_seconds=10.0)
        if not installed:
            print(
                "Perfetto diagnostics require a fresh Python process.",
                file=sys.stderr,
            )
            return 1

        table = deltafunnel.Session().table_from_sql(
            "SELECT SUM(LENGTH(REGEXP_REPLACE(CAST(value AS VARCHAR), "
            "'[0-9]', 'x', 'g'))) AS total "
            "FROM generate_series(1, 200000000) AS series(value)"
        )
        table.preview(limit=1, progress=False)
    except deltafunnel.DeltaFunnelError as error:
        print(f"Delta Funnel diagnostic example failed: {error.kind}", file=sys.stderr)
        return 1

    print("Delta Funnel Perfetto preview completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
