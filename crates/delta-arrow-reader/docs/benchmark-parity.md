# Reader benchmark parity

Issue #470 compares the extracted reader with the frozen Delta Funnel source
from issue #459. The baseline source is commit
`e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3`; its benchmark source blob is
`52284e22a5deb0cddce4aa7257012468dc88f25e`.

Only the 12 controlled reader cases move with this crate. Delta Funnel keeps
its synthetic policy, host probe, write workflow, tracing, Python, and SQL
Server benchmark modes.

## Method

Both release-profile binaries used their locked dependency graphs on the same
Linux x86_64 host, with the baseline and extracted runs alternated per case.
Each case ran two warm-up repetitions in a separate invocation followed by
five measured repetitions. Inputs retained the frozen seed, fixture, query,
backend, storage model, scheduling profile, Parquet controls, and schema-22
CSV fields.

The extracted invocation is:

```bash
cargo bench --locked -p delta-arrow-reader --bench reader --all-features -- \
  --mode provider-exec --seed 0 \
  --provider-exec-storage-profile local \
  --provider-exec-workload provider_few_larger_files \
  --provider-exec-query full_rows \
  --provider-exec-backend native_async \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --provider-exec-parquet-metadata-size-hint 65536 \
  --provider-exec-parquet-full-file-read-threshold disabled \
  --provider-exec-repetitions 5 \
  --output target/delta-arrow-reader-benchmark.csv
```

Change only the workload, query, backend, storage profile, and Parquet controls
to reproduce the other frozen cases listed below. The harness rejects cases
outside that matrix, validates each fixture fingerprint, and validates output
schema and row count before accepting a sample.

## Controlled result

Captured on 2026-08-10. Times are microseconds. The range is the minimum and
maximum of the five measured repetitions. Source rows per second is the raw
schema-22 value.

| Case | Baseline p50 | Baseline range | Extracted p50 | Extracted range | Ratio | Baseline rows/s | Extracted rows/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `local-native-dv` | 1300 | 1214-2887 | 922 | 813-2092 | 0.71x | 25206153 | 35540130 |
| `local-native-full` | 926 | 777-2247 | 797 | 719-1962 | 0.86x | 35386609 | 41114178 |
| `local-native-full-read-eligible` | 994 | 904-1949 | 943 | 773-2232 | 0.95x | 32965794 | 34748674 |
| `local-native-full-read-ineligible` | 1188 | 823-2847 | 903 | 813-2170 | 0.76x | 27582491 | 36287929 |
| `local-native-many-small` | 1803 | 1673-4207 | 1916 | 1880-4681 | 1.06x | 4543538 | 4275574 |
| `local-native-metadata-disabled` | 981 | 832-2112 | 910 | 890-2128 | 0.93x | 33402650 | 36008791 |
| `local-native-metadata-undersized` | 962 | 866-2805 | 1050 | 820-2720 | 1.09x | 34062370 | 31207619 |
| `local-native-projection` | 721 | 641-1723 | 664 | 615-2001 | 0.92x | 45447988 | 49349397 |
| `local-native-pruned-unequal` | 1649 | 1455-4087 | 1628 | 1489-4203 | 0.99x | 44089751 | 44658476 |
| `local-official-dv` | 1157 | 1069-2809 | 1035 | 908-2400 | 0.89x | 28321521 | 31659903 |
| `local-official-full` | 1433 | 1323-3369 | 1402 | 1257-3195 | 0.98x | 22866713 | 23372325 |
| `throttled-native-full` | 97507 | 92857-100125 | 94272 | 89223-101477 | 0.97x | 336057 | 347589 |

All deterministic CSV fields matched exactly, including fixture size and
fingerprint, planned and completed work, produced rows and batches, deletion
vector counters, Parquet request and byte counters, and configured bounds.
The measured ratios range from 0.71x to 1.09x, with no material extraction
regression outside the observed sample spread.

The redacted raw schema-22 rows are committed in
[`benchmark-parity-results.csv`](benchmark-parity-results.csv). Its
`implementation` and `comparison_case` prefix columns identify each baseline
and extracted pair; the remaining 80 columns are the unmodified benchmark
output. Each row records the five-repetition summary, including p50, p95, p99,
minimum, and maximum timing observations.

## Frozen test audit

The focused `benchmark_harness` integration target includes the applicable
frozen assertions for the 12-case matrix, accepted and rejected options,
fixture shapes and fingerprints, retained-fixture ownership, unequal-file
pruning, delayed HTTP reads, percentile and optional-value edges, Linux memory
parsing, CSV width, deletion-vector fields, and unavailable OfficialKernel I/O
metrics.

The remaining tests from the 71-test frozen benchmark binary stay in Delta
Funnel because they exercise code that did not move: synthetic partition
policy, host probes and local-I/O probes, tracing and detailed profiling,
large product-mimic schemas, phase-aligned write workflows, and the multi-mode
runner's unrelated CLI and CSV fields.

The per-invocation working files remain ignored under
`target/issue-470-comparison/`; the committed result file above is the durable
comparison evidence.
