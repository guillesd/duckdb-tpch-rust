# rusty_tpch

A DuckDB extension (written in Rust, on the C-extension template) that generates
[TPC-H](https://www.tpc.org/tpch/) data. It uses the pure-Rust
[`tpchgen-rs`](https://github.com/clflushopt/tpchgen-rs) generators and offers two modes:

- **Parquet files** — `tpch_gen(sf, output_dir, [tables])` writes Parquet files to disk.
- **DuckDB tables** — `tpch(sf, 'table')` is a table function that streams the rows of a single
  TPC-H table, so you can materialize it directly into the current database with
  `CREATE TABLE ... AS`.

Supported tables: `nation`, `region`, `part`, `supplier`, `partsupp`, `customer`, `orders`,
`lineitem`.

> Note: a DuckDB Rust table function cannot reach the calling connection, so the extension can't
> create the tables for you the way the C++ `dbgen` does. Use `CREATE TABLE ... AS FROM tpch(...)`,
> or `tpch_load(sf)` to get the full load script.

## Building

```sh
make configure   # one-time: sets up the venv and downloads DuckDB headers for the target version
make release     # or: make debug
```

This produces `build/release/extension/rusty_tpch/rusty_tpch.duckdb_extension`. The extension
targets the DuckDB version set in the `Makefile` (`TARGET_DUCKDB_VERSION`, currently `v1.5.3`) and
must be loaded by a matching DuckDB build.

## Usage

Load the extension (the `-unsigned` flag is required for locally-built extensions):

```sh
duckdb -unsigned
```

```sql
LOAD '/absolute/path/to/build/release/extension/rusty_tpch/rusty_tpch.duckdb_extension';
```

### DuckDB tables (recommended for in-database work)

```sql
-- Materialize a single table:
CREATE TABLE lineitem AS FROM tpch(1, 'lineitem');

-- Or query the generated rows directly:
SELECT count(*) FROM tpch(0.1, 'orders');

-- Generate the full schema: tpch_load returns a ready-to-run load script (the optimal
-- session setting plus a CREATE TABLE AS for each of the 8 tables).
SELECT statement FROM tpch_load(1);

-- Or restrict it to a subset (same `tables` named parameter as tpch_gen):
SELECT statement FROM tpch_load(1, tables := ['lineitem', 'orders']);
```

Generation is parallelized across CPU cores (each worker generates a partition of the table) and
streamed in bounded batches, so memory stays flat regardless of scale factor.

### Performance tip: parallelize the insert

DuckDB's default `CREATE TABLE AS` insert is single-threaded so it can preserve row order. For
generated data the order is irrelevant, so set:

```sql
SET preserve_insertion_order = false;
```

This lets DuckDB insert in parallel and is the single biggest throughput lever (the `tpch_load`
script emits it for you). It has **no effect on query performance** — the 22-query TPC-H suite runs
in the same time on ordered vs. unordered tables — and produces identical query answers.

Benchmark at SF=5 on a 10-core machine, all 8 tables, DuckDB 1.5.3:

| Mode | Time | Peak RSS |
|---|---|---|
| `tpch(...)` + CREATE TABLE AS, `preserve_insertion_order=false` | **~3.6s** | ~7.5 GB |
| official `dbgen(sf=5)` (with or without the pragma — uses its own appender) | ~10.0s | ~8.1 GB |
| `tpch(...)` + CREATE TABLE AS (default, ordered single-threaded insert) | ~14s | ~7.5 GB |
| `tpch_gen(...)` Parquet files | ~4.5s | ~0.9 GB |

### Parquet files

```sql
-- Write every table as a Parquet file into ./data:
CALL tpch_gen(1, 'data');

-- Only a subset, via the optional `tables` named parameter:
CALL tpch_gen(1, 'data', tables := ['lineitem', 'orders']);

-- Split each table into N files for smaller output files (controls file size):
--   data/orders/orders.1.parquet ... data/orders/orders.4.parquet
CALL tpch_gen(1, 'data', tables := ['orders'], parts := 4);

-- Tune the Parquet encoding: compression codec and target row-group size (bytes):
CALL tpch_gen(1, 'data', compression := 'zstd(3)', row_group_bytes := 16777216);
```

`sf` is the scale factor (`DOUBLE`) and `output_dir` the destination directory. The rest are
optional named parameters:

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `tables` | `LIST(VARCHAR)` | all 8 tables | subset to generate |
| `parts` | `INTEGER` | one file per table | split **every** table into N files at `{dir}/{table}/{table}.{i}.parquet` — the lever for **output file size** (more parts → smaller files). `nation`/`region` are too small to partition and always emit one file |
| `part` | `INTEGER` | — | generate only partition `i` of `parts` (for distributed/sharded generation) |
| `compression` | `VARCHAR` | `snappy` | `uncompressed`, `snappy`, `lz4`; level-based codecs need a level, e.g. `zstd(3)`, `gzip(6)`, `brotli(5)` |
| `row_group_bytes` | `BIGINT` | `7340032` (7 MB) | target bytes per **row group** *inside* each file (not file size) |

#### Sizing heuristics

Parquet output scales ~linearly with the scale factor. With the default **snappy** codec (measured,
~±10%):

- **Whole dataset** ≈ `0.37 GB × SF` (e.g. SF=10 → ~3.7 GB, SF=100 → ~37 GB).
- **`lineitem`** ≈ `0.24 GB × SF` — about 65% of the dataset and by far the largest table; the rest:
  `orders` ~0.066, `partsupp` ~0.045, `customer` ~0.013, `part` ~0.007 GB × SF; `supplier` tiny;
  `nation`/`region` a few KB.
- **`parts := N`** is a *uniform* split count applied to every table (except `nation`/`region`,
  always one file): each file ≈ `that table's size / N`. Because tables differ in size, file sizes
  differ per table — at SF=10 `parts := 2` yields ~1.2 GB `lineitem` files but only ~0.33 GB
  `orders` and ~33 MB `part` files. Size `parts` against the table you care about (usually
  `lineitem`); for uniform file sizes across tables, call `tpch_gen` once per table with a
  table-specific `parts`.
- **`zstd(3)`** is roughly **25% smaller** than snappy (SF=10 `lineitem`: ~1.8 GB vs ~2.4 GB), at
  some extra CPU.

To target a file size `F` (GB) for `lineitem`: `parts ≈ ceil(0.24 × SF / F)`. For example, ~1 GB
files at SF=10 → `parts := 3`; ~2 GB files at SF=100 → `parts := 12`.

### Scaling beyond RAM

In-memory tables cost RAM equal to the dataset (that's the deliverable, not generation overhead —
generation itself only holds a few small batches per thread). To generate datasets larger than
memory, either:

- use **Parquet mode** (streams to disk, ~1 GB RSS regardless of scale factor), or
- create the tables in a **file-backed** database with a `memory_limit`:

  ```sql
  -- duckdb mydata.db
  SET memory_limit = '2GB';
  SET preserve_insertion_order = false;
  CREATE TABLE lineitem AS FROM tpch(50, 'lineitem');  -- RSS stays ~2-3 GB
  ```

  At SF=5 with a 2 GB limit, peak RSS stays ~2.9 GB (vs ~7.5 GB unbounded). Under this memory
  pressure the parallel unordered insert is also far faster than the alternatives (~10s vs ~40s for
  the ordered insert or `dbgen`), because the insert, not generation, dominates when spilling.

### Benchmark at scale (SF=50)

SF=50 (≈300 M lineitem rows) on the same 10-core machine, DuckDB 1.5.3. The two DuckDB-table runs
use a **file-backed** database with `memory_limit = '16GB'`; Parquet streams to files:

| Mode | Time | Peak RSS | Output |
|---|---|---|---|
| `tpch_gen(...)` Parquet files | **~34s** | ~1.3 GB | 19 GB of `.parquet` |
| `tpch(...)` + CREATE TABLE AS, `preserve_insertion_order=false` | ~70s | ~11 GB | 14 GB `.db` |
| official `dbgen(sf=50)` | ~380s | ~14 GB | 13 GB `.db` |

At this scale the DuckDB-table path is **~5.5× faster than `dbgen`** and stays under the memory
limit. The gap is wider than at SF=5 because, under a file-backed sink, `dbgen`'s insert spills and
runs mostly single-threaded (~1.6× CPU utilization) while our parallel generation + parallel
unordered insert keeps all cores busy (~7× utilization). Parquet mode is fastest of all and barely
touches RAM, since it never materializes the dataset.

## Testing

```sh
make test_release   # or: make test_debug
```
