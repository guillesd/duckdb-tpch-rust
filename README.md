# TPCH data generation (tpchgen-rs)

This repository includes a small helper binary that uses the `tpchgen-rs` crates to generate TPC-H data (Arrow / Parquet).
The package depends on tpchgen-rs crates, arrow and parquet.

## Running the extension

To build the extension in debug mode:

```
make configure
make release
```

This will produce a debug extension in build/debug/ which you can load in DuckDB:

```
duckdb -unsigned
```

Then inside DuckDB:

```sql
LOAD './build/release/extension/rusty_tpch/rusty_tpch.duckdb_extension';
CALL tpch_gen(100.0, './output/dir');
```
