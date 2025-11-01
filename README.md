# TPCH data generation (tpchgen-rs)

This repository includes a small helper binary that uses the `tpchgen-rs` crates to generate TPC-H data (Arrow / Parquet).
The package depends on tpchgen-arrow / tpchgen and uses arrow / parquet (e.g. arrow = "56.2.0", parquet = "56.2.0").

## Running the extension

To build the extension in debug mode:

```
make configure
make debug
```

This will produce a debug extension in build/debug/ which you can load in DuckDB:

```
duckdb -unsigned
```

Then inside DuckDB:

```sql
LOAD './build/debug/extension/rusty_tpch/rusty_tpch.duckdb_extension';
CALL tpch_gen('1');
```