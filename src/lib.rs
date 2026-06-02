//! `rusty_tpch` — a DuckDB extension that generates TPC-H data.
//!
//! Two complementary entry points are registered:
//!
//! * `tpch_gen(sf, output_dir, [tables])` — writes TPC-H **Parquet files** to disk using the
//!   tokio-based `tpchgen-cli` runner. Optimised for bulk file generation.
//! * `tpch(sf, 'table')` — a **table function** that streams the rows of a single TPC-H table.
//!   Materialise it into the current database with
//!   `CREATE TABLE lineitem AS FROM tpch(1, 'lineitem');`.
//!   A duckdb-rs table function cannot reach the calling connection, so the extension cannot
//!   create the tables itself (unlike the C++ `dbgen`). `tpch_load_sql(sf)` returns the full set
//!   of `CREATE TABLE ... AS` statements as a convenience.

extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

use std::error::Error;
use std::fmt::{Display, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Mutex;

use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_cli::{OutputFormat, Table as CliTable, TpchGenerator};

/// Number of rows packed into one batch. Matches DuckDB's standard vector size so a batch maps
/// 1:1 to an output `DataChunk`.
const BATCH_SIZE: usize = 2048;

// =============================================================================================
// Parquet path: `tpch_gen(sf, output_dir, [tables])`
// =============================================================================================

struct TpchGenBindData {
    sf: f64,
    output_dir: String,
    tables: Vec<CliTable>,
}

struct TpchGenInitData {
    done: AtomicBool,
}

struct TpchGenVTab;

const ALL_CLI_TABLES: [CliTable; 8] = [
    CliTable::Nation,
    CliTable::Region,
    CliTable::Part,
    CliTable::Supplier,
    CliTable::Partsupp,
    CliTable::Customer,
    CliTable::Orders,
    CliTable::Lineitem,
];

impl VTab for TpchGenVTab {
    type BindData = TpchGenBindData;
    type InitData = TpchGenInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let sf: f64 = bind
            .get_parameter(0)
            .to_string()
            .parse()
            .map_err(|e| format!("invalid scale factor: {e}"))?;
        let output_dir = bind.get_parameter(1).to_string();

        // `tables` is an optional named parameter (a LIST(VARCHAR)). Absent / empty means
        // "all tables", e.g. `tpch_gen(1, 'data')` or `tpch_gen(1, 'data', tables := ['orders'])`.
        let tables = match bind.get_named_parameter("tables").and_then(|v| v.to_list()) {
            Some(list) if !list.is_empty() => {
                let mut tables = Vec::with_capacity(list.len());
                for item in list {
                    let name = item.to_string();
                    let table = name
                        .parse::<CliTable>()
                        .map_err(|_| format!("unknown TPC-H table: {name}"))?;
                    tables.push(table);
                }
                tables
            }
            _ => ALL_CLI_TABLES.to_vec(),
        };

        Ok(TpchGenBindData {
            sf,
            output_dir,
            tables,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(TpchGenInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();

        // DuckDB pulls a table function until it emits a zero-length chunk. Generation must
        // happen exactly once, on the first call — otherwise the data set is produced twice.
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let bind_data = func.get_bind_data();
        let rt = tokio::runtime::Runtime::new()?;
        let generator = TpchGenerator::builder()
            .with_scale_factor(bind_data.sf)
            .with_output_dir(PathBuf::from(bind_data.output_dir.clone()))
            .with_tables(bind_data.tables.clone())
            .with_format(OutputFormat::Parquet)
            .build();
        rt.block_on(generator.generate())?;

        output.flat_vector(0).insert(0, "Successfully generated TPC-H data");
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Double),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![(
            "tables".to_string(),
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        )])
    }
}

// =============================================================================================
// DuckDB-table path: `tpch(sf, 'table')`
// =============================================================================================

/// The eight TPC-H tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TpchTable {
    Nation,
    Region,
    Part,
    Supplier,
    Partsupp,
    Customer,
    Orders,
    Lineitem,
}

impl TpchTable {
    fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "nation" => Some(Self::Nation),
            "region" => Some(Self::Region),
            "part" => Some(Self::Part),
            "supplier" => Some(Self::Supplier),
            "partsupp" => Some(Self::Partsupp),
            "customer" => Some(Self::Customer),
            "orders" => Some(Self::Orders),
            "lineitem" => Some(Self::Lineitem),
            _ => None,
        }
    }

    /// Nation and Region are tiny fixed tables whose generators ignore the `part`/`part_count`
    /// arguments, so they must be generated by a single producer to avoid duplicated rows.
    fn is_partitionable(self) -> bool {
        !matches!(self, Self::Nation | Self::Region)
    }

    /// Column names and DuckDB logical types, in physical column order. This order must match the
    /// columns written by the table's `produce_*` function.
    fn schema(self) -> Vec<(&'static str, LogicalTypeHandle)> {
        match self {
            Self::Nation => vec![
                ("n_nationkey", bigint()),
                ("n_name", varchar()),
                ("n_regionkey", bigint()),
                ("n_comment", varchar()),
            ],
            Self::Region => vec![
                ("r_regionkey", bigint()),
                ("r_name", varchar()),
                ("r_comment", varchar()),
            ],
            Self::Part => vec![
                ("p_partkey", bigint()),
                ("p_name", varchar()),
                ("p_mfgr", varchar()),
                ("p_brand", varchar()),
                ("p_type", varchar()),
                ("p_size", integer()),
                ("p_container", varchar()),
                ("p_retailprice", decimal()),
                ("p_comment", varchar()),
            ],
            Self::Supplier => vec![
                ("s_suppkey", bigint()),
                ("s_name", varchar()),
                ("s_address", varchar()),
                ("s_nationkey", bigint()),
                ("s_phone", varchar()),
                ("s_acctbal", decimal()),
                ("s_comment", varchar()),
            ],
            Self::Partsupp => vec![
                ("ps_partkey", bigint()),
                ("ps_suppkey", bigint()),
                ("ps_availqty", integer()),
                ("ps_supplycost", decimal()),
                ("ps_comment", varchar()),
            ],
            Self::Customer => vec![
                ("c_custkey", bigint()),
                ("c_name", varchar()),
                ("c_address", varchar()),
                ("c_nationkey", bigint()),
                ("c_phone", varchar()),
                ("c_acctbal", decimal()),
                ("c_mktsegment", varchar()),
                ("c_comment", varchar()),
            ],
            Self::Orders => vec![
                ("o_orderkey", bigint()),
                ("o_custkey", bigint()),
                ("o_orderstatus", varchar()),
                ("o_totalprice", decimal()),
                ("o_orderdate", date()),
                ("o_orderpriority", varchar()),
                ("o_clerk", varchar()),
                ("o_shippriority", integer()),
                ("o_comment", varchar()),
            ],
            Self::Lineitem => vec![
                ("l_orderkey", bigint()),
                ("l_partkey", bigint()),
                ("l_suppkey", bigint()),
                ("l_linenumber", integer()),
                ("l_quantity", decimal()),
                ("l_extendedprice", decimal()),
                ("l_discount", decimal()),
                ("l_tax", decimal()),
                ("l_returnflag", varchar()),
                ("l_linestatus", varchar()),
                ("l_shipdate", date()),
                ("l_commitdate", date()),
                ("l_receiptdate", date()),
                ("l_shipinstruct", varchar()),
                ("l_shipmode", varchar()),
                ("l_comment", varchar()),
            ],
        }
    }
}

fn bigint() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Bigint)
}
fn integer() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Integer)
}
fn varchar() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Varchar)
}
fn date() -> LogicalTypeHandle {
    LogicalTypeHandle::from(LogicalTypeId::Date)
}
/// All TPC-H decimals are `DECIMAL(15, 2)`. With precision 15 (<= 18) DuckDB stores the value as
/// an `int64`, so the column data is written through an `i64` slice.
fn decimal() -> LogicalTypeHandle {
    LogicalTypeHandle::decimal(15, 2)
}

// --- Column writers: write one column of a chunk directly from a row iterator. ---
//
// The generator rows hold `&'static str` (into the static text pool), so a batch is collected
// with no string allocation and written straight into the output vectors. `i64` backs both
// `BIGINT` and `DECIMAL(15,2)` (raw scaled int); `i32` backs both `INTEGER` and `DATE` (days
// since the Unix epoch).

fn put_i64<I: Iterator<Item = i64>>(out: &DataChunkHandle, col: usize, values: I) {
    let mut vector = out.flat_vector(col);
    let slice = unsafe { vector.as_mut_slice::<i64>() };
    for (i, value) in values.enumerate() {
        slice[i] = value;
    }
}

fn put_i32<I: Iterator<Item = i32>>(out: &DataChunkHandle, col: usize, values: I) {
    let mut vector = out.flat_vector(col);
    let slice = unsafe { vector.as_mut_slice::<i32>() };
    for (i, value) in values.enumerate() {
        slice[i] = value;
    }
}

/// Write a `VARCHAR` column from string slices (no allocation — borrows the static text pool).
fn put_str<'a, I: Iterator<Item = &'a str>>(out: &DataChunkHandle, col: usize, values: I) {
    let vector = out.flat_vector(col);
    for (i, value) in values.enumerate() {
        vector.insert(i, value);
    }
}

/// Write a `VARCHAR` column from `Display` values (TPC-H wrapper types), reusing one buffer.
fn put_display<T: Display, I: Iterator<Item = T>>(out: &DataChunkHandle, col: usize, values: I) {
    let vector = out.flat_vector(col);
    let mut buf = String::new();
    for (i, value) in values.enumerate() {
        buf.clear();
        let _ = write!(buf, "{value}");
        vector.insert(i, buf.as_str());
    }
}


/// A unit of work sent from a generator thread to the table function: it writes one chunk
/// (up to [`BATCH_SIZE`] rows) into the output and returns the row count. The generated rows are
/// captured by the closure (they hold only `&'static` data), so no per-column owned copy is made.
type WriteJob = Box<dyn FnOnce(&mut DataChunkHandle) -> usize + Send>;

/// Generate one partition of `table` on a dedicated thread, pushing [`WriteJob`]s to `tx`.
///
/// Running generation on its own threads (rather than on DuckDB's scan threads) keeps it parallel
/// and overlapped with the insert even when DuckDB inserts single-threaded
/// (`preserve_insertion_order=true`, the default).
fn produce(table: TpchTable, sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    match table {
        TpchTable::Nation => produce_nation(sf, part, part_count, tx),
        TpchTable::Region => produce_region(sf, part, part_count, tx),
        TpchTable::Part => produce_part(sf, part, part_count, tx),
        TpchTable::Supplier => produce_supplier(sf, part, part_count, tx),
        TpchTable::Partsupp => produce_partsupp(sf, part, part_count, tx),
        TpchTable::Customer => produce_customer(sf, part, part_count, tx),
        TpchTable::Orders => produce_orders(sf, part, part_count, tx),
        TpchTable::Lineitem => produce_lineitem(sf, part, part_count, tx),
    }
}

/// Pull batches of `&'static` rows from `iter` and send each as a [`WriteJob`] built by `make_job`.
/// Stops when the partition is exhausted or the receiver (table function) has gone away.
fn pump<I, R, F>(mut iter: I, tx: &SyncSender<WriteJob>, make_job: F)
where
    I: Iterator<Item = R>,
    R: Send + 'static,
    F: Fn(Vec<R>) -> WriteJob,
{
    loop {
        let rows: Vec<R> = iter.by_ref().take(BATCH_SIZE).collect();
        if rows.is_empty() {
            break;
        }
        if tx.send(make_job(rows)).is_err() {
            break;
        }
    }
}

fn produce_nation(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = NationGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.n_nationkey));
            put_str(out, 1, rows.iter().map(|r| r.n_name));
            put_i64(out, 2, rows.iter().map(|r| r.n_regionkey));
            put_str(out, 3, rows.iter().map(|r| r.n_comment));
            rows.len()
        })
    });
}

fn produce_region(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = RegionGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.r_regionkey));
            put_str(out, 1, rows.iter().map(|r| r.r_name));
            put_str(out, 2, rows.iter().map(|r| r.r_comment));
            rows.len()
        })
    });
}

fn produce_part(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = PartGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.p_partkey));
            put_display(out, 1, rows.iter().map(|r| &r.p_name));
            put_display(out, 2, rows.iter().map(|r| &r.p_mfgr));
            put_display(out, 3, rows.iter().map(|r| &r.p_brand));
            put_str(out, 4, rows.iter().map(|r| r.p_type));
            put_i32(out, 5, rows.iter().map(|r| r.p_size));
            put_str(out, 6, rows.iter().map(|r| r.p_container));
            put_i64(out, 7, rows.iter().map(|r| r.p_retailprice.into_inner()));
            put_str(out, 8, rows.iter().map(|r| r.p_comment));
            rows.len()
        })
    });
}

fn produce_supplier(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = SupplierGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.s_suppkey));
            put_display(out, 1, rows.iter().map(|r| &r.s_name));
            put_display(out, 2, rows.iter().map(|r| &r.s_address));
            put_i64(out, 3, rows.iter().map(|r| r.s_nationkey));
            put_display(out, 4, rows.iter().map(|r| &r.s_phone));
            put_i64(out, 5, rows.iter().map(|r| r.s_acctbal.into_inner()));
            put_str(out, 6, rows.iter().map(|r| r.s_comment.as_str()));
            rows.len()
        })
    });
}

fn produce_partsupp(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = PartSuppGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.ps_partkey));
            put_i64(out, 1, rows.iter().map(|r| r.ps_suppkey));
            put_i32(out, 2, rows.iter().map(|r| r.ps_availqty));
            put_i64(out, 3, rows.iter().map(|r| r.ps_supplycost.into_inner()));
            put_str(out, 4, rows.iter().map(|r| r.ps_comment));
            rows.len()
        })
    });
}

fn produce_customer(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = CustomerGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.c_custkey));
            put_display(out, 1, rows.iter().map(|r| &r.c_name));
            put_display(out, 2, rows.iter().map(|r| &r.c_address));
            put_i64(out, 3, rows.iter().map(|r| r.c_nationkey));
            put_display(out, 4, rows.iter().map(|r| &r.c_phone));
            put_i64(out, 5, rows.iter().map(|r| r.c_acctbal.into_inner()));
            put_str(out, 6, rows.iter().map(|r| r.c_mktsegment));
            put_str(out, 7, rows.iter().map(|r| r.c_comment));
            rows.len()
        })
    });
}

fn produce_orders(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = OrderGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.o_orderkey));
            put_i64(out, 1, rows.iter().map(|r| r.o_custkey));
            put_display(out, 2, rows.iter().map(|r| &r.o_orderstatus));
            put_i64(out, 3, rows.iter().map(|r| r.o_totalprice.into_inner()));
            put_i32(out, 4, rows.iter().map(|r| r.o_orderdate.to_unix_epoch()));
            put_str(out, 5, rows.iter().map(|r| r.o_orderpriority));
            put_display(out, 6, rows.iter().map(|r| &r.o_clerk));
            put_i32(out, 7, rows.iter().map(|r| r.o_shippriority));
            put_str(out, 8, rows.iter().map(|r| r.o_comment));
            rows.len()
        })
    });
}

fn produce_lineitem(sf: f64, part: i32, part_count: i32, tx: &SyncSender<WriteJob>) {
    let g = LineItemGenerator::new(sf, part, part_count);
    pump(g.iter(), tx, |rows| {
        Box::new(move |out| {
            put_i64(out, 0, rows.iter().map(|r| r.l_orderkey));
            put_i64(out, 1, rows.iter().map(|r| r.l_partkey));
            put_i64(out, 2, rows.iter().map(|r| r.l_suppkey));
            put_i32(out, 3, rows.iter().map(|r| r.l_linenumber));
            // `l_quantity` is generated as a whole number; scale it to DECIMAL(15,2).
            put_i64(out, 4, rows.iter().map(|r| r.l_quantity * 100));
            put_i64(out, 5, rows.iter().map(|r| r.l_extendedprice.into_inner()));
            put_i64(out, 6, rows.iter().map(|r| r.l_discount.into_inner()));
            put_i64(out, 7, rows.iter().map(|r| r.l_tax.into_inner()));
            put_str(out, 8, rows.iter().map(|r| r.l_returnflag));
            put_str(out, 9, rows.iter().map(|r| r.l_linestatus));
            put_i32(out, 10, rows.iter().map(|r| r.l_shipdate.to_unix_epoch()));
            put_i32(out, 11, rows.iter().map(|r| r.l_commitdate.to_unix_epoch()));
            put_i32(out, 12, rows.iter().map(|r| r.l_receiptdate.to_unix_epoch()));
            put_str(out, 13, rows.iter().map(|r| r.l_shipinstruct));
            put_str(out, 14, rows.iter().map(|r| r.l_shipmode));
            put_str(out, 15, rows.iter().map(|r| r.l_comment));
            rows.len()
        })
    });
}

struct TpchTableBindData {
    sf: f64,
    table: TpchTable,
}

/// Shared scan state: the receiving end of the bounded channel fed by the generator threads.
/// The table function (possibly called from several DuckDB threads) drains [`WriteJob`]s from it.
struct TpchTableInitData {
    rx: Mutex<Receiver<WriteJob>>,
}

struct TpchTableVTab;

impl VTab for TpchTableVTab {
    type BindData = TpchTableBindData;
    type InitData = TpchTableInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let sf: f64 = bind
            .get_parameter(0)
            .to_string()
            .parse()
            .map_err(|e| format!("invalid scale factor: {e}"))?;
        let name = bind.get_parameter(1).to_string();
        let table = TpchTable::from_name(&name)
            .ok_or_else(|| format!("unknown TPC-H table: {name}"))?;

        for (column, ty) in table.schema() {
            bind.add_result_column(column, ty);
        }

        Ok(TpchTableBindData { sf, table })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        // SAFETY: the framework stores the value returned by `bind` as the bind data of this
        // table function, so it is a valid `TpchTableBindData` for the lifetime of the scan.
        let bind_data = unsafe { &*init.get_bind_data::<TpchTableBindData>() };
        let sf = bind_data.sf;
        let table = bind_data.table;

        let nthreads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        // Nation/Region generators ignore the partition args, so they must be a single partition.
        // Other tables split into one partition per generator thread for balanced generation.
        let part_count = if table.is_partitionable() { nthreads as i32 } else { 1 };

        // Bounded channel: generator threads block once a couple of batches per partition are
        // queued, so memory stays bounded (a few batches in flight) regardless of scale factor.
        let (tx, rx) = sync_channel::<WriteJob>(part_count as usize * 2);
        for part in 1..=part_count {
            let tx = tx.clone();
            std::thread::spawn(move || produce(table, sf, part, part_count, &tx));
        }
        // Drop our own sender so the channel disconnects once every generator thread is done.
        drop(tx);

        // Allow DuckDB to drain (and insert) from several threads when it parallelizes the scan.
        init.set_max_threads(nthreads as u64);
        Ok(TpchTableInitData { rx: Mutex::new(rx) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();

        // Take one job (briefly holding the lock), then run it without the lock so multiple
        // DuckDB threads can write/insert concurrently. `Err` means all generators have finished.
        let job = init.rx.lock().unwrap().recv();
        match job {
            Ok(job) => {
                let n = job(output);
                output.set_len(n);
            }
            Err(_) => output.set_len(0),
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Double),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// =============================================================================================
// Convenience: `tpch_load_sql(sf)` returns the CREATE TABLE ... AS statements for every table.
// =============================================================================================

struct TpchLoadSqlBindData {
    sf: f64,
}

struct TpchLoadSqlInitData {
    done: AtomicBool,
}

struct TpchLoadSqlVTab;

const TABLE_NAMES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

impl VTab for TpchLoadSqlVTab {
    type BindData = TpchLoadSqlBindData;
    type InitData = TpchLoadSqlInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("table_name", varchar());
        bind.add_result_column("statement", varchar());
        let sf: f64 = bind
            .get_parameter(0)
            .to_string()
            .parse()
            .map_err(|e| format!("invalid scale factor: {e}"))?;
        Ok(TpchLoadSqlBindData { sf })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(TpchLoadSqlInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let sf = func.get_bind_data().sf;
        let name_vector = output.flat_vector(0);
        let stmt_vector = output.flat_vector(1);

        // First row: let DuckDB parallelize the inserts (generated data has no meaningful order).
        // This is the single biggest throughput lever — see the README benchmark.
        name_vector.insert(0, "(settings)");
        stmt_vector.insert(0, "SET preserve_insertion_order = false;");
        for (i, name) in TABLE_NAMES.iter().enumerate() {
            name_vector.insert(i + 1, *name);
            let statement = format!("CREATE TABLE {name} AS FROM tpch({sf}, '{name}');");
            stmt_vector.insert(i + 1, statement.as_str());
        }
        output.set_len(TABLE_NAMES.len() + 1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Double)])
    }
}

// =============================================================================================
// Extension entry point.
// =============================================================================================

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<TpchGenVTab>("tpch_gen")?;
    con.register_table_function::<TpchTableVTab>("tpch")?;
    con.register_table_function::<TpchLoadSqlVTab>("tpch_load_sql")?;
    Ok(())
}
