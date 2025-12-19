extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;
extern crate tpchgen_cli;
extern crate tokio;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use tpchgen_cli::{TpchGenerator, Table, OutputFormat};
use std::path::PathBuf;
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicBool, Ordering},
};



macro_rules! debug_print {
    ($($arg:tt)*) => {
        if std::env::var("DEBUG").is_ok() {
            eprintln!("[PCAP Debug] {}", format!($($arg)*));
        }
    };
}

#[repr(C)]
struct TpchBindData {
    sf: f64,
}

#[repr(C)]
struct TpchInitData {
    done: AtomicBool,
}
struct TpchGenVTab;

impl VTab for TpchGenVTab {
    type BindData = TpchBindData;
    type InitData = TpchInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let sf: f64 = bind.get_parameter(0).to_string().parse().unwrap();
        Ok(TpchBindData { sf })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(TpchInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();
        // I need an async runtime to run the generator
        let rt = tokio::runtime::Runtime::new().unwrap();
        let generator = TpchGenerator::builder()
            .with_scale_factor(bind_data.sf)
            .with_output_dir(PathBuf::from("./tmp/data"))
            .with_tables(vec![Table::Customer, Table::Orders, Table::Lineitem])
            .with_format(OutputFormat::Parquet)
            .build();
        rt.block_on(generator.generate())?;

        //result
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let vector = output.flat_vector(0);
            let result = CString::new("Successfully generated TPC-H data")?;
            vector.insert(0, result);
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<TpchGenVTab>("tpch_gen")
        .expect("Failed to register tpch gen table function");
    Ok(())
}