extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;
extern crate tpchgen_arrow;
extern crate tpchgen;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use tpchgen_arrow::{LineItemArrow, OrderArrow, CustomerArrow};
use tpchgen::generators::{LineItemGenerator, OrderGenerator, CustomerGenerator};
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicBool, Ordering},
};

#[repr(C)]
struct HelloBindData {
    name: String,
}

#[repr(C)]
struct HelloInitData {
    done: AtomicBool,
}

struct HelloVTab;

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_parameter(0).to_string();
        Ok(HelloBindData { name })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(HelloInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let vector = output.flat_vector(0);
            let result = CString::new(format!("Rusty Quack {} 🐥", bind_data.name))?;
            vector.insert(0, result);
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
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
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        let line_item  = LineItemGenerator::new(bind_data.sf, 1, 1);
        let order = OrderGenerator::new(bind_data.sf, 1, 1);
        let customer  = CustomerGenerator::new(bind_data.sf, 1, 1);

        let mut customer_arrow_generator = CustomerArrow::new(customer);
        let mut order_arrow_generator = OrderArrow::new(order);
        let mut line_item_arrow_generator = LineItemArrow::new(line_item);

        //result
        let vector = output.flat_vector(0);
        let result = CString::new(format!("Successfully generated TPC-H data"))?;
        vector.insert(0, result);
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<HelloVTab>("example_hello_name")
        .expect("Failed to register hello table function");

    con.register_table_function::<TpchGenVTab>("tpch_gen")
        .expect("Failed to register tpch gen table function");
    Ok(())
}