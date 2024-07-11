use std::ffi::CStr;
use std::ops::Deref;
use std::os::raw::c_char;
use std::path::Path;
use std::sync::Arc;

use jni::objects::JClass;
use jni::JNIEnv;
use log::error;
use sqllogictest::{Record, Runner};
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended};
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_io_questdb_Sqllogictest_run(
    mut env: JNIEnv,
    _class: JClass,
    port: i16,
    path_ptr: *const c_char,
) {
    // Create a new Tokio runtime
    let runtime = Runtime::new().unwrap();

    let file_path = unsafe {
        let c_str = CStr::from_ptr(path_ptr);
        c_str.to_string_lossy().into_owned()
    };

    if !Path::new(&file_path).exists() {
        eprintln!("Test file not found: {}", file_path);
        throw_state_msg(
            &mut env,
            "run",
            &format!("file not found: {}", file_path),
            (),
        );
    }

    let mut config = PostgresConfig::new();
    config.port(port as u16);
    config.dbname("quest");
    config.user("admin");
    config.password("quest");
    config.host("localhost");

    let runner_future = async {
        let config = Arc::new(config);
        let mut runner = Runner::new({
            move || {
                let config = Arc::clone(&config);
                async move { PostgresExtended::connect(config.deref().clone()).await }
            }
        });

        // You can also parse the script and execute the records separately:
        let records = sqllogictest::parse_file(&file_path);
        match records {
            Ok(records) => {
                for record in records {
                    if let Record::Halt { .. } = record {
                        break;
                    }

                    let skip = match &record {
                        Record::Statement {
                            loc: _loc,
                            conditions: _conditions,
                            connection: _connection,
                            sql,
                            ..
                        } => sql.starts_with("PRAGMA"),
                        _ => false,
                    };

                    if skip {
                        continue;
                    }

                    let res = runner.run(record);
                    if let Err(e) = res {
                        return Err(anyhow::Error::from(e));
                    }
                }
                Ok(())
            }
            Err(e) => Err(anyhow::Error::from(e)),
        }
    };

    // Execute the future within the runtime
    let result = runtime.block_on(runner_future);
    if let Err(e) = result {
        error!("Error running test file '{}': {}", file_path, e);
        throw_state_msg(
            &mut env,
            "run",
            &format!("Error running test file '{}': {}", file_path, e),
            (),
        );
    }
}

fn throw_state_msg<T>(env: &mut JNIEnv, method_name: &str, err: &String, _def: T) {
    let msg = format!("error while {}: {}", method_name, err);
    let res = env.throw_new("java/lang/RuntimeException", msg);
    match res {
        Ok(_) => {}
        Err(e) => {
            error!("error while throwing exception: {}", e);
        }
    };
}
