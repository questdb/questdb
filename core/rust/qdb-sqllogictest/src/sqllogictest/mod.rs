use jni::objects::JClass;
use jni::JNIEnv;
use sqllogictest::{Record, Runner};
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended};
use std::ffi::CStr;
use std::net::{IpAddr, Ipv4Addr};
use std::os::raw::c_char;
use std::path::Path;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_io_questdb_test_Sqllogictest_setEnvVar(
    _env: JNIEnv,
    _class: JClass,
    var_name: *const c_char,
    var_value: *const c_char,
) {
    let var_name = unsafe { CStr::from_ptr(var_name).to_str().expect("Invalid UTF-8") };
    let var_value = unsafe { CStr::from_ptr(var_value).to_str().expect("Invalid UTF-8") };
    std::env::set_var(var_name, var_value);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_test_Sqllogictest_run(
    mut env: JNIEnv,
    _class: JClass,
    port: i16,
    path_ptr: *const c_char,
) {
    let file_path = Path::new(unsafe { CStr::from_ptr(path_ptr).to_str().expect("Invalid UTF-8") });
    if !file_path.exists() {
        let error_msg = format!("file not found: {:?}", file_path);
        eprintln!("{}", &error_msg);
        throw_state_msg(&mut env, "run", &error_msg, ());
        return;
    }

    let mut runner = Runner::new(|| async {
        let mut config = PostgresConfig::new();
        config.port(port as u16);
        config.dbname("quest");
        config.user("admin");
        config.password("quest");
        config.hostaddr(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        PostgresExtended::connect(config).await
    });

    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    let result = runtime.block_on(async {
        // You can also parse the script and execute the records separately:
        let records = sqllogictest::parse_file(file_path);
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
                        } => sql.to_lowercase().starts_with("pragma"),
                        _ => false,
                    };

                    if skip {
                        continue;
                    }

                    let res = runner.run_async(record).await;
                    if let Err(e) = res {
                        return Err(anyhow::Error::from(e));
                    }
                }
                Ok(())
            }
            Err(e) => Err(anyhow::Error::from(e)),
        }
    });

    if let Err(e) = result {
        let error_msg = format!("Error running test file '{}': {}", file_path.display(), e);
        eprintln!("{}", &error_msg);
        throw_state_msg(&mut env, "run", &error_msg, ());
    }
}

fn throw_state_msg<T>(env: &mut JNIEnv, method_name: &str, err: &String, _def: T) {
    let msg = format!("error while {}: {}", method_name, err);
    let res = env.throw_new("java/lang/RuntimeException", msg);
    match res {
        Ok(_) => {}
        Err(e) => {
            eprintln!("error while throwing exception: {}", e);
        }
    };
}
