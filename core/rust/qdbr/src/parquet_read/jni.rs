use std::path::Path;
use std::slice;

use jni::objects::JClass;
use jni::JNIEnv;
use parquet2::read::read_metadata;

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_describe(
    mut env: JNIEnv,
    _class: JClass,
    src_path: *const u8,
    src_path_len: i32,
    // [col_count: jint, row_count: jint, row_group_count: jint]
    descr_ptr: *mut i32,
) {
    let describe = || -> anyhow::Result<()> {
        let src_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(src_path, src_path_len as usize))
        };
        let src_path = Path::new(&src_path);

        let mut reader = std::fs::File::open(src_path)?;
        let metadata = read_metadata(&mut reader)?;

        unsafe {
            *descr_ptr = if metadata.row_groups.len() > 0 {
                metadata.row_groups[0].columns().len() as i32
            } else {
                0
            };
            *(descr_ptr.add(1)) = metadata.num_rows as i32;
            *(descr_ptr.add(2)) = metadata.row_groups.len() as i32;
        }

        return Ok(());
    };

    match describe() {
        Ok(_) => (),
        Err(err) => {
            if let Some(jni_err) = err.downcast_ref::<jni::errors::Error>() {
                match jni_err {
                    jni::errors::Error::JavaException => {
                        // Already thrown.
                    }
                    _ => {
                        let msg = format!("Failed to describe partition: {:?}", jni_err);
                        env.throw_new("java/lang/RuntimeException", msg)
                            .expect("failed to throw exception");
                    }
                }
            } else {
                let msg = format!("Failed to describe partition: {:?}", err);
                env.throw_new("java/lang/RuntimeException", msg)
                    .expect("failed to throw exception");
            }
        }
    }
}
