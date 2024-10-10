use crate::parquet::error::ParquetError;
use jni::JNIEnv;

pub trait DefaultReturn {
    fn default_return() -> Self;
}

impl DefaultReturn for () {
    fn default_return() -> Self {}
}

impl DefaultReturn for u32 {
    fn default_return() -> Self {
        0
    }
}

impl DefaultReturn for i64 {
    fn default_return() -> Self {
        0
    }
}

impl DefaultReturn for usize {
    fn default_return() -> Self {
        0
    }
}

impl<T> DefaultReturn for *mut T {
    fn default_return() -> Self {
        std::ptr::null_mut()
    }
}

impl<T> DefaultReturn for *const T {
    fn default_return() -> Self {
        std::ptr::null()
    }
}

pub(crate) fn throw_java_ex<T: DefaultReturn>(
    env: &mut JNIEnv,
    method_name: &str,
    err: &ParquetError,
) -> T {
    let msg = format!("error in {}: {}", method_name, err.display_with_backtrace());
    // TODO(amunra): Raise a CairoException instead (with appropriately concatenated arguments)
    env.throw_new("java/lang/RuntimeException", msg)
        .expect("failed to throw exception");
    T::default_return()
}
