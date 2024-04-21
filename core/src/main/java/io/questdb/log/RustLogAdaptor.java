package io.questdb.log;

/**
 * Adaptors to allow Rust code to call Java logging methods with fewer JNI calls.
 * Called by `jlog.rs` across JNI.
 */
@SuppressWarnings("unused")
public interface RustLogAdaptor {

    @SuppressWarnings("unused")
    static void advisoryUtf8(Log log, long lo, long hi) {
        log.advisory().$utf8(lo, hi).$();
    }

    @SuppressWarnings("unused")
    static void criticalUtf8(Log log, long lo, long hi) {
        log.critical().$utf8(lo, hi).$();
    }

    @SuppressWarnings("unused")
    static void debugUtf8(Log log, long lo, long hi) {
        log.debug().$utf8(lo, hi).$();
    }

    @SuppressWarnings("unused")
    static void errorUtf8(Log log, long lo, long hi) {
        log.error().$utf8(lo, hi).$();
    }

    @SuppressWarnings("unused")
    static void infoUtf8(Log log, long lo, long hi) {
        log.info().$utf8(lo, hi).$();
    }

    @SuppressWarnings("EmptyMethod")
    static void init() {
    }
}
