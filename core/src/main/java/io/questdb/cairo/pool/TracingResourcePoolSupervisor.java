package io.questdb.cairo.pool;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;

/**
 * Resource pool supervisor that records where (thread + stack trace) a single pooled
 * resource was borrowed, for the "left behind on pool shutdown" diagnostic.
 * <p>
 * Used only as the per-borrow fallback installed by {@link AbstractMultiTenantPool} when
 * {@code cairo.resource.pool.tracing.enabled} is set: each borrowed resource gets its own
 * instance, so it only ever holds one borrow stack. The stack is captured synchronously on
 * the borrowing thread inside {@link #onResourceBorrowed}, so the recorded thread is always
 * the actual borrower (no carrier/continuation identity is consulted later). Not thread-safe;
 * each instance is confined to one resource's borrow/return.
 */
public class TracingResourcePoolSupervisor<T extends Sinkable> implements ResourcePoolSupervisor<T> {
    private StackTraceElement[] borrowStackTrace;
    private String threadName;

    @Override
    public void onResourceBorrowed(T resource) {
        threadName = Thread.currentThread().getName();
        borrowStackTrace = Thread.currentThread().getStackTrace();
    }

    @Override
    public void onResourceReturned(T resource) {
        threadName = null;
        borrowStackTrace = null;
    }

    public void printResourceInfo(CharSink<?> sink, T resource) {
        final StackTraceElement[] stackTrace = borrowStackTrace;
        if (stackTrace != null) {
            sink.put(threadName).put(" borrowed ").put(resource).putAscii(':');
            for (int i = 0, n = stackTrace.length; i < n; i++) {
                sink.put("\n\tat ").put(stackTrace[i].toString());
            }
        }
    }
}
