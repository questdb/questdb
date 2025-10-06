package io.questdb.cairo.pool;

import io.questdb.std.ObjObjHashMap;
import io.questdb.std.str.CharSink;

/**
 * Resource pool supervisor that tracks borrowed resources and store information about the time
 * when they were borrowed.
 * This class implementation is not thread safe, each thread should have its own instance.
 */
public class TracingResourcePoolSupervisor<T> implements ResourcePoolSupervisor<T> {
    private final ObjObjHashMap<T, StackTraceElement[]> resources = new ObjObjHashMap<>();
    private String threadName;

    @Override
    public void onResourceBorrowed(T resource) {
        // We wait for a resource to be borrowed before we track the thread's name to ensure that
        // it was properly initialized.
        if (threadName == null) {
            threadName = Thread.currentThread().getName();
        }
        resources.put(resource, Thread.currentThread().getStackTrace());
    }

    @Override
    public void onResourceReturned(T resource) {
        resources.remove(resource);
    }

    @Override
    public void printResourceInfo(CharSink<?> sink, T resource) {
        StackTraceElement[] stackTrace = resources.get(resource);
        if (stackTrace != null) {
            sink.put(threadName).put(" borrowed ").put(resource.toString()).putAscii(':');
            for (StackTraceElement traceElement : stackTrace) {
                sink.put("\n\tat ").put(traceElement.toString());
            }
        }
    }
}
