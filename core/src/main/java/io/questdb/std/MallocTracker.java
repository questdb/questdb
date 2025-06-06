package io.questdb.std;

public class MallocTracker {

    private static class AllocationInfo {
        final long size;
        final int tag;
        final StackTraceElement[] stackTrace;

        AllocationInfo(long size, int tag, StackTraceElement[] stackTrace) {
            this.size = size;
            this.tag = tag;
            this.stackTrace = stackTrace;
        }
    }

    private final LongObjHashMap<AllocationInfo> allocations = new LongObjHashMap<>();

    private static StackTraceElement[] captureStackTrace() {
        StackTraceElement[] fullTrace = Thread.currentThread().getStackTrace();
        // skip getStackTrace() + captureStackTrace()
        if (fullTrace.length <= 2) {
            return new StackTraceElement[0];
        }
        StackTraceElement[] trimmed = new StackTraceElement[fullTrace.length - 2];
        System.arraycopy(fullTrace, 2, trimmed, 0, trimmed.length);
        return trimmed;
    }

    public synchronized void trackMalloc(long ptr, long size, int tag) {
        if (ptr != 0) {
            allocations.put(ptr, new AllocationInfo(size, tag, captureStackTrace()));
        }
    }

    public synchronized void trackRealloc(long oldPtr, long newPtr, long newSize, int tag) {
        if (oldPtr != 0) {
            allocations.remove(oldPtr);
        }
        if (newPtr != 0) {
            allocations.put(newPtr, new AllocationInfo(newSize, tag, captureStackTrace()));
        }
    }

    public synchronized void trackFree(long ptr) {
        if (ptr != 0) {
            allocations.remove(ptr);
        }
    }

    public synchronized long getAllocationSize(long ptr) {
        AllocationInfo info = allocations.get(ptr);
        return info != null ? info.size : -1;
    }

    public synchronized int getAllocationTag(long ptr) {
        AllocationInfo info = allocations.get(ptr);
        return info != null ? info.tag : -1;
    }

    public synchronized StackTraceElement[] getAllocationStackTrace(long ptr) {
        AllocationInfo info = allocations.get(ptr);
        return info != null ? info.stackTrace : null;
    }

    public synchronized void dumpLeaks() {
        if (allocations.size() == 0) {
            System.err.println("No memory leaks detected.");
            return;
        }

        System.err.println("Memory leaks detected: " + allocations.size());
        allocations.forEach((ptr, info) -> {
            String tagName = MemoryTag.nameOf(info.tag);
            System.err.println("Leaked ptr: 0x" + Long.toHexString(ptr) +
                    " size=" + info.size + " bytes tag=" + tagName + "(" + info.tag + ")");
            if (info.stackTrace != null) {
                for (StackTraceElement ste : info.stackTrace) {
                    System.err.println("\tat " + ste);
                }
            }
            System.err.println();
        });
    }
}
