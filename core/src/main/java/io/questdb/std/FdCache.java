package io.questdb.std;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.ParanoiaState.FD_PARANOIA_MODE;

public class FdCache {
    private static final int NON_CACHED = (2 << 30);
    private static final int RO_MASK = 0;
    private static final int RW_MASK = (1 << 30);
    private final AtomicInteger fdCounter = new AtomicInteger(1);
    private final Utf8SequenceObjHashMap<FdCacheRecord> openFdMap = new Utf8SequenceObjHashMap<>();
    private final LongObjHashMap<FdCacheRecord> openFdMapByFd = new LongObjHashMap<>();

    public synchronized void checkFdOpen(long fd) {
        if (openFdMapByFd.keyIndex(fd) > -1) {
            throw new IllegalStateException("fd " + fd + " is not open!");
        }
    }

    public synchronized int close(long fd) {
        int keyIndex = openFdMapByFd.keyIndex(fd);
        if (keyIndex > -1) {
            throw new IllegalStateException("fd " + fd + " is already closed!");
        }

        int fdKind = (Numbers.decodeLowInt(fd) >>> 30) & 3;
        if (fdKind > 1) {
            // RW_CLEAR or APPEND. Simply close the underlying fd.
            int osFd = Numbers.decodeHighInt(fd);
            int res = Files.close0(osFd);
            if (res != 0) {
                return res;
            }
            Files.OPEN_FILE_COUNT.decrementAndGet();
            openFdMapByFd.removeAt(keyIndex);
            return 0;
        }

        FdCacheRecord fdCacheRecord = openFdMapByFd.valueAt(keyIndex);
        if (fdCacheRecord.count == 1) {
            int res = Files.close0(fdCacheRecord.osFd);
            if (res != 0) {
                return res;
            }
            Files.OPEN_FILE_COUNT.decrementAndGet();
            openFdMapByFd.removeAt(keyIndex);
        }

        fdCacheRecord.count--;
        removeFdCacheSafe(fdCacheRecord);

        return 0;
    }

    public synchronized long createUniqueFdNonCached(int fd) {
        if (fd > -1) {
            int index = fdCounter.getAndIncrement();
            long markedFd = Numbers.encodeLowHighInts(index | NON_CACHED, fd);
            openFdMapByFd.put(markedFd, FdCacheRecord.EMPTY);
            Files.OPEN_FILE_COUNT.incrementAndGet();
            return markedFd;
        }
        return fd;
    }

    public synchronized long createUniqueFdNonCachedStdOut(int fd) {
        int index = fdCounter.getAndIncrement();
        long markedFd = Numbers.encodeLowHighInts(index | NON_CACHED, fd);
        openFdMapByFd.put(markedFd, FdCacheRecord.EMPTY);
        return markedFd;
    }

    public synchronized void detach(long fd) {
        openFdMapByFd.remove(fd);
        Files.OPEN_FILE_COUNT.decrementAndGet();
    }

    public synchronized String getOpenFdDebugInfo() {
        var sb = new StringSink();
        for (int i = 0, n = openFdMapByFd.keys.length; i < n; i++) {
            var key = openFdMapByFd.keys[i];
            if (key != -1) {
                if (sb.length() > 0) {
                    sb.put(',');
                }
                sb.put(key);
            }
        }
        return sb.toString();
    }

    public synchronized void markPathRemoved(LPSZ lpsz) {
        openFdMap.remove(lpsz);
    }

    public synchronized long openROCached(LPSZ lpsz) {
        final FdCacheRecord holder = getFdCacheRecord(lpsz, 0);
        if (holder == null) {
            // Failed to open
            return -1;
        }

        holder.count++;
        long uniqROFd = createUniqueFdRO(holder.osFd);
        openFdMapByFd.put(uniqROFd, holder);

        return uniqROFd;
    }

    public synchronized long openRWCached(LPSZ lpsz, long opts) {
        final FdCacheRecord holder = getFdCacheRecord(lpsz, opts | Files.O_CREAT);
        if (holder == null) {
            // Failed to open
            return -1;
        }

        holder.count++;
        long uniqROFd = createUniqueFdRW(holder.osFd);
        openFdMapByFd.put(uniqROFd, holder);

        return uniqROFd;
    }

    public int toOsFd(long fd) {
        if (FD_PARANOIA_MODE && fd != -1) {
            synchronized (this) {
                int keyIndex = openFdMapByFd.keyIndex(fd);
                assert keyIndex < 0 : "Invalid fd: " + fd + ", not found in cache";
            }
        }
        int osFd = Numbers.decodeHighInt(fd);
        assert fd == -1 || osFd > -1;
        return osFd;
    }

    private long createUniqueFdRO(int fd) {
        int index = fdCounter.getAndIncrement();
        return Numbers.encodeLowHighInts(index | RO_MASK, fd);
    }

    private long createUniqueFdRW(int fd) {
        int index = fdCounter.getAndIncrement();
        return Numbers.encodeLowHighInts(index | RW_MASK, fd);
    }

    @Nullable
    private FdCacheRecord getFdCacheRecord(LPSZ lpsz, long opts) {
        int keyIndex = openFdMap.keyIndex(lpsz);
        final FdCacheRecord holder;
        if (keyIndex > -1) {
            int roFd = Files.openRWOptsNoCreate(lpsz.ptr(), opts);
            if (roFd < 0) {
                // Failed to open
                holder = null;
            } else {
                Files.OPEN_FILE_COUNT.incrementAndGet();
                Utf8String path = Utf8String.newInstance(lpsz);
                holder = new FdCacheRecord(path);
                holder.osFd = roFd;
                openFdMapByFd.put(holder.osFd, holder);
                openFdMap.putAt(keyIndex, lpsz, holder);
            }
        } else {
            holder = openFdMap.valueAtQuick(keyIndex);
        }
        return holder;
    }

    private void removeFdCacheSafe(FdCacheRecord fdCacheRecord) {
        if (fdCacheRecord.count == 0) {
            int fdRecIndex = openFdMap.keyIndex(fdCacheRecord.path);
            if (fdRecIndex < 0) {
                // If the record is the same object, we can remove it
                if (openFdMap.valueAt(fdRecIndex) == fdCacheRecord) {
                    openFdMap.removeAt(fdRecIndex);
                }
            }
        }
    }

    private static class FdCacheRecord {
        private static final FdCacheRecord EMPTY = new FdCacheRecord(null);

        private final Utf8String path;
        private int count;
        private int osFd;

        public FdCacheRecord(Utf8String path) {
            this.path = path;
        }
    }
}
