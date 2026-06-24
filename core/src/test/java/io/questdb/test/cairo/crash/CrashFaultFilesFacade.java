package io.questdb.test.cairo.crash;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Fault-injection FilesFacade that models the OS durability contract:
 * msync flushes data pages but only fsync makes an extended file's size durable.
 * On {@link #crash}, every file is truncated to its last-fsynced size; queued torn-tail ranges are then zeroed for deterministic fault injection.
 */
public class CrashFaultFilesFacade extends TestFilesFacadeImpl {
    // fds are tracked so fsync can map a fd back to its file; only fsync advances durableSize.
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Map<String, Long> durableSize = new HashMap<>();
    private final Map<String, List<long[]>> tornTails = new HashMap<>();
    private final java.util.List<String> syncOrder = new java.util.ArrayList<>();

    private int durabilityOps = 0;
    private int crashAtOp = -1; // -1 = disarmed

    /** Ordered list of file paths as they were fsync'd/fsyncAndClose'd (for sync-order assertions). */
    public java.util.List<String> getSyncOrder() { return syncOrder; }

    @Override
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public boolean close(long fd) {
        fdToPath.remove(fd);
        return super.close(fd);
    }

    @Override
    public void fdatasync(long fd) {
        super.fdatasync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
        }
        recordDurable(fd);
        bumpDurabilityOp();
    }

    @Override
    public void fsync(long fd) {
        super.fsync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
        }
        recordDurable(fd);
        bumpDurabilityOp();
    }

    @Override
    public void fsyncAndClose(long fd) {
        final String p = fdToPath.get(fd);
        final long size = p != null ? super.length(fd) : -1L;
        super.fsyncAndClose(fd); // performs fsync + close; close() below drops the fd
        if (p != null) {
            syncOrder.add(p);
            if (size >= 0L) {
                durableSize.put(p, size);
            }
        }
        bumpDurabilityOp();
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        super.msync(addr, len, async);
        bumpDurabilityOp();
    }

    /** Clear all tracked fd→path and durable-size state (for reuse across crash/retry cycles). */
    public void reset() {
        fdToPath.clear();
        durableSize.clear();
        tornTails.clear();
        syncOrder.clear();
        durabilityOps = 0;
        crashAtOp = -1;
    }

    /** Record current sizes of all files under dbRoot as durable ("prior committed, log-journaled"). */
    public void markDurableBaseline(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            try {
                durableSize.put(p.toAbsolutePath().toString(), java.nio.file.Files.size(p));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
    }

    /** Zero [offset, offset+len) of the given file when crash() runs (deterministic torn-write injection). */
    public void tornTail(LPSZ name, long offset, long len) {
        tornTails.computeIfAbsent(toAbsPath(name), k -> new ArrayList<>())
                .add(new long[]{offset, len});
    }

    /** Roll every file under {@code dbRoot} back to its last-fsynced size, then apply any torn-tail ranges. */
    public void crash(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            Long durable = durableSize.get(key);
            long target = durable != null ? durable : 0L;
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                if (ch.size() > target) {
                    ch.truncate(target);
                }
                List<long[]> ranges = tornTails.get(key);
                if (ranges != null) {
                    for (long[] r : ranges) {
                        if (r[1] < 0 || r[1] > Integer.MAX_VALUE) throw new IllegalArgumentException("tornTail len out of range: " + r[1]);
                        int n = (int) r[1];
                        ByteBuffer zeros = ByteBuffer.allocate(n);
                        ch.write(zeros, r[0]);
                    }
                    ch.force(true);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /** Arm a simulated crash: throw CrashSimulationError on the n-th durability op (fsync/fsyncAndClose/msync). */
    public void armCrashAt(int n) {
        this.crashAtOp = n;
    }

    /** Number of durability ops (fsync/fsyncAndClose/msync) observed so far. */
    public int durabilityOpCount() {
        return durabilityOps;
    }

    private void bumpDurabilityOp() {
        durabilityOps++;
        if (crashAtOp > 0 && durabilityOps >= crashAtOp) {
            crashAtOp = -1; // one-shot
            throw new CrashSimulationError(durabilityOps);
        }
    }

    private void recordDurable(long fd) {
        String p = fdToPath.get(fd);
        if (p != null) {
            durableSize.put(p, super.length(fd));
        }
    }

    /** Convert LPSZ (null-terminated native bytes) to a canonical absolute path String. */
    private static String toAbsPath(LPSZ name) {
        return Paths.get(Utf8String.newInstance(name).toString()).toAbsolutePath().toString();
    }

    private static void walk(CharSequence dbRoot, java.util.function.Consumer<Path> fileFn) {
        Path root = Paths.get(dbRoot.toString());
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(Files::isRegularFile).forEach(fileFn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
