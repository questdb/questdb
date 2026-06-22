package io.questdb.test.cairo.crash;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Fault-injection FilesFacade that models the OS durability contract:
 * msync flushes data pages but only fsync makes an extended file's size durable.
 * On {@link #crash}, every file is truncated to its last-fsynced size.
 */
public class CrashFaultFilesFacade extends TestFilesFacadeImpl {
    // fds are tracked so fsync can map a fd back to its file; only fsync advances durableSize.
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Map<String, Long> durableSize = new HashMap<>();

    // openCleanRW and openAppend are intentionally not overridden; they are not used on the commit path this harness exercises.
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
    public void fsync(long fd) {
        super.fsync(fd);
        recordDurable(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        final String p = fdToPath.get(fd);
        final long size = p != null ? super.length(fd) : -1L;
        super.fsyncAndClose(fd); // performs fsync + close; close() below drops the fd
        if (p != null && size >= 0L) {
            durableSize.put(p, size);
        }
    }

    /** Clear all tracked fd→path and durable-size state (for reuse across crash/retry cycles). */
    public void reset() {
        fdToPath.clear();
        durableSize.clear();
    }

    /** Roll every file under {@code dbRoot} back to its last-fsynced size. */
    public void crash(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            Long durable = durableSize.get(key);
            long target = durable != null ? durable : 0L;
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                if (ch.size() > target) {
                    ch.truncate(target);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
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
