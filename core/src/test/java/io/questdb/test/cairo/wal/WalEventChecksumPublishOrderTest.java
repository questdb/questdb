/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * A record in a WAL segment's {@code _event} file must never become readable before the {@code _event.c}
 * entry that describes it exists.
 * <p>
 * The store of the record length at {@code startOffset} is what flips the slot from the {@code -1}
 * end-of-events marker to a real length, so it PUBLISHES the record. {@code finishRecord()} used to
 * perform that store first and fill the sidecar entry afterwards, leaving a window in which a reader
 * following the tail of a segment the writer is still appending to reads the entry's preallocated zeros
 * and reports {@code storedOffset=0, storedLen=0, expected=0} -- a torn record, on a system where nothing
 * went wrong. It suspended a live view on arm64 mac CI while every x86 Linux leg stayed green.
 */
public class WalEventChecksumPublishOrderTest extends AbstractCairoTest {

    /**
     * The length header sits OUTSIDE the hashed region -- the writer cannot hash a length it has not
     * written yet without publishing the record first -- so this pins the compensating check: a length
     * that does not match the sidecar's {@code storedLength} must still be rejected. Without it, moving
     * the region would silently drop the header from verification.
     */
    @Test
    public void testCorruptLengthHeaderStillRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            final TableToken tt = engine.verifyTableName("x");
            engine.releaseAllWalWriters();

            final Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            final byte[] event = Files.readAllBytes(eventPath);
            final int length = readInt(event, WalUtils.WALE_HEADER_SIZE);
            writeInt(event, WalUtils.WALE_HEADER_SIZE, length + 8);
            Files.write(eventPath, event);

            drainWalQueue();
            Assert.assertTrue(
                    "a length header that disagrees with the sidecar entry must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
        });
    }

    /**
     * Drives a reader over the tail of a segment while the writer appends to it, asserting the invariant
     * directly: every record readable in {@code _event} has a filled entry in {@code _event.c}.
     * <p>
     * The reader reads both files with bounded pread rather than mapping them. A mapping sized when the
     * reader opens the file is stale the moment the writer extends it, and touching past the file's end
     * is a SIGBUS -- the failure mode is the harness's, not the code's.
     */
    @Test
    public void testTailReaderNeverSeesRecordAheadOfItsSidecarEntry() throws Exception {
        // The reader reads files the writer holds open; QuestDB's Windows lock takes byte 0 exclusively,
        // so ReadFile returns ERROR_LOCK_VIOLATION. The ordering under test is platform-independent and
        // the POSIX legs cover it.
        Assume.assumeFalse("reads files a writer holds open, which the Windows lock forbids", Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tt = engine.verifyTableName("x");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");

            final java.nio.file.Path segment = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(), WalUtils.WAL_NAME_BASE + "1", "0");

            final int commits = 5_000;
            final AtomicBoolean done = new AtomicBoolean();
            final AtomicReference<String> violation = new AtomicReference<>();
            final AtomicReference<Throwable> readerError = new AtomicReference<>();
            final CyclicBarrier start = new CyclicBarrier(2);

            final Thread reader = new Thread(() -> {
                try {
                    start.await();
                    while (!done.get() && violation.get() == null) {
                        scanForUndescribedRecord(segment, violation);
                    }
                } catch (Throwable t) {
                    readerError.compareAndSet(null, t);
                }
            });
            reader.start();
            start.await();

            try {
                for (int i = 1; i <= commits && violation.get() == null; i++) {
                    execute("INSERT INTO x VALUES (" + (1_700_000_000_000_000L + i) + "::timestamp, " + i + ")");
                }
            } finally {
                done.set(true);
                reader.join();
            }

            if (readerError.get() != null) {
                throw new AssertionError("reader thread failed", readerError.get());
            }
            Assert.assertNull(violation.get(), violation.get());
        });
    }

    private static java.nio.file.Path findWalFile(CharSequence tableDirName, String name) throws Exception {
        java.nio.file.Path tableDir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tableDirName.toString());
        try (Stream<java.nio.file.Path> s = Files.walk(tableDir)) {
            return s.filter(p -> p.getFileName().toString().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no " + name + " under " + tableDir));
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static byte[] readSnapshot(java.nio.file.Path path) {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            final long size = ch.size();
            if (size <= 0 || size > Integer.MAX_VALUE) {
                return null;
            }
            final ByteBuffer buf = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            while (buf.hasRemaining() && ch.read(buf) >= 0) {
                // read to the snapshot boundary
            }
            return buf.array();
        } catch (Exception _fileIsBeingRolledOrNotThereYet) {
            return null;
        }
    }

    /**
     * Walks the records visible in one snapshot of {@code _event} and checks each against the same
     * snapshot's {@code _event.c}. Reading _event.c AFTER _event is deliberate: it can only make the
     * sidecar look MORE complete than it was, so a zero entry found here was genuinely missing while the
     * record was already readable.
     */
    private static void scanForUndescribedRecord(java.nio.file.Path segment, AtomicReference<String> violation) {
        final byte[] event = readSnapshot(segment.resolve(WalUtils.EVENT_FILE_NAME));
        if (event == null) {
            return;
        }
        final byte[] sidecar = readSnapshot(segment.resolve(WalUtils.EVENT_CHECKSUM_FILE_NAME));
        if (sidecar == null || sidecar.length < WalUtils.WALE_CHECKSUM_HEADER_SIZE) {
            return;
        }
        int offset = WalUtils.WALE_HEADER_SIZE;
        while (offset + Integer.BYTES <= event.length) {
            final int length = readInt(event, offset);
            if (length < 1 || offset + length > event.length) {
                return; // end-of-events marker, or a record past this snapshot's boundary
            }
            final long txn = ByteBuffer.wrap(event, offset + Integer.BYTES, Long.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN).getLong();
            if (txn < 0 || txn > 1 << 20) {
                return;
            }
            final long entry = WalUtils.WALE_CHECKSUM_HEADER_SIZE + txn * WalUtils.WALE_CHECKSUM_ENTRY_SIZE;
            if (entry + WalUtils.WALE_CHECKSUM_ENTRY_SIZE > sidecar.length) {
                violation.compareAndSet(null, "record txn=" + txn + " at offset=" + offset
                        + " is readable but the sidecar has no room for its entry [sidecarLen=" + sidecar.length + ']');
                return;
            }
            final long storedOffset = ByteBuffer.wrap(sidecar,
                            (int) (entry + WalUtils.WALE_CHECKSUM_ENTRY_OFFSET_OFFSET), Long.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN).getLong();
            final int storedLength = readInt(sidecar, (int) (entry + WalUtils.WALE_CHECKSUM_ENTRY_LENGTH_OFFSET));
            if (storedOffset != offset || storedLength != length) {
                violation.compareAndSet(null, "record txn=" + txn + " at offset=" + offset + ", len=" + length
                        + " was published ahead of its sidecar entry [storedOffset=" + storedOffset
                        + ", storedLen=" + storedLength + ']');
                return;
            }
            offset += length;
        }
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    }
}
