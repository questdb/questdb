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
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.std.str.Utf8s;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WalEventChecksumTest extends AbstractCairoTest {

    @Test
    public void testChecksumSidecarPreservesLegacyRecordLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            byte[] event = Files.readAllBytes(findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME));
            byte[] checksum = Files.readAllBytes(findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME));
            // _event's high short carries no feature bit: the sidecar's own presence is the capability.
            Assert.assertEquals(0, Numbers.decodeHighShort(readInt(event, WalUtils.WAL_FORMAT_OFFSET_32)));
            Assert.assertEquals(WalUtils.WALE_CHECKSUM_MAGIC, readLong(checksum, 0));
            final int recordLength = readInt(event, WalUtils.WALE_HEADER_SIZE);
            Assert.assertEquals(recordLength, readInt(checksum,
                    WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_LENGTH_OFFSET));
            Assert.assertEquals(WalUtils.WALE_HEADER_SIZE, readLong(checksum,
                    WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_OFFSET_OFFSET));
        });
    }

    /**
     * The sidecar mapping must be sized from the OPEN fd, not from a path stat taken beforehand.
     * <p>
     * {@code WalEventReader.of()} used to stat {@code _event.c} by path and then open and map it as two
     * separate steps. A writer finalising the segment truncates the preallocated sidecar down to its used
     * size in between, so the reader asks to map the PREALLOCATED length of a file that is now much
     * shorter. On Linux that oversized read-only mapping succeeds -- the reader only touches entries
     * inside the valid region, so nothing faults and the bug is invisible. On Windows
     * {@code CreateFileMapping} cannot extend a file under {@code PAGE_READONLY} and fails outright with
     * {@code ERROR_NOT_ENOUGH_MEMORY} (8), so {@code ApplyWal2TableJob} suspends the table:
     * <pre>
     * cannot read WAL event file for seqTxn=1179, could not mmap [size=65536, offset=0, fileLen=88]
     * </pre>
     * Reported against a 100M-row Windows ingest under adaptive, 4/4 runs, never on Linux with the same
     * jar and workload. Linux masks the defect rather than not having it, which is why this test models
     * the Windows mapping rule explicitly instead of relying on the host platform.
     */
    /**
     * Sizing from the open fd narrows the window but does not close it: the length is read inside
     * {@code MemoryCMRImpl.of()} and used by the {@code mmap} on the next line. A truncation landing
     * between those two statements asks Windows to map more than the file holds all over again, and the
     * table suspends with the same error the path stat produced.
     * <p>
     * The window is two adjacent statements with no syscall between them rather than the
     * stat-open-validate span the reported failure needed, so this is far less likely -- but the
     * truncation comes from another thread and can land anywhere, and the cost on Windows is a suspended
     * table needing manual intervention. {@code _event.i} already carries the same bounded retry a few
     * lines below, for this exact platform reason.
     */
    @Test
    public void testSidecarMappingSurvivesTruncationBetweenLengthAndMap() throws Exception {
        final StaleStatWindowsMappingFacade ff = new StaleStatWindowsMappingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            ff.armFdLengthRace();
            engine.releaseInactive();
            execute("insert into x values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertTrue(
                    "precondition: the fd-length race must have fired, else this asserts nothing",
                    ff.fdLengthRaceFired()
            );
            assertQuery("select count() from x").noRandomAccess().expectSize().returns("""
                    count
                    2
                    """);
        });
    }

    /**
     * The retry above must be bounded, not a swallow. With the over-report made permanent both attempts
     * fail, and the table must still suspend -- otherwise a genuine mapping fault would be retried once
     * and then silently ignored, which is worse than the bug the retry fixes.
     */
    @Test
    public void testPersistentMappingFailureStillSuspendsRatherThanBeingRetriedAway() throws Exception {
        final StaleStatWindowsMappingFacade ff = new StaleStatWindowsMappingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");

            ff.armFdLengthRacePermanently();
            engine.releaseInactive();
            execute("insert into x values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertTrue(
                    "precondition: the over-report must have been served",
                    ff.fdLengthRaceFired()
            );
            Assert.assertTrue(
                    "a mapping that fails every attempt must surface, not be retried away",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
        });
    }

    @Test
    public void testSidecarMappingIsSizedFromTheOpenFdNotAStalePathStat() throws Exception {
        final StaleStatWindowsMappingFacade ff = new StaleStatWindowsMappingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            // Arm only now: the table is built, so from here every _event.c stat reports the size the
            // sidecar had BEFORE the writer finalised the segment, which is what the reader would have
            // captured had it stat'd a moment earlier.
            ff.arm();
            engine.releaseInactive();
            execute("insert into x values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertTrue(
                    "precondition: the reader must have mapped a sidecar while the stale stat was armed",
                    ff.sawSidecarMapping()
            );
            Assert.assertFalse(
                    "the sidecar mapping must never exceed the file's real length; Windows rejects that"
                            + " outright and the table suspends. Oversized request: " + ff.oversizedDetail(),
                    ff.sawOversizedMapping()
            );
            assertQuery("select count() from x").noRandomAccess().expectSize().returns("""
                    count
                    2
                    """);
        });
    }

    @Test
    public void testCorruptBodySuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            byte[] event = Files.readAllBytes(eventPath);
            event[WalUtils.WALE_HEADER_SIZE + Integer.BYTES + Long.BYTES + 1] ^= 0x40;
            Files.write(eventPath, event);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
        });
    }

    @Test
    public void testCorruptSidecarSuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path checksumPath = findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME);
            byte[] checksum = Files.readAllBytes(checksumPath);
            checksum[WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_VALUE_OFFSET] ^= 0x40;
            Files.write(checksumPath, checksum);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
        });
    }

    /**
     * The inverse of what this used to assert. There is no capability marker to clear, so a segment whose
     * {@code _event} says nothing about checksums still verifies against a sidecar that is present -- and,
     * more importantly, a segment that arrives WITHOUT its sidecar reads unverified instead of suspending
     * the table. See {@code OlderBinaryWriteCompatTest#testWalSegmentMissingItsEventChecksumSidecarStillApplies}.
     */
    @Test
    public void testMissingSidecarReadsUnverifiedRatherThanSuspending() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            engine.releaseInactive();
            Files.delete(findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count() from x").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testGenuineLegacyRecordWithoutSidecarStillReads() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            engine.releaseAllWalWriters();
            Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            Path checksumPath = findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME);
            byte[] event = Files.readAllBytes(eventPath);
            writeInt(event, WalUtils.WAL_FORMAT_OFFSET_32,
                    Numbers.encodeLowHighShorts(Numbers.decodeLowShort(readInt(event, WalUtils.WAL_FORMAT_OFFSET_32)), (short) 0));
            Files.write(eventPath, event);
            Files.delete(checksumPath);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count() from x").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    static Path findEventFile(CharSequence tableDirName) throws Exception {
        return findWalFile(tableDirName, WalUtils.EVENT_FILE_NAME);
    }

    private static Path findWalFile(CharSequence tableDirName, String name) throws Exception {
        Path tableDir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tableDirName.toString());
        try (Stream<Path> s = Files.walk(tableDir)) {
            return s.filter(p -> p.getFileName().toString().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no " + name + " under " + tableDir));
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static long readLong(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    }

    /**
     * Models the two halves of the reported Windows failure: a path stat that reports the sidecar's
     * PREALLOCATED size (the writer truncates it immediately afterwards), and a read-only mmap that
     * refuses to extend the file, as {@code CreateFileMapping} does under {@code PAGE_READONLY}.
     * <p>
     * Fault injection only -- it models a documented platform rule, and asserts nothing about what a
     * correct reader would ask for beyond "not more than the file holds".
     */
    private static class StaleStatWindowsMappingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean fdLengthRaceArmed = new AtomicBoolean();
        private final AtomicBoolean fdLengthRaceFired = new AtomicBoolean();
        private final AtomicBoolean fdLengthRacePermanent = new AtomicBoolean();
        private final AtomicLong oversizedLen = new AtomicLong(-1);
        private final AtomicLong oversizedReal = new AtomicLong(-1);
        private final AtomicBoolean sawSidecarMapping = new AtomicBoolean();
        private final ConcurrentHashMap<Long, Long> sidecarFds = new ConcurrentHashMap<>();

        public void arm() {
            armed.set(true);
        }

        /**
         * Makes the NEXT length-of-open-fd call for the sidecar over-report, modelling a truncation that
         * lands after the reader has measured the file but before it maps it. One-shot, so a retry that
         * measures again sees the true length.
         */
        public void armFdLengthRace() {
            fdLengthRaceArmed.set(true);
        }

        /**
         * Over-reports the sidecar length on EVERY measurement, so the retry fails too. Discriminates a
         * bounded retry from a swallowed error.
         */
        public void armFdLengthRacePermanently() {
            fdLengthRacePermanent.set(true);
        }

        public boolean fdLengthRaceFired() {
            return fdLengthRaceFired.get();
        }

        @Override
        public long length(long fd) {
            final long real = super.length(fd);
            if (real > 0 && sidecarFds.containsKey(fd)
                    && (fdLengthRacePermanent.get() || fdLengthRaceArmed.compareAndSet(true, false))) {
                fdLengthRaceFired.set(true);
                return real * 2;
            }
            return real;
        }

        @Override
        public long length(LPSZ name) {
            final long real = super.length(name);
            if (armed.get() && Utf8s.endsWithAscii(name, WalUtils.EVENT_CHECKSUM_FILE_NAME) && real > 0) {
                // The stat the reader would have taken before the writer finalised the segment.
                return Math.max(real, PREALLOCATED_SIDECAR_SIZE);
            }
            return real;
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final Long real = sidecarFds.get(fd);
            if (real != null) {
                sawSidecarMapping.set(true);
                if (flags == io.questdb.std.Files.MAP_RO && offset + len > real) {
                    // CreateFileMapping under PAGE_READONLY cannot grow the file: ERROR_NOT_ENOUGH_MEMORY.
                    oversizedLen.set(len);
                    oversizedReal.set(real);
                    return -1;
                }
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            if (fd > -1 && Utf8s.endsWithAscii(name, WalUtils.EVENT_CHECKSUM_FILE_NAME)) {
                // Record the REAL length against the open fd, which is what Windows enforces against.
                sidecarFds.put(fd, super.length(fd));
            }
            return fd;
        }

        @Override
        public boolean close(long fd) {
            sidecarFds.remove(fd);
            return super.close(fd);
        }

        public String oversizedDetail() {
            return "size=" + oversizedLen.get() + " fileLen=" + oversizedReal.get();
        }

        public boolean sawOversizedMapping() {
            return oversizedLen.get() >= 0;
        }

        public boolean sawSidecarMapping() {
            return sawSidecarMapping.get();
        }
    }

    private static final long PREALLOCATED_SIDECAR_SIZE = 64 * 1024;

}
