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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;

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
import java.util.Arrays;
import java.util.function.Consumer;
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
            // The writer seals the entry.
            Assert.assertEquals(
                    WalUtils.sealEventChecksumEntry(
                            0,
                            WalUtils.WALE_HEADER_SIZE,
                            recordLength,
                            readLong(checksum, WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_VALUE_OFFSET)
                    ),
                    readInt(checksum, WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_SEAL_OFFSET)
            );
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
            // Release the pooled WalWriter before mutating _event: it still holds the file mapped, and
            // Windows refuses to write to a file with a user-mapped section open. Same pattern as
            // testMissingSidecarReadsUnverifiedRatherThanSuspending below.
            engine.releaseInactive();
            Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            byte[] event = Files.readAllBytes(eventPath);
            event[WalUtils.WALE_HEADER_SIZE + Integer.BYTES + Long.BYTES + 1] ^= 0x40;
            Files.write(eventPath, event);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
        });
    }

    /**
     * A flipped bit in the sidecar breaks the entry's seal, so the damage is the sidecar's, not the
     * record's: the intact record applies unverified. A record that disagrees with an intact entry still
     * suspends, see {@link #testCorruptBodySuspendsTable}.
     */
    @Test
    public void testCorruptSidecarEntryReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            damageSidecar(tt, checksum -> checksum[entryOffset(0) + WalUtils.WALE_CHECKSUM_ENTRY_VALUE_OFFSET] ^= 0x40);
            assertAllCommitsApplied(tt);
        });
    }

    /**
     * A NOSYNC/ASYNC power cut can zero a mid-segment {@code _event.c} entry while its record survives.
     * The record must apply unverified instead of suspending the table as torn.
     */
    @Test
    public void testLostSidecarEntryReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            damageSidecar(tt, checksum -> Arrays.fill(
                    checksum, entryOffset(1), entryOffset(1) + WalUtils.WALE_CHECKSUM_ENTRY_SIZE, (byte) 0));
            assertAllCommitsApplied(tt);
        });
    }

    /**
     * Entries straddle page boundaries, so a lost page can take half an entry. A half entry is absent,
     * not torn.
     */
    @Test
    public void testHalfLandedSidecarEntryReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            damageSidecar(tt, checksum -> {
                // Page boundary after the offset field: everything past it lost.
                Arrays.fill(checksum, entryOffset(1) + Long.BYTES, entryOffset(2), (byte) 0);
                // Page boundary before the checksum field: only the checksum lost.
                Arrays.fill(checksum, entryOffset(2) + WalUtils.WALE_CHECKSUM_ENTRY_VALUE_OFFSET,
                        entryOffset(3), (byte) 0);
            });
            assertAllCommitsApplied(tt);
        });
    }

    /**
     * After a power cut the sidecar can end before entries whose records survived. Those entries are
     * absent, not a corrupt sidecar.
     */
    @Test
    public void testSidecarShorterThanItsRecordsReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            final Path checksumPath = findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME);
            try (java.io.RandomAccessFile f = new java.io.RandomAccessFile(checksumPath.toFile(), "rw")) {
                f.setLength(entryOffset(1));
            }
            assertAllCommitsApplied(tt);
        });
    }

    /**
     * A zero sidecar header is a lost first page, so the segment reads as if it had no sidecar.
     */
    @Test
    public void testZeroSidecarHeaderReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            damageSidecar(tt, checksum -> Arrays.fill(checksum, 0, WalUtils.WALE_CHECKSUM_HEADER_SIZE, (byte) 0));
            assertAllCommitsApplied(tt);
        });
    }

    /**
     * A corrupt txn field must not point verification at an empty slot. The apply path knows which txn it
     * expects, so it checks the record against that entry and the corrupt field fails the hash.
     */
    @Test
    public void testCorruptRecordTxnStillSuspends() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            final Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            final byte[] event = Files.readAllBytes(eventPath);
            final int record0 = WalUtils.WALE_HEADER_SIZE;
            final int record1 = record0 + readInt(event, record0);
            Assert.assertEquals("precondition: record 1 holds txn 1", 1, readLong(event, record1 + Integer.BYTES));
            ByteBuffer.wrap(event, record1 + Integer.BYTES, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(1_000);
            Files.write(eventPath, event);

            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
            TestUtils.assertContains(
                    engine.getTableSequencerAPI().getTxnTracker(tt).getErrorMessage(),
                    "torn WAL event record [txn=1"
            );
        });
    }

    /**
     * A live-tail walk reaches entries past the sidecar mapping taken when the segment opened. The cursor
     * must grow the mapping and verify them, so a corrupt record there is still caught.
     */
    @Test
    public void testWalkVerifiesEntriesPastTheInitialSidecarMapping() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTableWithThreeCommits();
            final Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            final Path sidecarPath = findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME);
            final byte[] event = Files.readAllBytes(eventPath);
            final int record0 = WalUtils.WALE_HEADER_SIZE;
            final int record1 = record0 + readInt(event, record0);
            final int record2 = record1 + readInt(event, record1);
            // Past the length, txn and type, like testCorruptBodySuspendsTable.
            event[record2 + Integer.BYTES + Long.BYTES + 1] ^= 0x40;
            Files.write(eventPath, event);

            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (
                    io.questdb.std.str.Path path = new io.questdb.std.str.Path();
                    MemoryCMRImpl eventMem = new MemoryCMRImpl(ff, path.of(eventPath.toString()).$(), -1, MemoryTag.MMAP_TABLE_WAL_READER);
                    // Mapped as if opened before records 1 and 2 were appended: header and entry 0 only.
                    MemoryCMRImpl sidecarMem = new MemoryCMRImpl(ff, path.of(sidecarPath.toString()).$(), entryOffset(1), MemoryTag.MMAP_TABLE_WAL_READER)
            ) {
                final WalEventCursor cursor = new WalEventCursor(eventMem, sidecarMem);
                cursor.setChecksumRequired(true, null);
                cursor.reset();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, cursor.getTxn());
                Assert.assertTrue("record 1 is intact and its entry lies past the initial mapping", cursor.hasNext());
                Assert.assertEquals(1, cursor.getTxn());
                Assert.assertTrue("the cursor must have grown the sidecar mapping", sidecarMem.size() > entryOffset(1));
                try {
                    cursor.hasNext();
                    Assert.fail("record 2 is corrupt and its entry is intact, so it must be rejected, not read unverified");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "torn WAL event record [txn=2");
                }
            }
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

    private void assertAllCommitsApplied(TableToken tt) throws Exception {
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
        assertQuery("select count(), sum(v) from x").noLeakCheck().noRandomAccess().expectSize().returns("""
                count\tsum
                3\t6
                """);
    }

    private static TableToken createTableWithThreeCommits() throws Exception {
        execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
        execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
        execute("insert into x values ('2024-01-01T00:00:01.000000Z', 2)");
        execute("insert into x values ('2024-01-01T00:00:02.000000Z', 3)");
        final TableToken tt = engine.verifyTableName("x");
        // Release the pooled WalWriter before mutating its files -- see testCorruptBodySuspendsTable.
        engine.releaseAllWalWriters();
        final byte[] event = Files.readAllBytes(findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME));
        Assert.assertEquals(
                "precondition: one segment holds all three commits",
                2,
                readInt(event, (int) WalUtils.WALE_MAX_TXN_OFFSET_32)
        );
        return tt;
    }

    private static void damageSidecar(TableToken tt, Consumer<byte[]> damage) throws Exception {
        final Path checksumPath = findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME);
        final byte[] checksum = Files.readAllBytes(checksumPath);
        damage.accept(checksum);
        Files.write(checksumPath, checksum);
    }

    private static int entryOffset(int txn) {
        return WalUtils.WALE_CHECKSUM_HEADER_SIZE + txn * WalUtils.WALE_CHECKSUM_ENTRY_SIZE;
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
            if (rejectsSidecarMapping(fd, len, offset, flags)) {
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        /**
         * WalEventReader maps the sidecar with madviseOpts != -1, which routes MemoryCMRImpl.map()
         * through TableUtils.mapRONoCache() -> ff.mmapNoCache(), a method that does not delegate to
         * mmap(). Overriding mmap() alone leaves the real call path untouched, so this fake has to
         * intercept both entry points or it models Windows for a call nobody makes.
         */
        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            if (rejectsSidecarMapping(fd, len, offset, flags)) {
                return -1;
            }
            return super.mmapNoCache(fd, len, offset, flags, memoryTag);
        }

        private boolean rejectsSidecarMapping(long fd, long len, long offset, int flags) {
            final Long real = sidecarFds.get(fd);
            if (real == null) {
                return false;
            }
            sawSidecarMapping.set(true);
            if (flags == io.questdb.std.Files.MAP_RO && offset + len > real) {
                // CreateFileMapping under PAGE_READONLY cannot grow the file: ERROR_NOT_ENOUGH_MEMORY.
                oversizedLen.set(len);
                oversizedReal.set(real);
                return true;
            }
            return false;
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
