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
            Assert.assertEquals(WalUtils.WALE_CHECKSUM_FEATURE_VERSION, Numbers.decodeHighShort(readInt(event, WalUtils.WAL_FORMAT_OFFSET_32)));
            Assert.assertEquals(WalUtils.WALE_CHECKSUM_MAGIC, readLong(checksum, 0));
            final int recordLength = readInt(event, WalUtils.WALE_HEADER_SIZE);
            Assert.assertEquals(recordLength, readInt(checksum,
                    WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_LENGTH_OFFSET));
            Assert.assertEquals(WalUtils.WALE_HEADER_SIZE, readLong(checksum,
                    WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_OFFSET_OFFSET));
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

    @Test
    public void testCapabilityCannotBeClearedWhileSidecarExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path eventPath = findWalFile(tt.getDirName(), WalUtils.EVENT_FILE_NAME);
            byte[] event = Files.readAllBytes(eventPath);
            writeInt(event, WalUtils.WAL_FORMAT_OFFSET_32,
                    Numbers.encodeLowHighShorts(Numbers.decodeLowShort(readInt(event, WalUtils.WAL_FORMAT_OFFSET_32)), (short) 0));
            Files.write(eventPath, event);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
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
}
