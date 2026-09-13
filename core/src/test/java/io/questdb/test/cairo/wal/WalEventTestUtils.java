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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.Assert;

/**
 * Helpers for tests that tamper with a WAL segment's {@code _event} file.
 */
public final class WalEventTestUtils {

    private WalEventTestUtils() {
    }

    /**
     * Re-derives every {@code _event.c} sidecar entry in the segment {@code segmentPath} points at from
     * the {@code _event} bytes currently on disk.
     * <p>
     * Any test that edits {@code _event} in place must call this. Each record carries a mandatory sidecar
     * entry (offset, length, xxh3 of the body), {@code WalEventCursor.verifyRecordChecksum()} runs before
     * anything else looks at the record, and a record whose bytes no longer match its entry is TORN -- so
     * without a re-stamp the reader rejects the edit and the table suspends, long before the behaviour the
     * test is actually about. Re-deriving the entries keeps the edit "well-formed but wrong", which is what
     * such tests mean by it; detecting bytes that do NOT match their checksum is covered by
     * {@link WalEventChecksumTest}.
     *
     * @param segmentPath path to the segment directory; left unchanged on return
     */
    public static void restampEventChecksums(CairoConfiguration configuration, Path segmentPath, Log log) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int segmentLen = segmentPath.size();
        try {
            segmentPath.concat(WalUtils.EVENT_FILE_NAME);
            final long eventFd = TableUtils.openRW(ff, segmentPath.$(), log, configuration.getWriterFileOpenOpts());
            final long eventSize = ff.length(eventFd);
            final long eventMem = TableUtils.mapRW(ff, eventFd, eventSize, MemoryTag.NATIVE_DEFAULT);

            segmentPath.trimTo(segmentLen).concat(WalUtils.EVENT_CHECKSUM_FILE_NAME);
            final long sidecarFd = TableUtils.openRW(ff, segmentPath.$(), log, configuration.getWriterFileOpenOpts());
            final long sidecarSize = ff.length(sidecarFd);
            final long sidecarMem = TableUtils.mapRW(ff, sidecarFd, sidecarSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long offset = WalUtils.WALE_HEADER_SIZE;
                while (offset + Integer.BYTES <= eventSize) {
                    final int length = Unsafe.getUnsafe().getInt(eventMem + offset);
                    if (length < 1 || offset + length > eventSize) {
                        break; // end-of-events marker
                    }
                    final long txn = Unsafe.getUnsafe().getLong(eventMem + offset + Integer.BYTES);
                    final long entry = WalUtils.WALE_CHECKSUM_HEADER_SIZE + txn * WalUtils.WALE_CHECKSUM_ENTRY_SIZE;
                    Assert.assertTrue(
                            "sidecar has no entry for txn=" + txn + " [sidecarSize=" + sidecarSize + ']',
                            entry + WalUtils.WALE_CHECKSUM_ENTRY_SIZE <= sidecarSize
                    );
                    // Body only, matching WalEventWriter.finishRecord(): the writer fills the entry before
                    // it publishes the length, so the length header cannot be inside the hashed region.
                    final long checksum = TableUtils.calculateCvAreaChecksum(
                            eventMem + offset + Integer.BYTES, length - Integer.BYTES);
                    Unsafe.getUnsafe().putLong(sidecarMem + entry + WalUtils.WALE_CHECKSUM_ENTRY_OFFSET_OFFSET, offset);
                    Unsafe.getUnsafe().putInt(sidecarMem + entry + WalUtils.WALE_CHECKSUM_ENTRY_LENGTH_OFFSET, length);
                    Unsafe.getUnsafe().putLong(sidecarMem + entry + WalUtils.WALE_CHECKSUM_ENTRY_VALUE_OFFSET, checksum);
                    offset += length;
                }
            } finally {
                ff.munmap(sidecarMem, sidecarSize, MemoryTag.NATIVE_DEFAULT);
                ff.close(sidecarFd);
                ff.munmap(eventMem, eventSize, MemoryTag.NATIVE_DEFAULT);
                ff.close(eventFd);
            }
        } finally {
            segmentPath.trimTo(segmentLen);
        }
    }
}
