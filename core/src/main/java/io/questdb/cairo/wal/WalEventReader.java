/*+*****************************************************************************
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
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.*;

public class WalEventReader implements Closeable {
    private final WalEventCursor eventCursor;
    private final MemoryCMR eventChecksumMem;
    private final MemoryCMR eventIndexMem;
    private final MemoryCMR eventMem;
    private final FilesFacade ff;

    public WalEventReader(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        boolean bypassFdCache = configuration.getBypassWalFdCache();
        eventChecksumMem = Vm.getCMRInstance(bypassFdCache);
        eventIndexMem = Vm.getCMRInstance(bypassFdCache);
        eventMem = Vm.getCMRInstance(bypassFdCache);
        eventCursor = new WalEventCursor(eventMem, eventChecksumMem);
    }

    @Override
    public void close() {
        // WalEventReader is re-usable after close, don't assign nulls.
        // Closing is also idempotent.
        Misc.free(eventChecksumMem);
        Misc.free(eventIndexMem);
        Misc.free(eventMem);
    }

    public WalEventCursor of(Path path, long segmentTxn) {
        // The reader needs to deal with:
        //   * _event and _event.i truncation.
        //   * mmap-written data which was not persisted to disk and appears as zeros when read back.

        final int trimTo = path.size();
        try {
            final int pathLen = path.size();
            path.concat(EVENT_FILE_NAME);

            // We initially just map the header which contains the `maxTxn` value.
            // We will later `.extend(..)` this to contain the full file size.
            eventMem.of(
                    ff,
                    path.$(),
                    ff.getPageSize(),
                    WALE_HEADER_SIZE + Integer.BYTES,
                    MemoryTag.MMAP_TABLE_WAL_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            // Check only lower short to match the version
            // Higher short can be used to make a forward compatible change
            // by adding more data at the footer of each record
            final int version = eventMem.getInt(WAL_FORMAT_OFFSET_32);
            final short formatVersion = Numbers.decodeLowShort(version);
            if (formatVersion != WALE_FORMAT_VERSION
                    && formatVersion != WALE_MAT_VIEW_FORMAT_VERSION
                    && formatVersion != WALE_VIEW_FORMAT_VERSION
                    && formatVersion != WALE_LIVE_VIEW_FORMAT_VERSION) {
                throw TableUtils.validationException()
                        .put("WAL events file version does not match runtime version [expected=")
                        .put(WALE_FORMAT_VERSION).put(", ").put(WALE_MAT_VIEW_FORMAT_VERSION)
                        .put(", ").put(WALE_VIEW_FORMAT_VERSION).put(" or ").put(WALE_LIVE_VIEW_FORMAT_VERSION)
                        .put(", actual=").put(formatVersion).put(']');
            }
            // The sidecar's own presence is the capability. Nothing in _event promises one exists, because
            // such a promise is exactly what a copy of this segment cannot keep: a backup that skipped the
            // file, or a replica whose uploader enumerates segment files from a fixed list, would arrive
            // with the promise intact and the file gone, and the table would suspend on data that is fine.
            // Verification is therefore best-effort by construction -- present and valid means verify,
            // absent means read unverified, and a sidecar that IS present still has to be well-formed.
            final boolean checksumRequired;
            path.trimTo(pathLen).concat(EVENT_CHECKSUM_FILE_NAME);
            // Presence probe ONLY. This stat must not size the mapping: the writer preallocates the
            // sidecar and truncates it down to its used size when it finalises the segment, so a size
            // captured here is stale the moment that truncation lands.
            if (ff.length(path.$()) >= WALE_CHECKSUM_HEADER_SIZE) {
                // Size from the OPEN fd (size < 0), so the length used is the one the file has once this
                // reader holds it open. Passing the stat above instead asks to map more than the file
                // holds, which Linux allows -- the reader never touches past the valid entries, so it
                // goes unnoticed -- while Windows rejects it outright: CreateFileMapping cannot extend a
                // file under PAGE_READONLY, and ApplyWal2TableJob suspends the table. See
                // WalEventChecksumTest#testSidecarMappingIsSizedFromTheOpenFdNotAStalePathStat.
                eventChecksumMem.of(
                        ff,
                        path.$(),
                        ff.getPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WAL_READER,
                        CairoConfiguration.O_NONE,
                        Files.POSIX_MADV_RANDOM
                );
                // Re-validate against what was actually mapped, not what the stat promised.
                final long checksumSize = eventChecksumMem.size();
                if (checksumSize < WALE_CHECKSUM_HEADER_SIZE) {
                    throw TableUtils.validationException()
                            .put("WAL event checksum sidecar is truncated [path=").put(path)
                            .put(", size=").put(checksumSize).put(']');
                }
                checksumRequired = true;
                if (eventChecksumMem.getLong(0) != WALE_CHECKSUM_MAGIC
                        || eventChecksumMem.getInt(Long.BYTES) != WALE_CHECKSUM_FILE_VERSION
                        || eventChecksumMem.getInt(Long.BYTES + Integer.BYTES) != WALE_CHECKSUM_ENTRY_SIZE) {
                    throw TableUtils.validationException().put("invalid WAL event checksum sidecar header [path=").put(path).put(']');
                }
            } else {
                checksumRequired = false;
                eventChecksumMem.close();
            }
            eventCursor.setChecksumRequired(checksumRequired);
            path.trimTo(pathLen).concat(EVENT_FILE_NAME);

            if (segmentTxn > -1) {
                final int maxTxn = eventMem.getInt(WALE_MAX_TXN_OFFSET_32);
                if (maxTxn < -1) {
                    final int errno = 0;
                    throw CairoException.critical(errno).put("segment ")
                            .put(path).put(" does not have a valid maxTxn: ")
                            .put(maxTxn);
                }

                // To read the `_event.i` fully we need to map it so we can read the `maxTxn` transaction.
                // N.B. `+ 2 is for: (+1) to skip to one past the end and (+1) for the size of the data to read.
                final long fullEventIndexMapSize = (maxTxn + 2L) * Long.BYTES;

                // But if that mapping fails (say, because didn't persist all the data), we want to at least
                // map until the specific `segmentTxn` we want to read.
                // N.B. `+ 2` same as above.
                final long minEventIndexMapSize = (segmentTxn + 2L) * Long.BYTES;

                try {
                    eventIndexMem.of(
                            ff,
                            path.trimTo(pathLen).concat(EVENT_INDEX_FILE_NAME).$(),
                            ff.getPageSize(),
                            fullEventIndexMapSize,
                            MemoryTag.MMAP_TABLE_WAL_READER,
                            CairoConfiguration.O_NONE,
                            Files.POSIX_MADV_RANDOM
                    );
                } catch (CairoException _couldNotMapToMaxTxn) {
                    // Windows-only code:
                    // Mapping outside the file range is possible on non-windows platform.
                    // If data is accessed, however, it would fail.
                    // On windows, we'd observe an exception, for best effort we retry the mapping.
                    // See `testWalEventReaderMaxTxnTooLarge`.
                    eventIndexMem.of(
                            ff,
                            path.trimTo(pathLen).concat(EVENT_INDEX_FILE_NAME).$(),
                            ff.getPageSize(),
                            minEventIndexMapSize,
                            MemoryTag.MMAP_TABLE_WAL_READER,
                            CairoConfiguration.O_NONE,
                            Files.POSIX_MADV_RANDOM
                    );
                }

                try {
                    // The offset to the start of the record in `_event` pointed to by `segmentTxn`.
                    final long offset = readNonNegativeLong(eventIndexMem, segmentTxn * Long.BYTES);

                    // The `_event` file length, as determined by the `maxTxn` value in the `_event` header.
                    long size = readNonNegativeLong(eventIndexMem, (maxTxn + 1L) * Long.BYTES);

                    // N.B.
                    // The `_event` file starts with a header. If we're reading a section of the file that was grown
                    // by setting the file size (via the mmap writer logic) and either never written or persisted,
                    // we'd read a zero-value offset or size.
                    // The following two conditionals catch two partial and full corruption cases when data was not
                    // fully flushed to disk.

                    // Case 1: Lost some data, but have the `segmentTxn` record.
                    // The offset for the `segmentTxn` points to valid data,
                    // but the `maxTxn`-calculated `size` is nonsensical.
                    if (offset >= WALE_HEADER_SIZE && size < WALE_HEADER_SIZE + Integer.BYTES) {
                        // We curtail the `_event` size to just what we strictly need.
                        size = readNonNegativeLong(eventIndexMem, (segmentTxn + 1L) * Long.BYTES);
                    }

                    // Case 2: Data is corrupt or was never flushed and is all zeros.
                    // The `+ Integer.BYTES` here is to include the len-prefix of each `_event` entry.
                    if (offset < WALE_HEADER_SIZE || size < WALE_HEADER_SIZE + Integer.BYTES || offset >= size) {
                        final int errno = offset < 0 || size < 0 ? ff.errno() : 0;
                        final long fileSize = ff.length(eventMem.getFd());

                        throw CairoException.critical(errno).put("segment ")
                                .put(path).put(" does not have txn with id ").put(segmentTxn)
                                .put(", offset=").put(offset)
                                .put(", indexFileSize=").put(fileSize)
                                .put(", maxTxn=").put(maxTxn)
                                .put(", size=").put(size);
                    }

                    // The `size` here should _correctly_ mark the end of the last record we can read.
                    // There's a gotcha though:
                    // Each entry in the `_event` file has a 32-bit int byte len at the start.
                    // When we're done writing an entry we do two things:
                    //   * Update the length for the written record.
                    //   * Add an empty (zero) length for the next (future) record.
                    // We rely on this second entry to determine that we're done reading.
                    // As such, we need this extra `+ Integer.BYTES` here, or we would not be able to read it.
                    final long eventMapSize = size + Integer.BYTES;
                    eventMem.extend(eventMapSize);
                    eventCursor.openOffset(offset);
                } finally {
                    Misc.free(eventIndexMem);
                }
            } else {
                eventCursor.openOffset(-1);
            }

            return eventCursor;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(trimTo);
        }
    }

    /**
     * Read a non-negative long at the specified offset, or return -1 if out of bounds.
     */
    private static long readNonNegativeLong(MemoryCMR eventIndexMem, long offset) {
        if ((offset < 0) || ((offset + Long.BYTES) > eventIndexMem.size())) {
            return -1;
        }
        return eventIndexMem.getLong(offset);
    }
}
