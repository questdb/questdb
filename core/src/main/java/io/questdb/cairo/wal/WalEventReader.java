/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.openRO;
import static io.questdb.cairo.TableUtils.validateMetaVersion;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalEventReader implements Closeable {
    private final Log LOG = LogFactory.getLog(WalEventReader.class);
    private final WalEventCursor eventCursor;
    private final MemoryMR eventMem;
    private final FilesFacade ff;

    public WalEventReader(FilesFacade ff) {
        this.ff = ff;
        eventMem = Vm.getCMRInstance();
        eventCursor = new WalEventCursor(eventMem);
    }

    @Override
    public void close() {
        // WalEventReader is re-usable after close, don't assign nulls
        Misc.free(eventMem);
    }

    public WalEventCursor of(Path path, int expectedVersion, long segmentTxn) {
        int trimTo = path.size();
        try {
            final int pathLen = path.size();

            path.concat(EVENT_FILE_NAME);
            eventMem.of(
                    ff,
                    path.$(),
                    ff.getPageSize(),
                    WALE_HEADER_SIZE + Integer.BYTES,
                    MemoryTag.MMAP_TABLE_WAL_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            if (segmentTxn > -1) {
                // Read record offset and size
                long fdi = openRO(ff, path.trimTo(pathLen).concat(EVENT_INDEX_FILE_NAME).$(), LOG);
                try {
                    int maxTxn = eventMem.getInt(WALE_MAX_TXN_OFFSET_32);
                    long offset = ff.readNonNegativeLong(fdi, segmentTxn << 3);
                    long size = ff.readNonNegativeLong(fdi, (maxTxn + 1L) << 3);

                    if (offset > -1 && size < WALE_HEADER_SIZE + Integer.BYTES) {
                        // index file may not contain all records from data file, but it should contain
                        // the transaction we need to read, e.g. segmentTxn
                        size = ff.readNonNegativeLong(fdi, (segmentTxn + 1L) << 3);
                    }

                    if (offset < 0 || size < WALE_HEADER_SIZE + Integer.BYTES || offset >= size) {
                        int errno = offset < 0 || size < 0 ? ff.errno() : 0;
                        long fileSize = ff.length(fdi);

                        throw CairoException.critical(errno).put("segment ")
                                .put(path).put(" does not have txn with id ").put(segmentTxn)
                                .put(", offset=").put(offset)
                                .put(", indexFileSize=").put(fileSize)
                                .put(", maxTxn=").put(maxTxn)
                                .put(", size=").put(size);
                    }

                    // WAL-E file has record indicator for the next record always present
                    // so file size is the size read + 4 bytes
                    eventMem.extend(size + Integer.BYTES);
                    eventCursor.openOffset(offset);
                } finally {
                    ff.close(fdi);
                }
            } else {
                eventCursor.openOffset(-1);
            }

            validateMetaVersion(eventMem, WAL_FORMAT_OFFSET_32, expectedVersion);
            return eventCursor;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(trimTo);
        }
    }
}
