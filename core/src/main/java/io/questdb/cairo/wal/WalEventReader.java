/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalEventReader implements Closeable {
    private final Log LOG = LogFactory.getLog(WalEventReader.class);
    private final WalEventCursor eventCursor;
    private final MemoryMR eventMem;
    private final FilesFacade ff;

    public WalEventReader(FilesFacade ff) {
        this.ff = ff;
        eventMem = Vm.getMRInstance();
        eventCursor = new WalEventCursor(eventMem);
    }

    @Override
    public void close() {
        // WalEventReader is re-usable after close, don't assign nulls
        Misc.free(eventMem);
    }

    public WalEventCursor of(Path path, int expectedVersion, long segmentTxn) {
        int trimTo = path.length();
        try {
            final int fd = openRO(ff, path.concat(EVENT_FILE_NAME).$(), LOG);

            // read event file size to map to correct length.
            long size;
            try {
                size = ff.readNonNegativeLong(fd, WALE_SIZE_OFFSET);
            } finally {
                ff.close(fd);
            }

            if (size < WALE_HEADER_SIZE + Integer.BYTES) {
                // minimum we need is WAL_FORMAT_VERSION (int) and END_OF_EVENTS (long)
                throw CairoException.critical().put("File is too small, size=").put(size).put(", required=").put(WALE_HEADER_SIZE);
            }
            eventMem.of(ff, path, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_WAL_READER, CairoConfiguration.O_NONE, -1);
            validateMetaVersion(eventMem, WAL_FORMAT_OFFSET, expectedVersion);

            if (eventCursor.setPosition(segmentTxn)) {
                return eventCursor;
            }

            throw CairoException.critical().put("segment does not have txn with id ").put(segmentTxn);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(trimTo);
        }
    }
}
