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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

import static io.questdb.cairo.ColumnVersionWriter.*;

public class ColumnVersionReader implements Closeable {
    private final static Log LOG = LogFactory.getLog(ColumnVersionReader.class);
    private final MemoryCMR mem;
    private final LongList cachedList = new LongList();
    private long version;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionReader() {
        this.mem = Vm.getCMRInstance();
    }

    public ColumnVersionReader ofRO(FilesFacade ff, LPSZ fileName) {
        version = -1;
        this.mem.of(ff, fileName, 0, HEADER_SIZE, MemoryTag.MMAP_TABLE_READER);
        return this;
    }

    @Override
    public void close() {
        mem.close();
    }

    public LongList getCachedList() {
        return cachedList;
    }

    public long getVersion() {
        return version;
    }

    public void readSafe(MicrosecondClock microsecondClock, long spinLockTimeoutUs) {
        long deadline = microsecondClock.getTicks() + spinLockTimeoutUs;
        while (true) {
            long version = unsafeGetVersion();
            if (version == this.version) {
                return;
            }
            Unsafe.getUnsafe().loadFence();

            boolean areaA = version % 2 == 0;
            long offset = areaA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
            long size = areaA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);

            Unsafe.getUnsafe().loadFence();
            if (version == unsafeGetVersion()) {
                resize(offset + size);
                readUnsafe(offset, size);

                Unsafe.getUnsafe().loadFence();
                if (version == unsafeGetVersion()) {
                    this.version = version;
                    LOG.debug().$("read clean version ").$(version).$(", offset ").$(offset).$(", size ").$(size).$();
                    return;
                }
            }

            if (microsecondClock.getTicks() > deadline) {
                LOG.error().$("Column Version read timeout [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                throw CairoException.instance(0).put("Column Version read timeout");
            }
            Os.pause();
            LOG.debug().$("read dirty version ").$(version).$(", retrying").$();
        }
    }

    private void readUnsafe(long offset, long areaSize) {
        resize(offset + areaSize);

        int i = 0;
        long p = offset;
        long lim = offset + areaSize;

        assert areaSize % ColumnVersionWriter.BLOCK_SIZE_BYTES == 0;

        cachedList.setPos((int) ((areaSize / (ColumnVersionWriter.BLOCK_SIZE_BYTES)) * BLOCK_SIZE));

        while (p < lim) {
            cachedList.setQuick(i, mem.getLong(p));
            cachedList.setQuick(i + 1, mem.getLong(p + 8));
            cachedList.setQuick(i + 2, mem.getLong(p + 16));
            i += ColumnVersionWriter.BLOCK_SIZE;
            p += ColumnVersionWriter.BLOCK_SIZE_BYTES;
        }
    }

    private long unsafeGetVersion() {
        return mem.getLong(OFFSET_VERSION_64);
    }

    public void resize(long size) {
        mem.resize(size);
    }
}
