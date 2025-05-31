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

package io.questdb.cairo.filter;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import static io.questdb.cairo.filter.SkipFilterUtils.*;

public class SkipFilterReaderImpl implements SkipFilterReader {
    public static final String FILTER_CORRUPT = "cursor could not consistently read filter header [corrupt?]";
    protected static final Log LOG = LogFactory.getLog(SkipFilterReaderImpl.class);
    protected final MemoryMR bucketMem = Vm.getCMRInstance();
    protected MillisecondClock clock;
    protected int keyCount;
    protected long spinLockTimeoutMs;

    public SkipFilterReaderImpl(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long unIndexedNullCount
    ) {
        of(configuration, path, name, columnNameTxn, unIndexedNullCount);
    }

    @Override
    public void close() {
        // Do not add isOpen() condition here.
        // Failures in opening should have memories closed as well.
        Misc.free(bucketMem);
    }

    @Override
    public boolean isOpen() {
        return bucketMem.isOpen();
    }

    @Override
    public boolean maybeContainsHash(long hash) {
        int i1 = bucketFromHash(bucketMem, hash >> 32);
        int tag = tagFromHash(bucketMem, hash);
        int i2 = altBucket(bucketMem, i1, tag);

        assert i1 == altBucket(bucketMem, i2, tag);
        int victimTag = bucketMem.getInt(OFFSET_VICTIM_TAG);
        int victimIndex = bucketMem.getInt(OFFSET_VICTIM_INDEX);
        boolean found = (victimTag != 0 && tag == victimTag)
                && (i1 == victimIndex || i2 == victimIndex);
        return found || isTagInBuckets(i1, i2, tag);
    }


    @Override
    public void of(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn, long unIndexedNullCount) {
        final int plen = path.size();
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();

        try {
            bucketMem.wholeFile(configuration.getFilesFacade(), SkipFilterUtils.bucketFileName(path, name, columnNameTxn), MemoryTag.MMAP_INDEX_READER);
            this.clock = configuration.getMillisecondClock();

            // key file should already be created at least with header
            long bucketMemSize = bucketMem.size();
            if (bucketMemSize < SkipFilterUtils.HEADER_RESERVED) {
                LOG.error().$("file too short [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Filter file too short: ").put(path);
            }

            // verify header signature
            if (bucketMem.getByte(SkipFilterUtils.OFFSET_SIGNATURE) != SkipFilterUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    int findTagInBucket(int index, int tag) {
        for (int slot = 0; slot < DEFAULT_BUCKET_SIZE; slot++) {
            int value = readTag(bucketMem, index, slot);
            if (value == tag) {
                return slot;
            }
        }
        return -1;
    }

    boolean isTagInBucket(int index, int tag) {
        return findTagInBucket(index, tag) >= 0;
    }

    boolean isTagInBuckets(int i1, int i2, int tag) {
        return isTagInBucket(i1, tag) | isTagInBucket(i2, tag);
    }
}
