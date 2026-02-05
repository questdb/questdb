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

package io.questdb.cairo.vm;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

// This is read-only contiguous mapped memory that does not leave file descriptor open.
// It is intended to be used in TableReader exclusively to reduce file descriptor usage.
public class MemoryCMRDetachedImpl extends MemoryCMRImpl {
    private static final Log LOG = LogFactory.getLog(MemoryCMRDetachedImpl.class);

    public MemoryCMRDetachedImpl(FilesFacade ff, LPSZ name, long size, int memoryTag, boolean keepFdOpen) {
        this(ff, name, size, memoryTag, keepFdOpen, -1);
    }

    public MemoryCMRDetachedImpl(FilesFacade ff, LPSZ name, long size, int memoryTag, boolean keepFdOpen, int madviseOpts) {
        of(ff, name, 0, size, memoryTag, 0, madviseOpts, keepFdOpen);
    }

    @Override
    public void extend(long newSize) {
        throw new IllegalStateException("not supported");
    }

    @Override
    public long getFd() {
        throw new IllegalStateException("not supported");
    }

    @Override
    public boolean isOpen() {
        return pageAddress != 0;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        of(ff, name, extendSegmentSize, size, memoryTag, opts, madviseOpts, false);
    }

    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts, boolean keepFdOpen) {
        super.of(ff, name, extendSegmentSize, size, memoryTag, opts, madviseOpts);
        if (!keepFdOpen && ff != null && ff.close(fd)) {
            LOG.debug().$("closing [fd=").$(fd).I$();
            fd = -1;
        }
    }

    public boolean tryChangeSize(long newSize) {
        if (newSize == size()) {
            return true;
        }
        if (newSize > 0) {
            if (fd != -1) {
                super.changeSize(newSize);
                return true;
            }
        } else {
            close();
            return true;
        }
        return false;
    }

}
