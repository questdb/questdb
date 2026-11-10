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

package io.questdb.test.cairo;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxWriter;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fails, once when armed, the extension of the _txn file of one table by its writer.
 * <p>
 * The tests size their commits for Linux x86-64. Its 4 KB page holds the _txn file of a new table, and a
 * commit that adds 300 partitions writes a _txn record of about 10 KB: the writer extends its mapping of the
 * file, finds the file shorter than the new mapping and allocates disk space, which fails. The page size is
 * 16 KB on macOS arm64 and the allocation granularity 64 KB on Windows, so there the record fits in the
 * first page of the mapping, and the file of a new table is a page long. This facade makes every platform
 * behave as Linux: the _txn writers map their files in steps of 4 KB, and while armed the _txn file of the
 * table reads as empty, so that the next extension of the mapping allocates disk space.
 */
public class TxnExtendFailingFacade extends TestFilesFacadeImpl {
    private static final long TXN_WRITER_PAGE_SIZE = 4 * 1024;
    public final AtomicBoolean isArmed = new AtomicBoolean();
    private final String tableDirPrefix;
    private volatile long txnFd = -1;

    public TxnExtendFailingFacade(String tableName) {
        this.tableDirPrefix = Files.SEPARATOR + tableName;
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (fd == txnFd && isArmed.compareAndSet(true, false)) {
            return false;
        }
        return super.allocate(fd, size);
    }

    @Override
    public long getPageSize() {
        final long pageSize = super.getPageSize();
        return pageSize > TXN_WRITER_PAGE_SIZE && isTxnWriterCalling() ? TXN_WRITER_PAGE_SIZE : pageSize;
    }

    @Override
    public long length(long fd) {
        return fd == txnFd && isArmed.get() ? 0 : super.length(fd);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        if (
                Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                        && (Utf8s.containsAscii(name, tableDirPrefix + '~') || Utf8s.containsAscii(name, tableDirPrefix + Files.SEPARATOR))
        ) {
            txnFd = fd;
        }
        return fd;
    }

    // Whether a _txn writer asks for the page size, which it does only to open its file. The other callers keep
    // the page size of the platform: the configuration hands it to memory that maps pages at offsets of it
    private static boolean isTxnWriterCalling() {
        return StackWalker.getInstance().walk(
                frames -> frames.anyMatch(frame -> TxWriter.class.getName().equals(frame.getClassName()))
        );
    }
}
