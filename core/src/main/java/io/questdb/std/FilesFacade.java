/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public interface FilesFacade {
    long MAP_FAILED = -1;

    long append(long fd, long buf, int len);

    boolean close(long fd);

    int copy(LPSZ from, LPSZ to);

    int errno();

    boolean exists(LPSZ path);

    boolean exists(long fd);

    void findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    long getLastModified(LPSZ path);

    int msync(long addr, long len, boolean async);

    int fsync(long fd);

    long getMapPageSize();

    long getOpenFileCount();

    long getPageSize();

    boolean isRestrictedFileSystem();

    void iterateDir(LPSZ path, FindVisitor func);

    long length(long fd);

    long length(LPSZ name);

    int lock(long fd);

    int mkdir(LPSZ path, int mode);

    int mkdirs(LPSZ path, int mode);

    long mmap(long fd, long len, long offset, int flags, int memoryTag);

    long mmap(long fd, long len, long offset, int flags, long baseAddress, int memoryTag);

    long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag);

    void munmap(long address, long size, int memoryTag);

    long openAppend(LPSZ name);

    long openRO(LPSZ name);

    long openRW(LPSZ name);

    long openCleanRW(LPSZ name, long size);

    long read(long fd, long buf, long size, long offset);

    boolean remove(LPSZ name);

    boolean rename(LPSZ from, LPSZ to);

    int rmdir(Path name);

    boolean touch(LPSZ path);

    boolean truncate(long fd, long size);

    boolean allocate(long fd, long size);

    long write(long fd, long address, long len, long offset);
}
