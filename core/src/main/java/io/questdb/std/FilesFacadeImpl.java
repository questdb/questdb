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

import io.questdb.cairo.CairoException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public class FilesFacadeImpl implements FilesFacade {

    public static final FilesFacade INSTANCE = new FilesFacadeImpl();
    public static final int _16M = 16 * 1024 * 1024;
    private long mapPageSize = 0;

    @Override
    public long append(long fd, long buf, int len) {
        return Files.append(fd, buf, len);
    }

    @Override
    public boolean close(long fd) {
        return Files.close(fd) == 0;
    }

    @Override
    public int copy(LPSZ from, LPSZ to) {
        return Files.copy(from, to);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public boolean exists(LPSZ path) {
        return Files.exists(path);
    }

    @Override
    public boolean exists(long fd) {
        return Files.exists(fd);
    }

    @Override
    public void findClose(long findPtr) {
        Files.findClose(findPtr);
    }

    @Override
    public long findFirst(LPSZ path) {
        long ptr = Files.findFirst(path);
        if (ptr == -1) {
            throw CairoException.instance(Os.errno()).put("findFirst failed on ").put(path);
        }
        return ptr;
    }

    @Override
    public long findName(long findPtr) {
        return Files.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        int r = Files.findNext(findPtr);
        if (r == -1) {
            throw CairoException.instance(Os.errno()).put("findNext failed");
        }
        return r;
    }

    @Override
    public int findType(long findPtr) {
        return Files.findType(findPtr);
    }

    @Override
    public long getLastModified(LPSZ path) {
        return Files.getLastModified(path);
    }

    @Override
    public int msync(long addr, long len, boolean async) {
        return Files.msync(addr, len, async);
    }

    @Override
    public int fsync(long fd) {
        return Files.fsync(fd);
    }

    @Override
    public long getMapPageSize() {
        if (mapPageSize == 0) {
            mapPageSize = computeMapPageSize();
        }
        return mapPageSize;
    }

    @Override
    public long getOpenFileCount() {
        return Files.getOpenFileCount();
    }

    @Override
    public long getPageSize() {
        return Files.PAGE_SIZE;
    }

    @Override
    public boolean isRestrictedFileSystem() {
        return Os.type == Os.WINDOWS;
    }

    @Override
    public void iterateDir(LPSZ path, FindVisitor func) {
        long p = findFirst(path);
        if (p > 0) {
            try {
                do {
                    func.onFind(findName(p), findType(p));
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
            }
        }
    }

    @Override
    public long length(long fd) {
        return Files.length(fd);
    }

    @Override
    public long length(LPSZ name) {
        return Files.length(name);
    }

    @Override
    public int lock(long fd) {
        return Files.lock(fd);
    }

    @Override
    public int mkdir(LPSZ path, int mode) {
        return Files.mkdir(path, mode);
    }

    @Override
    public int mkdirs(LPSZ path, int mode) {
        return Files.mkdirs(path, mode);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        return Files.mmap(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mmap(long fd, long len, long flags, int mode, long baseAddress, int memoryTag) {
        return Files.mmap(fd, len, flags, mode, memoryTag);
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return Files.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        Files.munmap(address, size, memoryTag);
    }

    @Override
    public long openAppend(LPSZ name) {
        return Files.openAppend(name);
    }

    @Override
    public long openRO(LPSZ name) {
        return Files.openRO(name);
    }

    @Override
    public long openRW(LPSZ name) {
        return Files.openRW(name);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        // Open files and if file exists, try exclusively lock it
        // If exclusive lock worked the file will be cleaned and allocated to the given size
        // Shared lock will be left on the file which will be removed when file descriptor is closed
        // If file did not exist, it will be allocated to the size and shared lock set
        return Files.openCleanRW(name, size);
    }

    @Override
    public long read(long fd, long buf, long len, long offset) {
        return Files.read(fd, buf, len, offset);
    }

    @Override
    public boolean remove(LPSZ name) {
        return Files.remove(name);
    }

    @Override
    public boolean rename(LPSZ from, LPSZ to) {
        return Files.rename(from, to);
    }

    @Override
    public int rmdir(Path name) {
        return Files.rmdir(name);
    }

    @Override
    public boolean touch(LPSZ path) {
        return Files.touch(path);
    }

    @Override
    public boolean truncate(long fd, long size) {
        return Files.truncate(fd, size);
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (Os.type != Os.WINDOWS) {
            return Files.allocate(fd, size);
        }
        return true;
    }

    @Override
    public long write(long fd, long address, long len, long offset) {
        return Files.write(fd, address, len, offset);
    }

    private long computeMapPageSize() {
        long pageSize = getPageSize();
        long mapPageSize = pageSize * pageSize;
        if (mapPageSize < pageSize || mapPageSize > _16M) {
            if (_16M % pageSize == 0) {
                return _16M;
            }
            return pageSize;
        } else {
            return mapPageSize;
        }
    }

}
