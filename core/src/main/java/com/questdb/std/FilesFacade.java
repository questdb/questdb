/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;

public interface FilesFacade {
    long append(long fd, long buf, int len);

    boolean close(long fd);

    int errno();

    boolean exists(LPSZ path);

    boolean exists(long fd);

    void findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    long getLastModified(LPSZ path);

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

    long mmap(long fd, long size, long offset, int mode);

    void munmap(long address, long size);

    long openAppend(LPSZ name);

    long openRO(LPSZ name);

    long openRW(LPSZ name);

    long read(long fd, long buf, long size, long offset);

    boolean remove(LPSZ name);

    boolean rename(LPSZ from, LPSZ to);

    boolean rmdir(Path name);

    boolean touch(LPSZ path);

    boolean truncate(long fd, long size);

    long write(long fd, long address, long len, long offset);
}
