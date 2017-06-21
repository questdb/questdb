package com.questdb.misc;

import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;

public interface FilesFacade {
    int close(long fd);

    int errno();

    boolean exists(LPSZ path);

    void findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    boolean findNext(long findPtr);

    long getOpenFileCount();

    long getPageSize();

    long length(long fd);

    int mkdirs(LPSZ path, int mode);

    long mmap(long fd, long size, long offset, int mode);

    void munmap(long address, long size);

    long openRO(LPSZ name);

    long openRW(LPSZ name);

    long read(long fd, long buf, int size, long offset);

    boolean rmdir(CompositePath name);

    boolean truncate(long fd, long size);
}
