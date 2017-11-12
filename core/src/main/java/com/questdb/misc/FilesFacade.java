package com.questdb.misc;

import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;

public interface FilesFacade {
    long append(long fd, long buf, int len);

    boolean close(long fd);

    int errno();

    boolean exists(LPSZ path);

    void findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    long getOpenFileCount();

    long getPageSize();

    void iterateDir(LPSZ path, FindVisitor func);

    long length(long fd);

    long length(LPSZ name);

    int mkdirs(LPSZ path, int mode);

    long mmap(long fd, long size, long offset, int mode);

    void munmap(long address, long size);

    long openAppend(LPSZ name);

    long openRO(LPSZ name);

    long openRW(LPSZ name);

    long read(long fd, long buf, int size, long offset);

    boolean remove(LPSZ name);

    boolean rename(LPSZ from, LPSZ to);

    boolean rmdir(Path name);

    boolean truncate(long fd, long size);

    long write(long fd, long address, long len, long offset);

    boolean exists(long fd);

    boolean supportsTruncateMappedFiles();

    int lock(long fd);
}
