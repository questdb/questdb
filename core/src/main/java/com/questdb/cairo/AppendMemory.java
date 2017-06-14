package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.ObjectFactory;
import com.questdb.std.str.LPSZ;

public class AppendMemory extends VirtualMemory {
    public static final ObjectFactory<AppendMemory> FACTORY = AppendMemory::new;

    private long fd = -1;
    private long pageAddress = 0;
    private long size;

    public AppendMemory(LPSZ name, long pageSize, long size) {
        of(name, pageSize, size);
    }

    public AppendMemory() {
        size = 0;
    }

    @Override
    public void close() {
        long sz = getAppendOffset();
        super.close();
        releaseCurrentPage();
        if (fd != -1) {
            Files.truncate(fd, sz);
            Files.close(fd);
            fd = -1;
        }
    }

    @Override
    public void jumpTo(long offset) {
        updateSize();
        super.jumpTo(offset);
    }

    @Override
    protected long getPageAddress(int page) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        long address = mapPage(page);
        if (address == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        return pageAddress = address;
    }

    @Override
    protected void release(long address) {
        Files.munmap(address, pageSize);
    }

    public long getFd() {
        return fd;
    }

    public final void of(LPSZ name, long pageSize, long size) {
        of(name, pageSize);
        setSize(size);
    }

    public final void of(LPSZ name, long pageSize) {
        close();
        setPageSize(pageSize);
        fd = Files.openRW(name);
        if (fd == -1) {
            throw new RuntimeException("cannot open file");
        }
    }

    public final void setSize(long size) {
        this.size = size;
        releaseCurrentPage();
        jumpTo(size);
    }

    public long size() {
        if (size < getAppendOffset()) {
            size = getAppendOffset();
        }
        return size;
    }

    public void truncate() {
        this.size = 0;
        releaseCurrentPage();
        Files.truncate(fd, pageSize);
        updateLimits(0, pageAddress = mapPage(0));
    }

    private long mapPage(int page) {
        long target = pageOffset(page + 1);
        if (Files.length(fd) < target && !Files.truncate(fd, target)) {
            throw new RuntimeException("Cannot resize file");
        }
        return Files.mmap(fd, pageSize, pageOffset(page), Files.MAP_RW);
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(pageAddress);
            pageAddress = 0;
        }
    }

    private void updateSize() {
        this.size = Math.max(this.size, getAppendOffset());
    }
}
