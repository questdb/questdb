package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.str.LPSZ;

public class ReadWriteMemory extends VirtualMemory {
    private long fd = -1;
    private long size;

    public ReadWriteMemory(LPSZ name, long maxPageSize, long size, long defaultPageSize) {
        of(name, maxPageSize, size, defaultPageSize);
    }

    public ReadWriteMemory() {
        size = 0;
    }

    @Override
    public void close() {
        long size = size();
        super.close();
        if (fd != -1) {
            Files.truncate(fd, size);
            Files.close(fd);
            fd = -1;
        }
    }

    @Override
    public void jumpTo(long offset) {
        this.size = Math.max(this.size, getAppendOffset());
        super.jumpTo(offset);
    }

    @Override
    protected long allocateNextPage(int page) {
        return mapPage(page);
    }

    @Override
    protected long getPageAddress(int page) {
        long address;
        if (page < pages.size()) {
            address = pages.getQuick(page);
            if (address != 0) {
                return address;
            }
        }
        address = mapPage(page);
        cachePageAddress(page, address);
        return address;
    }

    @Override
    protected void release(long address) {
        Files.munmap(address, pageSize);
    }

    public long getFd() {
        return fd;
    }

    public final void of(LPSZ name, long maxPageSize, long size, long defaultPageSize) {
        close();

        fd = Files.openRW(name);
        if (fd == -1) {
            throw CairoException.instance().put("Cannot open file: ").put(name);
        }
        configurePageSize(size, defaultPageSize, maxPageSize);
    }

    public long size() {
        if (size < getAppendOffset()) {
            size = getAppendOffset();
        }
        return size;
    }

    protected final void configurePageSize(long size, long defaultPageSize, long maxPageSize) {
        if (size > maxPageSize) {
            setPageSize(maxPageSize);
        } else {
            setPageSize(Math.max(defaultPageSize, (size / Files.PAGE_SIZE) * Files.PAGE_SIZE));
        }
        pages.ensureCapacity((int) (size / this.pageSize + 1));
        this.size = size;
    }

    private long mapPage(int page) {
        long address;
        long offset = pageOffset(page);

        if (Files.length(fd) < offset + pageSize) {
            Files.truncate(fd, offset + pageSize);
        }

        address = Files.mmap(fd, pageSize, offset, Files.MAP_RW);

        if (address == -1) {
            throw CairoException.instance().put("Cannot mmap(RW) fd=").put(fd).put(", offset").put(offset).put(", size").put(pageSize);
        }
        return address;
    }
}
