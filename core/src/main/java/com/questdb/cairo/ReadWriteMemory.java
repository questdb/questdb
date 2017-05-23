package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.VirtualMemory;
import com.questdb.std.str.LPSZ;

public class ReadWriteMemory extends VirtualMemory {
    protected long fd = -1;

    public ReadWriteMemory(LPSZ name, int maxPageSize, long size, int defaultPageSize) {
        of(name, maxPageSize, size, defaultPageSize);
    }

    public ReadWriteMemory() {
    }

    @Override
    public void close() {
        super.close();
        if (fd != -1) {
            Files.truncate(fd, size());
            Files.close(fd);
            fd = -1;
        }
    }

    @Override
    protected void addPage(long address) {
        // don't call super, page will have already been added
    }

    @Override
    protected long allocateNextPage() {
        return mapPage(pages.size());
    }

    @Override
    protected long getPageAddress(int page) {
        long address = pages.getQuick(page);
        if (address != 0) {
            return address;
        }
        return mapPage(page);
    }

    @Override
    protected void release(long address) {
        Files.munmap0(address, pageSize);
    }

    public final void of(LPSZ name, int maxPageSize, long size, int defaultPageSize) {
        close();

        fd = Files.openRW(name);
        if (fd == -1) {
            throw new RuntimeException("cannot open file");
        }
        configurePageSize(size, defaultPageSize, maxPageSize);
    }

    protected final void configurePageSize(long size, int defaultPageSize, int maxPageSize) {
        if (size > maxPageSize) {
            setPageSize(maxPageSize);
        } else {
            setPageSize(Math.max(defaultPageSize, (int) ((size / Files.PAGE_SIZE) * Files.PAGE_SIZE)));
        }
        pages.ensureCapacity((int) (size / this.pageSize + 1));
    }

    private long mapPage(int page) {
        long address;
        long offset = pageOffset(page);

        if (Files.length(fd) < offset + pageSize) {
            Files.truncate(fd, offset + pageSize);
        }

        address = Files.mmap0(fd, pageSize, offset, Files.MAP_RW);

        if (address == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        pages.extendAndSet(page, address);
        return address;
    }
}
