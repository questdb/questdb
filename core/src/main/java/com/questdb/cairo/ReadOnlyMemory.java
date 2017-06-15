package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.str.LPSZ;

public class ReadOnlyMemory extends VirtualMemory {
    private long fd = -1;

    public ReadOnlyMemory(LPSZ name, long maxPageSize, long size) {
        of(name, maxPageSize, size);
    }

    public ReadOnlyMemory() {
    }

    @Override
    public void close() {
        super.close();
        if (fd != -1) {
            Files.close(fd);
            fd = -1;
        }
    }

    @Override
    protected long getPageAddress(int page) {
        long address = super.getPageAddress(page);
        if (address != 0) {
            return address;
        }
        return mapPage(page);
    }

    @Override
    protected void release(long address) {
        Files.munmap(address, pageSize);
    }

    public void of(LPSZ name, long maxPageSize, long size) {
        close();

        boolean exists = Files.exists(name);
        if (!exists) {
            throw new RuntimeException("file not  found");
        }
        fd = Files.openRO(name);
        if (fd == -1) {
            throw new RuntimeException("cannot open file");
        }

        if (size > maxPageSize) {
            setPageSize(maxPageSize);
        } else {
            setPageSize(Math.max(Files.PAGE_SIZE, (size / Files.PAGE_SIZE) * Files.PAGE_SIZE));
        }
        pages.ensureCapacity((int) (size / this.pageSize + 1));
    }

    private long mapPage(int page) {
        long address;
        address = Files.mmap(fd, pageSize, pageOffset(page), Files.MAP_RO);
        if (address == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        cachePageAddress(page, address);
        return address;
    }
}
