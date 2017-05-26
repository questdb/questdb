package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.VirtualMemory;
import com.questdb.std.str.LPSZ;

public class ReadOnlyMemory extends VirtualMemory {
    private long fd;

    public ReadOnlyMemory(LPSZ name, int maxPageSize, long size) {
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
            setPageSize((int) ((size / Files.PAGE_SIZE) * Files.PAGE_SIZE));
        }
        pages.ensureCapacity((int) (size / this.pageSize + 1));
    }

    @Override
    public void close() {
        super.close();
        Files.close(fd);
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
        Files.munmap(address, pageSize);
    }

    private long mapPage(int page) {
        long address;
        address = Files.mmap(fd, pageSize, pageOffset(page), Files.MAP_RO);
        if (address == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        pages.extendAndSet(page, address);
        return address;
    }
}
