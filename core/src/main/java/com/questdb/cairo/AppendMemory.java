package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.std.VirtualMemory;
import com.questdb.std.str.LPSZ;

public class AppendMemory extends VirtualMemory {
    private final long fd;
    private long pageAddress = 0;
    private int page;

    public AppendMemory(LPSZ name, int pageSize, long size) {
        super(pageSize);
        fd = Files.openRW(name);
        if (fd == -1) {
            throw new RuntimeException("cannot open file");
        }
        page = pageIndex(size);
        updateLimits(page + 1, pageAddress = mapPage(page));
        skip((size - pageOffset(page)));
    }

    @Override
    public void close() {
        super.close();
        Files.truncate(fd, size());
        if (pageAddress != 0) {
            Files.munmap0(pageAddress, pageSize);
            pageAddress = 0;
        }
        Files.close(fd);
    }

    @Override
    protected void addPage(long address) {
    }

    @Override
    protected long allocateNextPage() {
        if (pageAddress != 0) {
            release(pageAddress);
        }
        pageAddress = mapPage(++page);
        if (pageAddress == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        return pageAddress;
    }

    @Override
    protected int getMaxPage() {
        return page + 1;
    }

    @Override
    protected long getPageAddress(int page) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void release(long address) {
        Files.munmap0(address, pageSize);
    }

    public void truncate() {
        if (pageAddress != 0) {
            Files.munmap0(pageAddress, pageSize);
        }
        Files.truncate(fd, pageSize);
        page = 0;
        updateLimits(page + 1, pageAddress = mapPage(page));
    }

    private long mapPage(int page) {
        long target = pageOffset(page + 1);
        long fileSize = Files.length(fd);
        if (fileSize < target) {
            if (!Files.truncate(fd, target)) {
                throw new RuntimeException("Cannot resize file");
            }
        }
        return Files.mmap0(fd, pageSize, pageOffset(page), Files.MAP_RW);
    }

}
