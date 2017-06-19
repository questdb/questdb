package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.std.ObjectFactory;
import com.questdb.std.str.LPSZ;

public class AppendMemory extends VirtualMemory {
    public static final ObjectFactory<AppendMemory> FACTORY = AppendMemory::new;
    private static final Log LOG = LogFactory.getLog(AppendMemory.class);

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
            throw CairoException.instance().put("Cannot open ").put(name);
        }
        LOG.info().$("Open for append ").$(name).$(" [").$(fd).$(']').$();
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
            throw CairoException.instance().put("Appender resize fd=").put(fd).put(", size=").put(target);
        }
        long offset = pageOffset(page);
        long address = Files.mmap(fd, pageSize, offset, Files.MAP_RW);
        if (address == -1) {
            throw CairoException.instance().put("Cannot mmap(append) fd=").put(fd).put(", offset=").put(offset).put(", size=").put(pageSize);
        }
        return address;
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
