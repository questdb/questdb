package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Os;
import com.questdb.std.str.LPSZ;

public class AppendMemory extends VirtualMemory {
    private static final Log LOG = LogFactory.getLog(AppendMemory.class);
    private final FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;
    private long size;

    public AppendMemory(FilesFacade ff, LPSZ name, long pageSize, long size) {
        this.ff = ff;
        of(name, pageSize, size);
    }

    public AppendMemory(FilesFacade ff) {
        this.ff = ff;
        size = 0;
    }

    @Override
    public void close() {
        long sz = getAppendOffset();
        super.close();
        releaseCurrentPage();
        if (fd != -1) {
            ff.truncate(fd, sz);
            LOG.info().$("Truncated and closed [").$(fd).$(']').$();
            closeFd();
        }
    }

    @Override
    public void jumpTo(long offset) {
        updateSize();
        super.jumpTo(offset);
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        long address = mapPage(page);
        return pageAddress = address;
    }

    @Override
    protected void release(long address) {
        ff.munmap(address, pageSize);
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
        fd = ff.openRW(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open ").put(name);
        }
        LOG.info().$("Open ").$(name).$(" [").$(fd).$(']').$();
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
        if (fd == -1) {
            // we are closed ?
            return;
        }

        this.size = 0;
        releaseCurrentPage();
        if (!ff.truncate(fd, pageSize)) {
            throw CairoException.instance(Os.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(pageSize).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
    }

    private void closeFd() {
        ff.close(fd);
        fd = -1;
    }

    private long mapPage(int page) {
        long target = pageOffset(page + 1);
        if (ff.length(fd) < target && !ff.truncate(fd, target)) {
            throw CairoException.instance(ff.errno()).put("Appender resize failed fd=").put(fd).put(", size=").put(target);
        }
        long offset = pageOffset(page);
        long address = ff.mmap(fd, pageSize, offset, Files.MAP_RW);
        if (address == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot mmap(append) fd=").put(fd).put(", offset=").put(offset).put(", size=").put(pageSize);
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
