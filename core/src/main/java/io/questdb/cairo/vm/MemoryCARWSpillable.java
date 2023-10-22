package io.questdb.cairo.vm;

import java.util.UUID;

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

public class MemoryCARWSpillable implements MemoryCARW, Mutable {
    private static final Log LOG = LogFactory.getLog(MemoryCARWSpillable.class);
    // for non-mmap
    private final int maxPages;
    // flag
    private long diskSpillThreshold;
    private long extendSegmentMsb;
    private FilesFacade ff = new FilesFacadeImpl();
    private boolean isSpilled;
    private MemoryCARW mem;
    private long minMappedMemorySize = -1;
    private int nativeMemoryTag;
    private int opts;
    // general
    private long pageSize;
    private int spillMemoryTag;
    // for mmap
    private Path spillPath;
    private String tmpFile;

    // TODO: For now assume the same pageSize
    public MemoryCARWSpillable(long pageSize, int maxPages, long diskSpillThreshold, CairoConfiguration cairoConf){
        this(pageSize, maxPages, diskSpillThreshold, MemoryTag.NATIVE_DEFAULT, MemoryTag.MMAP_DEFAULT);
        this.spillPath = Path.getThreadLocal(cairoConf.getRoot()).concat(tmpFile).$();
        ff.touch(spillPath);
    }

    public MemoryCARWSpillable(long pageSize, int maxPages, long diskSpillThreshold, Path path){
        this(pageSize, maxPages, diskSpillThreshold, MemoryTag.NATIVE_DEFAULT, MemoryTag.MMAP_DEFAULT);
        this.spillPath = path.concat(tmpFile).$();
        ff.touch(path);
    }

    public MemoryCARWSpillable(long pageSize, int maxPages, long diskSpillThreshold, Path path, int nativeMemoryTag){
        this(pageSize, maxPages, diskSpillThreshold, nativeMemoryTag, MemoryTag.MMAP_DEFAULT);
        this.spillPath = path.concat(tmpFile).$();
        ff.touch(path);
    }

    public MemoryCARWSpillable(long pageSize, int maxPages, long diskSpillThreshold, Path path, int nativeMemoryTag, int spillMemoryTag){
        this(pageSize, maxPages, diskSpillThreshold, nativeMemoryTag, spillMemoryTag);
        this.spillPath = path.concat(tmpFile).$();
        ff.touch(path);
    }



    MemoryCARWSpillable(long pageSize, int maxPages, long diskSpillThreshold, int nativeMemoryTag, int spillMemoryTag){
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.extendSegmentMsb = Numbers.msb(pageSize);
        this.maxPages = maxPages;
        this.diskSpillThreshold = Numbers.ceilPow2(diskSpillThreshold);
        assert diskSpillThreshold > pageSize;
        this.nativeMemoryTag = nativeMemoryTag;
        this.spillMemoryTag = spillMemoryTag;
        this.mem = initCARW();
        this.isSpilled = false;
        this.tmpFile = UUID.randomUUID() + ".tmp";
    }

    @Override
    public long addressOf(long offset) {
        return mem.addressOf(offset);
    }

    @Override
    public long appendAddressFor(long bytes) {
        checkAndExtend(getAppendAddress() + bytes);
        return mem.appendAddressFor(bytes);
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        checkAndExtend(mem.getAddress() + offset + bytes);
        return mem.addressOf(offset);
    }

    /**
     * Since this is (initially) created for FastMap, it is expected to just reset the
     * append-pointer to 0 and treat the memory as empty.
     */
    @Override
    public void clear() {
        mem.jumpTo(0);
    }

    @Override
    public void close() {
        mem.close();
        if(isSpilled){
            ff.remove(spillPath);
            isSpilled = false;
        }
    }

    @Override
    public void extend(long size) {
        checkAndExtend(mem.getAddress() + size);
    }

    @Override
    public long getAppendOffset() {
        return mem.getAppendOffset();
    }

    @Override
    public BinarySequence getBin(long offset) {
        return mem.getBin(offset);
    }

    @Override
    public long getExtendSegmentSize() {
        return mem.getExtendSegmentSize();
    }

    @Override
    public Long256 getLong256A(long offset) {
        return mem.getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(long offset) {
        return mem.getLong256B(offset);
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return mem.getPageAddress(pageIndex);
    }

    @Override
    public int getPageCount() {
        return mem.getPageCount();
    }

    @Override
    public long getPageSize() {
        return getExtendSegmentSize();
    }

    @Override
    public CharSequence getStr(long offset) {
        return mem.getStr(offset);
    }

    @Override
    public CharSequence getStr2(long offset) {
        return mem.getStr2(offset);
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    public void jumpTo(long offset) {
        checkAndExtend(mem.getAddress() + offset);
        mem.jumpTo(offset);
    }

    @Override
    public long offsetInPage(long offset) {
        return mem.offsetInPage(offset);
    }

    @Override
    public int pageIndex(long offset) {
        return mem.pageIndex(offset);
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        mem.putLong256(hexString, start, end);
    }

    @Override
    public long resize(long size) {
        extend(size);
        return mem.getAddress();
    }

    @Override
    public void shiftAddressRight(long shiftRightOffset) {
        mem.shiftAddressRight(shiftRightOffset);
    }

    @Override
    public long size() {
        return mem.size();
    }

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that
     * no memory write takes place.
     *
     * @param bytes number of bytes to skip
     */
    @Override
    public void skip(long bytes) {
        checkAndExtend(getAppendAddress() + bytes);
        mem.skip(bytes);
    }

    /**
     * Reset the appendOffset and resize to 1 page
     */
    @Override
    public void truncate() {
        if(isSpilled){
            mem.close();
            ff.remove(spillPath);
            mem = initCARW();
            isSpilled = false;
        }
        mem.truncate();
        assert mem.size() == pageSize;
    }

    @Override
    public void zero() {
        mem.zero();
    }

    private void checkAndExtend(long address) {
        if (address <= mem.getAddress() + mem.size()) {
            return;
        }
        long sz = address - mem.getAddress();
        if (!isSpilled && (sz > diskSpillThreshold)){

            MemoryCARW cmarw = initCMARW(sz, mem.getAppendOffset());
            synchronized (this){
                Vect.memmove(cmarw.getAddress(), mem.getAddress(), mem.size());
                LOG.debug().$("Start spilling. curSz: ").$(mem.size()).$(", requestedSz: ").$(sz).$(", newSz: ").$(cmarw.size());
                mem.close(); // close CARW
                mem = cmarw;
                isSpilled = true;
            }
        } else {
            mem.extend(address - mem.getAddress());
        }
    }

    private long getAppendAddress() {
        return mem.getAddress() + mem.getAppendOffset();
    }

    private MemoryCARWImpl initCARW() {
        return new MemoryCARWImpl(pageSize, maxPages, nativeMemoryTag);
    }

    private MemoryCMARWImpl initCMARW(long size, long startAppendOffset) {
        long nPages = (size >>> extendSegmentMsb) + 1;
        size = nPages << extendSegmentMsb;
        MemoryCMARWImpl mem = new MemoryCMARWImpl(ff, spillPath, Numbers.ceilPow2(1L << extendSegmentMsb), size, spillMemoryTag, opts);
        mem.jumpTo(startAppendOffset);
        return mem;
    }


}
