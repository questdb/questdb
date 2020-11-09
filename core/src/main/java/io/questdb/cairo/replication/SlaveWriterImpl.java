package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.TableBlockWriter;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;

// TODO: Implement fine grain locking
public class SlaveWriterImpl implements SlaveWriter, Closeable {
    private final LongList columnAddresses = new LongList();
    private final LongList columnSizes = new LongList();
    private long timestampLo;
    private TableBlockWriter blockWriter;
    private final AtomicInteger nRemainingFrames = new AtomicInteger();

    public SlaveWriterImpl of(TableBlockWriter blockWriter) {
        this.blockWriter = blockWriter;
        timestampLo = Long.MIN_VALUE;
        nRemainingFrames.set(0);
        return this;
    }

    @Override
    public synchronized void commit() {
        blockWriter.commit();
        clear();
    }

    @Override
    public synchronized void cancel() {
        blockWriter.cancel();
        clear();
    }

    private void clear() {
        for (int i = 0, sz = columnAddresses.size(); i < sz; i++) {
            long address = columnAddresses.get(i);
            long size = columnSizes.get(i);
            Unsafe.free(address, size);
        }
        columnAddresses.clear();
        columnSizes.clear();
    }

    @Override
    public void close() {
        clear();
    }

    @Override
    public synchronized long mapColumnData(long timestampLo, int columnIndex, long offset, long size) {
        if (this.timestampLo != timestampLo) {
            blockWriter.startPageFrame(timestampLo);
            this.timestampLo = timestampLo;
        }
        long address = Unsafe.malloc(size);
        columnSizes.add(size);
        columnAddresses.add(address);
        return address;
    }

    @Override
    public synchronized boolean unmap(int columnIndex, long address, long size) {
        blockWriter.appendPageFrameColumn(columnIndex, size, address);
        return nRemainingFrames.incrementAndGet() == 0;
    }

    @Override
    public synchronized boolean markBlockNFrames(int nFrames) {
        return nRemainingFrames.addAndGet(-1 * nFrames) == 0;
    }
}
