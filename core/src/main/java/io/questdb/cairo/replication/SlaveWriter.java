package io.questdb.cairo.replication;

import java.io.Closeable;

public interface SlaveWriter extends Closeable {
    long getDataMap(long timestamp, int columnIndex, long offset, long size);

    long getSymbolDataMap(int columnIndex, long offset, long size);

    boolean completeFrame();

    boolean markBlockNFrames(int nFrames);

    void commit();

    void cancel();

    void clear();

    @Override
    void close();
}