package io.questdb.cairo.replication;

public interface ReplicationSlaveManager {
    SlaveWriter getSlaveWriter(int masterTableId);

    void releaseSlaveWriter(int masterTableId, SlaveWriter slaveWriter);

    public interface SlaveWriter {
        long mapColumnData(long timestampLo, int columnIndex, long offset, long size);

        void unmap(int columnIndex, long address, long size);

        void markBlockNFrames(int nFrames);

        void commit();

        void cancel();
    }
}
