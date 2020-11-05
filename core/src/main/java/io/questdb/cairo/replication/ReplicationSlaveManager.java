package io.questdb.cairo.replication;

public interface ReplicationSlaveManager {
    SlaveWriter getSlaveWriter(int masterTableId);

    void releaseSlaveWriter(int masterTableId, SlaveWriter slaveWriter);

    public interface SlaveWriter {
        int getFrameSequenceId();

        void startPageFrame(long timestampLo);

        long mapColumnAppend(int columnIndex, long size);

        void unmapColumnAppend(int columnIndex, long address, long size);

        void endPageFrame();

        void commit();

        void cancel();
    }
}
