package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

public class StoragePolicyImpl implements StoragePolicy {
    private static final Log LOG = LogFactory.getLog(StoragePolicyImpl.class);

    protected final TableMetadata metadata;
    protected final TableWriter tableWriter;
    protected final TimestampDriver timestampDriver;
    protected final TxWriter txWriter;

    protected StoragePolicyImpl(TableWriter tableWriter) {
        this.tableWriter = tableWriter;

        metadata = tableWriter.getMetadata();
        txWriter = tableWriter.getTxWriter();
        timestampDriver = ColumnType.getTimestampDriver(metadata.getTimestampType());
    }

    @Override
    public boolean enforceTtl() {
        final int ttl = metadata.getTtlHoursOrMonths();
        if (ttl == 0) {
            return false;
        }

        return evictExpiredPartitions(ttl);
    }

    protected boolean checkTtl(long partitionTimestamp, int ttl) {
        long maxTimestamp = txWriter.getMaxTimestamp();
        long partitionCeiling = txWriter.getNextLogicalPartitionTimestamp(partitionTimestamp);
        // TTL < 0 means it's in months
        return ttl > 0
                ? maxTimestamp - partitionCeiling >= timestampDriver.fromHours(ttl)
                : timestampDriver.monthsBetween(partitionCeiling, maxTimestamp) >= -ttl;
    }

    protected boolean evictExpiredPartitions(int ttl) {
        boolean evicted = false;

        long evictedPartitionTimestamp = -1;
        do {
            long partitionTimestamp = txWriter.getPartitionTimestampByIndex(0);
            long floorTimestamp = txWriter.getPartitionFloor(partitionTimestamp);
            if (evictedPartitionTimestamp != -1 && floorTimestamp == evictedPartitionTimestamp) {
                assert partitionTimestamp != floorTimestamp : "Expected a higher part of a split partition";
                evicted |= tableWriter.dropPartitionByExactTimestamp(partitionTimestamp);
                continue;
            }

            if (checkTtl(partitionTimestamp, ttl)) {
                LOG.info()
                        .$("Partition's TTL expired, evicting [table=").$(metadata.getTableToken())
                        .$(", partitionTs=").microTime(partitionTimestamp)
                        .I$();
                evicted |= tableWriter.dropPartitionByExactTimestamp(partitionTimestamp);
                evictedPartitionTimestamp = partitionTimestamp;
            } else {
                // Partitions are sorted by timestamp, no need to check the rest
                break;
            }
        } while (txWriter.getPartitionCount() > 1);

        return evicted;
    }
}
