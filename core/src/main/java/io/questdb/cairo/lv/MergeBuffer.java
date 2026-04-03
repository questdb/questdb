package io.questdb.cairo.lv;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A simple Java-heap sorted buffer that holds rows until they are old enough
 * to drain (past the LAG window). Rows are sorted by timestamp.
 * <p>
 * This V1 draft implementation uses Java collections (not zero-GC). A future
 * version would use native memory.
 */
public class MergeBuffer {
    private final long lagMicros;
    private final TreeMap<Long, List<Object[]>> rowsByTimestamp = new TreeMap<>();
    private long maxTimestampSeen = Long.MIN_VALUE;

    public MergeBuffer(long lagMicros) {
        this.lagMicros = lagMicros;
    }

    public void addRow(long timestamp, Object[] columnValues) {
        rowsByTimestamp.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(columnValues);
        if (timestamp > maxTimestampSeen) {
            maxTimestampSeen = timestamp;
        }
    }

    public void clear() {
        rowsByTimestamp.clear();
        maxTimestampSeen = Long.MIN_VALUE;
    }

    /**
     * Drains rows that are old enough (timestamp <= maxTimestampSeen - lagMicros).
     * Returns them in timestamp order.
     */
    public List<Object[]> drain() {
        long watermark = maxTimestampSeen - lagMicros;
        SortedMap<Long, List<Object[]>> toDrain = rowsByTimestamp.headMap(watermark, true);
        List<Object[]> result = new ArrayList<>();
        for (List<Object[]> list : toDrain.values()) {
            result.addAll(list);
        }
        toDrain.clear();
        return result;
    }

    /**
     * Drains all remaining rows regardless of lag. Used for final flush.
     */
    public List<Object[]> drainAll() {
        List<Object[]> result = new ArrayList<>();
        for (List<Object[]> list : rowsByTimestamp.values()) {
            result.addAll(list);
        }
        rowsByTimestamp.clear();
        return result;
    }

    public boolean isEmpty() {
        return rowsByTimestamp.isEmpty();
    }
}
