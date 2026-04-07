package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Runtime representation of a live view. Holds the InMemoryTable and metadata.
 * Thread safety is provided by a read-write lock: queries acquire the read lock,
 * the refresh job acquires the write lock.
 */
public class LiveViewInstance implements QuietCloseable {
    private final LiveViewDefinition definition;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final InMemoryTable table = new InMemoryTable();
    private volatile boolean isClosed;
    private volatile String invalidationReason;
    private volatile long lastProcessedSeqTxn = -1;

    public LiveViewInstance(LiveViewDefinition definition) {
        this.definition = definition;
        this.table.init(definition.getMetadata());
    }

    @Override
    public void close() {
        isClosed = true;
        lock.writeLock().lock();
        try {
            Misc.free(table);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LiveViewDefinition getDefinition() {
        return definition;
    }

    public long getLastProcessedSeqTxn() {
        return lastProcessedSeqTxn;
    }

    public String getInvalidationReason() {
        return invalidationReason;
    }

    public InMemoryTable getTable() {
        return table;
    }

    public void invalidate(String reason) {
        invalidationReason = reason;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isInvalid() {
        return invalidationReason != null;
    }

    public void lockForRead() {
        lock.readLock().lock();
    }

    /**
     * Populates the InMemoryTable from a cursor produced by compiling
     * the live view's SELECT query. Called by LiveViewRefreshJob while
     * holding the write lock.
     */
    public void refresh(RecordCursor cursor) {
        lock.writeLock().lock();
        try {
            table.clear();
            Record record = cursor.getRecord();
            int columnCount = table.getColumnCount();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    int type = table.getColumnType(i);
                    switch (ColumnType.tagOf(type)) {
                        case ColumnType.INT:
                            table.putInt(i, record.getInt(i));
                            break;
                        case ColumnType.LONG:
                            table.putLong(i, record.getLong(i));
                            break;
                        case ColumnType.TIMESTAMP:
                            table.putLong(i, record.getTimestamp(i));
                            break;
                        case ColumnType.DATE:
                            table.putLong(i, record.getDate(i));
                            break;
                        case ColumnType.DOUBLE:
                            table.putDouble(i, record.getDouble(i));
                            break;
                        case ColumnType.FLOAT:
                            table.putFloat(i, record.getFloat(i));
                            break;
                        case ColumnType.SHORT:
                            table.putShort(i, record.getShort(i));
                            break;
                        case ColumnType.BYTE:
                            table.putByte(i, record.getByte(i));
                            break;
                        case ColumnType.BOOLEAN:
                            table.putBool(i, record.getBool(i));
                            break;
                        case ColumnType.CHAR:
                            table.putChar(i, record.getChar(i));
                            break;
                        case ColumnType.SYMBOL:
                            table.putSymbol(i, record.getSymA(i));
                            break;
                        case ColumnType.STRING:
                            table.putStr(i, record.getStrA(i));
                            break;
                        case ColumnType.VARCHAR:
                            table.putVarchar(i, record.getVarcharA(i));
                            break;
                        case ColumnType.BINARY:
                            table.putBin(i, record.getBin(i));
                            break;
                        default:
                            if (ColumnType.isArray(type)) {
                                table.putArray(i, record.getArray(i, type), type);
                            } else {
                                table.putLong(i, record.getLong(i));
                            }
                            break;
                    }
                }
                table.incrementRowCount();
            }
            table.applyRetention(definition.getRetentionMicros());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setLastProcessedSeqTxn(long seqTxn) {
        this.lastProcessedSeqTxn = seqTxn;
    }

    public void unlockAfterRead() {
        lock.readLock().unlock();
    }
}
