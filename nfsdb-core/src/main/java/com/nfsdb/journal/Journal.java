/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.concurrent.TimerCache;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.exceptions.JournalUnsupportedTypeException;
import com.nfsdb.journal.factory.JournalClosingListener;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.factory.configuration.Constants;
import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.journal.iterators.JournalIterator;
import com.nfsdb.journal.iterators.JournalRowBufferedIterator;
import com.nfsdb.journal.locks.Lock;
import com.nfsdb.journal.locks.LockManager;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.query.api.Query;
import com.nfsdb.journal.query.spi.QueryImpl;
import com.nfsdb.journal.tx.Tx;
import com.nfsdb.journal.tx.TxLog;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Rows;
import com.nfsdb.journal.utils.Unsafe;
import gnu.trove.list.TLongList;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

public class Journal<T> implements Iterable<T>, Closeable {

    public static final long TX_LIMIT_EVAL = -1L;
    private static final Logger LOGGER = Logger.getLogger(Journal.class);
    protected final List<Partition<T>> partitions = new ArrayList<>();
    // empty container for current transaction
    protected final Tx tx = new Tx();
    protected TxLog txLog;
    private final JournalMetadata<T> metadata;
    private final File location;
    private final Map<String, SymbolTable> symbolTableMap = new HashMap<>();
    private final ArrayList<SymbolTable> symbolTables = new ArrayList<>();
    private final JournalKey<T> key;
    private final Query<T> query = new QueryImpl<>(this);
    private final TimerCache timerCache;
    private final long timestampOffset;
    private final Comparator<T> timestampComparator = new Comparator<T>() {
        @Override
        public int compare(T o1, T o2) {
            long x = Unsafe.getUnsafe().getLong(o1, timestampOffset);
            long y = Unsafe.getUnsafe().getLong(o2, timestampOffset);
            return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
    };
    private final NullsAdaptor<T> nullsAdaptor;
    private final BitSet inactiveColumns;
    private boolean open;
    private ColumnMetadata columnMetadata[];
    private Partition<T> irregularPartition;
    private JournalClosingListener closeListener;


    public Journal(JournalMetadata<T> metadata, JournalKey<T> key, TimerCache timerCache) throws JournalException {
        this.metadata = metadata;
        this.key = key;
        this.location = new File(metadata.getLocation());
        this.timerCache = timerCache;
        this.txLog = new TxLog(location, getMode());
        this.open = true;
        this.timestampOffset = getMetadata().getTimestampColumnMetadata() == null ? -1 : getMetadata().getTimestampColumnMetadata().offset;
        this.nullsAdaptor = getMetadata().getNullsAdaptor();
        this.inactiveColumns = new BitSet(metadata.getColumnCount());

        configure();
    }

    public void setCloseListener(JournalClosingListener closeListener) {
        this.closeListener = closeListener;
    }

    public long getTimestamp(T o) {
        return Unsafe.getUnsafe().getLong(o, timestampOffset);
    }

    /**
     * Closes all columns in all partitions.
     */
    public void close() {
        if (open) {

            if (closeListener != null) {
                if (!closeListener.closing(this)) {
                    return;
                }
            }

            closePartitions();
            for (SymbolTable tab : symbolTables) {
                tab.close();
            }
            txLog.close();
            open = false;
        } else {
            throw new JournalRuntimeException("Already closed: %s", this);
        }
    }

    public boolean refresh() throws JournalException {
        if (txLog.hasNext()) {
            txLog.head(tx);
            refreshInternal();
            for (int i = 0; i < symbolTables.size(); i++) {
                symbolTables.get(i).applyTx(tx.symbolTableSizes[i], tx.symbolTableIndexPointers[i]);
            }
            return true;
        }
        return false;
    }

    public int getSymbolTableCount() {
        return symbolTables.size();
    }

    public SymbolTable getSymbolTable(String columnName) {
        SymbolTable result = symbolTableMap.get(columnName);
        if (result == null) {
            throw new JournalRuntimeException("Column is not a symbol: %s", columnName);
        }
        return result;
    }

    public SymbolTable getSymbolTable(int index) {
        return symbolTables.get(index);
    }

    public <X> X iteratePartitions(AbstractResultSetBuilder<T, X> builder) throws JournalException {
        builder.setJournal(this);
        int count = getPartitionCount();
        for (int i = 0; i < count; i++) {
            if (builder.next(getPartition(i, false), true)) {
                break;
            }
        }
        return builder.getResult();
    }

    public int getPartitionCount() {
        if (irregularPartition == null) {
            return nonLagPartitionCount();
        } else {
            return nonLagPartitionCount() + 1;
        }
    }

    public Partition<T> getPartition(int partitionIndex, boolean open) throws JournalException {
        if (irregularPartition != null && partitionIndex == nonLagPartitionCount()) {
            return open ? irregularPartition.open() : irregularPartition;
        }

        Partition<T> partition = partitions.get(partitionIndex).access();
        if (open) {
            partition.open();
        }
        return partition;
    }

    public int nonLagPartitionCount() {
        return partitions.size();
    }

    /**
     * Selects column names to be accessed by this journal.
     *
     * @param columns the names of all the columns that have to be read.
     */
    public Journal<T> select(String... columns) {
        if (columns == null || columns.length == 0) {
            inactiveColumns.clear();
        } else {
            inactiveColumns.set(0, metadata.getColumnCount());
            for (int i = 0; i < columns.length; i++) {
                inactiveColumns.clear(metadata.getColumnIndex(columns[i]));
            }
        }
        return this;
    }

    public <X> X iteratePartitionsDesc(AbstractResultSetBuilder<T, X> builder) throws JournalException {
        builder.setJournal(this);
        int count = getPartitionCount();
        for (int i = count - 1; i >= 0; i--) {
            if (builder.next(getPartition(i, false), false)) {
                break;
            }
        }
        return builder.getResult();
    }

    public Partition<T> lastNonEmptyNonLag() throws JournalException {

        if (nonLagPartitionCount() > 0) {

            Partition<T> result = getPartition(nonLagPartitionCount() - 1, true);

            while (true) {
                if (result.size() > 0) {
                    return result;
                }

                if (result.getPartitionIndex() == 0) {
                    break;
                }

                result = getPartition(result.getPartitionIndex() - 1, true);
            }

            return result;
        } else {
            return null;
        }
    }

    public Partition<T> getIrregularPartition() {
        return irregularPartition;
    }

    public void setIrregularPartition(Partition<T> partition) {
        removeIrregularPartitionInternal();
        irregularPartition = partition;
        irregularPartition.setPartitionIndex(nonLagPartitionCount());
    }

    public boolean hasIrregularPartition() {
        return irregularPartition != null;
    }

    public void expireOpenFiles() {
        long ttl = getMetadata().getOpenFileTTL();
        if (ttl > 0) {
            long delta = System.currentTimeMillis() - ttl;
            for (int i = 0, partitionsSize = partitions.size(); i < partitionsSize; i++) {
                Partition<T> partition = partitions.get(i);
                if (delta > partition.getLastAccessed() && partition.isOpen()) {
                    partition.close();
                } else {
                    partition.expireOpenIndices();
                }
            }
        }
    }

    /**
     * Get the Journal's metadata (static information).
     *
     * @return the Journal's metadata
     */
    public JournalMetadata<T> getMetadata() {
        return metadata;
    }

    public Comparator<T> getTimestampComparator() {
        return timestampComparator;
    }

    /**
     * Get the disk location of the Journal.
     *
     * @return the disk location of the Journal
     */
    public File getLocation() {
        return location;
    }

    /**
     * Is the specified Journal type compatible with this one.
     *
     * @param that a Journal to test
     * @return true if the specified Journal type compatible with this one
     */
    public boolean isCompatible(Journal<T> that) {
        return this.getMetadata().getModelClass().equals(that.getMetadata().getModelClass());
    }

    /**
     * Get the highest global row id of the Journal.
     *
     * @return the highest global row id of the Journal
     * @throws com.nfsdb.journal.exceptions.JournalException if there is an error
     */
    public long getMaxRowID() throws JournalException {
        Partition<T> p = getLastPartition();
        if (p == null) {
            return -1;
        } else {
            return Rows.toRowID(p.getPartitionIndex(), p.size() - 1);
        }
    }

    public Partition<T> getLastPartition() throws JournalException {
        Partition<T> result;

        if (getPartitionCount() == 0) {
            return null;
        }

        if (irregularPartition != null) {
            result = irregularPartition.open();
        } else {
            result = partitions.get(partitions.size() - 1).open();
        }

        Partition<T> intermediate = result;
        while (true) {
            if (intermediate.size() > 0) {
                return intermediate;
            }

            if (intermediate.getPartitionIndex() == 0) {
                break;
            }

            intermediate = getPartition(intermediate.getPartitionIndex() - 1, true);
        }

        return result.size() > 0 ? result : null;
    }

    public long getMaxTimestamp() throws JournalException {

        Partition p = getLastPartition();
        if (p == null) {
            return 0;
        }
        FixedColumn column = p.getTimestampColumn();
        if (column.size() > 0) {
            return column.getLong(column.size() - 1);
        } else {
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    public T newObject() {
        return (T) getMetadata().newObject();
    }

    public void clearObject(T obj) {
        if (nullsAdaptor != null) {
            metadata.getNullsAdaptor().clear(obj);
        } else {
            for (int i = 0, count = metadata.getColumnCount(); i < count; i++) {
                com.nfsdb.journal.factory.configuration.ColumnMetadata m = metadata.getColumnMetadata(i);
                switch (m.type) {
                    case BOOLEAN:
                        Unsafe.getUnsafe().putBoolean(obj, m.offset, false);
                        break;
                    case BYTE:
                        Unsafe.getUnsafe().putByte(obj, m.offset, (byte) 0);
                        break;
                    case DOUBLE:
                        Unsafe.getUnsafe().putDouble(obj, m.offset, 0d);
                        break;
                    case INT:
                        Unsafe.getUnsafe().putInt(obj, m.offset, 0);
                        break;
                    case LONG:
                        Unsafe.getUnsafe().putLong(obj, m.offset, 0L);
                        break;
                    case STRING:
                    case SYMBOL:
                        Unsafe.getUnsafe().putObject(obj, m.offset, null);
                        break;
                    case BINARY:
                        ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, m.offset);
                        if (buf != null) {
                            buf.clear();
                        }
                        break;
                    default:
                        throw new JournalUnsupportedTypeException(m.type);
                }
            }
        }
    }

    /**
     * Get the specified column's metadata (static information).
     *
     * @param columnIndex the column index (0-indexed)
     * @return the specified column's metadata
     */
    public ColumnMetadata getColumnMetadata(int columnIndex) {
        return columnMetadata[columnIndex];
    }

    @Override
    public Iterator<T> iterator() {
        return query().all().iterator();
    }

    public Query<T> query() {
        return query;
    }

    public JournalIterator<T> bufferedIterator() {
        return query().all().bufferedIterator();
    }

    public JournalRowBufferedIterator<T> bufferedRowIterator() {
        return query().all().bufferedRowIterator();
    }

    public ConcurrentIterator<T> concurrentIterator() {
        return query().all().concurrentIterator();
    }

    /**
     * Read an object by global row id
     *
     * @param rowID the global row id to read
     * @throws com.nfsdb.journal.exceptions.JournalException if there is an error
     */
    public void read(long rowID, T obj) throws JournalException {
        getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID), obj);
    }

    /**
     * Read objects by global row id
     *
     * @param rowIDs the global row ids to read
     * @return some objects
     * @throws com.nfsdb.journal.exceptions.JournalException if there is an error
     */
    @SuppressWarnings("unchecked")
    public T[] read(TLongList rowIDs) throws JournalException {
        T[] result = (T[]) Array.newInstance(metadata.getModelClass(), rowIDs.size());
        for (int i = 0; i < rowIDs.size(); i++) {
            result[i] = read(rowIDs.get(i));
        }
        return result;
    }

    /**
     * Read an object by global row id
     *
     * @param rowID the global row id to read
     * @return an object
     * @throws com.nfsdb.journal.exceptions.JournalException if there is an error
     */
    public T read(long rowID) throws JournalException {
        return getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID));
    }

    public boolean isOpen() {
        return open;
    }

    public long size() throws JournalException {
        long result = 0;
        for (int i = 0; i < getPartitionCount(); i++) {
            result += getPartition(i, true).size();
        }
        return result;
    }

    public JournalKey<T> getKey() {
        return key;
    }

    @Override
    public String toString() {
        return getClass().getName() + "[location=" + location + ", " + "mode=" + getMode() + ", " + ", metadata=" + metadata + "]";
    }

    /**
     * Get the Journal's flow model (the probable query style).
     *
     * @return the Journal's flow model
     */
    public JournalMode getMode() {
        return JournalMode.READ;
    }

    public long incrementRowID(long rowID) throws JournalException {
        int partitionIndex = Rows.toPartitionIndex(rowID);
        long localRowID = Rows.toLocalRowID(rowID);

        Partition p = getPartition(partitionIndex, open);
        if (localRowID < p.size() - 1) {
            return Rows.toRowID(partitionIndex, localRowID + 1);
        }

        while (++partitionIndex < getPartitionCount()) {
            p = getPartition(partitionIndex, true);
            if (p.size() > 0) {
                return Rows.toRowID(partitionIndex, 0);
            }
        }

        return -1;
    }

    public long decrementRowID(long rowID) throws JournalException {
        int partitionIndex = Rows.toPartitionIndex(rowID);
        long localRowID = Rows.toLocalRowID(rowID);

        if (localRowID > 0) {
            return Rows.toRowID(partitionIndex, localRowID - 1);
        }

        while (--partitionIndex >= 0) {
            Partition p = getPartition(partitionIndex, true);
            if (p.size() > 0) {
                return Rows.toRowID(partitionIndex, p.size() - 1);
            }
        }

        return -1;
    }

    public TempPartition<T> createTempPartition(String name) throws JournalException {
        int lag = getMetadata().getLag();
        if (lag <= 0) {
            throw new JournalRuntimeException("Journal doesn't support temp partitions: %s", this);
        }

        Interval interval = null;
        if (getMetadata().getPartitionType() != PartitionType.NONE) {
            if (nonLagPartitionCount() > 0) {
                Interval lastPartitionInterval = partitions.get(nonLagPartitionCount() - 1).getInterval();
                interval = new Interval(lastPartitionInterval.getStart(), lastPartitionInterval.getEnd().plusHours(lag));
            } else {
                interval = Dates.intervalForDate(System.currentTimeMillis(), getMetadata().getPartitionType());
            }
        }
        return new TempPartition<>(this, interval, nonLagPartitionCount(), name);
    }

    protected long getTimestampOffset() {
        return timestampOffset;
    }

    protected void closePartitions() {
        if (irregularPartition != null) {
            irregularPartition.close();
        }
        for (Partition<T> p : partitions) {
            p.close();
        }
        partitions.clear();
    }

    protected void configure() throws JournalException {
        txLog.head(tx);
        configureColumns();
        configureSymbolTableSynonyms();
        configurePartitions();
    }

    protected void removeIrregularPartitionInternal() {
        if (irregularPartition != null) {
            if (irregularPartition.isOpen()) {
                irregularPartition.close();
            }
            irregularPartition = null;
        }
    }

    BitSet getInactiveColumns() {
        return inactiveColumns;
    }

    TimerCache getTimerCache() {
        return timerCache;
    }

    private void configureColumns() throws JournalException {
        int columnCount = getMetadata().getColumnCount();
        columnMetadata = new ColumnMetadata[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnMetadata[i] = new ColumnMetadata();
            com.nfsdb.journal.factory.configuration.ColumnMetadata meta = metadata.getColumnMetadata(i);
            if (meta.type == ColumnType.SYMBOL && meta.sameAs == null) {
                int tabIndex = symbolTables.size();
                int tabSize = tx.symbolTableSizes.length > tabIndex ? tx.symbolTableSizes[tabIndex] : 0;
                long indexTxAddress = tx.symbolTableIndexPointers.length > tabIndex ? tx.symbolTableIndexPointers[tabIndex] : 0;
                SymbolTable tab = new SymbolTable(meta.distinctCountHint, meta.avgSize, getMetadata().getTxCountHint(), location, meta.name, getMode(), tabSize, indexTxAddress);
                symbolTables.add(tab);
                symbolTableMap.put(meta.name, tab);
                columnMetadata[i].symbolTable = tab;
            }
            columnMetadata[i].meta = meta;
        }
    }

    private void configureSymbolTableSynonyms() {
        for (int i = 0, columnCount = getMetadata().getColumnCount(); i < columnCount; i++) {
            com.nfsdb.journal.factory.configuration.ColumnMetadata meta = metadata.getColumnMetadata(i);
            if (meta.type == ColumnType.SYMBOL && meta.sameAs != null) {
                SymbolTable tab = getSymbolTable(meta.sameAs);
                symbolTableMap.put(meta.name, tab);
                columnMetadata[i].symbolTable = tab;
            }
        }
    }

    private void configurePartitions() throws JournalException {
        File[] files = getLocation().listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory() && !f.getName().startsWith(Constants.TEMP_DIRECTORY_PREFIX);
            }
        });

        int partitionIndex = 0;
        if (files != null && tx.journalMaxRowID > 0) {
            Arrays.sort(files);
            for (File f : files) {

                if (partitionIndex > Rows.toPartitionIndex(tx.journalMaxRowID)) {
                    break;
                }

                Partition<T> partition = null;

                if (partitionIndex < partitions.size()) {
                    partition = partitions.get(partitionIndex);
                }

                long txLimit = Journal.TX_LIMIT_EVAL;
                long[] indexTxAddresses = null;
                if (partitionIndex == Rows.toPartitionIndex(tx.journalMaxRowID)) {
                    txLimit = Rows.toLocalRowID(tx.journalMaxRowID);
                    indexTxAddresses = tx.indexPointers;
                }

                Interval interval = Dates.intervalForDirName(f.getName(), getMetadata().getPartitionType());
                if (partition != null) {
                    if (partition.getInterval() == null || partition.getInterval().equals(interval)) {
                        partition.applyTx(txLimit, indexTxAddresses);
                        partitionIndex++;
                    } else {
                        if (partition.isOpen()) {
                            partition.close();
                        }
                        partitions.remove(partitionIndex);
                    }
                } else {
                    partitions.add(new Partition<>(this, interval, partitionIndex, txLimit, indexTxAddresses));
                    partitionIndex++;
                }
            }
        }
        configureIrregularPartition();
    }

    private void configureIrregularPartition() throws JournalException {
        // if journal is under intense write activity in another process
        // lag partition can keep changing
        // so we will be trying to pin lag partition
        while (true) {
            String lagPartitionName = tx.lagName;
            if (lagPartitionName != null && (irregularPartition == null || !lagPartitionName.equals(irregularPartition.getName()))) {
                // new lag partition
                // try to lock lag directory before any activity
                File lagLocation = new File(getLocation(), lagPartitionName);
                LOGGER.trace("Attempting to attach partition %s to %s", lagLocation.getName(), this);
                Lock lock = LockManager.lockShared(lagLocation);
                try {
                    // if our lock has been successful
                    // continue with replacing lag
                    if (lock != null && lock.isValid()) {
                        LOGGER.trace("Lock successful for %s", lagLocation.getName());
                        Partition<T> temp = createTempPartition(lagPartitionName);
                        temp.applyTx(tx.lagSize, tx.lagIndexPointers);
                        setIrregularPartition(temp);
                        // exit out of while loop
                        break;
                    } else {
                        LOGGER.debug("Lock unsuccessful for %s", lagLocation.getName());
                    }
                } finally {
                    LockManager.release(lock);
                }
            } else if (lagPartitionName != null && irregularPartition != null && lagPartitionName.equals(irregularPartition.getName())) {
                irregularPartition.applyTx(tx.lagSize, tx.lagIndexPointers);
                break;
            } else if (lagPartitionName == null && irregularPartition != null) {
                removeIrregularPartitionInternal();
                break;
            } else {
                // there is no lag partition, exit.
                break;
            }
        }
    }

    /**
     * Replaces current Lag partition, which is cached in this instance of Partition Manager with Lag partition,
     * which was written to _lag file by another process.
     */
    void refreshInternal() throws JournalException {

        assert tx.address > 0;

        int txPartitionIndex = Rows.toPartitionIndex(tx.journalMaxRowID);
        if (partitions.size() != txPartitionIndex + 1 || tx.journalMaxRowID == 0) {
            if (tx.journalMaxRowID == 0 || partitions.size() > txPartitionIndex + 1) {
                closePartitions();
            }
            configurePartitions();
        } else {
            long txPartitionSize = Rows.toLocalRowID(tx.journalMaxRowID);
            Partition<T> partition = partitions.get(txPartitionIndex);
            partition.applyTx(txPartitionSize, tx.indexPointers);
            configureIrregularPartition();
        }
    }

    public static class ColumnMetadata {
        public SymbolTable symbolTable;
        public com.nfsdb.journal.factory.configuration.ColumnMetadata meta;
    }
}
