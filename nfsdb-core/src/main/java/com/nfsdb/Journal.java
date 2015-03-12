/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb;

import com.nfsdb.collections.DirectLongList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalClosingListener;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.lang.cst.JournalRecordSource;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.lang.cst.impl.rsrc.AllRowSource;
import com.nfsdb.query.AbstractResultSetBuilder;
import com.nfsdb.query.api.Query;
import com.nfsdb.query.iterator.ConcurrentIterator;
import com.nfsdb.query.iterator.JournalPeekingIterator;
import com.nfsdb.query.iterator.JournalRowBufferedIterator;
import com.nfsdb.query.spi.QueryImpl;
import com.nfsdb.storage.*;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Rows;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Array;
import java.util.*;

public class Journal<T> implements Iterable<T>, Closeable {

    public static final long TX_LIMIT_EVAL = -1L;
    final List<Partition<T>> partitions = new ArrayList<>();
    // empty container for current transaction
    final Tx tx = new Tx();
    final JournalMetadata<T> metadata;
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
    private final BitSet inactiveColumns;
    TxLog txLog;
    boolean open;
    private Partition<T> irregularPartition;
    private JournalClosingListener closeListener;
    private TxIterator txIterator;


    public Journal(JournalMetadata<T> metadata, JournalKey<T> key, TimerCache timerCache) throws JournalException {
        this.metadata = metadata;
        this.key = key;
        this.location = new File(metadata.getLocation());
        this.timerCache = timerCache;
        this.txLog = new TxLog(location, getMode());
        this.open = true;
        this.timestampOffset = getMetadata().getTimestampMetadata() == null ? -1 : getMetadata().getTimestampMetadata().offset;
        this.inactiveColumns = new BitSet(metadata.getColumnCount());

        configure();
    }

    public JournalPeekingIterator<T> bufferedIterator() {
        return query().all().bufferedIterator();
    }

    public JournalRowBufferedIterator<T> bufferedRowIterator() {
        return query().all().bufferedRowIterator();
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
            for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
                symbolTables.get(i).close();
            }
            txLog.close();
            open = false;
        } else {
            throw new JournalRuntimeException("Already closed: %s", this);
        }
    }

    public ConcurrentIterator<T> concurrentIterator() {
        return query().all().concurrentIterator();
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
                interval = new Interval(lastPartitionInterval.getLo(), Dates.addHours(lastPartitionInterval.getHi(), lag));
            } else {
                interval = new Interval(System.currentTimeMillis(), getMetadata().getPartitionType());
            }
        }
        return new TempPartition<>(this, interval, nonLagPartitionCount(), name);
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

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && key.equals(((Journal) o).key);
    }

    public void expireOpenFiles() {
        long ttl = getMetadata().getOpenFileTTL();
        if (ttl > 0) {
            long delta = System.currentTimeMillis() - ttl;
            for (int i = 0, sz = partitions.size(); i < sz; i++) {
                Partition<T> partition = partitions.get(i);
                if (delta > partition.getLastAccessed() && partition.isOpen()) {
                    partition.close();
                } else {
                    partition.expireOpenIndices();
                }
            }
        }
    }

    public Tx find(long txn, long txPin) {
        long address = txLog.findAddress(txn, txPin);
        if (address == -1) {
            return null;
        }

        txLog.read(address, tx);
        return tx;
    }

    public Partition<T> getIrregularPartition() {
        return irregularPartition;
    }

    public void setIrregularPartition(Partition<T> partition) {
        removeIrregularPartitionInternal();
        irregularPartition = partition;
        irregularPartition.setPartitionIndex(nonLagPartitionCount());
    }

    public JournalKey<T> getKey() {
        return key;
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

    /**
     * Get the disk location of the Journal.
     *
     * @return the disk location of the Journal
     */
    public File getLocation() {
        return location;
    }

    /**
     * Get the highest global row id of the Journal.
     *
     * @return the highest global row id of the Journal
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public long getMaxRowID() throws JournalException {
        Partition<T> p = getLastPartition();
        if (p == null) {
            return -1;
        } else {
            return Rows.toRowID(p.getPartitionIndex(), p.size() - 1);
        }
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

    /**
     * Get the Journal's metadata (static information).
     *
     * @return the Journal's metadata
     */
    public JournalMetadata<T> getMetadata() {
        return metadata;
    }

    /**
     * Get the Journal's flow model (the probable query style).
     *
     * @return the Journal's flow model
     */
    public JournalMode getMode() {
        return JournalMode.READ;
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

    public int getPartitionCount() {
        if (irregularPartition == null) {
            return nonLagPartitionCount();
        } else {
            return nonLagPartitionCount() + 1;
        }
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

    public int getSymbolTableCount() {
        return symbolTables.size();
    }

    public long getTimestamp(T o) {
        return Unsafe.getUnsafe().getLong(o, timestampOffset);
    }

    public Comparator<T> getTimestampComparator() {
        return timestampComparator;
    }

    public long getTxPin() {
        return txLog.getCurrentTxnPin();
    }

    public long getTxn() {
        return txLog.getCurrentTxn();
    }

    public boolean hasIrregularPartition() {
        return irregularPartition != null;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    /**
     * Same as #incrementBuffered(). The only difference that new instance of T is created on every iteration.
     *
     * @return Iterator that traverses journal increment
     * @since 2.0.1
     */
    public JournalPeekingIterator<T> increment() {
        return query().all().incrementIterator();
    }

    /**
     * Creates buffered iterator for journal increment before and after #refresh() call.
     * This is useful when you have a two threads, one writer and another reader, tailing former writer.
     * Reader would be refreshed periodically and if there is data increment, returned iterator would
     * traverse it.
     *
     * @return Iterator that traverses journal increment
     * @since 2.0.1
     */
    public JournalPeekingIterator<T> incrementBuffered() {
        return query().all().incrementBufferedIterator();
    }

    public long incrementRowID(long rowID) throws JournalException {

        int count = getPartitionCount();
        if (rowID == -1) {
            if (count > 0 && getPartition(0, true).size() > 0) {
                return 0;
            } else {
                return -1;
            }
        }

        int partitionIndex = Rows.toPartitionIndex(rowID);
        long localRowID = Rows.toLocalRowID(rowID);

        Partition p = getPartition(partitionIndex, open);
        if (localRowID < p.size() - 1) {
            return Rows.toRowID(partitionIndex, localRowID + 1);
        }
        while (++partitionIndex < count) {
            p = getPartition(partitionIndex, true);
            if (p.size() > 0) {
                return Rows.toRowID(partitionIndex, 0);
            }
        }

        return -1;
    }

    /**
     * Is the specified Journal type compatible with this one.
     *
     * @param that a Journal to test
     * @return true if the specified Journal type compatible with this one
     */
    public boolean isCompatible(Journal<T> that) {
        return this.getMetadata().getId().equals(that.getMetadata().getId());
    }

    public boolean isOpen() {
        return open;
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

    @Override
    public Iterator<T> iterator() {
        return query().all().iterator();
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

    @SuppressWarnings("unchecked")
    public T newObject() {
        return (T) getMetadata().newObject();
    }

    public int nonLagPartitionCount() {
        return partitions.size();
    }

    public Query<T> query() {
        return query;
    }

    /**
     * Read an object by global row id
     *
     * @param rowID the global row id to read
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public void read(long rowID, T obj) throws JournalException {
        getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID), obj);
    }

    /**
     * Read objects by global row id
     *
     * @param rowIDs the global row ids to read
     * @return some objects
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    @SuppressWarnings("unchecked")
    public T[] read(DirectLongList rowIDs) throws JournalException {
        T[] result = (T[]) Array.newInstance(metadata.getModelClass(), rowIDs.size());
        for (int i = 0, sz = rowIDs.size(); i < sz; i++) {
            result[i] = read(rowIDs.get(i));
        }
        return result;
    }

    /**
     * Read an object by global row id
     *
     * @param rowID the global row id to read
     * @return an object
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public T read(long rowID) throws JournalException {
        return getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID));
    }

    public boolean refresh() throws JournalException {
        if (txLog.head(tx)) {
            refreshInternal();
            for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
                symbolTables.get(i).applyTx(tx.symbolTableSizes[i], tx.symbolTableIndexPointers[i]);
            }
            return true;
        }
        return false;
    }

    public JournalRecordSource rows() {
        return new JournalSourceImpl(
                new JournalPartitionSource(this, true)
                , new AllRowSource()
        );
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

    public void setCloseListener(JournalClosingListener closeListener) {
        this.closeListener = closeListener;
    }

    public long size() throws JournalException {
        long result = 0;
        int count = getPartitionCount();
        for (int i = 0; i < count; i++) {
            result += getPartition(i, true).size();
        }
        return result;
    }

    @Override
    public String toString() {
        return getClass().getName() + "[location=" + location + ", " + "mode=" + getMode() + ", " + ", metadata=" + metadata + "]";
    }

    public TxIterator transactions() {
        if (txIterator == null) {
            txIterator = new TxIterator(txLog);
        } else {
            txIterator.reset();
        }

        return txIterator;
    }

    void closePartitions() {
        if (irregularPartition != null) {
            irregularPartition.close();
        }
        for (int i = 0, sz = partitions.size(); i < sz; i++) {
            partitions.get(i).close();
        }
        partitions.clear();
    }

    void configure() throws JournalException {
        txLog.head(tx);
        configureColumns();
        configureSymbolTableSynonyms();
        configurePartitions();
    }

    BitSet getInactiveColumns() {
        return inactiveColumns;
    }

    TimerCache getTimerCache() {
        return timerCache;
    }

    long getTimestampOffset() {
        return timestampOffset;
    }

    /**
     * Replaces current Lag partition, which is cached in this instance of Partition Manager with Lag partition,
     * which was written to _lag file by another process.
     */
    void refreshInternal() throws JournalException {

        assert tx.address > 0;

        int txPartitionIndex = tx.journalMaxRowID == -1 ? 0 : Rows.toPartitionIndex(tx.journalMaxRowID);
        if (partitions.size() != txPartitionIndex + 1 || tx.journalMaxRowID <= 0) {
            if (tx.journalMaxRowID <= 0 || partitions.size() > txPartitionIndex + 1) {
                closePartitions();
            }
            configurePartitions();
        } else {
            long txPartitionSize = tx.journalMaxRowID == -1 ? 0 : Rows.toLocalRowID(tx.journalMaxRowID);
            partitions.get(txPartitionIndex).applyTx(txPartitionSize, tx.indexPointers);
            configureIrregularPartition();
        }
    }

    void removeIrregularPartitionInternal() {
        if (irregularPartition != null) {
            if (irregularPartition.isOpen()) {
                irregularPartition.close();
            }
            irregularPartition = null;
        }
    }

    private void configureColumns() throws JournalException {
        int columnCount = getMetadata().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            ColumnMetadata meta = metadata.getColumnMetadata(i);
            if (meta.type == ColumnType.SYMBOL && meta.sameAs == null) {
                int tabIndex = symbolTables.size();
                int tabSize = tx.symbolTableSizes.length > tabIndex ? tx.symbolTableSizes[tabIndex] : 0;
                long indexTxAddress = tx.symbolTableIndexPointers.length > tabIndex ? tx.symbolTableIndexPointers[tabIndex] : 0;
                SymbolTable tab = new SymbolTable(meta.distinctCountHint, meta.avgSize, getMetadata().getTxCountHint(), location, meta.name, getMode(), tabSize, indexTxAddress, meta.noCache);
                symbolTables.add(tab);
                symbolTableMap.put(meta.name, tab);
                meta.symbolTable = tab;
            }
        }
    }

    private void configureIrregularPartition() throws JournalException {
        String lagPartitionName = tx.lagName;
        if (lagPartitionName != null && (irregularPartition == null || !lagPartitionName.equals(irregularPartition.getName()))) {
            // new lag partition
            // try to lock lag directory before any activity
            Partition<T> temp = createTempPartition(lagPartitionName);
            temp.applyTx(tx.lagSize, tx.lagIndexPointers);
            setIrregularPartition(temp);
            // exit out of while loop
        } else if (lagPartitionName != null && irregularPartition != null && lagPartitionName.equals(irregularPartition.getName())) {
            irregularPartition.applyTx(tx.lagSize, tx.lagIndexPointers);
        } else if (lagPartitionName == null && irregularPartition != null) {
            removeIrregularPartitionInternal();
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
            for (int i = 0; i < files.length; i++) {
                File f = files[i];

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

                Interval interval = new Interval(f.getName(), getMetadata().getPartitionType());
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

    private void configureSymbolTableSynonyms() {
        for (int i = 0; i < getMetadata().getColumnCount(); i++) {
            ColumnMetadata meta = metadata.getColumnMetadata(i);
            if (meta.type == ColumnType.SYMBOL && meta.sameAs != null) {
                SymbolTable tab = getSymbolTable(meta.sameAs);
                symbolTableMap.put(meta.name, tab);
                meta.symbolTable = tab;
            }
        }
    }
}
