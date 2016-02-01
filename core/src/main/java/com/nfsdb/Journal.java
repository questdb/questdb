/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.factory.JournalClosingListener;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.*;
import com.nfsdb.query.AbstractResultSetBuilder;
import com.nfsdb.query.api.Query;
import com.nfsdb.query.spi.QueryImpl;
import com.nfsdb.std.LongList;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjObjHashMap;
import com.nfsdb.store.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;

@SuppressFBWarnings({"PATH_TRAVERSAL_IN", "LII_LIST_INDEXED_ITERATING", "CD_CIRCULAR_DEPENDENCY"})
public class Journal<T> implements Iterable<T>, Closeable {

    public static final long TX_LIMIT_EVAL = -1L;
    private static final Log LOG = LogFactory.getLog(Journal.class);
    final ObjList<Partition<T>> partitions = new ObjList<>();
    // empty container for current transaction
    final Tx tx = new Tx();
    final JournalMetadata<T> metadata;
    private final File location;
    private final ObjObjHashMap<String, SymbolTable> symbolTableMap = new ObjObjHashMap<>();
    private final ObjList<SymbolTable> symbolTables = new ObjList<>();
    private final JournalKey<T> key;
    private final Query<T> query = new QueryImpl<>(this);
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
    private volatile Partition<T> irregularPartition;
    private JournalClosingListener closeListener;
    private TxIterator txIterator;


    @SuppressFBWarnings({"PCOA_PARTIALLY_CONSTRUCTED_OBJECT_ACCESS"})
    public Journal(JournalMetadata<T> metadata, JournalKey<T> key) throws JournalException {
        this.metadata = metadata;
        this.key = key;
        this.location = new File(metadata.getLocation());
        this.txLog = new TxLog(location, getMode(), metadata.getTxCountHint());
        this.open = true;
        this.timestampOffset = getMetadata().getTimestampMetadata() == null ? -1 : getMetadata().getTimestampMetadata().offset;
        this.inactiveColumns = new BitSet(metadata.getColumnCount());

        configure();
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
                Misc.free(symbolTables.getQuick(i));
            }
            txLog.close();
            open = false;
        } else {
            throw new JournalRuntimeException("Already closed: %s", this);
        }
    }

    public TempPartition<T> createTempPartition(String name) throws JournalException {
        int lag = getMetadata().getLag();
        if (lag < 1) {
            throw new JournalRuntimeException("Journal doesn't support temp partitions: %s", this);
        }

        Interval interval = null;
        if (getMetadata().getPartitionType() != PartitionType.NONE) {
            Partition<T> p = partitions.getLast();
            if (p != null) {
                Interval lastPartitionInterval = p.getInterval();
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

        while (--partitionIndex > -1) {
            long sz = getPartition(partitionIndex, true).size();
            if (sz > 0) {
                return Rows.toRowID(partitionIndex, sz - 1);
            }
        }

        return -1;
    }

    public void expireOpenFiles() {
        long ttl = getMetadata().getOpenFileTTL();
        if (ttl > 0) {
            long delta = System.currentTimeMillis() - ttl;
            for (int i = 0, sz = partitions.size(); i < sz; i++) {
                Partition<T> partition = partitions.getQuick(i);
                if (delta > partition.getLastAccessed() && partition.isOpen()) {
                    partition.close();
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
            result = irregularPartition;
        } else {
            result = partitions.getLast();
        }

        if (result == null) {
            throw new JournalException("No available partitions");
        }

        Partition<T> intermediate = result.open();
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
     * @throws com.nfsdb.ex.JournalException if there is an error
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
        long sz = column.size();
        if (sz > 0) {
            return column.getLong(sz - 1);
        } else {
            return 0;
        }
    }

    /**
     * Get the Journal's metadata (static information).
     *
     * @return the Journal's metadata
     */
    public final JournalMetadata<T> getMetadata() {
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

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && key.equals(((Journal) o).key);
    }

    @Override
    public String toString() {
        return getClass().getName() + "[location=" + location + ", " + "mode=" + getMode() + ", " + ", metadata=" + metadata + ']';
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
        for (int i = count - 1; i > -1; i--) {
            if (builder.next(getPartition(i, false), false)) {
                break;
            }
        }
        return builder.getResult();
    }

    @Override
    public Iterator<T> iterator() {
        return JournalIterators.iterator(this);
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
     * @throws com.nfsdb.ex.JournalException if there is an error
     */
    public void read(long rowID, T obj) throws JournalException {
        getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID), obj);
    }

    /**
     * Read objects by global row id
     *
     * @param rowIDs the global row ids to read
     * @return some objects
     * @throws com.nfsdb.ex.JournalException if there is an error
     */
    @SuppressWarnings("unchecked")
    public T[] read(LongList rowIDs) throws JournalException {
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
     * @throws com.nfsdb.ex.JournalException if there is an error
     */
    public T read(long rowID) throws JournalException {
        return getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID));
    }

    public boolean refresh() throws JournalException {
        if (txLog.head(tx)) {
            refreshInternal();
            for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
                symbolTables.getQuick(i).applyTx(tx.symbolTableSizes[i], tx.symbolTableIndexPointers[i]);
            }
            return true;
        }
        return false;
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
            partitions.getQuick(i).close();
        }
        partitions.clear();
    }

    void configure() throws JournalException {
        txLog.head(tx);
        configureColumns();
        configureSymbolTableSynonyms();
        configurePartitions();
    }

    private void configureColumns() throws JournalException {
        int columnCount = getMetadata().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            ColumnMetadata meta = metadata.getColumnQuick(i);
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
        } else if (lagPartitionName != null && lagPartitionName.equals(irregularPartition.getName())) {
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

                long txLimit = Journal.TX_LIMIT_EVAL;
                long[] indexTxAddresses = null;
                if (partitionIndex == Rows.toPartitionIndex(tx.journalMaxRowID)) {
                    txLimit = Rows.toLocalRowID(tx.journalMaxRowID);
                    indexTxAddresses = tx.indexPointers;
                }

                try {
                    Interval interval = new Interval(f.getName(), getMetadata().getPartitionType());
                    if (partitionIndex < partitions.size()) {
                        Partition<T> partition = partitions.getQuick(partitionIndex);
                        Interval that = partition.getInterval();
                        if (that == null || that.equals(interval)) {
                            partition.applyTx(txLimit, indexTxAddresses);
                            partitionIndex++;
                        } else {
                            partition.close();
                            partitions.remove(partitionIndex);
                        }
                    } else {
                        partitions.add(new Partition<>(this, interval, partitionIndex++, txLimit, indexTxAddresses));
                    }
                } catch (NumericException e) {
                    LOG.info().$("Foreign directory: ").$(f.getName()).$();
                }
            }
        }
        configureIrregularPartition();
    }

    private void configureSymbolTableSynonyms() {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            ColumnMetadata meta = metadata.getColumnQuick(i);
            if (meta.type == ColumnType.SYMBOL && meta.sameAs != null) {
                SymbolTable tab = getSymbolTable(meta.sameAs);
                symbolTableMap.put(meta.name, tab);
                meta.symbolTable = tab;
            }
        }
    }

    BitSet getInactiveColumns() {
        return inactiveColumns;
    }

    long getTimestamp(T o) {
        return Unsafe.getUnsafe().getLong(o, timestampOffset);
    }

    long getTimestampOffset() {
        return timestampOffset;
    }

    /**
     * Is the specified Journal type compatible with this one.
     *
     * @param that a Journal to test
     * @return true if the specified Journal type compatible with this one
     */
    boolean isCompatible(Journal<T> that) {
        return this.getMetadata().isCompatible(that.getMetadata(), true);
    }

    Partition<T> lastNonEmptyNonLag() throws JournalException {

        int count = nonLagPartitionCount();
        if (count > 0) {

            Partition<T> result = getPartition(count - 1, true);

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

    /**
     * Replaces current Lag partition, which is cached in this instance of Partition Manager with Lag partition,
     * which was written to _lag file by another process.
     */
    private void refreshInternal() throws JournalException {

        assert tx.address > 0;

        int txPartitionIndex = tx.journalMaxRowID == -1 ? 0 : Rows.toPartitionIndex(tx.journalMaxRowID);
        if (partitions.size() != txPartitionIndex + 1 || tx.journalMaxRowID < 1) {
            if (tx.journalMaxRowID < 1 || partitions.size() > txPartitionIndex + 1) {
                closePartitions();
            }
            configurePartitions();
        } else {
            long txPartitionSize = tx.journalMaxRowID == -1 ? 0 : Rows.toLocalRowID(tx.journalMaxRowID);
            partitions.getQuick(txPartitionIndex).applyTx(txPartitionSize, tx.indexPointers);
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
}
