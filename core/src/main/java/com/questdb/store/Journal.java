/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store;

import com.questdb.common.ColumnType;
import com.questdb.common.JournalRuntimeException;
import com.questdb.common.PartitionBy;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.Dates;
import com.questdb.std.time.Interval;
import com.questdb.store.factory.JournalCloseInterceptor;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.Constants;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.query.AbstractResultSetBuilder;
import com.questdb.store.query.api.Query;
import com.questdb.store.query.spi.QueryImpl;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;

public class Journal<T> implements Iterable<T>, Closeable {

    public static final long TX_LIMIT_EVAL = -1L;
    private static final Log LOG = LogFactory.getLog(Journal.class);
    final ObjList<Partition<T>> partitions = new ObjList<>();
    // empty container for current transaction
    final Tx tx = new Tx();
    final JournalMetadata<T> metadata;
    private final File location;
    private final ObjObjHashMap<String, MMappedSymbolTable> symbolTableMap = new ObjObjHashMap<>();
    private final ObjList<MMappedSymbolTable> symbolTables = new ObjList<>();
    private final Query<T> query = new QueryImpl<>(this);
    private final long timestampOffset;
    private final Comparator<T> timestampComparator = new Comparator<T>() {
        @Override
        public int compare(T o1, T o2) {
            long x = Unsafe.getUnsafe().getLong(o1, timestampOffset);
            long y = Unsafe.getUnsafe().getLong(o2, timestampOffset);
            return Long.compare(x, y);
        }
    };
    private final BitSet inactiveColumns;
    private final long openFileTtl;
    private final long expireRecheckInterval;
    protected JournalCloseInterceptor closeInterceptor;
    protected boolean open;
    protected TxLog txLog;
    protected boolean sequentialAccess = false;
    private volatile Partition<T> irregularPartition;
    private TxIterator txIterator;
    private long lastExpireCheck = 0L;

    public Journal(JournalMetadata<T> metadata, File location) throws JournalException {
        this.metadata = metadata;
        this.location = location;
        this.txLog = new TxLog(location, getMode(), metadata.getTxCountHint());
        this.open = true;
        this.timestampOffset = getMetadata().getTimestampMetadata() == null ? -1 : getMetadata().getTimestampMetadata().offset;
        this.inactiveColumns = new BitSet(metadata.getColumnCount());
        this.openFileTtl = metadata.getOpenFileTTL();
        this.expireRecheckInterval = (long) (this.openFileTtl * 0.1);
        configure();
    }

    /**
     * Closes all columns in all partitions.
     */
    public void close() {
        if (open) {

            if (closeInterceptor != null && !closeInterceptor.canClose(this)) {
                return;
            }

            closePartitions();
            closeSymbolTables();
            Misc.free(txLog);
            open = false;
        } else {
            throw new JournalRuntimeException("Already closed: %s", this);
        }
    }

    public TempPartition<T> createTempPartition(String name) {
        int lag = getMetadata().getLag();
        if (lag < 1) {
            throw new JournalRuntimeException("Journal doesn't support temp partitions: %s", this);
        }

        Interval interval = null;
        if (getMetadata().getPartitionBy() != PartitionBy.NONE) {
            Partition<T> p = partitions.getLast();
            if (p != null) {
                Interval lastPartitionInterval = p.getInterval();
                interval = new Interval(lastPartitionInterval.getLo(), Dates.addHours(lastPartitionInterval.getHi(), lag));
            } else {
                interval = new Interval(System.currentTimeMillis(), getMetadata().getPartitionBy());
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

    public void expireOpenFiles0() {
        long t = System.currentTimeMillis();
        if (openFileTtl > 0 && t - lastExpireCheck > expireRecheckInterval) {
            expireOpenFiles0(t - openFileTtl);
        }
        lastExpireCheck = t;
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
     * @throws JournalException if there is an error
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
    public int getMode() {
        return JournalMode.READ;
    }

    public String getName() {
        return getMetadata().getKey().getName();
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

    public MMappedSymbolTable getSymbolTable(String columnName) {
        MMappedSymbolTable result = symbolTableMap.get(columnName);
        if (result == null) {
            throw new JournalRuntimeException("Column is not a symbol: %s", columnName);
        }
        return result;
    }

    public MMappedSymbolTable getSymbolTable(int index) {
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
        assert isOpen();
        return txLog.getCurrentTxn();
    }

    public boolean hasIrregularPartition() {
        return irregularPartition != null;
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

    public final boolean isSequentialAccess() {
        return sequentialAccess;
    }

    public final void setSequentialAccess(boolean sequentialAccess) {
        this.sequentialAccess = sequentialAccess;
        for (int i = 0, n = partitions.size(); i < n; i++) {
            Partition partition = partitions.getQuick(i);
            if (partition != null) {
                partition.setSequentialAccess(sequentialAccess);
            }
        }

        if (irregularPartition != null) {
            irregularPartition.setSequentialAccess(sequentialAccess);
        }

        txLog.setSequentialAccess(sequentialAccess);

        for (int i = 0, n = symbolTables.size(); i < n; i++) {
            symbolTables.getQuick(i).setSequentialAccess(sequentialAccess);
        }
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

    @NotNull
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
     * @param obj   object, which attributes will be populated from journal record
     * @throws JournalException if there is an error
     */
    public void read(long rowID, T obj) throws JournalException {
        getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID), obj);
    }

    /**
     * Read objects by global row id
     *
     * @param rowIDs the global row ids to read
     * @return some objects
     * @throws JournalException if there is an error
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
     * @throws JournalException if there is an error
     */
    public T read(long rowID) throws JournalException {
        return getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID));
    }

    public boolean refresh() {
        if (isOpen() && txLog.head(tx)) {
            refreshInternal();
            for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
                symbolTables.getQuick(i).applyTx(tx.symbolTableSizes[i], tx.symbolTableIndexPointers[i]);
            }
            return true;
        }
        return false;
    }

    /**
     * Selects column names to be accessed by this journal. Non-selected columns and their
     * associated files will not be accessed. This method used to improve performance of
     * journal reads.
     *
     * @param columns the names of all the columns that have to be read.
     * @return instance of journal with narrowed column selection
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

    public void setCloseInterceptor(JournalCloseInterceptor closeInterceptor) {
        this.closeInterceptor = closeInterceptor;
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
        return getClass().getName() + "[location=" + location + ", " + "mode=" + getMode() + ", " + ", metadata=" + metadata + ']';
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

    private void closeSymbolTables() {
        for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
            Misc.free(symbolTables.getQuick(i));
        }
    }

    void configure() throws JournalException {
        txLog.head(tx);
        configureColumns();
        configureSymbolTableSynonyms();
        configurePartitions();
    }

    private void configureColumns() throws JournalException {
        int columnCount = getMetadata().getColumnCount();
        try {
            for (int i = 0; i < columnCount; i++) {
                ColumnMetadata meta = metadata.getColumnQuick(i);
                if (meta.type == ColumnType.SYMBOL && meta.sameAs == null) {
                    int tabIndex = symbolTables.size();
                    int tabSize = tx.symbolTableSizes.length > tabIndex ? tx.symbolTableSizes[tabIndex] : 0;
                    long indexTxAddress = tx.symbolTableIndexPointers.length > tabIndex ? tx.symbolTableIndexPointers[tabIndex] : 0;
                    MMappedSymbolTable tab = new MMappedSymbolTable(meta.distinctCountHint, meta.avgSize, getMetadata().getTxCountHint(), location, meta.name, getMode(), tabSize, indexTxAddress, meta.noCache, sequentialAccess);
                    symbolTables.add(tab);
                    symbolTableMap.put(meta.name, tab);
                    meta.symbolTable = tab;
                }
            }
        } catch (JournalException e) {
            closeSymbolTables();
            throw e;
        }
    }

    private void configureIrregularPartition() {
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

    private void configurePartitions() {
        File[] files = getLocation().listFiles(f -> f.isDirectory() && !f.getName().startsWith(Constants.TEMP_DIRECTORY_PREFIX));

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
                    Interval interval = new Interval(f.getName(), getMetadata().getPartitionBy());
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
                        partitions.add(new Partition<>(this, interval, partitionIndex++, txLimit, indexTxAddresses, sequentialAccess));
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
                MMappedSymbolTable tab = getSymbolTable(meta.sameAs);
                symbolTableMap.put(meta.name, tab);
                meta.symbolTable = tab;
            }
        }
    }

    private void expireOpenFiles0(long deadline) {
        for (int i = 0, sz = partitions.size() - 1; i < sz; i++) {
            Partition<T> partition = partitions.getQuick(i);
            if (partition.getLastAccessed() < deadline && partition.isOpen()) {
                partition.close();
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

            while (result.size() <= 0) {

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
    private void refreshInternal() {

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
