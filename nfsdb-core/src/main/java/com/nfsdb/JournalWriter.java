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

import com.nfsdb.collections.PeekingListIterator;
import com.nfsdb.column.BSearchType;
import com.nfsdb.column.FixedColumn;
import com.nfsdb.column.HugeBuffer;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.concurrent.PartitionCleaner;
import com.nfsdb.concurrent.TimerCache;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.exp.FlexBufferSink;
import com.nfsdb.exp.RecordSourcePrinter;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.iterators.ConcurrentIterator;
import com.nfsdb.iterators.MergingIterator;
import com.nfsdb.iterators.PeekingIterator;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.psrc.JournalTailPartitionSource;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordSource;
import com.nfsdb.lang.cst.impl.rsrc.AllRowSource;
import com.nfsdb.locks.Lock;
import com.nfsdb.locks.LockManager;
import com.nfsdb.logging.Logger;
import com.nfsdb.tx.Tx;
import com.nfsdb.tx.TxListener;
import com.nfsdb.tx.TxLog;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Rows;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JournalWriter<T> extends Journal<T> {
    private static final Logger LOGGER = Logger.getLogger(JournalWriter.class);
    private final long lagMillis;
    private final long lagSwellMillis;
    private final boolean checkOrder;
    private final PeekingListIterator<T> peekingListIterator = new PeekingListIterator<>();
    private final MergingIterator<T> mergingIterator = new MergingIterator<>();
    private final JournalEntryWriterImpl journalEntryWriter;
    private Lock writeLock;
    private TxListener txListener;
    private boolean txActive = false;
    private int txPartitionIndex = -1;
    private long appendTimestampLo = -1;
    private PartitionCleaner partitionCleaner;
    private boolean commitOnClose = true;
    // irregular partition related
    private boolean doDiscard = true;
    private boolean doJournal = true;
    private Partition<T> appendPartition;
    private long appendTimestampHi = -1;

    public JournalWriter(JournalMetadata<T> metadata, JournalKey<T> key, TimerCache timerCache) throws JournalException {
        super(metadata, key, timerCache);
        if (metadata.isPartialMapped()) {
            close();
            throw new JournalException("Metadata is unusable for writer. Partially mapped?");
        }
        this.lagMillis = TimeUnit.HOURS.toMillis(getMetadata().getLag());
        this.lagSwellMillis = lagMillis * 3;
        this.checkOrder = key.isOrdered() && getTimestampOffset() != -1;
        this.journalEntryWriter = new JournalEntryWriterImpl(this);
    }

    /**
     * Add objects to the end of the Journal.
     *
     * @param objects objects to add
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public void append(Iterable<T> objects) throws JournalException {
        for (T o : objects) {
            append(o);
        }
    }

    /**
     * Add an object to the end of the Journal.
     *
     * @param obj the object to add
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public void append(T obj) throws JournalException {

        if (obj == null) {
            throw new JournalException("Cannot append NULL to %s", this);
        }

        if (!txActive) {
            beginTx();
        }

        if (checkOrder) {
            long timestamp = getTimestamp(obj);

            if (timestamp > appendTimestampHi) {
                switchAppendPartition(timestamp);
            }

            if (timestamp < appendTimestampLo) {
                throw new JournalException("Cannot insert records out of order. maxHardTimestamp=%d (%s), timestamp=%d (%s): %s"
                        , appendTimestampLo, Dates.toString(appendTimestampLo), timestamp, Dates.toString(timestamp), this);
            }

            appendPartition.append(obj);
            appendTimestampLo = timestamp;
        } else {
            getAppendPartition().append(obj);
        }
    }

    /**
     * Add objects to the end of the Journal.
     *
     * @param objects objects to add
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    @SafeVarargs
    public final void append(T... objects) throws JournalException {
        for (int i = 0; i < objects.length; i++) {
            append(objects[i]);
        }
    }

    /**
     * Copy the objects corresponding to the specified ids to the end of the Journal.
     *
     * @param resultSet the global row ids
     * @throws com.nfsdb.exceptions.JournalException if there is an error
     */
    public void append(ResultSet<T> resultSet) throws JournalException {
        if (isCompatible(resultSet.getJournal())) {
            for (T obj : resultSet.bufferedIterator()) {
                this.append(obj);
            }
        } else {
            throw new JournalException("%s is incompatible with %s", this, resultSet.getJournal());
        }
    }

    public void append(Journal<T> journal) throws JournalException {
        try (ConcurrentIterator<T> iterator = journal.concurrentIterator()) {
            for (T obj : iterator) {
                append(obj);
            }
        }
    }

    public void beginTx() {
        if (!txActive) {
            this.txActive = true;
            this.txPartitionIndex = nonLagPartitionCount() - 1;
        }
    }

    @Override
    public void close() {
        if (open) {
            if (partitionCleaner != null) {
                partitionCleaner.halt();
                partitionCleaner = null;
            }
            try {
                if (isCommitOnClose()) {
                    commit();
                    purgeUnusedTempPartitions(txLog);
                }
                super.close();
                if (writeLock != null) {
                    LockManager.release(writeLock);
                    writeLock = null;
                }
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    public void commit() throws JournalException {
        commit(false, -1L, -1L);
    }

    public void commit(boolean force, long txn, long txPin) throws JournalException {
        if (txActive) {
            commit(force ? Tx.TX_FORCE : Tx.TX_NORMAL, txn, txPin);
            notifyTxListener();
            expireOpenFiles();
            txActive = false;
        }
    }

    public void commitDurable() throws JournalException {
        commit(true, -1L, -1L);
    }

    public void compact() throws JournalException {
        int partitionCount = getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            getPartition(i, true).compact();
        }
    }

    public Partition<T> createPartition(Interval interval, int partitionIndex) throws JournalException {
        Partition<T> result = new Partition<>(this, interval, partitionIndex, TX_LIMIT_EVAL, null).open();
        partitions.add(result);
        return result;
    }

    /**
     * Deletes entire Journal.
     *
     * @throws com.nfsdb.exceptions.JournalException if the Journal is open (must be closed)
     */
    public void delete() throws JournalException {
        if (isOpen()) {
            throw new JournalException("Cannot delete open journal: %s", this);
        }
        Files.deleteOrException(getLocation());
    }

    public JournalEntryWriter entryWriter() throws JournalException {
        return entryWriter(0);
    }

    public JournalEntryWriter entryWriter(long timestamp) throws JournalException {
        if (!txActive) {
            beginTx();
        }

        if (checkOrder) {
            if (timestamp > appendTimestampHi) {
                switchAppendPartition(timestamp);
            }

            if (timestamp < appendTimestampLo) {
                throw new JournalException("Cannot insert records out of order. maxHardTimestamp=%d (%s), timestamp=%d (%s): %s"
                        , appendTimestampLo, Dates.toString(appendTimestampLo), timestamp, Dates.toString(timestamp), this);
            }

            journalEntryWriter.setPartition(appendPartition, timestamp);
            return journalEntryWriter;

        } else {
            journalEntryWriter.setPartition(getAppendPartition(), timestamp);
            return journalEntryWriter;
        }
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && getKey().equals(((Journal) o).getKey());
    }

    public Partition<T> getAppendPartition(long timestamp) throws JournalException {
        int sz = partitions.size();
        if (sz > 0) {
            Partition<T> result = partitions.get(sz - 1);
            if (result.getInterval() == null || result.getInterval().contains(timestamp)) {
                return result.open().access();
            } else if (result.getInterval().isBefore(timestamp)) {
                return createPartition(new Interval(timestamp, getMetadata().getPartitionType()), sz);
            } else {
                throw new JournalException("%s cannot be appended to %s", Dates.toString(timestamp), this);
            }
        } else {
            return createPartition(new Interval(timestamp, getMetadata().getPartitionType()), 0);
        }
    }

    /**
     * Max timestamp in journal for append operation. Objects with timestamp older then
     * this will always be rejected.
     *
     * @return max timestamp older then which append is impossible.
     * @throws com.nfsdb.exceptions.JournalException if journal cannot calculate timestamp.
     */
    public long getAppendTimestampLo() throws JournalException {
        if (appendTimestampLo == -1) {
            if (nonLagPartitionCount() == 0) {
                return 0;
            }

            FixedColumn column = lastNonEmptyNonLag().getTimestampColumn();
            long sz;
            if ((sz = column.size()) > 0) {
                appendTimestampLo = column.getLong(sz - 1);
            } else {
                return 0;
            }
        }
        return appendTimestampLo;
    }

    @Override
    public JournalMode getMode() {
        return JournalMode.APPEND;
    }

    public Partition<T> getPartitionForTimestamp(long timestamp) {
        int sz = partitions.size();
        for (int i = 0; i < sz; i++) {
            Partition<T> result = partitions.get(i);
            if (result.getInterval() == null || result.getInterval().contains(timestamp)) {
                return result.access();
            }
        }

        if (partitions.get(0).getInterval().isAfter(timestamp)) {
            return partitions.get(0).access();
        } else {
            return partitions.get(sz - 1).access();
        }
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    public boolean isCommitOnClose() {
        return commitOnClose;
    }

    public JournalWriter<T> setCommitOnClose(boolean commitOnClose) {
        this.commitOnClose = commitOnClose;
        return this;
    }

    public boolean isTxActive() {
        return txActive;
    }

    public void mergeAppend(List<T> list) throws JournalException {
        this.peekingListIterator.setDelegate(list);
        mergeAppend(this.peekingListIterator);
    }

    public void mergeAppend(ResultSet<T> resultSet) throws JournalException {
        mergeAppend(resultSet.bufferedIterator());
    }

    public void mergeAppend(PeekingIterator<T> data) throws JournalException {

        if (lagMillis == 0) {
            throw new JournalException("This journal is not configured to have lag partition");
        }

        beginTx();

        if (data == null || data.isEmpty()) {
            return;
        }

        long dataMaxTimestamp = getTimestamp(data.peekLast());
        long hard = getAppendTimestampLo();

        if (dataMaxTimestamp < hard) {
            return;
        }

        final Partition<T> lagPartition = openOrCreateLagPartition();
        this.doDiscard = true;
        this.doJournal = true;

        long dataMinTimestamp = getTimestamp(data.peekFirst());
        long lagMaxTimestamp = getMaxTimestamp();
        long lagMinTimestamp = lagPartition.size() == 0L ? 0 : getTimestamp(lagPartition.read(0));
        long soft = Math.max(dataMaxTimestamp, lagMaxTimestamp) - lagMillis;

        if (dataMinTimestamp > lagMaxTimestamp) {
            // this could be as simple as just appending data to lag
            // the only complication is that after adding records to lag it could swell beyond
            // the allocated "lagSwellTimestamp"
            // we should check if this is going to happen and optimise copying of data

            long lagSizeMillis;
            if (hard > 0L) {
                lagSizeMillis = dataMaxTimestamp - hard;
            } else if (lagMinTimestamp > 0L) {
                lagSizeMillis = dataMaxTimestamp - lagMinTimestamp;
            } else {
                lagSizeMillis = 0L;
            }

            if (lagSizeMillis > lagSwellMillis) {
                // data would  be too big and would stretch outside of swell timestamp
                // this is when lag partition should be split, but it is still a straight split without re-order

                Partition<T> tempPartition = createTempPartition().open();
                splitAppend(lagPartition.bufferedIterator(), hard, soft, tempPartition);
                splitAppend(data, hard, soft, tempPartition);
                replaceIrregularPartition(tempPartition);
            } else {
                // simplest case, just append to lag
                lagPartition.append(data);
            }
        } else {

            Partition<T> tempPartition = createTempPartition().open();
            if (dataMinTimestamp > lagMinTimestamp && dataMaxTimestamp < lagMaxTimestamp) {
                //
                // overlap scenario 1: data is fully inside of lag
                //

                // calc boundaries of lag that intersects with data
                long lagMid1 = lagPartition.indexOf(dataMinTimestamp, BSearchType.OLDER_OR_SAME);
                long lagMid2 = lagPartition.indexOf(dataMaxTimestamp, BSearchType.NEWER_OR_SAME);

                // copy part of lag above data
                splitAppend(lagPartition.bufferedIterator(0, lagMid1), hard, soft, tempPartition);

                // merge lag with data and copy result to temp partition
                splitAppendMerge(data, lagPartition.bufferedIterator(lagMid1 + 1, lagMid2 - 1), hard, soft, tempPartition);

                // copy part of lag below data
                splitAppend(lagPartition.bufferedIterator(lagMid2, lagPartition.size() - 1), hard, soft, tempPartition);

            } else if (dataMaxTimestamp < lagMinTimestamp && dataMaxTimestamp <= lagMinTimestamp) {
                //
                // overlap scenario 2: data sits directly above lag
                //
                splitAppend(data, hard, soft, tempPartition);
                splitAppend(lagPartition.bufferedIterator(), hard, soft, tempPartition);
            } else if (dataMinTimestamp <= lagMinTimestamp && dataMaxTimestamp < lagMaxTimestamp) {
                //
                // overlap scenario 3: bottom part of data overlaps top part of lag
                //

                // calc overlap line
                long split = lagPartition.indexOf(dataMaxTimestamp, BSearchType.NEWER_OR_SAME);

                // merge lag with data and copy result to temp partition
                splitAppendMerge(data, lagPartition.bufferedIterator(0, split - 1), hard, soft, tempPartition);

                // copy part of lag below data
                splitAppend(lagPartition.bufferedIterator(split, lagPartition.size() - 1), hard, soft, tempPartition);
            } else if (dataMinTimestamp > lagMinTimestamp && dataMaxTimestamp >= lagMaxTimestamp) {
                //
                // overlap scenario 4: top part of data overlaps with bottom part of lag
                //
                long split = lagPartition.indexOf(dataMinTimestamp, BSearchType.OLDER_OR_SAME);

                // copy part of lag above overlap
                splitAppend(lagPartition.bufferedIterator(0, split), hard, soft, tempPartition);

                // merge lag with data and copy result to temp partition
                splitAppendMerge(data, lagPartition.bufferedIterator(split + 1, lagPartition.size() - 1), hard, soft, tempPartition);
            } else if (dataMinTimestamp <= lagMinTimestamp && dataMaxTimestamp >= lagMaxTimestamp) {
                //
                // overlap scenario 5: lag is fully inside of data
                //

                // merge lag with data and copy result to temp partition
                splitAppendMerge(data, lagPartition.bufferedIterator(), hard, soft, tempPartition);
            } else {
                throw new JournalRuntimeException("Unsupported overlap type: lag min/max [%s/%s] data min/max: [%s/%s]"
                        , Dates.toString(lagMinTimestamp), Dates.toString(lagMaxTimestamp)
                        , Dates.toString(dataMinTimestamp), Dates.toString(dataMaxTimestamp));
            }

            replaceIrregularPartition(tempPartition);
        }
    }

    /**
     * Opens existing lag partition if it exists or creates new one if parent journal is configured to
     * have lag partitions.
     *
     * @return Lag partition instance.
     * @throws com.nfsdb.exceptions.JournalException
     */
    public Partition<T> openOrCreateLagPartition() throws JournalException {
        Partition<T> result = getIrregularPartition();
        if (result == null) {
            result = createTempPartition();
            setIrregularPartition(result);
        }
        return result.open();
    }

    public void purgeTempPartitions() {
        partitionCleaner.purge();
    }

    public void purgeUnusedTempPartitions(TxLog txLog) throws JournalException {
        final Tx tx = new Tx();
        final String lagPartitionName = hasIrregularPartition() ? getIrregularPartition().getName() : null;

        File[] files = getLocation().listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory() && f.getName().startsWith(Constants.TEMP_DIRECTORY_PREFIX) &&
                        (lagPartitionName == null || !lagPartitionName.equals(f.getName()));
            }
        });

        if (files != null) {

            Arrays.sort(files);

            for (int i = 0; i < files.length; i++) {

                if (!txLog.isEmpty()) {
                    txLog.head(tx);
                    if (files[i].getName().equals(tx.lagName)) {
                        continue;
                    }
                }

                // get exclusive lock
                Lock lock = LockManager.lockExclusive(files[i]);
                try {
                    if (lock != null && lock.isValid()) {
                        LOGGER.trace("Purging : %s", files[i]);
                        if (!Files.delete(files[i])) {
                            LOGGER.info("Could not purge: %s", files[i]);
                        }
                    } else {
                        LOGGER.trace("Partition in use: %s", files[i]);
                    }
                } finally {
                    LockManager.release(lock);
                }
            }
        }
    }

    public void rebuildIndexes() throws JournalException {
        int partitionCount = getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            getPartition(i, true).rebuildIndexes();
        }
    }

    public void removeIrregularPartition() {
        beginTx();
        removeIrregularPartitionInternal();
    }

    public void rollback() throws JournalException {
        if (txActive) {
            rollback0(txLog.getCurrentTxAddress(), false);
            txActive = false;
        }
    }

    public void rollback(long txn, long txPin) throws JournalException {
        rollback0(txLog.findAddress(txn, txPin), true);
    }

    public void setTxListener(TxListener txListener) {
        this.txListener = txListener;
    }

    public void truncate() throws JournalException {
        beginTx();
        int partitionCount = getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            Partition<T> partition = getPartition(i, true);
            partition.truncate(0);
            partition.close();
            Files.deleteOrException(partition.getPartitionDir());
        }

        closePartitions();

        for (int i = 0, sz = getSymbolTableCount(); i < sz; i++) {
            getSymbolTable(i).truncate();
        }
        appendTimestampLo = -1;
        commitDurable();
    }

    @Override
    void closePartitions() {
        super.closePartitions();
        appendPartition = null;
        appendTimestampHi = -1;
    }

    Partition<T> createTempPartition() throws JournalException {
        return createTempPartition(Constants.TEMP_DIRECTORY_PREFIX + "." + System.currentTimeMillis() + "." + UUID.randomUUID().toString());
    }

    Partition<T> getAppendPartition() throws JournalException {
        if (this.appendPartition != null) {
            return appendPartition;
        }

        int count = nonLagPartitionCount();
        if (count > 0) {
            return appendPartition = getPartition(count - 1, true);
        } else {
            if (getMetadata().getPartitionType() != PartitionType.NONE) {
                throw new JournalException("getAppendPartition() without timestamp on partitioned journal: %s", this);
            }
            return appendPartition = createPartition(new Interval((long) 0, getMetadata().getPartitionType()), 0);
        }
    }

    void updateTsLo(long ts) {
        if (checkOrder) {
            appendTimestampLo = ts;
        }
    }

    @Override
    protected void configure() throws JournalException {
        writeLock = LockManager.lockExclusive(getLocation());
        if (writeLock == null || !writeLock.isValid()) {
            close();
            throw new JournalException("Journal is already open for APPEND at %s", getLocation());
        }
        if (txLog.isEmpty()) {
            commit(Tx.TX_NORMAL, 0L, 0L);
        }
        txLog.head(tx);

        File meta = new File(getLocation(), "_meta2");
        if (!meta.exists()) {
            try (HugeBuffer hb = new HugeBuffer(meta, 12, JournalMode.APPEND)) {
                getMetadata().write(hb);
            }
        }

        super.configure();

        beginTx();
        rollback();
        rollbackPartitionDirs();

        if (tx.journalMaxRowID > 0 && getPartitionCount() <= Rows.toPartitionIndex(tx.journalMaxRowID)) {
            beginTx();
            commit();
        }
        if (getMetadata().getLag() != -1) {
            this.partitionCleaner = new PartitionCleaner(this, getLocation().getName());
            this.partitionCleaner.start();
        }

    }

    private void commit(byte command, long txn, long txPin) throws JournalException {
        boolean force = command == Tx.TX_FORCE;
        Partition<T> partition = lastNonEmptyNonLag();
        Partition<T> lag = getIrregularPartition();

        tx.command = command;
        tx.txn = txn;
        tx.txPin = txPin;
        tx.prevTxAddress = txLog.getCurrentTxAddress();
        tx.journalMaxRowID = partition == null ? -1 : Rows.toRowID(partition.getPartitionIndex(), partition.size());
        tx.lastPartitionTimestamp = partition == null || partition.getInterval() == null ? 0 : partition.getInterval().getLo();
        tx.lagSize = lag == null ? 0 : lag.open().size();
        tx.lagName = lag == null ? null : lag.getName();
        tx.symbolTableSizes = new int[getSymbolTableCount()];
        tx.symbolTableIndexPointers = new long[tx.symbolTableSizes.length];
        for (int i = 0; i < tx.symbolTableSizes.length; i++) {
            SymbolTable tab = getSymbolTable(i);
            tab.commit();
            if (force) {
                tab.force();
            }
            tx.symbolTableSizes[i] = tab.size();
            tx.symbolTableIndexPointers[i] = tab.getIndexTxAddress();
        }
        tx.indexPointers = new long[getMetadata().getColumnCount()];

        for (int i = Math.max(txPartitionIndex, 0), sz = nonLagPartitionCount(); i < sz; i++) {
            Partition<T> p = getPartition(i, true);
            p.commit();
            if (force) {
                p.force();
            }

        }

        if (partition != null) {
            partition.getIndexPointers(tx.indexPointers);
        }

        tx.lagIndexPointers = new long[tx.indexPointers.length];
        if (lag != null) {
            lag.commit();
            if (force) {
                lag.force();
            }
            lag.getIndexPointers(tx.lagIndexPointers);
        }

        txLog.write(tx, txn != -1);
        if (force) {
            txLog.force();
        }
    }

    private void notifyTxListener() {
        if (txListener != null) {
            txListener.onCommit();
        }
    }

    private void replaceIrregularPartition(Partition<T> temp) {
        setIrregularPartition(temp);
        purgeTempPartitions();
    }

    private void rollback0(long address, boolean writeDiscard) throws JournalException {

        if (address == -1L) {
            throw new JournalException("Wrong txn/txPin. Incompatible journals?");
        }
        txLog.read(address, tx);

        if (tx.address == 0) {
            throw new JournalException("Invalid transaction address");
        }

        if (writeDiscard) {
            LOGGER.info("Journal %s is rolling back to transaction #%d, timestamp %s", metadata.getId(), tx.txn, Dates.toString(tx.timestamp));
            writeDiscardFile(tx.journalMaxRowID);
        }

        // partitions need to be dealt with first to make sure new lag is assigned a correct partitionIndex
        rollbackPartitions(tx);

        Partition<T> lag = getIrregularPartition();
        if (tx.lagName != null && tx.lagName.length() > 0 && (lag == null || !tx.lagName.equals(lag.getName()))) {
            Partition<T> newLag = createTempPartition(tx.lagName);
            setIrregularPartition(newLag);
            newLag.applyTx(tx.lagSize, tx.lagIndexPointers);
        } else if (lag != null && tx.lagName == null) {
            removeIrregularPartitionInternal();
        } else if (lag != null) {
            lag.truncate(tx.lagSize);
        }


        if (tx.symbolTableSizes.length == 0) {
            for (int i = 0, sz = getSymbolTableCount(); i < sz; i++) {
                getSymbolTable(i).truncate();
            }
        } else {
            for (int i = 0, sz = getSymbolTableCount(); i < sz; i++) {
                getSymbolTable(i).truncate(tx.symbolTableSizes[i]);
            }
        }
        appendTimestampLo = -1;
        appendTimestampHi = -1;
        appendPartition = null;
        txLog.writeTxAddress(tx.address);
        txActive = false;
    }

    private void rollbackPartitionDirs() throws JournalException {
        File[] files = getLocation().listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory() && !f.getName().startsWith(Constants.TEMP_DIRECTORY_PREFIX);
            }
        });

        if (files != null) {
            Arrays.sort(files);
            for (int i = getPartitionCount(); i < files.length; i++) {
                Files.deleteOrException(files[i]);
            }
        }
    }

    private void rollbackPartitions(Tx tx) throws JournalException {
        int partitionIndex = tx.journalMaxRowID == -1 ? 0 : Rows.toPartitionIndex(tx.journalMaxRowID);
        for (Iterator<Partition<T>> it = partitions.iterator(); it.hasNext(); ) {
            Partition<T> partition = it.next();
            if (partition.getPartitionIndex() == partitionIndex) {
                partition.open();
                partition.truncate(tx.journalMaxRowID == -1 ? 0 : Rows.toLocalRowID(tx.journalMaxRowID));
            } else if (partition.getPartitionIndex() > partitionIndex) {
                it.remove();
                partition.close();
                Files.deleteOrException(partition.getPartitionDir());
            }
        }
    }

    private void splitAppend(Iterator<T> it, long hard, long soft, Partition<T> partition) throws JournalException {
        while (it.hasNext()) {
            T obj = it.next();
            if (doDiscard && getTimestamp(obj) < hard) {
                // discard
                continue;
            } else if (doDiscard) {
                doDiscard = false;
            }

            if (doJournal && getTimestamp(obj) < soft) {
                append(obj);
                continue;
            } else if (doJournal) {
                doJournal = false;
            }

            partition.append(obj);
        }
    }

    private void splitAppendMerge(Iterator<T> a, Iterator<T> b, long hard, long soft, Partition<T> temp) throws JournalException {
        splitAppend(mergingIterator.$new(a, b, getTimestampComparator()), hard, soft, temp);
    }

    private void switchAppendPartition(long timestamp) throws JournalException {
        boolean computeTimestampLo = appendPartition == null;

        appendPartition = getAppendPartition(timestamp);

        Interval interval = appendPartition.getInterval();
        if (interval == null) {
            appendTimestampHi = Long.MAX_VALUE;
        } else {
            appendTimestampHi = interval.getHi();
        }

        if (computeTimestampLo) {
            FixedColumn column = appendPartition.getTimestampColumn();
            long sz;
            if ((sz = column.size()) > 0) {
                appendTimestampLo = column.getLong(sz - 1);
            }
        } else {
            appendTimestampLo = appendPartition.getInterval().getLo();
        }
    }

    private void writeDiscardFile(long rowid) throws JournalException {

        File f = new File(metadata.getLocation(), "discard.txt");
        RecordSource<? extends Record> rs = new JournalSourceImpl(
                new JournalTailPartitionSource(this, false, rowid)
                , new AllRowSource()
        );


        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            try (FileChannel channel = raf.getChannel()) {
                try (FlexBufferSink sink = new FlexBufferSink(channel.position(channel.size()), 1024 * 1024)) {
                    RecordSourcePrinter printer = new RecordSourcePrinter(sink);
                    printer.print(rs);
                    LOGGER.info("Discarded records are appended to %s", f);
                }
            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }
}
