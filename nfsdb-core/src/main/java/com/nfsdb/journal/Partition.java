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

import com.nfsdb.journal.column.*;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.JournalMetadata;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.journal.iterators.PartitionBufferedIterator;
import com.nfsdb.journal.iterators.PartitionConcurrentIterator;
import com.nfsdb.journal.iterators.PartitionIterator;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.utils.*;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class Partition<T> implements Iterable<T>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(Partition.class);
    private final Journal<T> journal;
    private final ArrayList<SymbolIndexProxy<T>> indexProxies = new ArrayList<>();
    private final ArrayList<SymbolIndexProxy<T>> columnIndexProxies = new ArrayList<>();
    private final Interval interval;
    private final BitSet nulls;
    private final NullsAdaptor<T> nullsAdaptor;
    private final int columnCount;
    private final int appendKeyCache[];
    private final long appendSizeCache[];
    private AbstractColumn[] columns;
    private NullsColumn nullsColumn;
    private int partitionIndex;
    private File partitionDir;
    private long lastAccessed = System.currentTimeMillis();
    private long txLimit;
    private FixedColumn timestampColumn;
    private BinarySearch.LongTimeSeriesProvider indexOfVisitor;

    public NullsColumn getNullsColumn() {
        return nullsColumn;
    }

    public Partition<T> open() throws JournalException {
        access();
        if (columns == null) {

            columns = new AbstractColumn[journal.getMetadata().getColumnCount()];

            int nullsRecordSize = ((columns.length >>> 6) + (columns.length % 64 == 0 ? 0 : 1)) * 8;
            nullsColumn = new NullsColumn(
                    new MappedFileImpl(new File(partitionDir, "_nulls.d"),
                            ByteBuffers.getBitHint(nullsRecordSize, journal.getMetadata().getRecordHint()),
                            journal.getMode()),
                    nullsRecordSize,
                    columns.length
            );

            for (int i = 0; i < columns.length; i++) {
                open(i);
            }

            timestampColumn = getFixedWidthColumn(journal.getMetadata().getTimestampColumnIndex());

            if (timestampColumn != null) {
                this.indexOfVisitor = new BinarySearch.LongTimeSeriesProvider() {
                    @Override
                    public long readLong(long index) {
                        return timestampColumn.getLong(index);
                    }

                    @Override
                    public long size() {
                        return timestampColumn.size();
                    }
                };
            }
        }
        return this;
    }

    public Journal<T> getJournal() {
        return journal;
    }

    public Interval getInterval() {
        return interval;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public File getPartitionDir() {
        return partitionDir;
    }

    public String getName() {
        return partitionDir.getName();
    }

    public void close() {
        if (isOpen()) {
            for (int i = 0, columnsLength = columns.length; i < columnsLength; i++) {
                AbstractColumn ch = columns[i];
                if (ch != null) {
                    ch.close();
                }
            }
            nullsColumn.close();
            nullsColumn = null;
            columns = null;
            LOGGER.trace("Partition %s closed", partitionDir);
        }

        for (SymbolIndexProxy<T> proxy : indexProxies) {
            proxy.close();
        }
    }

    public boolean isOpen() {
        return columns != null;
    }

    public String getString(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);
        if (getNullsColumn().getBitSet(localRowID).get(columnIndex)) {
            return null;
        } else {
            return ((VariableColumn) columns[columnIndex]).getString(localRowID);
        }
    }

    public String getSymbol(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);
        if (getNullsColumn().getBitSet(localRowID).get(columnIndex)) {
            return null;
        } else {
            int symbolIndex = ((FixedColumn) columns[columnIndex]).getInt(localRowID);
            if (symbolIndex == SymbolTable.VALUE_NOT_FOUND) {
                return null;
            } else {
                return journal.getColumnMetadata(columnIndex).symbolTable.value(symbolIndex);
            }
        }
    }

    public long getLong(long localRowID, int columnIndex) {
        return getFixedColumnOrNPE(localRowID, columnIndex).getLong(localRowID);
    }

    public long getLong(long localRowID, int columnIndex, long defaultValue) {
        FixedColumn column = getFixedColumnOrNull(localRowID, columnIndex);
        if (column == null) {
            return defaultValue;
        } else {
            return column.getLong(localRowID);
        }
    }

    public int getInt(long localRowID, int columnIndex) {
        return getFixedColumnOrNPE(localRowID, columnIndex).getInt(localRowID);
    }

    public int getInt(long localRowID, int columnIndex, int defaultValue) {
        FixedColumn column = getFixedColumnOrNull(localRowID, columnIndex);
        if (column == null) {
            return defaultValue;
        } else {
            return column.getInt(localRowID);
        }
    }

    public double getDouble(long localRowID, int columnIndex) {
        return getFixedColumnOrNPE(localRowID, columnIndex).getDouble(localRowID);
    }

    public double getDouble(long localRowID, int columnIndex, double defaultValue) {
        FixedColumn column = getFixedColumnOrNull(localRowID, columnIndex);
        if (column == null) {
            return defaultValue;
        } else {
            return column.getDouble(localRowID);
        }
    }

    public boolean isNull(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);
        return nullsColumn.getBitSet(Rows.toLocalRowID(localRowID)).get(columnIndex);
    }


    public AbstractColumn getAbstractColumn(int i) {
        checkColumnIndex(i);
        return columns[i];
    }

    public KVIndex getIndexForColumn(String columnName) throws JournalException {
        return getIndexForColumn(journal.getMetadata().getColumnIndex(columnName));
    }

    public KVIndex getIndexForColumn(final int columnIndex) throws JournalException {
        SymbolIndexProxy h = columnIndexProxies.get(columnIndex);
        if (h == null) {
            throw new JournalException("There is no index for column '%s' in %s", journal.getMetadata().getColumnMetadata(columnIndex).name, this);
        }
        return h.getIndex();
    }

    public T read(long localRowID) {
        T obj = journal.newObject();
        read(localRowID, obj);
        return obj;
    }

    public void read(long localRowID, T obj) {

        sun.misc.Unsafe u = Unsafe.getUnsafe();
        BitSet nulls = nullsColumn.getBitSet(localRowID);
        nulls.or(journal.getInactiveColumns());
        for (int i = 0; i < columnCount; i++) {

            // fail fast
            if (nulls.get(i)) {
                continue;
            }
            Journal.ColumnMetadata m = journal.getColumnMetadata(i);
            switch (m.meta.type) {
                case BOOLEAN:
                    u.putBoolean(obj, m.meta.offset, ((FixedColumn) columns[i]).getBool(localRowID));
                    break;
                case BYTE:
                    u.putByte(obj, m.meta.offset, ((FixedColumn) columns[i]).getByte(localRowID));
                    break;
                case DOUBLE:
                    u.putDouble(obj, m.meta.offset, ((FixedColumn) columns[i]).getDouble(localRowID));
                    break;
                case INT:
                    u.putInt(obj, m.meta.offset, ((FixedColumn) columns[i]).getInt(localRowID));
                    break;
                case LONG:
                    u.putLong(obj, m.meta.offset, ((FixedColumn) columns[i]).getLong(localRowID));
                    break;
                case SHORT:
                    u.putShort(obj, m.meta.offset, ((FixedColumn) columns[i]).getShort(localRowID));
                    break;
                case STRING:
                    String s = ((VariableColumn) columns[i]).getString(localRowID);
                    if (s != null) {
                        u.putObject(obj, m.meta.offset, s);
                    }
                    break;
                case SYMBOL:
                    int symbolIndex = ((FixedColumn) columns[i]).getInt(localRowID);
                    // check if symbol was null
                    if (symbolIndex > SymbolTable.VALUE_IS_NULL) {
                        u.putObject(obj, m.meta.offset, m.symbolTable.value(symbolIndex));
                    }
                    break;
                case BINARY:
                    int size = ((VariableColumn) columns[i]).getBufferSize(localRowID);
                    ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, m.meta.offset);
                    if (buf == null || buf.capacity() < size) {
                        buf = ByteBuffer.allocate(size);
                        u.putObject(obj, m.meta.offset, buf);
                    }

                    if (buf.remaining() < size) {
                        buf.rewind();
                    }
                    buf.limit(size);
                    ((VariableColumn) columns[i]).getBuffer(localRowID, buf, size);
                    buf.flip();

            }
        }

        if (nullsAdaptor != null) {
            nullsAdaptor.setNulls(obj, nulls);
        }
    }

    public void append(Iterator<T> it) throws JournalException {
        while (it.hasNext()) {
            append(it.next());
        }
    }


    public void append(T obj) throws JournalException {
        boolean checkNulls = false;
        sun.misc.Unsafe u = Unsafe.getUnsafe();
        nulls.clear();


        if (nullsAdaptor != null) {
            nullsAdaptor.getNulls(obj, nulls);
            checkNulls = true;
        }

        int key;
        for (int i = 0; i < columnCount; i++) {
            Journal.ColumnMetadata meta = journal.getColumnMetadata(i);
            switch (meta.meta.type) {
                case BOOLEAN:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putBool(u.getBoolean(obj, meta.meta.offset));
                    }
                    break;
                case BYTE:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putByte(u.getByte(obj, meta.meta.offset));
                    }
                    break;
                case DOUBLE:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putDouble(u.getDouble(obj, meta.meta.offset));
                    }
                    break;
                case INT:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putInt(u.getInt(obj, meta.meta.offset));
                    }
                    break;
                case LONG:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putLong(u.getLong(obj, meta.meta.offset));
                    }
                    break;
                case SHORT:
                    if (checkNulls && nulls.get(i)) {
                        ((FixedColumn) columns[i]).putNull();
                    } else {
                        ((FixedColumn) columns[i]).putShort(u.getShort(obj, meta.meta.offset));
                    }
                    break;
                case STRING:
                    String s = (String) u.getObject(obj, meta.meta.offset);
                    if (s == null) {
                        nulls.set(i);
                        if (meta.meta.indexed) {
                            appendKeyCache[i] = SymbolTable.VALUE_IS_NULL;
                            appendSizeCache[i] = ((VariableColumn) columns[i]).putNull();
                        } else {
                            ((VariableColumn) columns[i]).putNull();
                        }
                    } else {
                        if (meta.meta.indexed) {
                            appendKeyCache[i] = Checksum.hash(s, meta.meta.distinctCountHint);
                            appendSizeCache[i] = ((VariableColumn) columns[i]).putString(s);
                        } else {
                            ((VariableColumn) columns[i]).putString(s);
                        }
                    }
                    break;
                case SYMBOL:
                    String sym = (String) u.getObject(obj, meta.meta.offset);
                    if (sym == null) {
                        nulls.set(i);
                        key = SymbolTable.VALUE_IS_NULL;
                    } else {
                        key = meta.symbolTable.put(sym);
                    }
                    if (meta.meta.indexed) {
                        appendKeyCache[i] = key;
                        appendSizeCache[i] = ((FixedColumn) columns[i]).putInt(key);
                    } else {
                        appendKeyCache[i] = -3;
                        ((FixedColumn) columns[i]).putInt(key);
                    }
                    break;
                case BINARY:
                    ByteBuffer buf = (ByteBuffer) u.getObject(obj, meta.meta.offset);
                    if (buf == null || buf.remaining() == 0) {
                        nulls.set(i);
                        ((VariableColumn) columns[i]).putNull();
                    } else {
                        ((VariableColumn) columns[i]).putBuffer(buf);
                    }
            }
        }
        nullsColumn.putBitSet(nulls);
        commitColumns();

        for (int i = 0, len = appendKeyCache.length; i < len; i++) {
            if (appendKeyCache[i] > -3) {
                columnIndexProxies.get(i).getIndex().add(appendKeyCache[i], appendSizeCache[i]);
            }
        }

        applyTx(Journal.TX_LIMIT_EVAL, null);
    }

    public void commitColumns() {
        nullsColumn.commit();
        // have to commit columns from first to last
        // this is because size of partition is calculated by size of
        // last column. If below loop is to break in the middle partition will assume smallest
        // column size.
        for (int i = 0; i < columnCount; i++) {
            AbstractColumn col = columns[i];
            if (col != null) {
                col.commit();
            }
        }
    }

    public void applyTx(long txLimit, long[] indexTxAddresses) {
        if (this.txLimit != txLimit) {
            this.txLimit = txLimit;
            for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
                SymbolIndexProxy<T> proxy = indexProxies.get(i);
                proxy.setTxAddress(indexTxAddresses == null ? 0 : indexTxAddresses[proxy.getColumnIndex()]);
            }
        }
    }

    public long indexOf(long timestamp, BinarySearch.SearchType type) {
        if (indexOfVisitor == null) {
            throw new JournalRuntimeException("There is no timestamp column in: " + this);
        }
        return BinarySearch.indexOf(indexOfVisitor, timestamp, type);
    }

    public FixedColumn getTimestampColumn() {
        return getFixedWidthColumn(journal.getMetadata().getTimestampColumnIndex());
    }

    public void rebuildIndexes() throws JournalException {
        if (!isOpen()) {
            throw new JournalException("Cannot rebuild indexes in closed partition: %s", this);
        }
        JournalMetadata<T> m = journal.getMetadata();
        for (int i = 0; i < columnCount; i++) {
            if (m.getColumnMetadata(i).indexed) {
                rebuildIndex(i);
            }
        }
    }

    /**
     * Rebuild the index of a column using the default keyCountHint and recordCountHint values from nfsdb.xml.
     *
     * @param columnIndex the column index
     * @throws com.nfsdb.journal.exceptions.JournalException if the operation fails
     */
    public void rebuildIndex(int columnIndex) throws JournalException {
        JournalMetadata<T> meta = journal.getMetadata();
        rebuildIndex(columnIndex,
                meta.getColumnMetadata(columnIndex).distinctCountHint,
                meta.getRecordHint(),
                meta.getTxCountHint());
    }

    /**
     * Rebuild the index of a column.
     *
     * @param columnIndex     the column index
     * @param keyCountHint    the key count hint override
     * @param recordCountHint the record count hint override
     * @throws com.nfsdb.journal.exceptions.JournalException if the operation fails
     */
    public void rebuildIndex(int columnIndex, int keyCountHint, int recordCountHint, int txCountHint) throws JournalException {
        final long time = LOGGER.isInfoEnabled() ? System.nanoTime() : 0L;

        getIndexForColumn(columnIndex).close();

        File base = journal.getMetadata().getColumnIndexBase(partitionDir, columnIndex);
        KVIndex.delete(base);

        try (KVIndex index = new KVIndex(base, keyCountHint, recordCountHint, txCountHint, JournalMode.APPEND, 0)) {
            FixedColumn col = getFixedWidthColumn(columnIndex);
            for (long localRowID = 0, sz = size(); localRowID < sz; localRowID++) {
                index.add(col.getInt(localRowID), localRowID);
            }
            index.commit();
        }

        LOGGER.debug("REBUILT %s [%dms]", base, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
    }

    public long size() {
        if (!isOpen()) {
            throw new JournalRuntimeException("Closed partition: %s", this);
        }

        if (txLimit != Journal.TX_LIMIT_EVAL) {
            return txLimit;
        }

        long sz = 0;
        for (int i = columns.length - 1; i >= 0; i--) {
            AbstractColumn c = columns[i];
            if (c != null) {
                sz = c.size();
                break;
            }
        }

        txLimit = sz;
        return sz;
    }

    public long getLastAccessed() {
        return lastAccessed;
    }

    public Iterator<T> iterator() {
        return new PartitionIterator<>(this, 0, size() - 1);
    }

    public Iterator<T> iterator(long start, long end) {
        return new PartitionIterator<>(this, start, end);
    }

    public PartitionBufferedIterator<T> bufferedIterator() {
        return new PartitionBufferedIterator<>(this, 0, size() - 1);
    }

    public PartitionBufferedIterator<T> bufferedIterator(long lo, long hi) {
        return new PartitionBufferedIterator<>(this, lo, hi);
    }

    public ConcurrentIterator<T> parallelIterator() {
        return parallelIterator(0, size() - 1);
    }

    public ConcurrentIterator<T> parallelIterator(long lo, long hi) {
        return parallelIterator(lo, hi, 1024);
    }

    public ConcurrentIterator<T> parallelIterator(long lo, long hi, int bufferSize) {
        return new PartitionConcurrentIterator<>(this, lo, hi, bufferSize);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partitionIndex=" + partitionIndex +
                ", open=" + isOpen() +
                ", partitionDir=" + partitionDir +
                ", interval=" + interval +
                ", lastAccessed=" + Dates.toString(lastAccessed) +
                '}';
    }

    public void compact() throws JournalException {
        if (columns == null || columns.length == 0) {
            throw new JournalException("Cannot compact closed partition: %s", this);
        }

        for (int i = 0, columnsLength = columns.length; i < columnsLength; i++) {
            AbstractColumn col = columns[i];
            if (col != null) {
                col.compact();
            }
        }

        for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.get(i);
            proxy.getIndex().compact();
        }
    }

    // TODO: rethink visibility
    public void updateIndexes(long oldSize, long newSize) {
        if (oldSize < newSize) {
            try {
                for (int i1 = 0, indexProxiesSize = indexProxies.size(); i1 < indexProxiesSize; i1++) {
                    SymbolIndexProxy<T> proxy = indexProxies.get(i1);
                    KVIndex index = proxy.getIndex();
                    FixedColumn col = getFixedWidthColumn(proxy.getColumnIndex());
                    for (long i = oldSize; i < newSize; i++) {
                        index.add(col.getInt(i), i);
                    }
                    index.commit();
                }
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    private FixedColumn getFixedColumnOrNull(long localRowID, int columnIndex) {
        if (getNullsColumn().getBitSet(localRowID).get(columnIndex)) {
            return null;
        } else {
            return getFixedWidthColumn(columnIndex);
        }
    }

    private FixedColumn getFixedColumnOrNPE(long localRowID, int columnIndex) {
        FixedColumn result = getFixedColumnOrNull(localRowID, columnIndex);
        if (result == null) {
            throw new NullPointerException("NULL value for " + journal.getMetadata().getColumnMetadata(columnIndex).name + "; rowID [" + Rows.toRowID(partitionIndex, localRowID) + "]");
        }
        return result;
    }

    private FixedColumn getFixedWidthColumn(int i) {
        checkColumnIndex(i);
        return (FixedColumn) columns[i];
    }

    void clearTx() {
        applyTx(Journal.TX_LIMIT_EVAL, null);
    }

    void setPartitionDir(File partitionDir, long[] indexTxAddresses) {
        boolean create = partitionDir != null && !partitionDir.equals(this.partitionDir);
        this.partitionDir = partitionDir;
        if (create) {
            createSymbolIndexProxies(indexTxAddresses);
        }
    }

    private void open(int columnIndex) throws JournalException {

        JournalMetadata.ColumnMetadata m = journal.getMetadata().getColumnMetadata(columnIndex);
        switch (m.type) {
            case STRING:
            case BINARY:
                columns[columnIndex] = new VariableColumn(
                        new MappedFileImpl(new File(partitionDir, m.name + ".d"), m.bitHint, journal.getMode())
                        , new MappedFileImpl(new File(partitionDir, m.name + ".i"), m.indexBitHint, journal.getMode()));
                break;
            default:
                columns[columnIndex] = new FixedColumn(
                        new MappedFileImpl(new File(partitionDir, m.name + ".d"), m.bitHint, journal.getMode()), m.size);
        }
    }

    Partition<T> access() {
        this.lastAccessed = getJournal().getTimerCache().getMillis();
        return this;
    }

    private void checkColumnIndex(int i) {
        if (columns == null) {
            throw new JournalRuntimeException("Partition is closed: %s", this);
        }

        if (i < 0 || i >= columns.length) {
            throw new JournalRuntimeException("Invalid column index: %d in %s", i, this);
        }
    }

    void truncate(long newSize) throws JournalException {
        if (isOpen() && size() > newSize) {
            for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
                SymbolIndexProxy<T> proxy = indexProxies.get(i);
                proxy.getIndex().truncate(newSize);
            }
            for (AbstractColumn column : columns) {
                if (column != null) {
                    column.truncate(newSize);
                }
            }

            commitColumns();
            clearTx();
        }
    }

    void expireOpenIndices() {
        long expiry = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(journal.getMetadata().getOpenPartitionTTL());
        for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.get(i);
            if (expiry > proxy.getLastAccessed()) {
                proxy.close();
            }
        }
    }

    void getIndexPointers(long[] pointers) throws JournalException {
        for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.get(i);
            pointers[proxy.getColumnIndex()] = proxy.getIndex().getTxAddress();
        }
    }

    void commit() throws JournalException {
        for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.get(i);
            proxy.getIndex().commit();
        }
    }

    void force() throws JournalException {
        for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.get(i);
            proxy.getIndex().force();
        }

        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                AbstractColumn column = columns[i];
                if (column != null) {
                    column.force();
                }
            }
            nullsColumn.force();
        }
    }

    private void createSymbolIndexProxies(long[] indexTxAddresses) {
        indexProxies.clear();
        columnIndexProxies.clear();
        JournalMetadata<T> meta = journal.getMetadata();
        for (int i = 0; i < columnCount; i++) {
            if (meta.getColumnMetadata(i).indexed) {
                SymbolIndexProxy<T> proxy = new SymbolIndexProxy<>(this, i, indexTxAddresses == null ? 0 : indexTxAddresses[i]);
                indexProxies.add(proxy);
                columnIndexProxies.add(proxy);
            } else {
                columnIndexProxies.add(null);
            }
        }
    }

    Partition(Journal<T> journal, Interval interval, int partitionIndex, long txLimit, long[] indexTxAddresses) {
        this.journal = journal;
        this.partitionIndex = partitionIndex;
        this.interval = interval;
        this.txLimit = txLimit;
        this.columnCount = journal.getMetadata().getColumnCount();
        this.nulls = new BitSet(columnCount);
        this.nullsAdaptor = journal.getMetadata().getNullsAdaptor();
        this.appendKeyCache = new int[columnCount];
        this.appendSizeCache = new long[columnCount];
        Arrays.fill(appendKeyCache, -3);

        String dateStr = Dates.dirNameForIntervalStart(interval, journal.getMetadata().getPartitionType());
        if (dateStr.length() > 0) {
            setPartitionDir(new File(this.journal.getLocation(), dateStr), indexTxAddresses);
        } else {
            setPartitionDir(this.journal.getLocation(), indexTxAddresses);
        }
    }
}
