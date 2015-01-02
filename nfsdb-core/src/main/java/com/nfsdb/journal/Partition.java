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
import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.journal.iterators.PartitionBufferedIterator;
import com.nfsdb.journal.iterators.PartitionConcurrentIterator;
import com.nfsdb.journal.iterators.PartitionIterator;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Interval;
import com.nfsdb.journal.utils.Unsafe;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class Partition<T> implements Iterable<T>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(Partition.class);
    private final Journal<T> journal;
    private final ArrayList<SymbolIndexProxy<T>> indexProxies = new ArrayList<>();
    private final Interval interval;
    private final int columnCount;
    SymbolIndexProxy<T> sparseIndexProxies[];
    AbstractColumn[] columns;
    private int partitionIndex;
    private File partitionDir;
    private long lastAccessed = System.currentTimeMillis();
    private long txLimit;
    private FixedColumn timestampColumn;
    private BinarySearch.LongTimeSeriesProvider indexOfVisitor;

    Partition(Journal<T> journal, Interval interval, int partitionIndex, long txLimit, long[] indexTxAddresses) {
        this.journal = journal;
        this.partitionIndex = partitionIndex;
        this.interval = interval;
        this.txLimit = txLimit;
        this.columnCount = journal.getMetadata().getColumnCount();
        setPartitionDir(new File(this.journal.getLocation(), interval.getDirName(journal.getMetadata().getPartitionType())), indexTxAddresses);
    }

    public Partition<T> open() throws JournalException {
        access();
        if (columns == null) {

            columns = new AbstractColumn[journal.getMetadata().getColumnCount()];

            for (int i = 0; i < columns.length; i++) {
                open(i);
            }

            int tsIndex = journal.getMetadata().getTimestampColumnIndex();
            if (tsIndex >= 0) {
                timestampColumn = getFixedWidthColumn(tsIndex);
            }

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
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null) {
                    columns[i].close();
                }
            }
            columns = null;
            LOGGER.trace("Partition %s closed", partitionDir);
        }

        for (int i = 0, sz = indexProxies.size(); i < sz; i++) {
            indexProxies.get(i).close();
        }
    }

    public boolean isOpen() {
        return columns != null;
    }

    public String getStr(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);

        return ((VariableColumn) columns[columnIndex]).getStr(localRowID);
    }

    public void getBin(long localRowID, int columnIndex, OutputStream s) {
        checkColumnIndex(columnIndex);
        ((VariableColumn) columns[columnIndex]).getBin(localRowID, s);
    }

    public InputStream getBin(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);
        return ((VariableColumn) columns[columnIndex]).getBin(localRowID);
    }

    public String getSym(long localRowID, int columnIndex) {
        checkColumnIndex(columnIndex);
        int symbolIndex = ((FixedColumn) columns[columnIndex]).getInt(localRowID);
        switch (symbolIndex) {
            case SymbolTable.VALUE_IS_NULL:
            case SymbolTable.VALUE_NOT_FOUND:
                return null;
            default:
                return journal.getColumnMetadata(columnIndex).symbolTable.value(symbolIndex);
        }
    }

    public long getLong(long localRowID, int columnIndex) {
        return getFixedWidthColumn(columnIndex).getLong(localRowID);
    }

    public short getShort(long localRowID, int columnIndex) {
        return getFixedWidthColumn(columnIndex).getShort(localRowID);
    }

    public int getInt(long localRowID, int columnIndex) {
        return getFixedWidthColumn(columnIndex).getInt(localRowID);
    }

    public double getDouble(long localRowID, int columnIndex) {
        return getFixedWidthColumn(columnIndex).getDouble(localRowID);
    }

    public boolean getBoolean(long localRowID, int columnIndex) {
        return getFixedWidthColumn(columnIndex).getBool(localRowID);
    }

    public AbstractColumn getAbstractColumn(int i) {
        checkColumnIndex(i);
        return columns[i];
    }

    public KVIndex getIndexForColumn(String columnName) throws JournalException {
        return getIndexForColumn(journal.getMetadata().getColumnIndex(columnName));
    }

    public KVIndex getIndexForColumn(final int columnIndex) throws JournalException {
        SymbolIndexProxy h = sparseIndexProxies[columnIndex];
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
        for (int i = 0; i < columnCount; i++) {
            Journal.ColumnMetadata m;
            if (journal.getInactiveColumns().get(i) || (m = journal.columnMetadata[i]).meta.offset == 0) {
                continue;
            }

            switch (m.meta.type) {
                case BOOLEAN:
                    Unsafe.getUnsafe().putBoolean(obj, m.meta.offset, ((FixedColumn) columns[i]).getBool(localRowID));
                    break;
                case BYTE:
                    Unsafe.getUnsafe().putByte(obj, m.meta.offset, ((FixedColumn) columns[i]).getByte(localRowID));
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(obj, m.meta.offset, ((FixedColumn) columns[i]).getDouble(localRowID));
                    break;
                case INT:
                    Unsafe.getUnsafe().putInt(obj, m.meta.offset, ((FixedColumn) columns[i]).getInt(localRowID));
                    break;
                case LONG:
                case DATE:
                    Unsafe.getUnsafe().putLong(obj, m.meta.offset, ((FixedColumn) columns[i]).getLong(localRowID));
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(obj, m.meta.offset, ((FixedColumn) columns[i]).getShort(localRowID));
                    break;
                case STRING:
                    Unsafe.getUnsafe().putObject(obj, m.meta.offset, ((VariableColumn) columns[i]).getStr(localRowID));
                    break;
                case SYMBOL:
                    Unsafe.getUnsafe().putObject(obj, m.meta.offset, m.symbolTable.value(((FixedColumn) columns[i]).getInt(localRowID)));
                    break;
                case BINARY:
                    readBin(localRowID, obj, i, m);

            }
        }
    }

    private void readBin(long localRowID, T obj, int i, Journal.ColumnMetadata m) {
        int size = ((VariableColumn) columns[i]).getBinSize(localRowID);
        ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, m.meta.offset);
        if (size == -1) {
            if (buf != null) {
                buf.clear();
            }
        } else {
            if (buf == null || buf.capacity() < size) {
                buf = ByteBuffer.allocate(size);
                Unsafe.getUnsafe().putObject(obj, m.meta.offset, buf);
            }

            if (buf.remaining() < size) {
                buf.rewind();
            }
            buf.limit(size);
            ((VariableColumn) columns[i]).getBin(localRowID, buf);
            buf.flip();
        }
    }

    public void commitColumns() {
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
        return indexOf(timestamp, type, 0, size() - 1);
    }

    public long indexOf(long timestamp, BinarySearch.SearchType type, long lo, long hi) {
        if (indexOfVisitor == null) {
            throw new JournalRuntimeException("There is no timestamp column in: " + this);
        }
        return BinarySearch.indexOf(indexOfVisitor, timestamp, type, lo, hi);
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
     * Rebuild the index of a column using the default keyCountHint and recordCountHint values.
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
        return iterator(0, size() - 1);
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

        for (int i1 = 0; i1 < columns.length; i1++) {
            if (columns[i1] != null) {
                columns[i1].compact();
            }
        }

        for (int i = 0, sz = indexProxies.size(); i < sz; i++) {
            indexProxies.get(i).getIndex().compact();
        }
    }

    // TODO: rethink visibility
    public void updateIndexes(long oldSize, long newSize) {
        if (oldSize < newSize) {
            try {
                for (int i1 = 0, sz = indexProxies.size(); i1 < sz; i1++) {
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

    void append(Iterator<T> it) throws JournalException {
        while (it.hasNext()) {
            append(it.next());
        }
    }

    void append(T obj) throws JournalException {

        try {
            for (int i = 0; i < columnCount; i++) {
                Journal.ColumnMetadata meta = journal.getColumnMetadata(i);

                switch (meta.meta.type) {
                    case INT:
                        int v = Unsafe.getUnsafe().getInt(obj, meta.meta.offset);
                        if (meta.meta.indexed) {
                            sparseIndexProxies[i].getIndex().add(v % meta.meta.distinctCountHint, ((FixedColumn) columns[i]).putInt(v));
                        } else {
                            ((FixedColumn) columns[i]).putInt(v);
                        }
                        break;
                    case STRING:
                        String s = (String) Unsafe.getUnsafe().getObject(obj, meta.meta.offset);
                        long offset = ((VariableColumn) columns[i]).putStr(s);
                        if (meta.meta.indexed) {
                            sparseIndexProxies[i].getIndex().add(
                                    s == null ? SymbolTable.VALUE_IS_NULL : Checksum.hash(s, meta.meta.distinctCountHint)
                                    , offset
                            );
                        }
                        break;
                    case SYMBOL:
                        int key;
                        String sym = (String) Unsafe.getUnsafe().getObject(obj, meta.meta.offset);
                        if (sym == null) {
                            key = SymbolTable.VALUE_IS_NULL;
                        } else {
                            key = meta.symbolTable.put(sym);
                        }
                        if (meta.meta.indexed) {
                            sparseIndexProxies[i].getIndex().add(key, ((FixedColumn) columns[i]).putInt(key));
                        } else {
                            ((FixedColumn) columns[i]).putInt(key);
                        }
                        break;
                    case BINARY:
                        appendBin(obj, i, meta);
                        break;
                    default:
                        ((FixedColumn) columns[i]).copy(obj, meta.meta.offset);
                        break;
                }

                columns[i].commit();
            }

            applyTx(Journal.TX_LIMIT_EVAL, null);
        } catch (Throwable e) {
            ((JournalWriter) this.journal).rollback();
            throw e;
        }
    }

    private void appendBin(T obj, int i, Journal.ColumnMetadata meta) {
        ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, meta.meta.offset);
        if (buf == null || buf.remaining() == 0) {
            ((VariableColumn) columns[i]).putNull();
        } else {
            ((VariableColumn) columns[i]).putBin(buf);
        }
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

        ColumnMetadata m = journal.getMetadata().getColumnMetadata(columnIndex);
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
        this.lastAccessed = getJournal().getTimerCache().getCachedMillis();
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
            for (int i = 0, sz = indexProxies.size(); i < sz; i++) {
                SymbolIndexProxy<T> proxy = indexProxies.get(i);
                proxy.getIndex().truncate(newSize);
            }
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null) {
                    columns[i].truncate(newSize);
                }
            }

            commitColumns();
            clearTx();
        }
    }

    void expireOpenIndices() {
        long expiry = System.currentTimeMillis() - journal.getMetadata().getOpenFileTTL();
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
        }
    }

    @SuppressWarnings("unchecked")
    private void createSymbolIndexProxies(long[] indexTxAddresses) {
        indexProxies.clear();
        if (sparseIndexProxies == null || sparseIndexProxies.length != columnCount) {
            sparseIndexProxies = new SymbolIndexProxy[columnCount];
        }

        for (int i = 0; i < columnCount; i++) {
            if (journal.metadata.getColumnMetadata(i).indexed) {
                SymbolIndexProxy<T> proxy = new SymbolIndexProxy<>(this, i, indexTxAddresses == null ? 0 : indexTxAddresses[i]);
                indexProxies.add(proxy);
                sparseIndexProxies[i] = proxy;
            } else {
                sparseIndexProxies[i] = null;
            }
        }
    }
}
