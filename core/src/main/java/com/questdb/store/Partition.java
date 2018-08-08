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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.std.time.Dates;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.query.iter.PartitionBufferedIterator;

import java.io.Closeable;
import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class Partition<T> implements Closeable {
    private static final Log LOG = LogFactory.getLog(Partition.class);
    private final Journal<T> journal;
    private final ObjList<SymbolIndexProxy<T>> indexProxies = new ObjList<>();
    private final Interval interval;
    private final int columnCount;
    private final ColumnMetadata[] columnMetadata;
    SymbolIndexProxy<T> sparseIndexProxies[];
    AbstractColumn[] columns;
    private int partitionIndex;
    private File partitionDir;
    private long lastAccessed = System.currentTimeMillis();
    private long txLimit;
    private FixedColumn timestampColumn;
    private boolean sequentialAccess;

    Partition(Journal<T> journal, Interval interval, int partitionIndex, long txLimit, long[] indexTxAddresses, boolean sequentialAccess) {
        JournalMetadata<T> meta = journal.getMetadata();
        this.journal = journal;
        this.partitionIndex = partitionIndex;
        this.interval = interval;
        this.txLimit = txLimit;
        this.columnCount = meta.getColumnCount();
        this.columnMetadata = new ColumnMetadata[columnCount];
        meta.copyColumnMetadata(columnMetadata);
        if (interval != null) {
            setPartitionDir(new File(this.journal.getLocation(), interval.getDirName(meta.getPartitionBy())), indexTxAddresses);
        }
        this.sequentialAccess = sequentialAccess;
    }

    public void applyTx(long txLimit, long[] indexTxAddresses) {
        if (this.txLimit != txLimit) {
            this.txLimit = txLimit;
            for (int i = 0, k = indexProxies.size(); i < k; i++) {
                SymbolIndexProxy<T> proxy = indexProxies.getQuick(i);
                proxy.setTxAddress(indexTxAddresses == null ? 0 : indexTxAddresses[proxy.getColumnIndex()]);
            }
        }
    }

    public PartitionBufferedIterator<T> bufferedIterator() {
        return new PartitionBufferedIterator<>(this, 0, size() - 1);
    }

    public PartitionBufferedIterator<T> bufferedIterator(long lo, long hi) {
        return new PartitionBufferedIterator<>(this, lo, hi);
    }

    public void close() {
        if (isOpen()) {
            for (int i = 0; i < columns.length; i++) {
                Misc.free(Unsafe.arrayGet(columns, i));
            }
            columns = null;
            LOG.debug().$("Partition").$(partitionDir).$(" is closed").$();
        }

        for (int i = 0, k = indexProxies.size(); i < k; i++) {
            Misc.free(indexProxies.getQuick(i));
        }
    }

    public void commitColumns() {
        // have to commit columns from first to last
        // this is because size of partition is calculated by size of
        // last column. If below loop is to break in the middle partition will assume smallest
        // column size.
        for (int i = 0; i < columnCount; i++) {
            AbstractColumn col = Unsafe.arrayGet(columns, i);
            if (col != null) {
                col.commit();
            }
        }
    }

    public void compact() throws JournalException {
        if (columns == null || columns.length == 0) {
            throw new JournalException("Cannot compact closed partition: %s", this);
        }

        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                columns[i].compact();
            }
        }

        for (int i = 0, k = indexProxies.size(); i < k; i++) {
            indexProxies.getQuick(i).getIndex().compact();
        }
    }

    public FixedColumn fixCol(int i) {
        return (FixedColumn) Unsafe.arrayGet(columns, i);
    }

    public AbstractColumn getAbstractColumn(int i) {
        return Unsafe.arrayGet(columns, i);
    }

    public void getBin(long localRowID, int columnIndex, OutputStream s) {
        varCol(columnIndex).getBin(localRowID, s);
    }

    public DirectInputStream getBin(long localRowID, int columnIndex) {
        return varCol(columnIndex).getBin(localRowID);
    }

    public long getBinLen(long localRowID, int columnIndex) {
        return varCol(columnIndex).getBinLen(localRowID);
    }

    public boolean getBool(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getBool(localRowID);
    }

    public byte getByte(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getByte(localRowID);
    }

    public double getDouble(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getDouble(localRowID);
    }

    public float getFloat(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getFloat(localRowID);
    }

    public CharSequence getFlyweightStr(long localRowID, int columnIndex) {
        return varCol(columnIndex).getFlyweightStr(localRowID);
    }

    public CharSequence getFlyweightStrB(long localRowID, int columnIndex) {
        return varCol(columnIndex).getFlyweightStrB(localRowID);
    }

    public KVIndex getIndexForColumn(String columnName) throws JournalException {
        return getIndexForColumn(journal.getMetadata().getColumnIndex(columnName));
    }

    public KVIndex getIndexForColumn(final int columnIndex) throws JournalException {
        SymbolIndexProxy h = sparseIndexProxies[columnIndex];
        if (h == null) {
            throw new JournalException("There is no index for column '%s' in %s", columnMetadata[columnIndex].name, this);
        }
        return h.getIndex();
    }

    public int getInt(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getInt(localRowID);
    }

    public Interval getInterval() {
        return interval;
    }

    public Journal<T> getJournal() {
        return journal;
    }

    public long getLastAccessed() {
        return lastAccessed;
    }

    public long getLong(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getLong(localRowID);
    }

    public String getName() {
        return partitionDir.getName();
    }

    public File getPartitionDir() {
        return partitionDir;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public short getShort(long localRowID, int columnIndex) {
        return fixCol(columnIndex).getShort(localRowID);
    }

    public String getStr(long localRowID, int columnIndex) {
        return varCol(columnIndex).getStr(localRowID);
    }

    public void getStr(long localRowID, int columnIndex, CharSink sink) {
        varCol(columnIndex).getStr(localRowID, sink);
    }

    public int getStrLen(long localRowID, int columnIndex) {
        return varCol(columnIndex).getStrLen(localRowID);
    }

    public CharSequence getSym(long localRowID, int columnIndex) {
        int symbolIndex = fixCol(columnIndex).getInt(localRowID);
        switch (symbolIndex) {
            case SymbolTable.VALUE_IS_NULL:
            case SymbolTable.VALUE_NOT_FOUND:
                return null;
            default:
                return columnMetadata[columnIndex].symbolTable.value(symbolIndex);
        }
    }

    public FixedColumn getTimestampColumn() {
        if (timestampColumn == null) {
            throw new JournalRuntimeException("There is no timestamp column in: " + this);
        }
        return timestampColumn;
    }

    public long indexOf(long timestamp, BSearchType type) {
        return getTimestampColumn().bsearchEdge(timestamp, type);
    }

    public long indexOf(long timestamp, BSearchType type, long lo, long hi) {
        return getTimestampColumn().bsearchEdge(timestamp, type, lo, hi);
    }

    public boolean isOpen() {
        return columns != null;
    }

    public Partition<T> open() throws JournalException {
        access();
        if (columns == null) {
            open0();
        }
        return this;
    }

    public T read(long localRowID) {
        T obj = journal.newObject();
        read(localRowID, obj);
        return obj;
    }

    public void read(long localRowID, T obj) {
        for (int i = 0; i < columnCount; i++) {
            ColumnMetadata m;
            if (journal.getInactiveColumns().get(i) || (m = Unsafe.arrayGet(columnMetadata, i)).offset == 0) {
                continue;
            }

            switch (m.type) {
                case ColumnType.BOOLEAN:
                    Unsafe.getUnsafe().putBoolean(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getBool(localRowID));
                    break;
                case ColumnType.BYTE:
                    Unsafe.getUnsafe().putByte(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getByte(localRowID));
                    break;
                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putDouble(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getDouble(localRowID));
                    break;
                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putFloat(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getFloat(localRowID));
                    break;
                case ColumnType.INT:
                    Unsafe.getUnsafe().putInt(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getInt(localRowID));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                    Unsafe.getUnsafe().putLong(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getLong(localRowID));
                    break;
                case ColumnType.SHORT:
                    Unsafe.getUnsafe().putShort(obj, m.offset, ((FixedColumn) Unsafe.arrayGet(columns, i)).getShort(localRowID));
                    break;
                case ColumnType.STRING:
                    Unsafe.getUnsafe().putObject(obj, m.offset, ((VariableColumn) Unsafe.arrayGet(columns, i)).getStr(localRowID));
                    break;
                case ColumnType.SYMBOL:
                    Unsafe.getUnsafe().putObject(obj, m.offset, m.symbolTable.value(((FixedColumn) Unsafe.arrayGet(columns, i)).getInt(localRowID)));
                    break;
                case ColumnType.BINARY:
                    readBin(localRowID, obj, i, m);
                    break;
                default:
                    break;
            }
        }
    }

    public void rebuildIndexes() throws JournalException {
        if (!isOpen()) {
            throw new JournalException("Cannot rebuild indexes in closed partition: %s", this);
        }
        for (int i = 0; i < columnCount; i++) {
            if (Unsafe.arrayGet(columnMetadata, i).indexed) {
                rebuildIndex(i);
            }
        }
    }

    public void setSequentialAccess(boolean sequentialAccess) {
        this.sequentialAccess = sequentialAccess;
        if (columns != null) {
            for (int i = 0, n = columns.length; i < n; i++) {
                AbstractColumn c = Unsafe.arrayGet(columns, i);
                if (c != null) {
                    c.setSequentialAccess(sequentialAccess);
                }
            }
        }

        for (int i = 0, n = indexProxies.size(); i < n; i++) {
            indexProxies.getQuick(i).setSequentialAccess(sequentialAccess);
        }
    }

    public long size() {
        if (!isOpen()) {
            throw new JournalRuntimeException("Closed partition: %s", this);
        }

        if (txLimit != Journal.TX_LIMIT_EVAL) {
            return txLimit;
        }

        long sz = 0;
        for (int i = columns.length - 1; i > -1; i--) {
            AbstractColumn c = Unsafe.arrayGet(columns, i);
            if (c != null) {
                sz = c.size();
                break;
            }
        }

        txLimit = sz;
        return sz;
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

    public void updateIndexes(long oldSize, long newSize) {
        if (oldSize < newSize) {
            try {
                for (int n = 0, k = indexProxies.size(); n < k; n++) {
                    SymbolIndexProxy<T> proxy = indexProxies.getQuick(n);
                    KVIndex index = proxy.getIndex();
                    FixedColumn col = fixCol(proxy.getColumnIndex());
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

    public VariableColumn varCol(int i) {
        return (VariableColumn) Unsafe.arrayGet(columns, i);
    }

    Partition<T> access() {
        switch (journal.getMetadata().getPartitionBy()) {
            case PartitionBy.NONE:
                return this;
            default:
                long t = System.currentTimeMillis();
                if (lastAccessed < t) {
                    lastAccessed = t;
                }
                return this;
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
                ColumnMetadata m = Unsafe.arrayGet(columnMetadata, i);
                switch (m.type) {
                    case ColumnType.LONG:
                        long l = Unsafe.getUnsafe().getLong(obj, m.offset);
                        if (m.indexed) {
                            sparseIndexProxies[i].getIndex().add(
                                    (int) (l & m.distinctCountHint), ((FixedColumn) Unsafe.arrayGet(columns, i)).putLong(l));
                        } else {
                            ((FixedColumn) Unsafe.arrayGet(columns, i)).putLong(l);
                        }
                        break;
                    case ColumnType.INT:
                        int v = Unsafe.getUnsafe().getInt(obj, m.offset);
                        if (m.indexed) {
                            sparseIndexProxies[i].getIndex().add(v & m.distinctCountHint, ((FixedColumn) Unsafe.arrayGet(columns, i)).putInt(v));
                        } else {
                            ((FixedColumn) Unsafe.arrayGet(columns, i)).putInt(v);
                        }
                        break;
                    case ColumnType.STRING:
                        String s = (String) Unsafe.getUnsafe().getObject(obj, m.offset);
                        long offset = ((VariableColumn) Unsafe.arrayGet(columns, i)).putStr(s);
                        if (m.indexed) {
                            sparseIndexProxies[i].getIndex().add(
                                    s == null ? SymbolTable.VALUE_IS_NULL : Hash.boundedHash(s, m.distinctCountHint)
                                    , offset
                            );
                        }
                        break;
                    case ColumnType.SYMBOL:
                        int key;
                        String sym = (String) Unsafe.getUnsafe().getObject(obj, m.offset);
                        if (sym == null) {
                            key = SymbolTable.VALUE_IS_NULL;
                        } else {
                            key = m.symbolTable.put(sym);
                        }
                        if (m.indexed) {
                            sparseIndexProxies[i].getIndex().add(key, ((FixedColumn) Unsafe.arrayGet(columns, i)).putInt(key));
                        } else {
                            ((FixedColumn) Unsafe.arrayGet(columns, i)).putInt(key);
                        }
                        break;
                    case ColumnType.BINARY:
                        appendBin(obj, i, m);
                        break;
                    default:
                        ((FixedColumn) Unsafe.arrayGet(columns, i)).copy(obj, m.offset);
                        break;
                }

                Unsafe.arrayGet(columns, i).commit();
            }

            applyTx(Journal.TX_LIMIT_EVAL, null);
        } catch (Throwable e) {
            ((JournalWriter) this.journal).rollback();
            throw e;
        }
    }

    private void appendBin(T obj, int i, ColumnMetadata meta) {
        ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, meta.offset);
        if (buf == null) {
            ((VariableColumn) Unsafe.arrayGet(columns, i)).putNull();
        } else {
            ((VariableColumn) Unsafe.arrayGet(columns, i)).putBin(buf);
        }
    }

    private void clearTx() {
        applyTx(Journal.TX_LIMIT_EVAL, null);
    }

    private void closePartiallyOpenColumns() {
        for (int i = 0, n = columns.length; i < n; i++) {
            AbstractColumn c = columns[i];
            if (c != null) {
                c.close();
                columns[i] = null;
            }
        }
    }

    void commit() throws JournalException {
        for (int i = 0, k = indexProxies.size(); i < k; i++) {
            indexProxies.getQuick(i).getIndex().commit();
        }
    }

    @SuppressWarnings("unchecked")
    private void createSymbolIndexProxies(long[] indexTxAddresses) {
        indexProxies.clear();
        if (sparseIndexProxies == null || sparseIndexProxies.length != columnCount) {
            sparseIndexProxies = new SymbolIndexProxy[columnCount];
        }

        for (int i = 0; i < columnCount; i++) {
            if (Unsafe.arrayGet(columnMetadata, i).indexed) {
                SymbolIndexProxy<T> proxy = new SymbolIndexProxy<>(this, i, indexTxAddresses == null ? 0 : indexTxAddresses[i]);
                indexProxies.add(proxy);
                sparseIndexProxies[i] = proxy;
            } else {
                sparseIndexProxies[i] = null;
            }
        }
    }

    void force() throws JournalException {
        for (int i = 0, k = indexProxies.size(); i < k; i++) {
            indexProxies.getQuick(i).getIndex().force();
        }

        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                AbstractColumn column = Unsafe.arrayGet(columns, i);
                if (column != null) {
                    column.force();
                }
            }
        }
    }

    void getIndexPointers(long[] pointers) throws JournalException {
        for (int i = 0, k = indexProxies.size(); i < k; i++) {
            SymbolIndexProxy<T> proxy = indexProxies.getQuick(i);
            pointers[proxy.getColumnIndex()] = proxy.getIndex().getTxAddress();
        }
    }

    private void open0() throws JournalException {
        columns = new AbstractColumn[journal.getMetadata().getColumnCount()];

        try {
            for (int i = 0; i < columns.length; i++) {
                switch (Unsafe.arrayGet(columnMetadata, i).type) {
                    case ColumnType.STRING:
                    case ColumnType.BINARY:
                        Unsafe.arrayPut(columns, i,
                                new VariableColumn(
                                        new MemoryFile(
                                                new File(partitionDir, Unsafe.arrayGet(columnMetadata, i).name + ".d"),
                                                Unsafe.arrayGet(columnMetadata, i).bitHint, journal.getMode(), sequentialAccess
                                        ),
                                        new MemoryFile(
                                                new File(partitionDir, Unsafe.arrayGet(columnMetadata, i).name + ".i"),
                                                Unsafe.arrayGet(columnMetadata, i).indexBitHint,
                                                journal.getMode(), sequentialAccess
                                        )
                                )
                        );
                        break;
                    default:
                        Unsafe.arrayPut(columns, i,
                                new FixedColumn(
                                        new MemoryFile(
                                                new File(partitionDir, Unsafe.arrayGet(columnMetadata, i).name + ".d"),
                                                Unsafe.arrayGet(columnMetadata, i).bitHint,
                                                journal.getMode(), sequentialAccess
                                        ),
                                        Unsafe.arrayGet(columnMetadata, i).size
                                )
                        );
                        break;
                }
            }
        } catch (JournalException e) {
            closePartiallyOpenColumns();
            throw e;
        }

        int tsIndex = journal.getMetadata().getTimestampIndex();
        if (tsIndex > -1) {
            timestampColumn = fixCol(tsIndex);
        }
    }

    private void readBin(long localRowID, T obj, int i, ColumnMetadata m) {
        int size = ((VariableColumn) Unsafe.arrayGet(columns, i)).getBinLen(localRowID);
        ByteBuffer buf = (ByteBuffer) Unsafe.getUnsafe().getObject(obj, m.offset);
        if (size == -1) {
            if (buf != null) {
                buf.clear();
            }
        } else {
            if (buf == null || buf.capacity() < size) {
                buf = ByteBuffer.allocate(size);
                Unsafe.getUnsafe().putObject(obj, m.offset, buf);
            }

            if (buf.remaining() < size) {
                buf.rewind();
            }
            buf.limit(size);
            ((VariableColumn) Unsafe.arrayGet(columns, i)).getBin(localRowID, buf);
            buf.flip();
        }
    }

    /**
     * Rebuild the index of a column using the default keyCountHint and recordCountHint values.
     *
     * @param columnIndex the column index
     * @throws JournalException if the operation fails
     */
    private void rebuildIndex(int columnIndex) throws JournalException {
        JournalMetadata<T> meta = journal.getMetadata();
        rebuildIndex(columnIndex,
                columnMetadata[columnIndex].distinctCountHint,
                meta.getRecordHint(),
                meta.getTxCountHint());
    }

    /**
     * Rebuild the index of a column.
     *
     * @param columnIndex     the column index
     * @param keyCountHint    the key count hint override
     * @param recordCountHint the record count hint override
     * @throws JournalException if the operation fails
     */
    private void rebuildIndex(int columnIndex, int keyCountHint, int recordCountHint, int txCountHint) throws JournalException {
        final long time = System.nanoTime();

        getIndexForColumn(columnIndex).close();

        File base = new File(partitionDir, columnMetadata[columnIndex].name);
        KVIndex.delete(base);

        try (KVIndex index = new KVIndex(base, keyCountHint, recordCountHint, txCountHint, JournalMode.APPEND, 0, sequentialAccess)) {
            FixedColumn col = fixCol(columnIndex);
            for (long localRowID = 0, sz = size(); localRowID < sz; localRowID++) {
                index.add(col.getInt(localRowID), localRowID);
            }
            index.commit();
        }

        LOG.debug().$("REBUILT ").$(base).$(" in ").$(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time)).$("ms").$();
    }

    final void setPartitionDir(File partitionDir, long[] indexTxAddresses) {
        boolean create = partitionDir != null && !partitionDir.equals(this.partitionDir);
        this.partitionDir = partitionDir;
        if (create) {
            createSymbolIndexProxies(indexTxAddresses);
        }
    }

    void truncate(long newSize) throws JournalException {
        if (isOpen() && size() > newSize) {
            for (int i = 0, k = indexProxies.size(); i < k; i++) {
                indexProxies.getQuick(i).getIndex().truncate(newSize);
            }
            for (int i = 0; i < columns.length; i++) {
                if (Unsafe.arrayGet(columns, i) != null) {
                    Unsafe.arrayGet(columns, i).truncate(newSize);
                }
            }

            commitColumns();
            clearTx();
        }
    }
}
