package com.questdb.ql.impl.analytic.prev;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.*;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class PrevRowAnalyticFunction implements AnalyticFunction, Closeable {
    private final MultiMap map;
    private final IntList indices;
    private final ObjList<ColumnType> types;
    private final ColumnType valueType;
    private final int valueIndex;
    private final RecordColumnMetadata valueMetadata;
    private final long bufPtr;
    private boolean nextNull = true;
    private StorageFacade storageFacade;
    private boolean closed = false;

    public PrevRowAnalyticFunction(int pageSize, RecordMetadata parentMetadata, @Transient ObjHashSet<String> partitionBy, String columnName) {

        // value column particulars
        this.valueIndex = parentMetadata.getColumnIndex(columnName);
        RecordColumnMetadata m = parentMetadata.getColumn(columnName);
        this.valueType = m.getType();
        ObjList<RecordColumnMetadata> valueColumns = new ObjList<>(1);
        valueColumns.add(m);

        this.map = new MultiMap(pageSize, parentMetadata, partitionBy, valueColumns, null);

        // key column particulars
        this.indices = new IntList(partitionBy.size());
        this.types = new ObjList<>(partitionBy.size());
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            int index = parentMetadata.getColumnIndexQuiet(partitionBy.get(i));
            indices.add(index);
            types.add(parentMetadata.getColumn(index).getType());
        }

        // buffer where "current" value is kept
        this.bufPtr = Unsafe.getUnsafe().allocateMemory(8);

        // metadata
        this.valueMetadata = new RecordColumnMetadataImpl(columnName, valueType);
    }

    @Override
    public void addRecord(Record record, long rowid) {

    }

    @Override
    public byte get() {
        return nextNull ? 0 : Unsafe.getUnsafe().getByte(bufPtr);
    }

    @Override
    public void getBin(OutputStream s) {

    }

    @Override
    public DirectInputStream getBin() {
        return null;
    }

    @Override
    public long getBinLen() {
        return 0;
    }

    @Override
    public boolean getBool() {
        return !nextNull && Unsafe.getUnsafe().getByte(bufPtr) == 1;
    }

    @Override
    public long getDate() {
        return getLong();
    }

    @Override
    public double getDouble() {
        return nextNull ? Double.NaN : Unsafe.getUnsafe().getDouble(bufPtr);
    }

    @Override
    public float getFloat() {
        return nextNull ? Float.NaN : Unsafe.getUnsafe().getFloat(bufPtr);
    }

    @Override
    public CharSequence getFlyweightStr() {
        return null;
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return null;
    }

    @Override
    public int getInt() {
        return nextNull ? Numbers.INT_NaN : Unsafe.getUnsafe().getInt(bufPtr);
    }

    @Override
    public long getLong() {
        return nextNull ? Numbers.LONG_NaN : Unsafe.getUnsafe().getLong(bufPtr);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueMetadata;
    }

    @Override
    public short getShort() {
        return nextNull ? 0 : Unsafe.getUnsafe().getShort(bufPtr);
    }

    @Override
    public void getStr(CharSink sink) {

    }

    @Override
    public CharSequence getStr() {
        return null;
    }

    @Override
    public int getStrLen() {
        return 0;
    }

    @Override
    public String getSym() {
        return nextNull ? null : storageFacade.getSymbolTable(valueIndex).value(getInt());
    }

    @Override
    public void prepare(RecordList base) {
    }

    @Override
    public void reset() {
        map.clear();
        nextNull = true;
    }

    @Override
    public void scroll(Record record) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = types.size(); i < n; i++) {
            kw.put(record, indices.getQuick(i), types.getQuick(i));
        }

        MapValues values = map.getOrCreateValues(kw);
        if (values.isNew()) {
            nextNull = true;
            store(record, values);
        } else {
            nextNull = false;
            switch (valueType) {
                case BOOLEAN:
                    Unsafe.getUnsafe().putByte(bufPtr, values.getByte(0));
                    values.putByte(0, (byte) (record.getBool(valueIndex) ? 1 : 0));
                    break;
                case BYTE:
                    Unsafe.getUnsafe().putByte(bufPtr, values.getByte(0));
                    values.putByte(0, record.get(valueIndex));
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(bufPtr, values.getDouble(0));
                    values.putDouble(0, record.getDouble(valueIndex));
                    break;
                case FLOAT:
                    Unsafe.getUnsafe().putFloat(bufPtr, values.getFloat(0));
                    values.putFloat(0, record.getFloat(valueIndex));
                    break;
                case SYMBOL:
                case INT:
                    Unsafe.getUnsafe().putInt(bufPtr, values.getInt(0));
                    values.putInt(0, record.getInt(valueIndex));
                    break;
                case LONG:
                case DATE:
                    Unsafe.getUnsafe().putLong(bufPtr, values.getLong(0));
                    values.putLong(0, record.getLong(valueIndex));
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(bufPtr, values.getShort(0));
                    values.putShort(0, record.getShort(valueIndex));
                    break;
                default:
                    throw new JournalRuntimeException("Unsupported type: " + valueType);
            }
        }
    }

    @Override
    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        Unsafe.getUnsafe().freeMemory(bufPtr);
        Misc.free(map);
        closed = true;
    }

    private void store(Record record, MapValues values) {
        switch (valueType) {
            case BOOLEAN:
                values.putByte(0, (byte) (record.getBool(valueIndex) ? 1 : 0));
                break;
            case BYTE:
                values.putByte(0, record.get(valueIndex));
                break;
            case DOUBLE:
                values.putDouble(0, record.getDouble(valueIndex));
                break;
            case FLOAT:
                values.putFloat(0, record.getFloat(valueIndex));
                break;
            case SYMBOL:
            case INT:
                values.putInt(0, record.getInt(valueIndex));
                break;
            case LONG:
            case DATE:
                values.putLong(0, record.getLong(valueIndex));
                break;
            case SHORT:
                values.putShort(0, record.getShort(valueIndex));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + valueType);
        }

    }
}
