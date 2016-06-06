package com.questdb.ql.impl.analytic.prev;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.IntList;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class PrevStrAnalyticFunction extends AbstractPrevValueAnalyticFunction implements Closeable {
    private final MultiMap map;
    private final IntList indices;
    private final ObjList<ColumnType> types;

    public PrevStrAnalyticFunction(int pageSize, RecordMetadata parentMetadata, @Transient ObjHashSet<String> partitionBy, String columnName, String alias) {

        super(parentMetadata, columnName, alias);
        // value column particulars
        RecordColumnMetadata m = parentMetadata.getColumnQuick(this.valueIndex);
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
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        Misc.free(map);
    }

    @Override
    public void reset() {
        super.reset();
        map.clear();
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
