package com.questdb.ql.impl.analytic.prev;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;

import java.io.Closeable;
import java.io.IOException;

public class PrevRowNonPartAnalyticFunction extends AbstractPrevRowAnalyticFunction implements Closeable {
    private final long prevPtr;
    private boolean firstPass = true;

    public PrevRowNonPartAnalyticFunction(RecordMetadata parentMetadata, String columnName, String alias) {
        super(parentMetadata, columnName, alias);
        this.prevPtr = Unsafe.getUnsafe().allocateMemory(8);
    }

    @Override
    public void reset() {
        super.reset();
        firstPass = true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        Unsafe.getUnsafe().freeMemory(prevPtr);
    }

    @Override
    public void scroll(Record record) {
        if (firstPass) {
            nextNull = true;
            firstPass = false;
        } else {
            if (nextNull) {
                nextNull = false;
            }
            Unsafe.getUnsafe().putLong(bufPtr, Unsafe.getUnsafe().getLong(prevPtr));
        }

        switch (valueType) {
            case BOOLEAN:
                Unsafe.getUnsafe().putByte(prevPtr, (byte) (record.getBool(valueIndex) ? 1 : 0));
                break;
            case BYTE:
                Unsafe.getUnsafe().putByte(prevPtr, record.get(valueIndex));
                break;
            case DOUBLE:
                Unsafe.getUnsafe().putDouble(prevPtr, record.getDouble(valueIndex));
                break;
            case FLOAT:
                Unsafe.getUnsafe().putFloat(prevPtr, record.getFloat(valueIndex));
                break;
            case SYMBOL:
            case INT:
                Unsafe.getUnsafe().putInt(prevPtr, record.getInt(valueIndex));
                break;
            case LONG:
            case DATE:
                Unsafe.getUnsafe().putLong(prevPtr, record.getLong(valueIndex));
                break;
            case SHORT:
                Unsafe.getUnsafe().putShort(prevPtr, record.getShort(valueIndex));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + valueType);
        }
    }
}
