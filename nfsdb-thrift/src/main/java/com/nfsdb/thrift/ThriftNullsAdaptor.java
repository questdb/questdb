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

package com.nfsdb.thrift;

import com.nfsdb.journal.collections.IntArrayList;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalUnsupportedTypeException;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.utils.Unsafe;
import org.apache.thrift.TBase;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.BitSet;

public class ThriftNullsAdaptor<T extends TBase> implements NullsAdaptor<T> {

    private final IntArrayList fieldMapping = new IntArrayList();
    private BitFieldType bitFieldType;
    private long bitFieldOffset;

    public ThriftNullsAdaptor(Class<T> modelClass) throws JournalConfigurationException {

        int fieldCount = 0;
        Field[] classFields = modelClass.getDeclaredFields();
        for (Field f : classFields) {

            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            if ("__isset_bitfield".equals(f.getName())) {
                Class type = f.getType();
                if (type == byte.class) {
                    bitFieldType = BitFieldType.BYTE;
                } else if (type == short.class) {
                    bitFieldType = BitFieldType.SHORT;
                } else if (type == int.class) {
                    bitFieldType = BitFieldType.INT;
                } else if (type == long.class) {
                    bitFieldType = BitFieldType.LONG;
                } else if (type == BitSet.class) {
                    bitFieldType = BitFieldType.BIT_SET;
                } else {
                    throw new JournalConfigurationException("Unsupported bitfield type: " + type + ". Unsupported Thrift version?");
                }
                bitFieldOffset = Unsafe.getUnsafe().objectFieldOffset(f);
                continue;
            }

            if ("__isset_bit_vector".equals(f.getName())) {
                bitFieldType = BitFieldType.BIT_SET;
                bitFieldOffset = Unsafe.getUnsafe().objectFieldOffset(f);
                continue;
            }

            Class type = f.getType();
            for (ColumnType t : ColumnType.values()) {
                if (t.matches(type)) {
                    if (t.primitive()) {
                        fieldMapping.add(fieldCount);
                    }
                    fieldCount++;
                    break;
                }
            }
        }
    }

    public void setNulls(T obj, BitSet src) {
        switch (bitFieldType) {
            case BIT_SET:
                setNullsBitSet(obj, src);
                break;
            default:
                setNullsBitField(obj, src);
        }
    }

    public void getNulls(T obj, BitSet dst) {

        switch (bitFieldType) {
            case BIT_SET:
                getNullsBitSet(obj, dst);
                break;
            default:
                getNullsBitField(obj, dst);
        }
    }

    public void clear(T obj) {
        obj.clear();
    }

    private void setNullsBitField(T obj, BitSet src) {

        long bitField = 0;

        for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
            if (!src.get(fieldMapping.getQuick(i))) {
                // null?
                bitField = bitField | (1 << i);
            }
        }

        switch (bitFieldType) {
            case BYTE:
                Unsafe.getUnsafe().putByte(obj, bitFieldOffset, (byte) bitField);
                break;
            case SHORT:
                Unsafe.getUnsafe().putShort(obj, bitFieldOffset, (short) bitField);
                break;
            case INT:
                Unsafe.getUnsafe().putInt(obj, bitFieldOffset, (int) bitField);
                break;
            case LONG:
                Unsafe.getUnsafe().putLong(obj, bitFieldOffset, bitField);
                break;
            default:
                throw new JournalUnsupportedTypeException(bitFieldType);
        }

    }

    private void setNullsBitSet(T obj, BitSet src) {
        BitSet bitField = (BitSet) Unsafe.getUnsafe().getObject(obj, bitFieldOffset);

        for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
            if (!src.get(fieldMapping.getQuick(i))) {
                bitField.set(i);
            }
        }

        Unsafe.getUnsafe().putObject(obj, bitFieldOffset, bitField);
    }

    private void getNullsBitField(T obj, BitSet dst) {
        long bitField;
        switch (bitFieldType) {
            case BYTE:
                bitField = Unsafe.getUnsafe().getByte(obj, bitFieldOffset);
                break;
            case SHORT:
                bitField = Unsafe.getUnsafe().getShort(obj, bitFieldOffset);
                break;
            case INT:
                bitField = Unsafe.getUnsafe().getInt(obj, bitFieldOffset);
                break;
            case LONG:
                bitField = Unsafe.getUnsafe().getLong(obj, bitFieldOffset);
                break;
            default:
                throw new JournalUnsupportedTypeException(bitFieldType);
        }

        for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
            if ((bitField & (1 << i)) == 0) {
                dst.set(fieldMapping.getQuick(i));
            }
        }
    }

    private void getNullsBitSet(T obj, BitSet dst) {
        BitSet bitField = (BitSet) Unsafe.getUnsafe().getObject(obj, bitFieldOffset);

        for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
            if (!bitField.get(i)) {
                dst.set(fieldMapping.getQuick(i));
            }
        }
    }

    private enum BitFieldType {
        BYTE, SHORT, INT, LONG, BIT_SET
    }
}
