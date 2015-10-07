/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.collections;

import com.nfsdb.collections.CharSequenceHashSet;
import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.IntHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.impl.SelectedColumnsMetadata;
import com.nfsdb.ql.impl.SelectedColumnsRecord;
import com.nfsdb.storage.ColumnType;

public class LastRowIdRecordMap implements LastRecordMap {
    private static final ObjList<RecordColumnMetadata> valueMetadata = new ObjList<>();
    private final static CharSequenceObjHashMap<String> EMPTY_MAP = new CharSequenceObjHashMap<>();
    private final MultiMap map;
    private final IntHashSet slaveKeyIndexes;
    private final IntHashSet masterKeyIndexes;
    private final ObjList<ColumnType> slaveKeyTypes;
    private final ObjList<ColumnType> masterKeyTypes;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private RecordCursor<? extends Record> slaveCursor;

    // todo: extract config
    public LastRowIdRecordMap(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns
    ) {
        final int ksz = masterKeyColumns.size();
        this.masterKeyTypes = new ObjList<>(ksz);
        this.slaveKeyTypes = new ObjList<>(ksz);
        this.masterKeyIndexes = new IntHashSet(ksz);
        this.slaveKeyIndexes = new IntHashSet(ksz);

        // collect key field indexes for slave
        ObjList<RecordColumnMetadata> keyCols = new ObjList<>(ksz);

        for (int i = 0; i < ksz; i++) {
            int idx;
            idx = masterMetadata.getColumnIndex(masterKeyColumns.get(i));
            masterKeyTypes.add(masterMetadata.getColumn(idx).getType());
            masterKeyIndexes.add(idx);

            idx = slaveMetadata.getColumnIndex(slaveKeyColumns.get(i));
            slaveKeyIndexes.add(idx);
            slaveKeyTypes.add(slaveMetadata.getColumn(idx).getType());
            keyCols.add(slaveMetadata.getColumn(idx));
        }

        ObjList<CharSequence> slaveColumnNames = new ObjList<>();
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            if (slaveKeyIndexes.contains(i)) {
                continue;
            }
            slaveColumnNames.add(slaveMetadata.getColumn(i).getName());
        }

        this.map = new MultiMap(valueMetadata, keyCols, null);
        this.metadata = new SelectedColumnsMetadata(slaveMetadata, slaveColumnNames, EMPTY_MAP);
        this.record = new SelectedColumnsRecord(slaveMetadata, slaveColumnNames);
    }

    @Override
    public void close() {
        map.close();
    }

    public Record get(Record master) {
        MapValues values = getByMaster(master);
        if (values == null || values.getByte(1) == 1) {
            return null;
        }
        values.putByte(1, (byte) 1);
        return record.of(slaveCursor.getByRowId(values.getLong(0)));
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public void put(Record record) {
        MapValues values = getBySlave(record);
        values.putLong(0, record.getRowId());
        values.putByte(1, (byte) 0);
    }

    @Override
    public void reset() {
        map.clear();
    }

    public void setSlaveCursor(RecordCursor<? extends Record> cursor) {
        this.slaveCursor = cursor;
    }

    private static MultiMap.KeyWriter get(MultiMap map, Record record, IntHashSet indices, ObjList<ColumnType> types) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            int idx = indices.get(i);
            switch (types.getQuick(i)) {
                case INT:
                    kw.putInt(record.getInt(idx));
                    break;
                case LONG:
                    kw.putLong(record.getLong(idx));
                    break;
                case FLOAT:
                    kw.putFloat(record.getFloat(idx));
                    break;
                case DOUBLE:
                    kw.putDouble(record.getDouble(idx));
                    break;
                case BOOLEAN:
                    kw.putBoolean(record.getBool(idx));
                    break;
                case BYTE:
                    kw.putByte(record.get(idx));
                    break;
                case SHORT:
                    kw.putShort(record.getShort(idx));
                    break;
                case DATE:
                    kw.putLong(record.getDate(idx));
                    break;
                case STRING:
                    kw.putStr(record.getFlyweightStr(idx));
                    break;
                case SYMBOL:
                    // this is key field
                    // we have to write out string rather than int
                    // because master int values for same strings can be different
                    kw.putStr(record.getSym(idx));
                    break;
            }
        }
        return kw;
    }

    private MapValues getByMaster(Record record) {
        return map.getValues(get(map, record, masterKeyIndexes, masterKeyTypes));
    }

    private MapValues getBySlave(Record record) {
        return map.getOrCreateValues(get(map, record, slaveKeyIndexes, slaveKeyTypes));
    }

    static {
        valueMetadata.add(LongMetadata.INSTANCE);
        valueMetadata.add(ByteMetadata.INSTANCE);
    }
}
