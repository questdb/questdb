/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.join.asof;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.ByteMetadata;
import com.questdb.ql.impl.LongMetadata;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

public class LastRowIdRecordMap implements LastRecordMap {
    private static final ObjList<RecordColumnMetadata> valueMetadata = new ObjList<>();
    private final MultiMap map;
    private final IntHashSet slaveKeyIndexes;
    private final IntHashSet masterKeyIndexes;
    private final ObjList<ColumnType> slaveKeyTypes;
    private final ObjList<ColumnType> masterKeyTypes;
    private final RecordMetadata metadata;
    private RecordCursor slaveCursor;

    public LastRowIdRecordMap(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns,
            int pageSize) {
        final int ksz = masterKeyColumns.size();
        this.masterKeyTypes = new ObjList<>(ksz);
        this.slaveKeyTypes = new ObjList<>(ksz);
        this.masterKeyIndexes = new IntHashSet(ksz);
        this.slaveKeyIndexes = new IntHashSet(ksz);

        // collect key field indexes for slave
        ObjHashSet<String> keyCols = new ObjHashSet<>(ksz);

        for (int i = 0; i < ksz; i++) {
            int idx;
            idx = masterMetadata.getColumnIndex(masterKeyColumns.get(i));
            masterKeyTypes.add(masterMetadata.getColumnQuick(idx).getType());
            masterKeyIndexes.add(idx);

            idx = slaveMetadata.getColumnIndex(slaveKeyColumns.get(i));
            slaveKeyIndexes.add(idx);
            slaveKeyTypes.add(slaveMetadata.getColumnQuick(idx).getType());
            keyCols.add(slaveMetadata.getColumnName(idx));
        }
        this.map = new MultiMap(pageSize, slaveMetadata, keyCols, valueMetadata, null);
        this.metadata = slaveMetadata;
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
        return slaveCursor.recordAt(values.getLong(0));
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return slaveCursor.getStorageFacade();
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

    public void setSlaveCursor(RecordCursor cursor) {
        this.slaveCursor = cursor;
    }

    public SymbolTable getSymbolTable(String name) {
        return getStorageFacade().getSymbolTable(name);
    }

    private MapValues getByMaster(Record record) {
        return map.getValues(RecordUtils.createKey(map, record, masterKeyIndexes, masterKeyTypes));
    }

    private MapValues getBySlave(Record record) {
        return map.getOrCreateValues(RecordUtils.createKey(map, record, slaveKeyIndexes, slaveKeyTypes));
    }

    static {
        valueMetadata.add(LongMetadata.INSTANCE);
        valueMetadata.add(ByteMetadata.INSTANCE);
    }
}
