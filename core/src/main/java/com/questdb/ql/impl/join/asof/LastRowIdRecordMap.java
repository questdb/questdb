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

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import com.questdb.std.IntList;
import com.questdb.store.ColumnType;

public class LastRowIdRecordMap implements LastRecordMap {
    private static final IntList valueMetadata = new IntList();
    private final DirectMap map;
    private final IntHashSet slaveKeyIndexes;
    private final IntHashSet masterKeyIndexes;
    private final IntList slaveKeyTypes;
    private final IntList masterKeyTypes;
    private final RecordMetadata metadata;
    private final Record passThruRecord;
    private RecordCursor slaveCursor;

    public LastRowIdRecordMap(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns,
            int pageSize,
            Record passThruRecord) {
        final int ksz = masterKeyColumns.size();
        this.masterKeyTypes = new IntList(ksz);
        this.slaveKeyTypes = new IntList(ksz);
        this.masterKeyIndexes = new IntHashSet(ksz);
        this.slaveKeyIndexes = new IntHashSet(ksz);
        this.passThruRecord = passThruRecord;

        for (int i = 0; i < ksz; i++) {
            int idx;
            idx = masterMetadata.getColumnIndex(masterKeyColumns.get(i));
            masterKeyTypes.add(masterMetadata.getColumnQuick(idx).getType());
            masterKeyIndexes.add(idx);

            idx = slaveMetadata.getColumnIndex(slaveKeyColumns.get(i));
            slaveKeyIndexes.add(idx);
            slaveKeyTypes.add(slaveMetadata.getColumnQuick(idx).getType());
        }
        this.map = new DirectMap(pageSize, ksz, valueMetadata);
        this.metadata = slaveMetadata;
    }

    @Override
    public void close() {
        map.close();
    }

    public Record get(Record master) {
        DirectMapValues values = getByMaster(master);
        if (values == null || values.get(1) == 1) {
            return null;
        }
        values.putByte(1, (byte) 1);
        return slaveCursor.recordAt(values.getLong(0));
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public Record getRecord() {
        return passThruRecord;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return slaveCursor.getStorageFacade();
    }

    public void put(Record record) {
        DirectMapValues values = getBySlave(record);
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

    private DirectMapValues getByMaster(Record record) {
        return map.getValues(RecordUtils.createKey(map, record, masterKeyIndexes, masterKeyTypes));
    }

    private DirectMapValues getBySlave(Record record) {
        return map.getOrCreateValues(RecordUtils.createKey(map, record, slaveKeyIndexes, slaveKeyTypes));
    }

    static {
        valueMetadata.add(ColumnType.LONG);
        valueMetadata.add(ColumnType.BYTE);
    }
}
