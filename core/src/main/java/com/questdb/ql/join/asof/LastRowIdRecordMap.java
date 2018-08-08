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

package com.questdb.ql.join.asof;

import com.questdb.ql.map.*;
import com.questdb.std.Transient;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;

public class LastRowIdRecordMap implements LastRecordMap {
    private final DirectMap map;
    private final RecordKeyCopier masterCopier;
    private final RecordKeyCopier slaveCopier;
    private final RecordMetadata metadata;
    private final Record passThruRecord;
    private RecordCursor slaveCursor;

    public LastRowIdRecordMap(
            @Transient ColumnTypeResolver masterResolver,
            RecordMetadata slaveMetadata,
            RecordKeyCopier masterCopier,
            RecordKeyCopier slaveCopier,
            int pageSize,
            Record passThruRecord) {
        this.masterCopier = masterCopier;
        this.slaveCopier = slaveCopier;
        this.passThruRecord = passThruRecord;
        this.map = new DirectMap(pageSize, masterResolver, LongByteResolver.INSTANCE);
        this.metadata = slaveMetadata;
    }

    @Override
    public void close() {
        map.close();
    }

    public Record get(Record master) {
        DirectMapValues values = getByMaster(master);
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
        map.locate(masterCopier, record);
        return map.getValues();
    }

    private DirectMapValues getBySlave(Record record) {
        map.locate(slaveCopier, record);
        return map.getOrCreateValues();
    }
}
