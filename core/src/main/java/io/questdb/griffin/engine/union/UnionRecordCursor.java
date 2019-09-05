/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.IntList;
import io.questdb.std.Misc;

class UnionRecordCursor implements NoRandomAccessRecordCursor {
    private final UnionRecord record = new UnionRecord();
    private final IntList masterColumnIndexMap;
    private final IntList slaveColumnIndexMap;
    private final Map map;
    private final RecordSink recordSink;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private Record masterRecord;
    private Record slaveRecord;
    private NextMethod nextMethod;

    public UnionRecordCursor(
            Map map,
            IntList masterColumnIndexMap,
            IntList slaveColumnIndexMap,
            RecordSink recordSink) {
        this.map = map;
        this.masterColumnIndexMap = masterColumnIndexMap;
        this.recordSink = recordSink;
        this.slaveColumnIndexMap = slaveColumnIndexMap;
    }

    void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        this.masterRecord = masterCursor.getRecord();
        this.slaveRecord = slaveCursor.getRecord();
        map.clear();
        toTop();
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        return nextMethod.next();
    }

    private boolean nextSlave() {

        while (true) {
            boolean next = slaveCursor.hasNext();
            if (next) {
                MapKey key = map.withKey();
                key.put(record, recordSink);
                if (key.create()) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    private boolean nextMaster() {
        if (masterCursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            return true;
        }
        record.of(slaveRecord, slaveColumnIndexMap);
        nextMethod = this::nextSlave;
        return nextMethod.next();
    }

    @Override
    public void toTop() {
        record.of(masterRecord, masterColumnIndexMap);
        nextMethod = this::nextMaster;
        masterCursor.toTop();
        slaveCursor.toTop();
    }

    interface NextMethod {
        boolean next();
    }
}
