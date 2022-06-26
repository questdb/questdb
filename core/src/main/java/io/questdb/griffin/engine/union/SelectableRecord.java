/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public final class SelectableRecord implements Record {
    private RecordComparator comparator;
    private Record recordA;
    private Record recordB;
    private Record selectedRecord;

    public void of(Record recordA, Record recordB, RecordComparator comparator) {
        this.recordA = recordA;
        this.recordB = recordB;
        this.comparator = comparator;
    }

    @Override
    public BinarySequence getBin(int col) {
        return selectedRecord.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        return selectedRecord.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        return selectedRecord.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return selectedRecord.getByte(col);
    }

    @Override
    public char getChar(int col) {
        return selectedRecord.getChar(col);
    }

    @Override
    public long getDate(int col) {
        return selectedRecord.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        return selectedRecord.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return selectedRecord.getFloat(col);
    }

    @Override
    public int getInt(int col) {
        return selectedRecord.getInt(col);
    }

    @Override
    public long getLong(int col) {
        return selectedRecord.getLong(col);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        selectedRecord.getLong256(col, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return selectedRecord.getLong256A(col);
    }

    @Override
    public CharSequence getSym(int col) {
        // todo: check what to do with symbols. is this working? UnionRecord does not support symbols at all. for a reason?
        return selectedRecord.getSym(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        return selectedRecord.getLong256B(col);
    }

    @Override
    public short getShort(int col) {
        return selectedRecord.getShort(col);
    }

    @Override
    public CharSequence getStr(int col) {
        return selectedRecord.getStr(col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        selectedRecord.getStr(col, sink);
    }

    @Override
    public CharSequence getStrB(int col) {
        return selectedRecord.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        return selectedRecord.getStrLen(col);
    }

    @Override
    public long getTimestamp(int col) {
        return selectedRecord.getTimestamp(col);
    }

    @Override
    public byte getGeoByte(int col) {
        return selectedRecord.getGeoByte(col);
    }

    @Override
    public short getGeoShort(int col) {
        return selectedRecord.getGeoShort(col);
    }

    @Override
    public int getGeoInt(int col) {
        return selectedRecord.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return selectedRecord.getGeoLong(col);
    }

    /**
     * Select the next record by using a comparator
     * <br>
     *
     * @return true when selecting a record from cursorA, otherwise false
     */
    boolean selectByComparing() {
        boolean selectedA = (comparator.compare(recordB) <= 0);
        selectedRecord = selectedA ? recordA : recordB;
        return selectedA;
    }
    
    void selectA() {
        selectedRecord = recordA;
    }
    
    void selectB() {
        selectedRecord = recordB;
    }

    void resetComparatorLeft() {
        this.comparator.setLeft(recordA);
    }
}
