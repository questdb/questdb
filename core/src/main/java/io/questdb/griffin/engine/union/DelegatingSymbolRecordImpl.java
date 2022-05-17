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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;

public class DelegatingSymbolRecordImpl extends DelegatingRecordImpl {
    private final ObjList<DelegateSymbolTable> delegateSymbolTables = new ObjList<>();
    private boolean atMaster;

    public DelegatingSymbolRecordImpl(RecordMetadata metadata) {
        int columnCount = metadata.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(columnIndex);
            if (columnType == ColumnType.SYMBOL) {
                delegateSymbolTables.add(new DelegateSymbolTable());
            } else {
                delegateSymbolTables.add(null);
            }
        }
    }

    @Override
    public int getInt(int col) {
        DelegateSymbolTable symbolTable = delegateSymbolTables.getQuick(col);
        int value = super.getInt(col);

        if (symbolTable == null) {
            return value;
        } else {
            if (atMaster) {
                return symbolTable.translateMaster(value);
            } else {
                return symbolTable.translateSlave(value);
            }
        }
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return delegateSymbolTables.getQuick(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void of(RecordCursor master, RecordCursor slave) {
        int columnCount = delegateSymbolTables.size();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            DelegateSymbolTable symbolTable = delegateSymbolTables.getQuick(columnIndex);
            if (symbolTable != null) {
                symbolTable.of(
                        master.getSymbolTable(columnIndex),
                        slave.getSymbolTable(columnIndex)
                );
            }
        }
    }

    @Override
    public void ofMaster(Record masterRecord) {
        super.ofMaster(masterRecord);
        atMaster = true;
    }

    @Override
    public void ofSlave(Record slaveRecord) {
        super.ofSlave(slaveRecord);
        atMaster = false;
    }

    @Override
    public void toTop() {
        for (int columnIndex = 0, n = delegateSymbolTables.size(); columnIndex < n; columnIndex++) {
            DelegateSymbolTable delegateSymbolTable = delegateSymbolTables.getQuick(columnIndex);
            if (delegateSymbolTable != null) {
                delegateSymbolTable.resetMasterMax();
            }
        }
    }

    private static class DelegateSymbolTable implements SymbolTable {
        private SymbolTable masterSymTbl;
        private SymbolTable slaveSymTbl;
        private int maxMaster = 0;

        public void resetMasterMax() {
            this.maxMaster = 0;
        }

        public int translateMaster(int key) {
            maxMaster = Math.max(maxMaster, key + 1);
            return key;
        }

        public int translateSlave(int key) {
            return key + maxMaster;
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key < maxMaster) {
                return masterSymTbl.valueOf(key);
            }
            return slaveSymTbl.valueOf(key - maxMaster);
        }

        @Override
        public CharSequence valueBOf(int key) {
            if (key < maxMaster) {
                return masterSymTbl.valueBOf(key);
            }
            return slaveSymTbl.valueBOf(key - maxMaster);
        }

        void of(SymbolTable masterSymTbl, SymbolTable slaveSymTbl) {
            this.masterSymTbl = masterSymTbl;
            this.slaveSymTbl = slaveSymTbl;
            this.maxMaster = 0;
        }
    }
}
