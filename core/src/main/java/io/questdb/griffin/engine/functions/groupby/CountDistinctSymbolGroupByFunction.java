/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;

import static io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;

public class CountDistinctSymbolGroupByFunction extends AbstractCountDistinctIntGroupByFunction {
    private int knownSymbolCount = -1;

    public CountDistinctSymbolGroupByFunction(Function arg, int setInitialCapacity, double setLoadFactor) {
        super(
                arg,
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, VALUE_IS_NULL),
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, VALUE_IS_NULL)
        );
    }

    @Override
    public void clear() {
        super.clear();
        knownSymbolCount = -1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final int key = arg.getInt(record);
        if (key != VALUE_IS_NULL) {
            mapValue.putLong(valueIndex, 1);
            mapValue.putLong(valueIndex + 1, key);
            cardinality++;
        } else {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int key = arg.getInt(record);
        if (key != VALUE_IS_NULL) {
            final long cnt = mapValue.getLong(valueIndex);
            if (cnt == 0) {
                mapValue.putLong(valueIndex, 1);
                mapValue.putLong(valueIndex + 1, key);
                cardinality++;
            } else if (cnt == 1) { // inlined value
                final int keyB = (int) mapValue.getLong(valueIndex + 1);
                if (key != keyB) {
                    setA.of(0).add(key);
                    setA.add(keyB);
                    mapValue.putLong(valueIndex, 2);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                    cardinality++;
                }
            } else { // non-empty set
                final long ptr = mapValue.getLong(valueIndex + 1);
                final long index = setA.of(ptr).keyIndex(key);
                if (index >= 0) {
                    setA.addAt(index, key);
                    mapValue.putLong(valueIndex, cnt + 1);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                    cardinality++;
                }
            }
        }
    }

    @Override
    public boolean earlyExit(MapValue mapValue) {
        // Fast path for the case when we've reached total number of symbols.
        return knownSymbolCount != -1 && mapValue.getLong(valueIndex) == knownSymbolCount;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        knownSymbolCount = -1;
        if (arg instanceof SymbolColumn argCol) {
            final StaticSymbolTable symbolTable = argCol.getStaticSymbolTable();
            if (symbolTable != null) {
                knownSymbolCount = symbolTable.getSymbolCount();
            }
        }
    }

    @Override
    public boolean isEarlyExitSupported() {
        return true;
    }
}
