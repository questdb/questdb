/*+*****************************************************************************
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

package io.questdb.test.griffin.fuzz;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnTypes;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;

/**
 * Builds a random WAL table and populates it with random rows whose
 * timestamps step forward in time so that a configurable chunk of rows
 * spans multiple DAY partitions.
 * <p>
 * Every table carries at least one SYMBOL column named {@code sym} so a
 * join fuzzer has a predictable key to target. The designated timestamp
 * column is always the last column and is named {@code ts}.
 */
public final class FuzzTableFactory {
    private static final String JOIN_KEY_COLUMN = "sym";
    private static final String TS_COLUMN = "ts";

    private final FuzzConfig config;

    public FuzzTableFactory(FuzzConfig config) {
        this.config = config;
    }

    public FuzzTable create(Rnd rnd, String tableName, SqlExecutor executor) throws SqlException {
        ObjList<FuzzColumn> columns = buildColumnList(rnd);

        StringSink ddl = new StringSink();
        ddl.put("CREATE TABLE ").put(tableName).put(" (");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                ddl.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            ddl.put(c.getName()).put(' ').put(c.getType().getDdl());
        }
        ddl.put(") TIMESTAMP(").put(TS_COLUMN).put(") PARTITION BY DAY WAL");
        executor.execute(ddl.toString());

        StringSink dml = new StringSink();
        dml.put("INSERT INTO ").put(tableName).put(" SELECT ");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                dml.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            // The ts column uses a sequence instead of the type's rnd_call so
            // rows are monotonically ordered and spread across partitions.
            if (c.getName().equals(TS_COLUMN)) {
                dml.put("timestamp_sequence(to_timestamp('").put(config.getTsStart())
                        .put("', 'yyyy-MM-dd'), ").put(config.getStepMicros()).put("L)");
            } else {
                dml.put(c.getType().getRndCall());
            }
            dml.put(' ').put(c.getName());
        }
        dml.put(" FROM long_sequence(").put(config.getRowsPerTable()).put(')');
        executor.execute(dml.toString());

        return new FuzzTable(tableName, columns, TS_COLUMN);
    }

    private ObjList<FuzzColumn> buildColumnList(Rnd rnd) {
        int numExtra = config.getMinColumnsPerTable()
                + rnd.nextInt(config.getMaxColumnsPerTable() - config.getMinColumnsPerTable() + 1);
        ObjList<FuzzColumn> columns = new ObjList<>();

        // Shared join key. Always SYMBOL so ASOF/LT/SPLICE on (sym) has a target.
        columns.add(new FuzzColumn(JOIN_KEY_COLUMN, SymbolType.INSTANCE));

        for (int i = 0; i < numExtra; i++) {
            FuzzColumnType type = FuzzColumnTypes.pickRandom(rnd);
            columns.add(new FuzzColumn("c" + i, type));
        }

        // Designated timestamp last.
        columns.add(new FuzzColumn(TS_COLUMN, TimestampType.INSTANCE));
        return columns;
    }

    @FunctionalInterface
    public interface SqlExecutor {
        void execute(String sql) throws SqlException;
    }
}
