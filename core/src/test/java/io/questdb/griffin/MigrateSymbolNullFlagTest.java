/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.std.Rnd;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrateSymbolNullFlagTest extends AbstractGriffinTest {

    @Test
    public void testTableWithoutData() {
        String tableName = "test";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE).col("aaa", ColumnType.SYMBOL)
        ) {
            CairoTestUtils.createTableWithVersion(model, 404);
            assertFalse(engine.migrateNullFlag(sqlExecutionContext.getCairoSecurityContext(), tableName));
        }
    }

    @Test
    public void testTableWithoutNullSymbol() {
        String tableName = "test";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE).col("aaa", ColumnType.SYMBOL)
        ) {
            CairoTestUtils.createTableWithVersion(model, 404);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
                appendRows(rnd, writer);
                writer.commit();
            }
            assertFalse(engine.migrateNullFlag(sqlExecutionContext.getCairoSecurityContext(), tableName));
        }
    }

    @Test
    public void testTableWithNullSymbol() {
        String tableName = "test";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE).col("aaa", ColumnType.SYMBOL)
        ) {
            CairoTestUtils.createTableWithVersion(model, 404);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
                appendRows(rnd, writer);
                TableWriter.Row r = writer.newRow();
                r.putSym(writer.getColumnIndex("aaa"), null);
                r.append();
                writer.commit();
            }
            assertTrue(engine.migrateNullFlag(sqlExecutionContext.getCairoSecurityContext(), tableName));
        }
    }

    private void appendRows(Rnd rnd, TableWriter writer) {
        int sym = writer.getColumnIndex("aaa");
        for (int i = 0; i < 10; i++) {
            TableWriter.Row r = writer.newRow();
            r.putSym(sym, rnd.nextString(4));
            r.append();
        }
    }
}
