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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableHysteresisTest extends AbstractGriffinTest {

    @Test
    public void setMaxUncommitedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CairoTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    String alterCommand = "ALTER TABLE X SET PARAM O3MaxUncommittedRows = 11111";
                    compiler.compile(alterCommand, sqlExecutionContext);

                    assertSql("SELECT o3MaxUncommittedRows FROM tables() WHERE name = 'X'", "o3MaxUncommittedRows\n11111\n");
                    rdr.reload();
                    Assert.assertEquals(11111, rdr.getMetadata().getMaxUncommittedRows());
                }
            }
        });
    }

    @Test
    public void setCommitHysteresis() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tbl = new TableModel(configuration, "X", PartitionBy.DAY)) {
                CairoTestUtils.create(tbl.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try (TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "X")) {
                    String alterCommand = "ALTER TABLE X SET PARAM O3CommitHysteresis = 111s";
                    compiler.compile(alterCommand, sqlExecutionContext);

                    assertSql("SELECT o3CommitHysteresisMicros FROM tables() WHERE name = 'X'", "o3CommitHysteresisMicros\n111000000\n");
                    rdr.reload();
                    Assert.assertEquals(111000000L, rdr.getMetadata().getO3CommitHysteresisMicros());
                }
            }
        });
    }
}
