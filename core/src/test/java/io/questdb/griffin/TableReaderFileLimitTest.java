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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderFileLimitTest extends AbstractGriffinTest {

    @Test
    public void testFileLimitExceeded() throws Exception {
        try {
            testLimitExceeded(150, Integer.MAX_VALUE, "OS file limit set too low to open all files on table");
        } finally {
            FilesFacadeImpl.INSTANCE.setOpenFileLimit(defaultOpenFileLimit);
            FilesFacadeImpl.INSTANCE.setMapLimit(defaultMapLimit);
        }
    }

    @Test
    public void testMapLimitExceeded() throws Exception {
        try {
            testLimitExceeded(Integer.MAX_VALUE, 150, "OS vm.max_map_count set too low to map all files of the table");
        } finally {
            FilesFacadeImpl.INSTANCE.setOpenFileLimit(defaultOpenFileLimit);
            FilesFacadeImpl.INSTANCE.setMapLimit(defaultMapLimit);
        }
    }

    private void testLimitExceeded(int fileLimit, int mapLimit, String errorMessage) throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            String tableName = "x";
            try (TableModel model =
                         new TableModel(configuration, tableName, PartitionBy.HOUR)
                                 .col("a", ColumnType.SYMBOL)
                                 .col("b", ColumnType.SYMBOL).indexed(true, 4096)
                                 .timestamp()
                                 .col("l", ColumnType.LONG)
                                 .col("l2", ColumnType.INT)
                                 .col("l3", ColumnType.CHAR)
                                 .col("l4", ColumnType.BOOLEAN)
                                 .col("l5", ColumnType.STRING)
            ) {
                TestUtils.createPopulateTable(tableName, compiler, sqlExecutionContext, model, 20, "2022-02-24", 10);
            }
            engine.releaseInactive();

            // 10 files x 10 partitions = 100 files to open / map estimation
            FilesFacade ff = configuration.getFilesFacade();
            ff.setOpenFileLimit(fileLimit);
            ff.setMapLimit(mapLimit);

            try (TableReader reader1 = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                reader1.checkOsLimitsCapacity();
                // Open and map all files
                TestUtils.printCursor(reader1.getCursor(), reader1.getMetadata(), false, sink, printer);

                try (TableReader reader2 = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    reader2.checkOsLimitsCapacity();
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), errorMessage);
                }

                // reader1 has all files open, should be good to re-check
                reader1.checkOsLimitsCapacity();
            }
            engine.releaseInactive();
        });
    }
}
