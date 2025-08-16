/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TableWriterInteractionTest extends AbstractCairoTest {
    @Test
    public void testRowCancelRowIndexUpdate() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "xyz", PartitionBy.DAY);
            model.timestamp();
            model.col("x", ColumnType.SYMBOL).indexed(true, 256);
            AbstractCairoTest.create(model);

            long ts = MicrosFormatUtils.parseTimestamp("2019-04-29T12:00:04.877721Z");
            try (TableWriter w = getWriter("xyz")) {
                TableWriter.Row r = w.newRow(ts);

                // this used to caused dense indexer list to be freed
                r.cancel();

                w.addColumn("y", ColumnType.STRING);

                r = w.newRow(ts);
                r.putSym(1, "ELLO");
                r.putStr(2, "test");
                r.append();
                w.commit();
            }

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "xyz where x = 'ELLO'",
                    sink,
                    "timestamp\tx\ty\n" +
                            "2019-04-29T12:00:04.877721Z\tELLO\ttest\n"
            );
        });
    }
}
