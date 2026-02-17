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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ReaderLeftBehindTest extends AbstractCairoTest {

    private static final LogCapture capture = new LogCapture();

    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        super.setUp();
        capture.start();
    }

    @After
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(QueryProgress.class);
    }

    @Test
    public void testClosePoolWhenReaderIsOut() throws Exception {
        assertMemoryLeak(() -> {
            try (ReaderPool pool = new ReaderPool(configuration, engine.getTxnScoreboardPool(), messageBus)) {
                TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
                AbstractCairoTest.create(model);

                // Reference, we're retrieving the reader right after this
                StackTraceElement elem = Thread.currentThread().getStackTrace()[1];
                try (TableReader reader = pool.get(engine.verifyTableName("x"))) {
                    Assert.assertNotNull(reader);
                    try {
                        pool.close();
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(),
                                "table is left behind on pool shutdown [table=" + reader.getTableToken().getDirName() + "]");
                        capture.waitFor(elem.getFileName() + ":" + (elem.getLineNumber() + 1));
                        capture.assertLogged("shutting down, table is left behind");
                    }
                    Assert.assertTrue(reader.isOpen());
                }
            }
        });
    }
}