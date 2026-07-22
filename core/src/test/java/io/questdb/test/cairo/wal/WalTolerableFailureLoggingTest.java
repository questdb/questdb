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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.tasks.TableWriterTask.CMD_ALTER_TABLE;
import static io.questdb.test.tools.TestUtils.assertEventually;

public class WalTolerableFailureLoggingTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testToleratedNonUpdateFailureIsLoggedAndApplied() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_log (ts timestamp) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.getTableTokenIfExists("wal_log");
            Assert.assertNotNull(tableToken);

            try (TableWriter writer = engine.getWriter(tableToken, "tolerable failure logging test")) {
                final long seqTxn = writer.getSeqTxn() + 1;
                final AlterOperation operation = new AlterOperation() {
                    @Override
                    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
                        throw CairoException.partitionManipulationRecoverable()
                                .put("injected tolerable alter failure");
                    }

                    @Override
                    public int getCmdType() {
                        return CMD_ALTER_TABLE;
                    }
                };

                Assert.assertEquals(0, writer.apply(operation, seqTxn));

                Assert.assertEquals("the tolerated command still advances the apply watermark",
                        seqTxn, writer.getSeqTxn());
                assertEventually(
                        () -> capture.assertLoggedRE("tolerated WAL command failure \\[table=.*wal_log.*"
                                + "seqTxn=" + seqTxn
                                + ", command=ALTER TABLE"
                                + ", error=injected tolerable alter failure"
                                + ", errno=" + CairoException.PARTITION_MANIPULATION_RECOVERABLE
                                + "\\]"),
                        2
                );
            }
        });
    }
}
