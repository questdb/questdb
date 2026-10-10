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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.QueryModel;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class WalUpdateTest extends AbstractCairoTest {
    @Test
    public void testNativeUpdateRemapsDeletedWriterColumns() throws Exception {
        assertMemoryLeak(() -> {
            final int[] updateCounts = {0, 0};
            try (
                    CairoEngine statementEngine = newUpdateCountingEngine(updateCounts);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                statementEngine.execute("""
                        CREATE TABLE lp_update (unused INT, source INT, copied INT, active BOOLEAN, ts TIMESTAMP)
                        TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """, executionContext);
                statementEngine.execute("""
                        INSERT INTO lp_update VALUES
                            (1,10,0,true,'2020-01-01T00:00:00.000000Z'),
                            (2,20,0,false,'2020-01-01T00:00:01.000000Z')
                        """, executionContext);
                statementEngine.execute("ALTER TABLE lp_update DROP COLUMN unused", executionContext);
                statementEngine.execute("UPDATE lp_update SET copied = source WHERE active", executionContext);
                Assert.assertEquals(1, updateCounts[0]);
                Assert.assertEquals(0, updateCounts[1]);
                assertRows(statementEngine, executionContext, "SELECT source,copied FROM lp_update ORDER BY source", """
                        source	copied
                        10	10
                        20	0
                        """);
            }
        });
    }

    @Test
    public void testWalUpdateBindsUnappliedSequencerColumns() throws Exception {
        assertMemoryLeak(() -> {
            final int[] updateCounts = {0, 0};
            try (
                    CairoEngine statementEngine = newUpdateCountingEngine(updateCounts);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                statementEngine.execute("""
                        CREATE TABLE lp_update (unused INT, id INT, active BOOLEAN, ts TIMESTAMP)
                        TIMESTAMP(ts) PARTITION BY DAY WAL
                        """, executionContext);
                statementEngine.execute("""
                        INSERT INTO lp_update VALUES
                            (1,1,true,'2020-01-01T00:00:00.000000Z'),
                            (2,2,false,'2020-01-01T00:00:01.000000Z')
                        """, executionContext);
                drainWalQueue(statementEngine);
                final TableToken tableToken = statementEngine.verifyTableName("lp_update");
                statementEngine.execute("ALTER TABLE lp_update DROP COLUMN unused", executionContext);
                statementEngine.execute("ALTER TABLE lp_update ADD COLUMN source INT", executionContext);
                statementEngine.execute("ALTER TABLE lp_update ADD COLUMN copied INT", executionContext);
                statementEngine.execute("""
                        INSERT INTO lp_update (id,active,ts,source,copied) VALUES
                            (3,true,'2020-01-01T00:00:02.000000Z',30,0),
                            (4,false,'2020-01-01T00:00:03.000000Z',40,0)
                        """, executionContext);

                // Reader metadata still has the old schema; client compilation must bind the
                // sequencer schema and sequence the statement without opening a scan cursor.
                try (TableReader reader = statementEngine.getReader(tableToken)) {
                    Assert.assertEquals(-1, reader.getMetadata().getColumnIndexQuiet("source"));
                    Assert.assertEquals(-1, reader.getMetadata().getColumnIndexQuiet("copied"));
                    Assert.assertEquals(0, reader.getMetadata().getColumnIndexQuiet("unused"));
                }
                statementEngine.execute("UPDATE lp_update SET copied = source WHERE active", executionContext);
                Assert.assertEquals(1, updateCounts[0]);
                Assert.assertEquals(0, updateCounts[1]);

                drainWalQueue(statementEngine);
                Assert.assertFalse(statementEngine.getTableSequencerAPI().isSuspended(tableToken));
                Assert.assertEquals(1, updateCounts[0]);
                Assert.assertEquals(1, updateCounts[1]);
                assertRows(statementEngine, executionContext, "SELECT id,source,copied FROM lp_update ORDER BY id", """
                        id	source	copied
                        1	null	null
                        2	null	null
                        3	30	30
                        4	40	0
                        """);
            }
        });
    }

    private void assertRows(CairoEngine statementEngine, SqlExecutionContext executionContext, String sql, String expected) throws Exception {
        try (
                SqlCompiler compiler = statementEngine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()
        ) {
            assertFactory(factory).withContext(executionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
        }
    }

    private CairoEngine newUpdateCountingEngine(int[] updateCounts) throws IOException {
        return new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath())) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return engine -> {
                    final SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                        @Override
                        protected RecordCursorFactory generateSelectOneShot(
                                QueryModel model,
                                SqlExecutionContext executionContext,
                                boolean generateProgressLogger
                        ) throws SqlException {
                            Assert.assertNotNull(getPlanForTesting());
                            if (model.isUpdate()) {
                                updateCounts[executionContext.isWalApplication() ? 1 : 0]++;
                            }
                            return super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                        }
                    };
                    return compiler;
                };
            }
        };
    }
}
