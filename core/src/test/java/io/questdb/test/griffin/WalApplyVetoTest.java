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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * {@code CairoEngine.checkTableSuspendable} and {@code checkTableResumableFromTxn} veto stopping
 * a table's WAL apply and skipping its committed transactions, independently of permissions.
 * Enterprise overrides both for the view audit table, whose rows a skip would remove.
 */
public class WalApplyVetoTest extends AbstractCairoTest {

    private static final String PROTECTED = "protected_wal";
    private static final String RESUME_VETO_MESSAGE = "this table cannot skip transactions";
    private static final String SUSPEND_VETO_MESSAGE = "this table cannot be suspended";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public void checkTableResumableFromTxn(TableToken tableToken, int tableNamePosition) {
                if (Chars.equalsIgnoreCase(tableToken.getTableName(), PROTECTED)) {
                    throw CairoException.nonCritical().position(tableNamePosition).put(RESUME_VETO_MESSAGE);
                }
            }

            @Override
            public void checkTableSuspendable(TableToken tableToken, int tableNamePosition) {
                if (Chars.equalsIgnoreCase(tableToken.getTableName(), PROTECTED)) {
                    throw CairoException.nonCritical().position(tableNamePosition).put(SUSPEND_VETO_MESSAGE);
                }
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testPlainResumeDoesNotConsultTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + PROTECTED + " (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tableToken = engine.verifyTableName(PROTECTED);
            // Suspended by the server rather than by a client, which the veto leaves alone.
            engine.getTableSequencerAPI().suspendTable(tableToken, ErrorTag.NONE, "test");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            // Neither form can skip a transaction.
            execute("ALTER TABLE " + PROTECTED + " RESUME WAL");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            engine.getTableSequencerAPI().suspendTable(tableToken, ErrorTag.NONE, "test");
            execute("ALTER TABLE " + PROTECTED + " RESUME WAL FROM TXN 0");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testResumeFromTxnHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + PROTECTED + " (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + PROTECTED + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName(PROTECTED);
            engine.getTableSequencerAPI().suspendTable(tableToken, ErrorTag.NONE, "test");
            execute("INSERT INTO " + PROTECTED + " VALUES ('2024-01-01T00:00:01.000000Z', 2)");
            execute("INSERT INTO " + PROTECTED + " VALUES ('2024-01-01T00:00:02.000000Z', 3)");

            assertExceptionNoLeakCheck("ALTER TABLE " + PROTECTED + " RESUME WAL FROM TXN 4", 12, RESUME_VETO_MESSAGE);
            Assert.assertTrue("a vetoed resume must leave the table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));

            // The rows the skip would have lost reach the table once it is resumed plainly.
            execute("ALTER TABLE " + PROTECTED + " RESUME WAL");
            drainWalQueue();
            assertQuery("SELECT x FROM " + PROTECTED)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testSuspendHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + PROTECTED + " (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE ordinary (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            assertExceptionNoLeakCheck("ALTER TABLE " + PROTECTED + " SUSPEND WAL", 12, SUSPEND_VETO_MESSAGE);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(PROTECTED)));

            execute("ALTER TABLE ordinary SUSPEND WAL");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("ordinary")));
        });
    }
}
