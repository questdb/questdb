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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableSequencerCursorPool;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WalTransactionsFunctionTest extends AbstractCairoTest {
    private static final String INJECTED_ERROR = "injected toMinTxn failure";
    private static CloseCountingCursor injectedCursor;
    private static boolean injectCursor;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            private final InjectedSequencerAPI injectedSequencerAPI = new InjectedSequencerAPI(this, conf);

            @Override
            public TableSequencerAPI getTableSequencerAPI() {
                return injectCursor ? injectedSequencerAPI : super.getTableSequencerAPI();
            }

            @Override
            public void close() {
                injectedSequencerAPI.close();
                super.close();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            try (RecordCursorFactory ignore = select("select * from wal_transactions('x')")) {
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table is not a WAL table: x");
                Assert.assertEquals("select * from wal_transactions(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory ignore = select("select * from wal_transactions('x')")) {
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist: x");
                Assert.assertEquals("select * from wal_transactions(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testToMinTxnFailureClosesCursorImmediately() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, value int) timestamp(ts) PARTITION BY DAY WAL");
            try (RecordCursorFactory factory = select("select * from wal_transactions('x')")) {
                injectedCursor = new CloseCountingCursor();
                injectCursor = true;
                try {
                    factory.getCursor(sqlExecutionContext);
                    Assert.fail("expected injected toMinTxn failure");
                } catch (CairoException e) {
                    Assert.assertEquals(INJECTED_ERROR, e.getFlyweightMessage().toString());
                    Assert.assertEquals(1, injectedCursor.acquisitionCount);
                    Assert.assertEquals(1, injectedCursor.toMinTxnCount);
                    Assert.assertEquals(1, injectedCursor.closeCount);
                    Assert.assertNotNull(injectedCursor.pool);
                } finally {
                    injectCursor = false;
                }
            } finally {
                injectedCursor = null;
            }
        });
    }

    @Test
    public void testWalTransactionIdempotency() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'trades' (\s
                    \tsymbol SYMBOL CAPACITY 256 CACHE,
                    \tside SYMBOL CAPACITY 256 CACHE,
                    \tprice DOUBLE,
                    \tamount DOUBLE,
                    \ttimestamp TIMESTAMP
                    ) timestamp(timestamp) PARTITION BY DAY WAL
                    """);

            assertQuery("""
                    with segments as (
                    \tselect walid, segmentId from wal_transactions('trades')
                    \twhere sequencerTxn = 10
                    )
                    select max(wt.sequencerTxn) + 1 from wal_transactions('trades') wt
                    join segments s on s.segmentId = wt.segmentId and s.walId = wt.walId
                    where sequencerTxn > 10;
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column
                            null
                            """);
        });
    }

    @Test
    public void testWalTransactions() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            drainWalQueue();

            assertQuery("select * from wal_transactions('x')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType
                            1\t2023-11-22T19:00:53.950468Z\t1\t0\t0\t0\t\t\tnull\t0
                            2\t2023-11-22T19:00:53.950468Z\t1\t0\t1\t0\t\t\tnull\t0
                            3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t0
                            """);
        });
    }

    @Test
    public void testWalTransactionsLastLine() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            drainWalQueue();

            assertQuery("select * from wal_transactions('x') limit -1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType
                            3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t0
                            """);
        });
    }

    @Test
    public void testWalTransactionsV2() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-02-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");
            execute("alter table x drop column z");

            drainWalQueue();

            assertQuery("select * from wal_transactions('x')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType
                            1\t2023-11-22T19:00:53.950468Z\t1\t0\t0\t0\t2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\t1\t0
                            2\t2023-11-22T19:00:53.950468Z\t1\t0\t1\t0\t2020-02-01T00:00:00.000000Z\t2020-02-01T00:00:00.000000Z\t1\t0
                            3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t1
                            4\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t2\t\t\tnull\t8
                            """);
        });
    }

    @Test
    public void testWalTransactionsV2WithTimestampNs() throws Exception {
        // Regression test for https://github.com/questdb/questdb/issues/6677
        // Verifies that timestamp columns are correctly converted to microseconds
        // when the table uses TIMESTAMP_NS as the designated timestamp type
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2026-01-22T19:00:53.950468Z"));
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table x (ts timestamp_ns, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-02-01T00:00:00.000000Z', 2, 3)");

            drainWalQueue();

            // The commit timestamp should show 2026-01-22 (system time), not 1970
            // The minTimestamp/maxTimestamp should show 2020-01-01 and 2020-02-01, not year 57000+
            assertQuery("select sequencerTxn, timestamp, minTimestamp, maxTimestamp from wal_transactions('x')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            sequencerTxn\ttimestamp\tminTimestamp\tmaxTimestamp
                            1\t2026-01-22T19:00:53.950468Z\t2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            2\t2026-01-22T19:00:53.950468Z\t2020-02-01T00:00:00.000000Z\t2020-02-01T00:00:00.000000Z
                            """);
        });
    }

    private static class CloseCountingCursor implements TransactionLogCursor {
        private int acquisitionCount;
        private int closeCount;
        private TableSequencerCursorPool pool;
        private int toMinTxnCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public boolean extend() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCommitTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMaxTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSegmentId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSegmentTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getStructureVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMaxTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMinTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnRowCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getWalId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPosition(long txn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toMinTxn() {
            toMinTxnCount++;
            throw CairoException.nonCritical().put(INJECTED_ERROR);
        }

        @Override
        public void toTop() {
            throw new UnsupportedOperationException();
        }
    }

    private static class InjectedSequencerAPI extends TableSequencerAPI {
        private InjectedSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
            super(engine, configuration);
        }

        @Override
        public TransactionLogCursor getCursor(TableToken tableToken, long seqTxn, TableSequencerCursorPool cursorPool) {
            injectedCursor.pool = cursorPool;
            injectedCursor.acquisitionCount++;
            return injectedCursor;
        }
    }
}
