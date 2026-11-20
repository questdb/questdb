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

package io.questdb.test.tools;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TestUtilsTest extends AbstractCairoTest {

    @Test
    public void testAssertAsciiCompliance() {
        TestUtils.assertAsciiCompliance(null);
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, true));
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, false));

        Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{(byte) 0xc3, (byte) 0xa9}, true))
        );
    }

    @Test
    public void testAssertEqualsOfCursorsChecksAsciiFlagOfVarchar() {
        final byte[] nonAscii = {(byte) 0xc3, (byte) 0xa9};
        final GenericRecordMetadata varchar = new GenericRecordMetadata();
        varchar.add(new TableColumnMetadata("v", ColumnType.VARCHAR));
        final GenericRecordMetadata string = new GenericRecordMetadata();
        string.add(new TableColumnMetadata("v", ColumnType.STRING));

        // equal bytes, flagged right on both sides
        TestUtils.assertEquals(varcharCursor(nonAscii, false), varchar, varcharCursor(nonAscii, false), varchar, false);
        // equal bytes, the expected side or the actual side flagged ASCII
        assertAsciiFlagRejected(varcharCursor(nonAscii, true), varchar, varcharCursor(nonAscii, false), varchar, false);
        assertAsciiFlagRejected(varcharCursor(nonAscii, false), varchar, varcharCursor(nonAscii, true), varchar, false);
        // against a STRING column, on either side
        assertAsciiFlagRejected(varcharCursor(nonAscii, true), varchar, stringCursor(), string, true);
        assertAsciiFlagRejected(stringCursor(), string, varcharCursor(nonAscii, true), varchar, true);
        TestUtils.assertEquals(varcharCursor(nonAscii, false), varchar, stringCursor(), string, true);
    }

    @Test
    public void testAssertReverseLinesEqual() {
        Assert.assertThrows(AssertionError.class, () -> {
            TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "123\n456\n789\n");
            TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n456\n123\n");
        });
        TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "789\n456\n123\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n56\n1234\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n", "1234\n");
    }

    @Test
    public void testJoinThreadsInterruptsTimedOutWorker() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final Thread worker = new Thread(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        }, "timed-out-test-worker");
        worker.setDaemon(true);
        worker.start();
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

        final AssertionError error = Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.joinThreads(50, worker)
        );
        Assert.assertTrue(error.getMessage().contains("did not finish within"));
        Assert.assertTrue(interrupted.get());
        Assert.assertFalse(worker.isAlive());
    }

    @Test
    public void testOrderTolerantRecordComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x long, ts timestamp) timestamp(ts) partition by day");
            execute("create table y (x long, ts timestamp) timestamp(ts) partition by day");

            execute("insert into x values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (3, '2022-02-24T00:00:02.000000Z')");

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (3, '2022-02-24T00:00:02.000000Z')");

            HashMap<String, Integer> mapX = new HashMap<>();
            HashMap<String, Integer> mapY = new HashMap<>();

            addAllRecordsToMap("x", mapX);
            addAllRecordsToMap("y", mapY);

            Assert.assertEquals(mapX, mapY);

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");

            // now the maps should be different since we've added an extra record

            mapY.clear();
            addAllRecordsToMap("y", mapY);

            Assert.assertNotEquals(mapX, mapY);
        });
    }

    @Test
    public void testWorkerPoolModeSeededSelectionIsStable() {
        final Rnd expected = new Rnd(123, 456);
        final Rnd actual = new Rnd(123, 456);
        for (int i = 0; i < 100; i++) {
            Assert.assertSame(
                    expected.nextBoolean() ? WorkerPoolMode.FIBER_HOST : WorkerPoolMode.LEGACY,
                    TestUtils.getWorkerPoolMode(actual)
            );
        }
    }

    @Test
    public void testWorkerPoolModeSelectsBothModes() {
        final Rnd rnd = new Rnd(123, 456);
        boolean hasFiberHost = false;
        boolean hasLegacy = false;
        for (int i = 0; i < 100 && !(hasFiberHost && hasLegacy); i++) {
            switch (TestUtils.getWorkerPoolMode(rnd)) {
                case FIBER_HOST -> hasFiberHost = true;
                case LEGACY -> hasLegacy = true;
            }
        }
        Assert.assertTrue(hasFiberHost);
        Assert.assertTrue(hasLegacy);
    }

    @Test
    public void testRunConcurrentlyRethrowsWorkerFailure() {
        final AssertionError error = Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.runConcurrently(2, worker -> {
                    if (worker == 1) {
                        throw new IllegalStateException("worker failure");
                    }
                })
        );
        Assert.assertTrue(error.getCause() instanceof IllegalStateException);
        Assert.assertEquals("worker failure", error.getCause().getMessage());
    }

    private static void assertAsciiFlagRejected(
            RecordCursor expected,
            RecordMetadata expectedMetadata,
            RecordCursor actual,
            RecordMetadata actualMetadata,
            boolean genericStringMatch
    ) {
        try {
            TestUtils.assertEquals(expected, expectedMetadata, actual, actualMetadata, genericStringMatch);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), "ascii flag set to 'true' for non-ASCII value 'é'");
            return;
        }
        Assert.fail("a non-ASCII value flagged ASCII must fail the comparison");
    }

    // one row of the STRING "é"
    private static RecordCursor stringCursor() {
        return new SingleRowCursor(new Record() {
            @Override
            public CharSequence getStrA(int col) {
                return "é";
            }
        });
    }

    // one row of a VARCHAR of the bytes, flagged as told
    private static RecordCursor varcharCursor(byte[] bytes, boolean isAscii) {
        final Utf8String value = new Utf8String(bytes, isAscii);
        return new SingleRowCursor(new Record() {
            @Override
            public Utf8Sequence getVarcharA(int col) {
                return value;
            }
        });
    }

    private static void addAllRecordsToMap(String query, Map<String, Integer> map) throws SqlException {
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            TestUtils.addAllRecordsToMap(sink, cursor, factory.getMetadata(), map);
        }
    }

    private static class SingleRowCursor implements RecordCursor {
        private final Record record;
        private boolean hasNext = true;

        private SingleRowCursor(Record record) {
            this.record = record;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return record;
        }

        @Override
        public boolean hasNext() {
            final boolean result = hasNext;
            hasNext = false;
            return result;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }
    }
}
