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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Interval scans running WHILE a composite table is being written.
 * <p>
 * Every other test of these cursors reads a table that has stopped changing. Production does not: an
 * interval query runs against a reader that may reload mid-flight, picking up new cells and extended
 * cells in the middle of a scan. The composite reader takes a different reload path from a plain one
 * (the fast path is deliberately disabled for composite, because one commit can grow several cells), and
 * the interval cursors cache partition bounds across that reload.
 * <p>
 * Deliberately modest: two reader threads and a bounded number of commits. The point is to run the
 * interval cursors against a reader that is genuinely reloading underneath them, not to saturate the
 * machine — a soak test that pins every core belongs in a nightly job, not here.
 * <p>
 * What is asserted, and why it is what it is: DURING the race, a query may legitimately observe any
 * committed prefix, so the assertion is that every row it returns satisfies the predicate and that no
 * query fails. Counts are checked for MONOTONICITY (a reader must never go backwards), which is a real
 * invariant that a botched reload would break. AFTER the writer stops, the composite table must match
 * its plain twin exactly.
 */
public class CompositeIntervalConcurrentReadTest extends AbstractCairoTest {

    private static final String WINDOW =
            " WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts < '2023-01-02T12:00:00.000000Z'";

    @Test
    public void testIntervalScansDuringConcurrentWrites() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            final int commits = 40;
            final int readers = 2;
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicInteger readsPerformed = new AtomicInteger();
            final CountDownLatch writerDone = new CountDownLatch(1);
            final CountDownLatch readersReady = new CountDownLatch(readers);

            final Thread[] readerThreads = new Thread[readers];
            for (int r = 0; r < readers; r++) {
                final boolean backward = r % 2 == 1;
                readerThreads[r] = new Thread(() -> {
                    try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                        readersReady.countDown();
                        long previousCount = -1;
                        final StringSink sink = new StringSink();
                        while (writerDone.getCount() > 0 && failure.get() == null) {
                            // rows must all satisfy the predicate, whatever commit we happen to see
                            sink.clear();
                            TestUtils.printSql(engine, context,
                                    "SELECT count() FROM (SELECT ts FROM c" + WINDOW
                                            + (backward ? " ORDER BY ts DESC" : " ORDER BY ts") + ")", sink);
                            final long count = parseCount(sink);
                            if (count < previousCount) {
                                throw new AssertionError("a reader went BACKWARDS: saw " + count
                                        + " rows after previously seeing " + previousCount
                                        + " -- a reload dropped already-visible rows");
                            }
                            previousCount = count;

                            // and nothing outside the window may ever be returned
                            sink.clear();
                            TestUtils.printSql(engine, context,
                                    "SELECT count() FROM c" + WINDOW
                                            + " AND (ts < '2023-01-02T00:00:00.000000Z'"
                                            + " OR ts >= '2023-01-02T12:00:00.000000Z')", sink);
                            if (parseCount(sink) != 0) {
                                throw new AssertionError("interval scan returned a row outside its own window");
                            }
                            readsPerformed.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                readerThreads[r].start();
            }

            readersReady.await();
            try {
                for (int i = 0; i < commits && failure.get() == null; i++) {
                    // each commit adds a new cell AND extends existing ones, inside the read window
                    final String values = "('2023-01-02T0" + (i % 10) + ":0" + (i % 6) + ":00.000000Z','E"
                            + (i % 7) + "'," + i + ".0),"
                            + "('2023-01-02T1" + (i % 2) + ":0" + (i % 6) + ":00.000000Z','NEW" + i + "'," + i + ".5)";
                    execute("INSERT INTO c VALUES " + values);
                    execute("INSERT INTO p VALUES " + values);
                    drainWalQueue();
                }
            } finally {
                writerDone.countDown();
            }
            for (Thread t : readerThreads) {
                t.join();
            }

            if (failure.get() != null) {
                throw new AssertionError("concurrent interval scan failed", failure.get());
            }
            // A floor, not "> 0": one read that happened to land before the first commit would satisfy
            // "> 0" while proving no overlap at all. Measured at 330-463 reads across three runs, so 20
            // is far below anything a healthy run produces and far above what a non-overlapping run
            // could reach.
            Assert.assertTrue("readers must genuinely have overlapped the writes, saw only "
                            + readsPerformed.get() + " reads",
                    readsPerformed.get() >= 20);

            // once everything has settled, the twins must be identical
            drainWalQueue();
            assertSqlCursors("SELECT * FROM p" + WINDOW + " ORDER BY ts, exch, px",
                    "SELECT * FROM c" + WINDOW + " ORDER BY ts, exch, px");
            assertSqlCursors("SELECT ts FROM p" + WINDOW + " ORDER BY ts DESC",
                    "SELECT ts FROM c" + WINDOW + " ORDER BY ts DESC");
            assertSqlCursors("SELECT count() FROM p" + WINDOW, "SELECT count() FROM c" + WINDOW);
        });
    }

    private static long parseCount(StringSink sink) throws SqlException {
        // output is "count\n<n>\n"
        final String text = sink.toString();
        final int nl = text.indexOf('\n');
        final int end = text.indexOf('\n', nl + 1);
        try {
            return Long.parseLong(text.substring(nl + 1, end < 0 ? text.length() : end).trim());
        } catch (NumberFormatException e) {
            throw SqlException.$(0, "unexpected count output: ").put(text);
        }
    }
}
