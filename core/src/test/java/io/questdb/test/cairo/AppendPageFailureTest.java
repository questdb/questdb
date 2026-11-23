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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A column file that cannot grow when the append memory needs its next page, as on a full disk. The write fails,
 * and the next writes must neither crash nor lose the rows committed before it.
 */
public class AppendPageFailureTest extends AbstractCairoTest {
    // rows of 8 bytes that fill the first page of a column file
    private static final long ROWS_PER_PAGE = Files.PAGE_SIZE / Long.BYTES;

    @Test
    public void testAlterAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (ROWS_PER_PAGE + 1) + "::TIMESTAMP, -1)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());

            // ALTER commits through the pooled writer the INSERT failed on
            execute("ALTER TABLE tab ADD COLUMN y INT");
            assertCommittedRows("");
        });
    }

    @Test
    public void testInsertAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (ROWS_PER_PAGE + 1) + "::TIMESTAMP, -1)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());

            final long x = ROWS_PER_PAGE + 2;
            execute("INSERT INTO tab VALUES (" + x + "::TIMESTAMP, " + x + ")");
            assertCommittedRows(x + "\t" + timestampOf(x) + "\n");
        });
    }

    @Test
    public void testNewRowAfterFailedTimestampPage() throws Exception {
        // ILP and the other writers that keep the writer after a failed row
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            final long x = ROWS_PER_PAGE + 2;
            try (TableWriter writer = getWriter("tab")) {
                ff.isArmed.set(true);
                try {
                    writer.newRow(ROWS_PER_PAGE + 1);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertFalse(writer.isDistressed());
                TableWriter.Row row = writer.newRow(x);
                row.putLong(1, x);
                row.append();
                writer.commit();
            }
            assertCommittedRows(x + "\t" + timestampOf(x) + "\n");
        });
    }

    @Test
    public void testWalInsertAfterFailedVarcharPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tabw", "v.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tabw (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // values of 100 bytes: n of them fill the data file up to n * 100, and the next one crosses the
            // end of the first page, since a page size is not a multiple of 100
            final int len = 100;
            final long n = Files.PAGE_SIZE / len;
            final String a = "a".repeat(len);
            execute("INSERT INTO tabw SELECT x::TIMESTAMP, '" + a + "' FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tabw VALUES (" + (n + 1) + "::TIMESTAMP, '" + "b".repeat(len) + "')", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            final String c = "c".repeat(len);
            execute("INSERT INTO tabw VALUES (" + (n + 2) + "::TIMESTAMP, '" + c + "')");
            drainWalQueue();

            assertQuery("SELECT count(), count_distinct(v) FROM tabw")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tcount_distinct\n" + (n + 1) + "\t2\n");
            assertQuery("SELECT ts, v FROM tabw WHERE ts >= " + (n - 1) + "::TIMESTAMP")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tv\n"
                            + timestampOf(n - 1) + "\t" + a + "\n"
                            + timestampOf(n) + "\t" + a + "\n"
                            + timestampOf(n + 2) + "\t" + c + "\n");
        });
    }

    // the rows the table must hold: x = 1..ROWS_PER_PAGE, committed before the failure, then the extra rows
    private void assertCommittedRows(String extraRows) throws Exception {
        assertQuery("SELECT x, ts FROM tab WHERE x >= " + (ROWS_PER_PAGE - 1))
                .noLeakCheck()
                .timestamp("ts")
                .returns("x\tts\n"
                        + (ROWS_PER_PAGE - 1) + "\t" + timestampOf(ROWS_PER_PAGE - 1) + "\n"
                        + ROWS_PER_PAGE + "\t" + timestampOf(ROWS_PER_PAGE) + "\n"
                        + extraRows);
        final long extra = extraRows.isEmpty() ? 0 : 1;
        assertQuery("SELECT count(), sum(x) FROM tab WHERE x <= " + ROWS_PER_PAGE)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\tsum\n" + ROWS_PER_PAGE + "\t" + (ROWS_PER_PAGE * (ROWS_PER_PAGE + 1) / 2) + "\n");
        assertQuery("SELECT count() FROM tab")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + (ROWS_PER_PAGE + extra) + "\n");
    }

    // a table whose ts.d and x.d end exactly at the end of their first page, with the writer that appended them
    // in the pool, the next row of which maps the next page
    private static void createTableWithFullPage() throws Exception {
        execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tab SELECT x::TIMESTAMP, x FROM long_sequence(" + ROWS_PER_PAGE + ")");
    }

    private static String timestampOf(long micros) {
        final StringBuilder sb = new StringBuilder();
        sb.append("1970-01-01T00:00:00.");
        final String digits = Long.toString(micros);
        sb.append("0".repeat(6 - digits.length())).append(digits).append('Z');
        return sb.toString();
    }

    /**
     * Fails, once when armed, the allocation of disk space for one file of one table: the file that the writer
     * opened last under that name.
     */
    private static class AllocateFailingFacade extends TestFilesFacadeImpl {
        final AtomicBoolean isArmed = new AtomicBoolean();
        private final String fileSuffix;
        private final String tableDirPrefix;
        private volatile long fd = -1;

        AllocateFailingFacade(String tableName, String fileName) {
            this.tableDirPrefix = Files.SEPARATOR + tableName + '~';
            this.fileSuffix = Files.SEPARATOR + fileName;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (fd == this.fd && isArmed.compareAndSet(true, false)) {
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.containsAscii(name, tableDirPrefix) && Utf8s.endsWithAscii(name, fileSuffix)) {
                this.fd = fd;
            }
            return fd;
        }
    }
}
