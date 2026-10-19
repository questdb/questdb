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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that a covering posting index fails the query, rather than reading an
 * absent sidecar, when a filesystem fault interrupts loading the covering info.
 * <p>
 * The covering plan is fixed at compile time and the covered read path has no
 * base-table fallback, so {@code AbstractPostingIndexReader} must not swallow a
 * sidecar-load failure: degrading silently leaves a covered read walking an
 * empty {@code sidecarMems}, which throws {@code AssertionError} (an
 * out-of-bounds read in production). The query fuzzer's file fault injection
 * surfaced this on a covered VARCHAR column.
 */
public class CoveringIndexSidecarFaultTest extends AbstractCairoTest {

    @Test
    public void testCoveredVarcharSurvivesSidecarLoadFault() throws Exception {
        final FailureFileFacade faultFf = new FailureFileFacade(engine.getConfiguration().getFilesFacade());
        engine.clear();
        assertMemoryLeak(faultFf, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, tag VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A' || (x % 4), 'V' || (x % 4) FROM long_sequence(40)");
            drainWalQueue();
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (tag)");
            drainWalQueue();

            final String query = "SELECT tag FROM t WHERE sym = 'A0'";

            // Warm the pools and capture the fault-free result for the recovery check.
            final StringSink expected = new StringSink();
            drain(query, expected);

            // Sweep the failing filesystem op across the covering reader's open
            // path. releaseInactive() before each run drops the pooled reader so
            // the index files -- including the covering sidecars -- reopen and the
            // fault can land inside openSidecarFilesIfPresent. Any Exception
            // (SqlException at compile time, CairoException at run time) is an
            // acceptable graceful error; an AssertionError (absent-sidecar read)
            // is an Error, so it escapes the catch and fails the test.
            final StringSink scratch = new StringSink();
            for (int failAfter = 1; failAfter <= 80; failAfter++) {
                engine.releaseInactive();
                faultFf.setToFailAfter(failAfter);
                try {
                    drain(query, scratch);
                } catch (Exception ignore) {
                    // a filesystem fault surfaced as a query error -- acceptable
                } finally {
                    faultFf.clearFailures();
                }
            }

            // Recovery: with no fault armed the covered read returns the same rows.
            engine.releaseInactive();
            final StringSink recovered = new StringSink();
            drain(query, recovered);
            Assert.assertEquals(expected.toString(), recovered.toString());
        });
    }

    private static void drain(String query, StringSink out) throws Exception {
        out.clear();
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final RecordMetadata metadata = factory.getMetadata();
                final int columnCount = metadata.getColumnCount();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0; i < columnCount; i++) {
                        CursorPrinter.printColumn(record, metadata, i, out, false);
                        out.put('\t');
                    }
                    out.put('\n');
                }
            }
        }
    }
}
