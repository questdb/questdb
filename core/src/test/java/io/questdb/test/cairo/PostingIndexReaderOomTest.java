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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that constructing a posting index reader frees its native buffers
 * when an allocation fails part-way.
 * <p>
 * {@code AbstractPostingIndexReader} builds a {@code PostingGenLookup} whose
 * constructor allocates a {@code DirectLongList} and a {@code DirectIntLongHashMap}.
 * If the second allocation trips the RSS memory limit, the half-built lookup is
 * never assigned to the reader, so the reader's {@code close()} cannot reach the
 * first buffer and it leaks. The query fuzzer's malloc fault injection surfaced
 * this as a small {@code NATIVE_INDEX_READER} leak when reading through a posting
 * index.
 */
public class PostingIndexReaderOomTest extends AbstractCairoTest {

    @Test
    public void testReaderConstructionCleansUpOnOom() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 's' || (x % 8) FROM long_sequence(120)");
            drainWalQueue();

            final String query = "SELECT ts FROM t WHERE sym IN ('s7', 's6') LIMIT 1";

            // Warm the pools, then capture the fault-free result for the recovery check.
            final StringSink expected = new StringSink();
            drain(query, expected);

            boolean sawOom = false;
            // Sweep the native-memory ceiling so it lands inside posting index
            // reader construction. releaseInactive() before each run drops the
            // pooled reader so it rebuilds and the fault can land in the
            // PostingGenLookup buffer allocations.
            final StringSink scratch = new StringSink();
            for (int slack = 0; slack <= 48 * 1024; slack += 64) {
                engine.releaseInactive();
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try {
                    drain(query, scratch);
                } catch (CairoException e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                    sawOom = true;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", sawOom);

            // Recovery: with no ceiling the query returns the same row. The
            // enclosing assertMemoryLeak is the authoritative net leak check.
            Unsafe.setRssMemLimit(0);
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
