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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.lang.reflect.Constructor;

public class EncodedSortRecordCursorReuseTest extends AbstractCairoTest {

    @Test
    public void testChainResetOnReuse() throws Exception {
        // EncodedSortRecordCursor.of() runs only after construction or a close() (it asserts
        // baseCursor == null), so reuse goes through close() -> of(), as the factory drives it.
        // This exercises that cycle 5x under a tight 4 KiB / 1-page value budget: each run must
        // start from a clean recordChain, else a carried-forward varAppendOffset overflows the
        // single page. Per row (key LONG, payload LONG, ts TIMESTAMP): 8B header + 24B = 32B;
        // 100 rows = 3_200B fit one 4_096B page, but two runs' worth (6_400B) would not.
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_PAGES, 1);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (key LONG, payload LONG, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t " +
                    "SELECT cast(100 - x AS LONG), x, " +
                    "       timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000) " +
                    "FROM long_sequence(100)");

            final BytecodeAssembler asm = new BytecodeAssembler();
            final EntityColumnFilter ecf = new EntityColumnFilter();
            final IntList sortFilter = new IntList();
            sortFilter.add(1);  // sort ascending by column index 0 (key); IntList encodes as columnIndex+1

            try (TableReader reader = newOffPoolReader(configuration, "t")) {
                ecf.of(reader.getMetadata().getColumnCount());
                final RecordSink sink = RecordSinkFactory.getInstance(
                        configuration, asm, reader.getMetadata(), ecf);

                Class<?> cursorClass = Class.forName(
                        "io.questdb.griffin.engine.orderby.EncodedSortRecordCursor");
                Constructor<?> ctor = cursorClass.getDeclaredConstructor(
                        CairoConfiguration.class, RecordMetadata.class,
                        IntList.class, RecordSink.class);
                ctor.setAccessible(true);

                DelegatingRecordCursor sortCursor = (DelegatingRecordCursor) ctor.newInstance(
                        configuration, reader.getMetadata(), sortFilter, sink);
                try (TestTableReaderRecordCursor base = new TestTableReaderRecordCursor().of(reader)) {
                    for (int i = 0; i < 5; i++) {
                        base.toTop();
                        sortCursor.of(base, sqlExecutionContext);
                        Record record = sortCursor.getRecord();
                        long lastKey = Long.MIN_VALUE;
                        int rows = 0;
                        while (sortCursor.hasNext()) {
                            long key = record.getLong(0);
                            Assert.assertTrue(
                                    "iteration " + i + " row " + rows + " key must be ascending",
                                    key > lastKey);
                            lastKey = key;
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i + " row count", 100, rows);
                        // of() requires a preceding close(); close before the next reuse.
                        sortCursor.close();
                    }
                } finally {
                    Misc.free((Closeable) sortCursor);
                }
            }
        });
    }
}
