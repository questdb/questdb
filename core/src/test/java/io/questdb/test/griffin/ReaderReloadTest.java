/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.std.datetime.microtime.Micros.HOUR_MICROS;

public class ReaderReloadTest extends AbstractCairoTest {


    @Test
    public void testReaderReloadDoesNotReopenPartitionsNonWal() throws Exception {
        testReaderReloadDoesNotReopenPartitions();
    }

    @Test
    public void testReaderReloadDoesNotReopenPartitionsWal() throws Exception {
        testReaderReloadDoesNotReopenPartitions();
    }

    @Test
    public void testReaderReloadFails() throws Exception {
        AtomicBoolean failToOpen = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "new_col.d.1") && failToOpen.get()) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table x as (select x, x as y, timestamp_sequence('2022-02-24', 1000000000) ts from long_sequence(100)) timestamp(ts) partition by DAY");

            TableToken xTableToken = engine.verifyTableName("x");
            TableReader reader1 = engine.getReader(xTableToken);
            Assert.assertNotNull(reader1);
            reader1.close();
            assertSql("column\n" +
                    "1\n", "select sum(x) / sum(x) from x");

            execute("alter table x add column new_col int");
            execute("insert into x select x, x, timestamp_sequence('2022-02-25T14', 1000000000) ts, x % 2 from long_sequence(100)");
            failToOpen.set(true);

            try (TableReader reader2 = engine.getReader(xTableToken)) {
                Assert.assertSame(reader1, reader2);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open");
            }

            failToOpen.set(false);
            try (TableReader reader2 = engine.getReader(xTableToken)) {
                Assert.assertNotEquals(reader1, reader2);
            }
        });
    }

    @Test
    public void testSymbolNullFlagIsRefreshedOnReaderReload() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("create table x (sym symbol, ts timestamp) timestamp(ts) partition by year;");

            TableToken tableToken = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertFalse(reader.getSymbolMapReader(0).containsNullValue());

                execute(
                        "insert into x values ('foo', '2024-05-14T16:00:00.000000Z')," +
                                "('bar', '2024-05-14T16:00:01.000000Z')," +
                                "('baz', '2024-05-14T16:00:02.000000Z')," +
                                "(null, '2024-05-14T16:00:02.000000Z');"
                );

                reader.goPassive();
                reader.goActive();

                Assert.assertTrue(reader.getSymbolMapReader(0).containsNullValue());
            }
        });
    }

    private static void testReaderReloadDoesNotReopenPartitions() throws Exception {
        AtomicLong openCount = new AtomicLong();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "x.d")) {
                    openCount.incrementAndGet();
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table x as (select x, timestamp_sequence('2022-02-24', 1000000000) ts from long_sequence(1)) timestamp(ts) partition by HOUR" + " WAL");

            TableToken xTableToken = engine.verifyTableName("x");
            drainWalQueue();
            try (TableReader reader = engine.getReader(xTableToken)) {
                reader.openPartition(0);
                long openCountBeforeReload = openCount.get();

                reader.openPartition(0);
                Assert.assertEquals("partition should not be reloaded, file open count should stay the same", openCountBeforeReload, openCount.get());

                reader.goPassive();

                reader.goActive();
                reader.openPartition(0);
                Assert.assertEquals("partition should not be reloaded, file open count should stay the same", openCountBeforeReload, openCount.get());
                reader.goPassive();

                for (int i = 1; i < 50; i++) {
                    // Add PARTITION
                    execute("insert into x(x, ts) values (1, " + (MicrosTimestampDriver.floor("2024-02-24") + i * HOUR_MICROS) + "L)");
                    drainWalQueue();

                    reader.goActive();
                    openCountBeforeReload = openCount.get();
                    reader.openPartition(0);
                    Assert.assertEquals("partition should not be reloaded, file open count should stay the same", openCountBeforeReload, openCount.get());
                    reader.goPassive();
                }
            }
        });
    }
}
