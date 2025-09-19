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

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT;
import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT;

public class UpdateConcurrentTest extends AbstractCairoTest {
    private static final ThreadLocal<SCSequence> eventSubSequence = new ThreadLocal<>(SCSequence::new);
    private static final ThreadLocal<StringSink> readerSink = new ThreadLocal<>(StringSink::new);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 128);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 128);
        super.setUp();
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.SINGLE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 10, 25, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 10, 40, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 10, 40, PartitionMode.SINGLE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 1, 30, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 1, 50, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 1, 50, PartitionMode.SINGLE);
    }

    private void assertReader(TableReader reader, IntObjHashMap<CharSequence[]> expectedValues, IntObjHashMap<Validator> validators) throws SqlException {
        final RecordMetadata metadata = reader.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            validators.get(i).reset();
        }
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            final Record record = cursor.getRecord();
            int recordIndex = 0;
            while (cursor.hasNext()) {
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    final StringSink readerSink = UpdateConcurrentTest.readerSink.get();
                    readerSink.clear();
                    CursorPrinter.printColumn(record, metadata, i, readerSink);
                    CharSequence[] expectedValueArray = expectedValues.get(i);
                    CharSequence expectedValue = expectedValueArray != null ? expectedValueArray[recordIndex] : null;
                    if (!validators.get(i).validate(expectedValue, readerSink)) {
                        throw SqlException.$(0, "assertSql failed, recordIndex=").put(recordIndex)
                                .put(", columnIndex=").put(i)
                                .put(", expected=").put(expectedValue)
                                .put(", actual=").put(readerSink);
                    }
                }
                recordIndex++;
            }
        }
    }

    private void testConcurrency(int numOfWriters, int numOfReaders, int numOfUpdates, PartitionMode partitionMode) throws Exception {
        setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 20000); // On in CI Windows updates are particularly slow
        setProperty(CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, 30_000L);
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 20_000L);
        spinLockTimeout = 20_000L;
        assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(numOfWriters + numOfReaders);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger current = new AtomicInteger();
            ObjList<Thread> threads = new ObjList<>(numOfWriters + numOfReaders + 1);

            execute("create table up as" +
                    " (select timestamp_sequence(0, " + PartitionMode.getTimestampSeq(partitionMode) + ") ts," +
                    " 0 as x" +
                    " from long_sequence(5))" +
                    " timestamp(ts)" +
                    (PartitionMode.isPartitioned(partitionMode) ? " partition by DAY" : ""));

            Thread tick = new Thread(() -> {
                while (current.get() < numOfWriters * numOfUpdates && exceptions.isEmpty()) {
                    try (TableWriter tableWriter = getWriter(
                            "up"
                    )) {
                        tableWriter.tick();
                    } catch (EntryUnavailableException e) {
                        // ignore and re-try
                    }
                    Os.sleep(100);
                }
            });
            threads.add(tick);
            tick.start();

            for (int k = 0; k < numOfWriters; k++) {
                Thread writer = new Thread(() -> {
                    try (SqlCompiler updateCompiler = engine.getSqlCompiler()) {
                        final SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                        barrier.await();
                        for (int i = 0; i < numOfUpdates; i++) {
                            CompiledQuery cc = updateCompiler.compile("UPDATE up SET x = " + i, sqlExecutionContext);
                            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());

                            try (OperationFuture fut = cc.execute(eventSubSequence.get())) {
                                fut.await(10 * Micros.SECOND_MILLIS);
                            }
                            current.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        LOG.error().$("writer error ").$(th).$();
                        exceptions.add(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                threads.add(writer);
                writer.start();
            }

            for (int k = 0; k < numOfReaders; k++) {
                Thread reader = new Thread(() -> {
                    IntObjHashMap<CharSequence[]> expectedValues = new IntObjHashMap<>();
                    expectedValues.put(0, PartitionMode.getExpectedTimestamps(partitionMode));

                    IntObjHashMap<Validator> validators = new IntObjHashMap<>();
                    validators.put(0, Chars::equals);
                    validators.put(1, new Validator() {
                        private CharSequence value;

                        @Override
                        public void reset() {
                            value = null;
                        }

                        @Override
                        public boolean validate(CharSequence expected, CharSequence actual) {
                            if (value == null) {
                                value = actual.toString();
                            }
                            return actual.equals(value);
                        }
                    });

                    try {
                        barrier.await();
                        try (TableReader rdr = getReader("up")) {
                            while (current.get() < numOfWriters * numOfUpdates && exceptions.isEmpty()) {
                                rdr.reload();
                                assertReader(rdr, expectedValues, validators);
                                Os.sleep(1);
                            }
                        }
                    } catch (Throwable th) {
                        LOG.error().$("reader error ").$(th).$();
                        exceptions.add(th);
                    }

                    Path.clearThreadLocals();
                });
                threads.add(reader);
                reader.start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
        });
    }

    private enum PartitionMode {
        NONE, SINGLE, MULTIPLE;

        static CharSequence[] getExpectedTimestamps(PartitionMode mode) {
            return mode == MULTIPLE ?
                    new CharSequence[]{
                            "1970-01-01T00:00:00.000000Z",
                            "1970-01-01T12:00:00.000000Z",
                            "1970-01-02T00:00:00.000000Z",
                            "1970-01-02T12:00:00.000000Z",
                            "1970-01-03T00:00:00.000000Z"
                    } :
                    new CharSequence[]{
                            "1970-01-01T00:00:00.000000Z",
                            "1970-01-01T00:00:01.000000Z",
                            "1970-01-01T00:00:02.000000Z",
                            "1970-01-01T00:00:03.000000Z",
                            "1970-01-01T00:00:04.000000Z"
                    };
        }

        static long getTimestampSeq(PartitionMode mode) {
            return mode == MULTIPLE ? 43200000000L : 1000000L;
        }

        static boolean isPartitioned(PartitionMode mode) {
            return mode != NONE;
        }
    }

    private interface Validator {
        default void reset() {
        }

        boolean validate(CharSequence expected, CharSequence actual);
    }
}
