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

import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecord;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;
import io.questdb.mp.SCSequence;
import io.questdb.std.Os;
import io.questdb.std.str.Utf8Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

public class DumpThreadStacksTest extends AbstractCairoTest {

    @Test
    public void testRenderingFailureFallsBackAndReleasesRecord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicReference<SCSequence> consumerSequence = new AtomicReference<>();
            try (LogFactory factory = new LogFactory()) {
                factory.add(new LogWriterConfig(LogLevel.ADVISORY, (ring, sequence, level) -> {
                    consumerSequence.set(sequence);
                    return new LogWriter() {
                        @Override
                        public void bindProperties(LogFactory factory) {
                        }

                        @Override
                        public boolean run(@NotNull WorkerContext workerContext) {
                            return false;
                        }
                    };
                }));
                factory.bind();

                final Log logger = factory.create("dump-thread-stacks-test");
                final LogRecord record = logger.advisory();
                final RuntimeException renderingFailure = new RuntimeException("rendering failure");
                final RuntimeException recordReuseFailure = new RuntimeException("released record reused");
                final Field sinkField = record.getClass().getDeclaredField("sink");
                sinkField.setAccessible(true);
                final Object originalSink = sinkField.get(record);
                sinkField.set(record, new LogRecordUtf8Sink(0, 0) {
                    @Override
                    public Utf8Sink putAscii(char c) {
                        throw renderingFailure;
                    }

                    @Override
                    public Utf8Sink putAscii(CharSequence cs) {
                        throw recordReuseFailure;
                    }

                    @Override
                    public Utf8Sink putEOL() {
                        return this;
                    }
                });
                final ThreadInfo threadInfo = ManagementFactory.getThreadMXBean().getThreadInfo(
                        Thread.currentThread().threadId(),
                        20
                );
                Assert.assertNotNull(threadInfo);

                try {
                    final ByteArrayOutputStream errorBuffer = new ByteArrayOutputStream();
                    try (PrintStream errorStream = new PrintStream(errorBuffer, true, StandardCharsets.UTF_8)) {
                        DumpThreadStacksFunctionFactory.dumpThreadStack(threadInfo, record, errorStream);
                    }

                    final String error = errorBuffer.toString(StandardCharsets.UTF_8);
                    TestUtils.assertContains(error, "error dumping threads");
                    TestUtils.assertContains(error, "java.lang.RuntimeException: rendering failure");

                    final SCSequence sequence = consumerSequence.get();
                    long cursor = sequence.next();
                    Assert.assertEquals(0, cursor);
                    sequence.done(cursor);

                    logger.advisory().$("after failure").$();
                    cursor = sequence.next();
                    Assert.assertEquals(1, cursor);
                    sequence.done(cursor);
                } finally {
                    sinkField.set(record, originalSink);
                    record.$();
                }
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("select dump_thread_stacks")
                .expectSize()
                .returns("""
                        dump_thread_stacks
                        true
                        """);
        // this sleep to allow async logger to print out the values,
        // although we don't assert them it is less awkward than calling
        // the dump and see no output in the logs
        Os.sleep(500);
    }
}
