/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.test.tools.StationaryMicrosClock;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimestampSequenceFunctionFactoryTest extends AbstractGriffinTest {

    @BeforeClass
    public static void setUp2() {
        engine = new CairoEngine(new StaticClockCairoConfiguration(root));
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null, -1, null, queryConstants);
        bindVariableService.clear();
    }

    @Test
    public void testInitCall() throws Exception {
        final String expected = "ts\n" +
                "2021-04-25T00:00:00.000000Z\n" +
                "2021-04-25T00:00:00.300000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:01.300000Z\n" +
                "2021-04-25T00:00:01.300000Z\n" +
                "2021-04-25T00:00:01.800000Z\n" +
                "2021-04-25T00:00:02.700000Z\n" +
                "2021-04-25T00:00:03.700000Z\n";

        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("SELECT timestamp_sequence(\n" +
                            "         to_timestamp('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                            "         rnd_long(1,10,2) * 100000L\n" +
                            ") ts from long_sequence(10, 900, 800)",
                    sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }

    @Test
    public void testTimestampSequenceWithSystimestampCall() throws Exception {
        final String expected = "ac\tts\n" +
                "1\t1970-01-01T00:00:00.000000Z\n" +
                "2\t1970-01-01T00:00:00.001000Z\n" +
                "3\t1970-01-01T00:00:00.002000Z\n" +
                "4\t1970-01-01T00:00:00.003000Z\n" +
                "5\t1970-01-01T00:00:00.004000Z\n" +
                "6\t1970-01-01T00:00:00.005000Z\n" +
                "7\t1970-01-01T00:00:00.006000Z\n" +
                "8\t1970-01-01T00:00:00.007000Z\n" +
                "9\t1970-01-01T00:00:00.008000Z\n" +
                "10\t1970-01-01T00:00:00.009000Z\n";

        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("select x ac, timestamp_sequence(systimestamp(), 1000) ts from long_sequence(10)",
                    sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }

    @Test
    public void testTimestampSequenceWithZeroStartValue() throws Exception {
        final String expected = "ac\tts\n" +
                "1\t1970-01-01T00:00:00.000000Z\n" +
                "2\t1970-01-01T00:00:00.001000Z\n" +
                "3\t1970-01-01T00:00:00.002000Z\n" +
                "4\t1970-01-01T00:00:00.003000Z\n" +
                "5\t1970-01-01T00:00:00.004000Z\n" +
                "6\t1970-01-01T00:00:00.005000Z\n" +
                "7\t1970-01-01T00:00:00.006000Z\n" +
                "8\t1970-01-01T00:00:00.007000Z\n" +
                "9\t1970-01-01T00:00:00.008000Z\n" +
                "10\t1970-01-01T00:00:00.009000Z\n";

        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("select x ac, timestamp_sequence(0, 1000) ts from long_sequence(10)",
                    sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }

    private final static class StaticClockCairoConfiguration extends DefaultCairoConfiguration {

        public StaticClockCairoConfiguration(CharSequence root) {
            super(root);
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return StationaryMicrosClock.INSTANCE;
        }
    }
}