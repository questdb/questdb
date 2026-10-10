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

package io.questdb.test.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.text.types.DateUtf8Adapter;
import io.questdb.cutlass.text.types.OtherToTimestampAdapter;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.griffin.RowAsserter;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OtherToTimestampAdapterTest {

    @Test
    public void testWriteNonAsciiDateUtf8IntoCallerSink() throws Exception {
        // parallel COPY workers pass their own UTF-16 sink; the wrapped DateUtf8Adapter has none
        assertWriteNonAsciiDateUtf8(ColumnType.TIMESTAMP, 1_499_040_000_000_000L);
        assertWriteNonAsciiDateUtf8(ColumnType.TIMESTAMP_NANO, 1_499_040_000_000_000_000L);
    }

    private static void assertWriteNonAsciiDateUtf8(int timestampType, long expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectUtf16Sink workerUtf16Sink = new DirectUtf16Sink(16);
                    DirectUtf8Sink value = new DirectUtf8Sink(16)
            ) {
                value.put("3 июля 2017 г.");
                final DateUtf8Adapter dateAdapter = new DateUtf8Adapter(null).of(
                        DateFormatFactory.INSTANCE.get("d MMMM y г."),
                        DateLocaleFactory.INSTANCE.getLocale("ru-RU")
                );
                final OtherToTimestampAdapter adapter = new OtherToTimestampAdapter().of(dateAdapter, timestampType);
                final TimestampCapturingRow row = new TimestampCapturingRow();
                adapter.write(row, 1, value, workerUtf16Sink, null, null);
                Assert.assertEquals(1, row.column);
                Assert.assertEquals(expected, row.value);
            }
        });
    }

    private static class TimestampCapturingRow extends RowAsserter {
        int column = -1;
        long value = Long.MIN_VALUE;

        @Override
        public void putTimestamp(int columnIndex, long value) {
            this.column = columnIndex;
            this.value = value;
        }
    }
}
