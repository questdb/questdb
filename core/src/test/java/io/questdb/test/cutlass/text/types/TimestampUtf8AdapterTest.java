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

import io.questdb.cutlass.text.types.TimestampUtf8Adapter;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.MicrosFormatFactory;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampUtf8AdapterTest {

    @Test
    public void testGetTimestampNonAsciiIntoCallerSink() throws Exception {
        // parallel COPY parses the designated timestamp into the worker's UTF-16 sink; the adapter has none
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectUtf16Sink workerUtf16Sink = new DirectUtf16Sink(16);
                    DirectUtf8Sink value = new DirectUtf8Sink(16)
            ) {
                value.put("10 déc. 2018");
                final TimestampUtf8Adapter adapter = new TimestampUtf8Adapter(null).of(
                        MicrosFormatFactory.INSTANCE.get("d MMM y"),
                        DateLocaleFactory.INSTANCE.getLocale("fr-FR"),
                        "d MMM y"
                );
                Assert.assertEquals(1_544_400_000_000_000L, adapter.getTimestamp(value, workerUtf16Sink));
            }
        });
    }
}
