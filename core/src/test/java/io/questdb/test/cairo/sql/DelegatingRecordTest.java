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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.junit.Assert;
import org.junit.Test;

public class DelegatingRecordTest {

    @Test
    public void testDelegatesArrayDouble1d2d() {
        final DelegatingRecord record = new DelegatingRecord();
        record.of(new Record() {
            @Override
            public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
                Assert.assertEquals(1, col);
                Assert.assertEquals(2, columnType);
                Assert.assertEquals(3, idx0);
                Assert.assertEquals(4, idx1);
                return 42.5;
            }
        });

        Assert.assertEquals(42.5, record.getArrayDouble1d2d(1, 2, 3, 4), 0.0);
    }

    @Test
    public void testDelegatesLongIPv4() {
        final DelegatingRecord record = new DelegatingRecord();
        record.of(new Record() {
            @Override
            public long getLongIPv4(int col) {
                Assert.assertEquals(1, col);
                return 0x01020304L;
            }
        });

        Assert.assertEquals(0x01020304L, record.getLongIPv4(1));
    }

    @Test
    public void testDelegatesVarcharUtf16Sink() {
        final DelegatingRecord record = new DelegatingRecord();
        record.of(new Record() {
            @Override
            public void getVarchar(int col, Utf16Sink utf16Sink) {
                Assert.assertEquals(1, col);
                utf16Sink.put("fast");
            }
        });
        final StringSink sink = new StringSink();

        record.getVarchar(1, sink);

        Assert.assertEquals("fast", sink.toString());
    }
}
