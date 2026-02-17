/*******************************************************************************
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

package io.questdb.test.std;

import io.questdb.std.Decimal128;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf8Sink;
import org.junit.Assert;
import org.junit.Test;

public class NumericExceptionTest {
    @Test
    public void testBasic() {
        NumericException e = NumericException.instance();
        e.position(5);
        e.put("test");
        Assert.assertEquals(5, e.getPosition());
        Assert.assertEquals("test", e.getFlyweightMessage().toString());
    }

    @Test
    public void testInstanceCleaned() {
        NumericException e1 = NumericException.instance()
                .put("Overflow");
        Assert.assertEquals("Overflow", e1.getMessage());
        NumericException e2 = NumericException.instance();
        Assert.assertEquals("", e2.getMessage());
    }

    @Test
    public void testInstanceUniquePerThread() {
        NumericException e1 = NumericException.instance();
        Thread t1 = new Thread(() -> {
            NumericException e2 = NumericException.instance();
            Assert.assertNotEquals(e1, e2);
        });
        try {
            t1.start();
            t1.join();
        } catch (InterruptedException ignored) {
            Assert.fail("interrupted");
        }
    }

    @Test
    public void testPutMethods() {
        try (
                DirectUtf8Sink utf8sink = new DirectUtf8Sink(8);
                DirectUtf8Sink utf8sink2 = new DirectUtf8Sink(8)
        ) {
            Decimal128 d = new Decimal128(0, 10, 1);
            utf8sink.put(-1);
            NumericException e1 = NumericException.instance()
                    .put(1L) // 1
                    .put(2.0) // 2.0
                    .put("Overflow") // Overflow
                    .put(utf8sink) // -1
                    .put(d) // 1.0
                    .put('a') // a
                    .put(true) // true
                    .putAsPrintable("\u0000"); // \u0000

            String r = e1.getMessage();
            Assert.assertEquals("12.0Overflow-11.0atrue\\u0000", r);
            e1.toSink(utf8sink2);
            Assert.assertEquals("12.0Overflow-11.0atrue\\u0000", utf8sink2.toString());
        }
    }
}
