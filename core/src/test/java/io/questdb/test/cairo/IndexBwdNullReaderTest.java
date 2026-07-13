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

import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class IndexBwdNullReaderTest {
    private static final IndexBwdNullReader reader = new IndexBwdNullReader(-1, -1);

    @Test
    public void testAlwaysOpen() {
        Assert.assertTrue(reader.isOpen());
    }

    @Test
    public void testCursor() {
        final Rnd rnd = new Rnd();
        for (int i = 0; i < 10; i++) {
            final int n = rnd.nextPositiveInt() % 1024;

            int m = n;
            try (RowCursor cursor = reader.getCursor(0, 0, n)) {
                while (cursor.hasNext()) {
                    Assert.assertEquals(m--, cursor.next());
                }
            }
            Assert.assertEquals(-1, m);

            // non-null key
            try (RowCursor cursor = reader.getCursor(42, 0, n)) {
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testCursorNotRepooledWhenClosedOffOperatingThread() throws Exception {
        // Fresh reader so the free-cursor pool is not shared with the other tests.
        final IndexBwdNullReader localReader = new IndexBwdNullReader(-1, -1);

        // Positive control: an on-thread close re-pools the cursor, so the next
        // getCursor() on the same (operating) thread hands back the same instance.
        final RowCursor c1 = localReader.getCursor(0, 0, 16);
        c1.close();
        final RowCursor c2 = localReader.getCursor(0, 0, 16);
        Assert.assertSame("on-thread close should re-pool the cursor", c1, c2);

        // c2 is checked out again. Close it on a different thread: the operating-
        // thread gate must skip re-pooling (getCursor() stamped c2's reader to
        // this thread), so the pool stays empty and the next getCursor() allocates
        // a fresh instance. Without the gate the off-thread close would re-pool c2
        // and getCursor() would hand back the very cursor still being torn down.
        closeOffThread(c2);
        final RowCursor c3 = localReader.getCursor(0, 0, 16);
        Assert.assertNotSame("off-thread close must not re-pool the cursor", c2, c3);
        c3.close();
    }

    @Test
    public void testKeyCount() {
        // has to be always 1
        Assert.assertEquals(1, reader.getKeyCount());
    }

    @Test
    public void testMemoryGetters() {
        Assert.assertEquals(0, reader.getKeyBaseAddress());
        Assert.assertEquals(0, reader.getKeyMemorySize());
        Assert.assertEquals(0, reader.getValueBaseAddress());
        Assert.assertEquals(0, reader.getValueMemorySize());
        Assert.assertEquals(0, reader.getColumnTop());
        Assert.assertEquals(0, reader.getValueBlockCapacity());
    }

    private static void closeOffThread(RowCursor cursor) throws InterruptedException {
        final Thread t = new Thread(cursor::close);
        t.start();
        t.join();
    }
}
