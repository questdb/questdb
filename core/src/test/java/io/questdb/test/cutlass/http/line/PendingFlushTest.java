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

package io.questdb.test.cutlass.http.line;

import io.questdb.cutlass.line.http.PendingFlush;
import io.questdb.cutlass.line.tcp.v4.IlpV4TableBuffer;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.TYPE_DOUBLE;
import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.TYPE_LONG;
import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.TYPE_SYMBOL;

/**
 * Tests for PendingFlush class.
 */
public class PendingFlushTest {

    @Test
    public void testEmptyPendingFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            PendingFlush flush = new PendingFlush(buffers, order, 0, 0, true, false);

            Assert.assertEquals(0, flush.getPendingRows());
            Assert.assertEquals(0, flush.getTableCount());
            Assert.assertFalse(flush.hasData());
            Assert.assertNull(flush.getBuffer("nonexistent"));
        });
    }

    @Test
    public void testSingleTablePendingFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_table");
            buffer.getOrCreateColumn("value", TYPE_LONG, false).addLong(100);
            buffer.nextRow();
            buffer.getOrCreateColumn("value", TYPE_LONG, false).addLong(200);
            buffer.nextRow();

            buffers.put("test_table", buffer);
            order.add("test_table");

            PendingFlush flush = new PendingFlush(buffers, order, 2, 0, true, false);

            Assert.assertEquals(2, flush.getPendingRows());
            Assert.assertEquals(1, flush.getTableCount());
            Assert.assertTrue(flush.hasData());
            Assert.assertSame(buffer, flush.getBuffer("test_table"));
            Assert.assertNull(flush.getBuffer("nonexistent"));
        });
    }

    @Test
    public void testMultiTablePendingFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            // Create first table
            IlpV4TableBuffer table1 = new IlpV4TableBuffer("table1");
            table1.getOrCreateColumn("x", TYPE_DOUBLE, false).addDouble(1.0);
            table1.nextRow();
            buffers.put("table1", table1);
            order.add("table1");

            // Create second table
            IlpV4TableBuffer table2 = new IlpV4TableBuffer("table2");
            table2.getOrCreateColumn("tag", TYPE_SYMBOL, true).addSymbol("a");
            table2.nextRow();
            table2.getOrCreateColumn("tag", TYPE_SYMBOL, true).addSymbol("b");
            table2.nextRow();
            buffers.put("table2", table2);
            order.add("table2");

            PendingFlush flush = new PendingFlush(buffers, order, 3, 0, true, false);

            Assert.assertEquals(3, flush.getPendingRows());
            Assert.assertEquals(2, flush.getTableCount());
            Assert.assertTrue(flush.hasData());
            Assert.assertSame(table1, flush.getBuffer("table1"));
            Assert.assertSame(table2, flush.getBuffer("table2"));

            // Verify table order is preserved
            Assert.assertEquals("table1", flush.getTableOrder().get(0));
            Assert.assertEquals("table2", flush.getTableOrder().get(1));
        });
    }

    @Test
    public void testResetBuffers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
            buffer.nextRow();
            buffers.put("test", buffer);
            order.add("test");

            Assert.assertEquals(1, buffer.getRowCount());

            PendingFlush flush = new PendingFlush(buffers, order, 1, 0, true, false);
            flush.resetBuffers();

            // After reset, row count should be 0 but column should still exist
            Assert.assertEquals(0, buffer.getRowCount());
            Assert.assertEquals(1, buffer.getColumnCount()); // Column definition preserved
        });
    }

    @Test
    public void testClearBuffers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(1);
            buffer.nextRow();
            buffers.put("test", buffer);
            order.add("test");

            PendingFlush flush = new PendingFlush(buffers, order, 1, 0, true, false);
            flush.clearBuffers();

            // After clear, buffer should be completely empty
            Assert.assertEquals(0, buffer.getRowCount());
            Assert.assertEquals(0, buffer.getColumnCount()); // Column definitions removed
            Assert.assertEquals(0, buffers.size());
            Assert.assertEquals(0, order.size());
        });
    }

    @Test
    public void testHasDataWithZeroRowsButTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("empty");
            buffers.put("empty", buffer);
            order.add("empty");

            // Has table but zero rows
            PendingFlush flush = new PendingFlush(buffers, order, 0, 0, true, false);

            Assert.assertFalse(flush.hasData());
        });
    }

    @Test
    public void testHasDataWithRowsButNoTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            // Edge case: rows > 0 but no tables (shouldn't happen but test anyway)
            PendingFlush flush = new PendingFlush(buffers, order, 5, 0, true, false);

            Assert.assertFalse(flush.hasData());
        });
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("t");
            buffer.getOrCreateColumn("x", TYPE_LONG, false).addLong(1);
            buffer.nextRow();
            buffers.put("t", buffer);
            order.add("t");

            PendingFlush flush = new PendingFlush(buffers, order, 1, 0, true, false);
            String str = flush.toString();

            Assert.assertTrue(str.contains("tables=1"));
            Assert.assertTrue(str.contains("rows=1"));
        });
    }

    @Test
    public void testBufferOwnershipTransfer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // This test verifies that PendingFlush holds references, not copies
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(42);
            buffer.nextRow();
            buffers.put("test", buffer);
            order.add("test");

            PendingFlush flush = new PendingFlush(buffers, order, 1, 0, true, false);

            // Modify original buffer
            buffer.getOrCreateColumn("v", TYPE_LONG, false).addLong(99);
            buffer.nextRow();

            // PendingFlush should see the modification (same reference)
            Assert.assertEquals(2, flush.getBuffer("test").getRowCount());

            // This demonstrates why caller should not modify buffers after creating PendingFlush
        });
    }

    @Test
    public void testInitialAddressIndexIsCaptured() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, IlpV4TableBuffer> buffers = new HashMap<>();
            ObjList<String> order = new ObjList<>();

            // Test with different address indices for thread-safety
            PendingFlush flush0 = new PendingFlush(buffers, order, 0, 0, true, false);
            Assert.assertEquals(0, flush0.getInitialAddressIndex());

            PendingFlush flush1 = new PendingFlush(buffers, order, 0, 1, true, false);
            Assert.assertEquals(1, flush1.getInitialAddressIndex());

            PendingFlush flush5 = new PendingFlush(buffers, order, 0, 5, true, false);
            Assert.assertEquals(5, flush5.getInitialAddressIndex());
        });
    }
}
