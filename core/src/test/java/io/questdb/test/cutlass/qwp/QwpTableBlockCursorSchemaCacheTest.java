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


package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpSchemaCache;
import io.questdb.cutlass.qwp.protocol.QwpSchemaCacheEntry;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableHeader;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_LONG;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * The block cursor keeps each table's last schema parse in its {@link QwpSchemaCache},
 * which owns the entries: the cursor itself keeps none.
 */
public class QwpTableBlockCursorSchemaCacheTest {

    @Test
    public void testIdenticalSchemaReusesParseAndDifferentSchemaDoesNot() throws Exception {
        // assertMemoryLeak() tracks only the server's native memory counter. The
        // client encoder and table buffer allocate through the client's own.
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                decoder.setSchemaCache(new TestSchemaCache("test_table"));
                ObjList<QwpColumnDef> first = decode(decoder, "val");
                Assert.assertEquals("val", first.getQuick(0).getName());
                // Same column names and types, next message: the cursor must hand
                // back the previous parse rather than allocate a new one.
                ObjList<QwpColumnDef> second = decode(decoder, "val");
                Assert.assertSame(first, second);
                // Same column count and name length, different bytes: a fresh parse.
                ObjList<QwpColumnDef> third = decode(decoder, "vbl");
                Assert.assertNotSame(first, third);
                Assert.assertEquals("vbl", third.getQuick(0).getName());
                // And back again: the cache now holds the latest schema.
                Assert.assertSame(third, decode(decoder, "vbl"));
                Assert.assertNotSame(third, decode(decoder, "val"));
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    /** Blocks alternating between tables, even with identical columns, parse each table's schema once. */
    @Test
    public void testInterleavedTablesKeepTheirParses() throws Exception {
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                final TestSchemaCache cache = new TestSchemaCache("a", "b", "c");
                decoder.setSchemaCache(cache);
                ObjList<QwpColumnDef> a = decode(decoder, "a", "val");
                ObjList<QwpColumnDef> b = decode(decoder, "b", "other");
                ObjList<QwpColumnDef> c = decode(decoder, "c", "val");
                Assert.assertNotSame(a, b);
                Assert.assertNotSame("a table keeps its own parse, whatever its columns", a, c);
                for (int i = 0; i < 3; i++) {
                    Assert.assertSame(a, decode(decoder, "a", "val"));
                    Assert.assertSame(b, decode(decoder, "b", "other"));
                    Assert.assertSame(c, decode(decoder, "c", "val"));
                }
                // A table's new schema replaces its parse and leaves the others alone.
                ObjList<QwpColumnDef> b2 = decode(decoder, "b", "val");
                Assert.assertNotSame(b, b2);
                Assert.assertSame(a, decode(decoder, "a", "val"));
                Assert.assertSame(b2, decode(decoder, "b", "val"));
                Assert.assertSame(b2, cache.entry("b").getColumns());
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    /** Also a decoder without a cache. */
    @Test
    public void testTableWithoutSlotParsesEveryBlock() throws Exception {
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                ObjList<QwpColumnDef> first = decode(decoder, "a", "val");
                Assert.assertNotSame(first, decode(decoder, "a", "val"));
                decoder.setSchemaCache(new TestSchemaCache("b"));
                first = decode(decoder, "a", "val");
                Assert.assertNotSame("a has no slot", first, decode(decoder, "a", "val"));
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    /** A block whose data ends inside its schema section parses it, fails, and leaves the slot without a parse. */
    @Test
    public void testTruncatedSchemaSkipsCacheAndLeavesNoParse() throws Exception {
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                final TestSchemaCache cache = new TestSchemaCache("a");
                decoder.setSchemaCache(cache);
                Assert.assertSame(decode(decoder, "a", "val"), cache.entry("a").getColumns());
                Assert.assertTrue(cache.entry("a").getRetainedBytes() > 0);
                try (
                        QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                        QwpTableBuffer buffer = new QwpTableBuffer("a")
                ) {
                    buffer.getOrCreateColumn("val", TYPE_LONG, false).addLong(1);
                    buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP).addLong(1_000_000_000_000L);
                    buffer.nextRow();
                    final int size = encoder.encode(buffer);
                    final long address = encoder.getBuffer().getBufferPtr();
                    // Cut the payload one byte short of the end of the schema section.
                    final long payload = address + QwpConstants.HEADER_SIZE;
                    final QwpTableHeader header = new QwpTableHeader(QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE);
                    header.parse(payload, size - QwpConstants.HEADER_SIZE);
                    final QwpSchema.ParseResult schema = new QwpSchema.ParseResult();
                    QwpSchema.parse(
                            payload + header.getBytesConsumed(),
                            size - QwpConstants.HEADER_SIZE - header.getBytesConsumed(),
                            header.getColumnCount(),
                            schema
                    );
                    final int payloadLength = header.getBytesConsumed() + schema.bytesConsumed - 1;
                    Unsafe.getUnsafe().putInt(address + 8, payloadLength);
                    try {
                        decoder.decode(address, QwpConstants.HEADER_SIZE + payloadLength).nextTable();
                        Assert.fail("a truncated schema section must not parse");
                    } catch (QwpParseException expected) {
                        // The schema parse's error: a cache hit would have skipped the parse
                        // and failed later, on column data past the block's end.
                        Assert.assertEquals(QwpParseException.ErrorCode.HEADER_TOO_SHORT, expected.getErrorCode());
                    }
                }
                Assert.assertNull("a failed parse leaves no parse in the slot", cache.entry("a").getColumns());
                Assert.assertEquals(0, cache.entry("a").getRetainedBytes());
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    private static ObjList<QwpColumnDef> decode(QwpStreamingDecoder decoder, String columnName) throws Exception {
        return decode(decoder, "test_table", columnName);
    }

    private static ObjList<QwpColumnDef> decode(QwpStreamingDecoder decoder, String tableName, String columnName) throws Exception {
        // The encoder does not own the table buffer; both hold native memory.
        try (
                QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer buffer = new QwpTableBuffer(tableName)
        ) {
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn(columnName, TYPE_LONG, false);
            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
            for (int i = 0; i < 3; i++) {
                col.addLong(i);
                tsCol.addLong(1_000_000_000_000L + i);
                buffer.nextRow();
            }
            int size = encoder.encode(buffer);
            QwpMessageCursor msg = decoder.decode(encoder.getBuffer().getBufferPtr(), size);
            Assert.assertTrue(msg.hasNextTable());
            QwpTableBlockCursor table = msg.nextTable();
            Assert.assertEquals(3, table.getRowCount());
            return table.getSchema();
        }
    }

    // A slot per named table, like a connection's table cache gives the tables it holds open.
    private static final class TestSchemaCache implements QwpSchemaCache {
        private final Utf8SequenceObjHashMap<QwpSchemaCacheEntry> entries = new Utf8SequenceObjHashMap<>();

        TestSchemaCache(String... tables) {
            for (String table : tables) {
                entries.put(new Utf8String(table), new QwpSchemaCacheEntry());
            }
        }

        @Override
        public QwpSchemaCacheEntry getSchemaCacheEntry(DirectUtf8Sequence tableName) {
            return entries.get(tableName);
        }

        QwpSchemaCacheEntry entry(String table) {
            return entries.get(new Utf8String(table));
        }
    }
}
