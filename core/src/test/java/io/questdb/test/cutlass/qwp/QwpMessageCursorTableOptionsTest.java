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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.FLAG_TABLE_OPTIONS;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TABLE_OPTION_TAG_DESIGNATED_TIMESTAMP_NAME;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpMessageCursorTableOptionsTest {

    @Test
    public void testBlockLengthOverrunIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            byte[] tableData = tableBlocks("t");
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            writeBytes(payload, tableData);
            payload.write(5);
            writeIntLE(payload, 1);

            withCursor(message(1, payload.toByteArray()), cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected block overrun");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("overruns trailer"));
                }
            });
        });
    }

    @Test
    public void testMissingTableOptionsBlockIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            byte[] tableData = tableBlocks("a", "b");
            byte[] optionBlock = emptyBlock();
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            writeBytes(payload, tableData);
            writeBytes(payload, optionBlock);
            writeIntLE(payload, optionBlock.length);

            withCursor(message(2, payload.toByteArray()), cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected missing table options block");
                } catch (QwpParseException e) {
                    Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                    Assert.assertEquals("missing table options block [tableIndex=1]", e.getMessage());
                }
            });
        });
    }

    @Test
    public void testMultiTableMixedOptions() throws Exception {
        assertMemoryLeak(() -> {
            byte[] message = validMessage(
                    new String[]{"a", "b", "c"},
                    timestampBlock("first_ts"),
                    emptyBlock(),
                    timestampBlock("third_ts")
            );
            withCursor(message, cursor -> {
                cursor.ofAddress();
                assertName(cursor.getCursor(), 0, "first_ts");
                Assert.assertNull(cursor.getCursor().getDesignatedTsName(1));
                assertName(cursor.getCursor(), 2, "third_ts");
                Assert.assertEquals("a", cursor.getCursor().nextTable().getTableName().toString());
                Assert.assertEquals("b", cursor.getCursor().nextTable().getTableName().toString());
                Assert.assertEquals("c", cursor.getCursor().nextTable().getTableName().toString());
                Assert.assertFalse(cursor.getCursor().hasNextTable());
            });
        });
    }

    @Test
    public void testTrailerLengthOutOfBoundsIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            writeIntLE(payload, 1);
            withCursor(message(0, payload.toByteArray()), cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected out-of-bounds trailer length");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("trailer length out of bounds"));
                }
            });
        });
    }

    @Test
    public void testTruncatedFooterIsRejected() throws Exception {
        assertMemoryLeak(() -> withCursor(
                message(0, new byte[]{1, 2, 3}),
                cursor -> {
                    try {
                        cursor.ofAddress();
                        Assert.fail("expected truncated footer");
                    } catch (QwpParseException e) {
                        Assert.assertTrue(e.getMessage().contains("truncated table options footer"));
                    }
                }
        ));
    }

    @Test
    public void testUnexpectedBytesAfterTableOptionsBlocksAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            byte[] tableData = tableBlocks("t");
            byte[] optionBlock = emptyBlock();
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            writeBytes(payload, tableData);
            writeBytes(payload, optionBlock);
            payload.write(0x7f);
            writeIntLE(payload, optionBlock.length + 1);

            withCursor(message(1, payload.toByteArray()), cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected unexpected bytes after table options blocks");
                } catch (QwpParseException e) {
                    Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                    Assert.assertEquals("unexpected bytes after table options blocks: 1", e.getMessage());
                }
            });
        });
    }

    @Test
    public void testUnknownTagsAreSkipped() throws Exception {
        assertMemoryLeak(() -> {
            ByteArrayOutputStream content = new ByteArrayOutputStream();
            content.write(0x7f);
            writeVarint(content, 3);
            writeBytes(content, new byte[]{'a', 'b', 'c'});
            writeTimestampTlv(content, "event_time");

            byte[] message = validMessage(
                    new String[]{"t"},
                    optionsBlock(content.toByteArray())
            );
            withCursor(message, cursor -> {
                cursor.ofAddress();
                assertName(cursor.getCursor(), 0, "event_time");
                Assert.assertEquals("t", cursor.getCursor().nextTable().getTableName().toString());
            });
        });
    }

    @Test
    public void testUnsignedBlockLengthOverflowIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            byte[] tableData = tableBlocks("t");
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            writeBytes(payload, tableData);
            for (int i = 0; i < 9; i++) {
                payload.write(0x80);
            }
            payload.write(0x01);
            writeIntLE(payload, 10);

            withCursor(message(1, payload.toByteArray()), cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected unsigned block length overflow");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("block overruns trailer"));
                }
            });
        });
    }

    @Test
    public void testUnsignedValueLengthOverflowIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            ByteArrayOutputStream content = new ByteArrayOutputStream();
            content.write(TABLE_OPTION_TAG_DESIGNATED_TIMESTAMP_NAME);
            for (int i = 0; i < 9; i++) {
                content.write(0x80);
            }
            content.write(0x01);

            byte[] message = validMessage(
                    new String[]{"t"},
                    optionsBlock(content.toByteArray())
            );
            withCursor(message, cursor -> {
                try {
                    cursor.ofAddress();
                    Assert.fail("expected unsigned value length overflow");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("value overruns block"));
                }
            });
        });
    }

    @Test
    public void testValidTrailer() throws Exception {
        assertMemoryLeak(() -> {
            byte[] message = validMessage(
                    new String[]{"trades"},
                    timestampBlock("event_time")
            );
            withCursor(message, cursor -> {
                cursor.ofAddress();
                assertName(cursor.getCursor(), 0, "event_time");
                Assert.assertEquals("trades", cursor.getCursor().nextTable().getTableName().toString());
                Assert.assertFalse(cursor.getCursor().hasNextTable());
            });
        });
    }

    @Test
    public void testZeroLengthBlocks() throws Exception {
        assertMemoryLeak(() -> {
            byte[] message = validMessage(
                    new String[]{"a", "b"},
                    emptyBlock(),
                    emptyBlock()
            );
            withCursor(message, cursor -> {
                cursor.ofAddress();
                Assert.assertNull(cursor.getCursor().getDesignatedTsName(0));
                Assert.assertNull(cursor.getCursor().getDesignatedTsName(1));
                cursor.getCursor().nextTable();
                cursor.getCursor().nextTable();
                Assert.assertFalse(cursor.getCursor().hasNextTable());
            });
        });
    }

    private static void assertName(QwpMessageCursor cursor, int tableIndex, String expected) {
        Utf8Sequence name = cursor.getDesignatedTsName(tableIndex);
        Assert.assertNotNull(name);
        Assert.assertEquals(expected, name.toString());
    }

    private static byte[] emptyBlock() {
        return new byte[]{0};
    }

    private static byte[] message(int tableCount, byte[] payload) {
        ByteArrayOutputStream message = new ByteArrayOutputStream();
        message.write('Q');
        message.write('W');
        message.write('P');
        message.write('1');
        message.write(QwpConstants.VERSION);
        message.write(FLAG_TABLE_OPTIONS);
        message.write(tableCount & 0xff);
        message.write((tableCount >>> 8) & 0xff);
        writeIntLE(message, payload.length);
        writeBytes(message, payload);
        return message.toByteArray();
    }

    private static byte[] optionsBlock(byte[] content) {
        ByteArrayOutputStream block = new ByteArrayOutputStream();
        writeVarint(block, content.length);
        writeBytes(block, content);
        return block.toByteArray();
    }

    private static byte[] tableBlocks(String... tableNames) {
        ByteArrayOutputStream tables = new ByteArrayOutputStream();
        for (String tableName : tableNames) {
            byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
            writeVarint(tables, nameBytes.length);
            writeBytes(tables, nameBytes);
            writeVarint(tables, 0);
            writeVarint(tables, 0);
        }
        return tables.toByteArray();
    }

    private static byte[] timestampBlock(String name) {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        writeTimestampTlv(content, name);
        return optionsBlock(content.toByteArray());
    }

    private static byte[] validMessage(String[] tableNames, byte[]... optionBlocks) {
        Assert.assertEquals(tableNames.length, optionBlocks.length);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeBytes(payload, tableBlocks(tableNames));
        ByteArrayOutputStream trailer = new ByteArrayOutputStream();
        for (byte[] optionBlock : optionBlocks) {
            writeBytes(trailer, optionBlock);
        }
        byte[] trailerBytes = trailer.toByteArray();
        writeBytes(payload, trailerBytes);
        writeIntLE(payload, trailerBytes.length);
        return message(tableNames.length, payload.toByteArray());
    }

    private static void withCursor(byte[] message, CursorConsumer consumer) throws Exception {
        long address = Unsafe.malloc(message.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < message.length; i++) {
                Unsafe.putByte(address + i, message[i]);
            }
            consumer.accept(new NativeCursor(address, message.length));
        } finally {
            Unsafe.free(address, message.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void writeBytes(ByteArrayOutputStream sink, byte[] bytes) {
        sink.write(bytes, 0, bytes.length);
    }

    private static void writeIntLE(ByteArrayOutputStream sink, int value) {
        sink.write(value & 0xff);
        sink.write((value >>> 8) & 0xff);
        sink.write((value >>> 16) & 0xff);
        sink.write((value >>> 24) & 0xff);
    }

    private static void writeTimestampTlv(ByteArrayOutputStream sink, String name) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        sink.write(TABLE_OPTION_TAG_DESIGNATED_TIMESTAMP_NAME);
        writeVarint(sink, nameBytes.length);
        writeBytes(sink, nameBytes);
    }

    private static void writeVarint(ByteArrayOutputStream sink, long value) {
        while ((value & ~0x7fL) != 0) {
            sink.write((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        sink.write((int) value);
    }

    @FunctionalInterface
    private interface CursorConsumer {
        void accept(NativeCursor cursor) throws Exception;
    }

    private static final class NativeCursor {
        private final long address;
        private final QwpMessageCursor cursor = new QwpMessageCursor();
        private final int length;

        private NativeCursor(long address, int length) {
            this.address = address;
            this.length = length;
        }

        private QwpMessageCursor getCursor() {
            return cursor;
        }

        private void ofAddress() throws QwpParseException {
            cursor.of(address, length, null);
        }
    }
}
