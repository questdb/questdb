/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Characterizes existing unflagged FLOAT/DOUBLE to text ingestion only.
 */
public class QwpSchemaFloatingTextE2ETest extends AbstractQwpWebSocketTest {
    private static final long[] DOUBLE_BITS = {
            0x0000000000000000L,
            0x8000000000000000L,
            0x3ff8000000000000L,
            0x3fb999999999999aL,
            0x7ff0000000000000L,
            0xfff0000000000000L,
            0x7ff8000000000042L,
            0xfff8000000000043L
    };
    private static final int[] FLOAT_BITS = {
            0x00000000,
            0x80000000,
            0x3fc00000,
            0x3dcccccd,
            0x7f800000,
            0xff800000,
            0x7fc12345,
            0xffc54321
    };
    private static final String[] DOUBLE_TEXT = {
            "0.0", "-0.0", "1.5", "0.1", "Infinity", "-Infinity", "NaN", "NaN"
    };
    private static final String[] FLOAT_TEXT = {
            "0.0", "-0.0", "1.5", "0.10000000149011612", "Infinity", "-Infinity", "NaN", "NaN"
    };

    @Test
    public void testLegacyFloatingTextDependsOnNullBitmapPresence() throws Exception {
        runInContext(port -> {
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    assertLegacyFloatingText(port, input, target, false);
                    assertLegacyFloatingText(port, input, target, true);
                }
            }
        });
    }

    private void assertLegacyFloatingText(int port, Input input, Target target, boolean bitmap) throws Exception {
        String tableName = "legacy_" + input.suffix + '_' + target.name().toLowerCase(Locale.ROOT)
                + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName + " (case_id long, v " + target.name()
                + ", ts timestamp) timestamp(ts) partition by day wal");
        int valueCount = input == Input.FLOAT ? FLOAT_BITS.length : DOUBLE_BITS.length;
        int rowCount = valueCount + (bitmap ? 1 : 0);
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            connectLegacy(client, port);
            QwpTableBuffer.ColumnBuffer caseId = table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false);
            QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("v", input.wireType, true);
            for (int row = 0; row < valueCount; row++) {
                caseId.addLong(row);
                if (input == Input.FLOAT) {
                    value.addFloat(Float.intBitsToFloat(FLOAT_BITS[row]));
                } else {
                    value.addDouble(Double.longBitsToDouble(DOUBLE_BITS[row]));
                }
                table.nextRow();
            }
            if (bitmap) {
                caseId.addLong(valueCount);
                table.nextRow();
            }
            int length = encoder.encode(table);
            assertRawWire(encoder, length, input, bitmap, rowCount);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client);
        }
        drainWalQueue();
        String[] expectedText = input == Input.FLOAT ? FLOAT_TEXT : DOUBLE_TEXT;
        StringBuilder expected = new StringBuilder("case_id\tv\tn\n");
        for (int row = 0; row < valueCount; row++) {
            boolean nan = row >= valueCount - 2;
            expected.append(row).append('\t');
            if (!nan || bitmap) {
                expected.append(expectedText[row]);
            }
            expected.append('\t').append(nan && !bitmap).append('\n');
        }
        if (bitmap) {
            expected.append(valueCount).append("\t\ttrue\n");
        }
        assertQuery("select case_id, v, v is null n from " + tableName + " order by case_id")
                .noLeakCheck().expectSize().returns(expected.toString());
    }

    private static void assertRawWire(
            QwpWebSocketEncoder encoder,
            int length,
            Input input,
            boolean bitmap,
            int rowCount
    ) throws Exception {
        long frame = encoder.getBuffer().getBufferPtr();
        Assert.assertEquals(0, Unsafe.getByte(frame + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(frame, length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertFalse(message.hasNextTable());
        Assert.assertEquals(rowCount, table.getRowCount());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals("case_id", table.getColumnDef(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, table.getColumnDef(0).getTypeCode());
        Assert.assertEquals("v", table.getColumnDef(1).getName());
        Assert.assertEquals(input.wireType, table.getColumnDef(1).getTypeCode());
        QwpFixedWidthColumnCursor values = table.getFixedWidthColumn(1);
        Assert.assertEquals(input.valueSize, values.getValueSize());
        Assert.assertEquals(input == Input.FLOAT ? FLOAT_BITS.length : DOUBLE_BITS.length, values.getValueCount());
        if (bitmap) {
            Assert.assertNotEquals(0, values.getNullBitmapAddress());
            Assert.assertEquals(0, Unsafe.getByte(values.getNullBitmapAddress()) & 0xff);
            Assert.assertEquals(1, Unsafe.getByte(values.getNullBitmapAddress() + 1) & 0xff);
        } else {
            Assert.assertEquals(0, values.getNullBitmapAddress());
        }
        for (int row = 0; row < values.getValueCount(); row++) {
            long address = values.getValuesAddress() + (long) row * input.valueSize;
            if (input == Input.FLOAT) {
                Assert.assertEquals(FLOAT_BITS[row], Unsafe.getInt(address));
            } else {
                Assert.assertEquals(DOUBLE_BITS[row], Unsafe.getLong(address));
            }
        }
        for (int row = 0; row < rowCount; row++) {
            Assert.assertTrue(table.hasNextRow());
            table.nextRow();
            boolean nan = row >= values.getValueCount() - 2 && row < values.getValueCount();
            Assert.assertEquals(bitmap ? row == rowCount - 1 : nan, table.isColumnNull(1));
        }
        Assert.assertFalse(table.hasNextRow());
    }

    private static void connectLegacy(WebSocketClient client, int port) throws Exception {
        client.connect("127.0.0.1", port);
        client.upgrade("/write/v4", null);
        Assert.assertFalse(client.isQwpSchemaEnabled());
    }

    private static void assertOk(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse value = new WebSocketResponse();
                Assert.assertTrue(value.readFrom(ptr, len));
                response.set(value);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        Assert.assertEquals(WebSocketResponse.STATUS_OK, response.get().getStatus());
    }

    private enum Input {
        FLOAT("float", QwpConstants.TYPE_FLOAT, Float.BYTES),
        DOUBLE("double", QwpConstants.TYPE_DOUBLE, Double.BYTES);

        private final String suffix;
        private final int valueSize;
        private final byte wireType;

        Input(String suffix, byte wireType, int valueSize) {
            this.suffix = suffix;
            this.wireType = wireType;
            this.valueSize = valueSize;
        }
    }

    private enum Target {
        STRING,
        VARCHAR,
        SYMBOL
    }
}
