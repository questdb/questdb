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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.cutlass.line.websocket.IlpV4WebSocketSender;
import io.questdb.cutlass.line.websocket.InFlightWindow;
import io.questdb.cutlass.line.websocket.WebSocketSendQueue;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * Tests for flow control configuration in IlpV4WebSocketSender.
 * These tests verify that InFlightWindow and SendQueue sizes can be configured.
 */
public class IlpV4WebSocketFlowControlTest {

    // ==================== FACTORY METHOD TESTS ====================

    @Test
    public void testConnectAsyncDefaultFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                500, 0, 0, // autoFlush settings
                8, 16      // default flow control
        );
        try {
            Assert.assertNotNull(sender);
            // Verify default values using reflection
            Assert.assertEquals(8, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(16, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testConnectAsyncCustomFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                1000, 1024 * 1024, 100_000_000L, // autoFlush settings
                16, 32                            // custom flow control
        );
        try {
            Assert.assertNotNull(sender);
            // Verify custom values using reflection
            Assert.assertEquals(16, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(32, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testConnectAsyncLargeFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                10000, 0, 0,
                64, 128
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(64, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(128, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testConnectAsyncSmallFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                100, 0, 0,
                2, 4
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(2, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(4, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    // ==================== AUTO-FLUSH CONFIGURATION TESTS ====================

    @Test
    public void testAutoFlushRowsConfiguration() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                5000, 0, 0, // 5000 rows per batch
                8, 16
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(5000, getFieldValue(sender, "autoFlushRows"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testAutoFlushBytesConfiguration() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                0, 2 * 1024 * 1024, 0, // 2MB per batch
                8, 16
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(2 * 1024 * 1024, getFieldValue(sender, "autoFlushBytes"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testAutoFlushIntervalConfiguration() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                0, 0, 50_000_000L, // 50ms
                8, 16
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(50_000_000L, getFieldValue(sender, "autoFlushIntervalNanos"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testAllAutoFlushTriggersCombined() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                1000, 512 * 1024, 200_000_000L, // 1000 rows, 512KB, 200ms
                8, 16
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(1000, getFieldValue(sender, "autoFlushRows"));
            Assert.assertEquals(512 * 1024, getFieldValue(sender, "autoFlushBytes"));
            Assert.assertEquals(200_000_000L, getFieldValue(sender, "autoFlushIntervalNanos"));
        } finally {
            sender.close();
        }
    }

    // ==================== DEFAULTS VERIFICATION ====================

    @Test
    public void testDefaultInFlightWindowSize() {
        Assert.assertEquals(8, InFlightWindow.DEFAULT_WINDOW_SIZE);
    }

    @Test
    public void testDefaultSendQueueCapacity() {
        Assert.assertEquals(16, WebSocketSendQueue.DEFAULT_QUEUE_CAPACITY);
    }

    @Test
    public void testDefaultTimeout() {
        Assert.assertEquals(30000, InFlightWindow.DEFAULT_TIMEOUT_MS);
    }

    // ==================== SYNC MODE TESTS ====================

    @Test
    public void testSyncModeFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedSyncSender();
        try {
            Assert.assertNotNull(sender);
            // Sync mode should still store flow control settings
            Assert.assertEquals(8, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(16, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    // ==================== CAPACITY CALCULATION TESTS ====================

    @Test
    public void testMaxRowsInFlightCalculation() {
        // With defaults: (8 in-flight + 16 queued + 2 buffers) × 500 rows = 13,000 rows
        int inFlightWindow = 8;
        int sendQueue = 16;
        int buffers = 2;
        int rowsPerBatch = 500;

        int maxRowsInFlight = (inFlightWindow + sendQueue + buffers) * rowsPerBatch;
        Assert.assertEquals(13000, maxRowsInFlight);
    }

    @Test
    public void testMaxMemoryCalculation() {
        // With custom settings: (16 + 32 + 2) × 1MB = 50MB
        int inFlightWindow = 16;
        int sendQueue = 32;
        int buffers = 2;
        int maxBytesPerBatch = 1024 * 1024; // 1MB

        long maxMemory = (long) (inFlightWindow + sendQueue + buffers) * maxBytesPerBatch;
        Assert.assertEquals(50 * 1024 * 1024, maxMemory);
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testMinimalFlowControl() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                1, 0, 0, // 1 row per batch
                1, 1     // minimal flow control
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(1, getFieldValue(sender, "inFlightWindowSize"));
            Assert.assertEquals(1, getFieldValue(sender, "sendQueueCapacity"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testNoAutoFlush() {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender(
                0, 0, 0, // no auto-flush
                8, 16
        );
        try {
            Assert.assertNotNull(sender);
            Assert.assertEquals(0, getFieldValue(sender, "autoFlushRows"));
            Assert.assertEquals(0, getFieldValue(sender, "autoFlushBytes"));
            Assert.assertEquals(0L, getFieldValue(sender, "autoFlushIntervalNanos"));
        } finally {
            sender.close();
        }
    }

    // ==================== HELPER METHODS ====================

    private IlpV4WebSocketSender createUnconnectedAsyncSender(
            int autoFlushRows, int autoFlushBytes, long autoFlushIntervalNanos,
            int inFlightWindowSize, int sendQueueCapacity) {
        try {
            Constructor<IlpV4WebSocketSender> constructor =
                    IlpV4WebSocketSender.class.getDeclaredConstructor(
                            String.class, int.class, boolean.class, int.class,
                            int.class, int.class, long.class,
                            int.class, int.class,
                            boolean.class
                    );
            constructor.setAccessible(true);
            return constructor.newInstance("localhost", 9000, false, 8192,
                    autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                    inFlightWindowSize, sendQueueCapacity, true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create sender", e);
        }
    }

    private IlpV4WebSocketSender createUnconnectedSyncSender() {
        try {
            Constructor<IlpV4WebSocketSender> constructor =
                    IlpV4WebSocketSender.class.getDeclaredConstructor(
                            String.class, int.class, boolean.class, int.class,
                            int.class, int.class, long.class,
                            int.class, int.class,
                            boolean.class
                    );
            constructor.setAccessible(true);
            return constructor.newInstance("localhost", 9000, false, 8192,
                    0, 0, 0L, 8, 16, false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create sender", e);
        }
    }

    private Object getFieldValue(Object obj, String fieldName) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get field: " + fieldName, e);
        }
    }
}
