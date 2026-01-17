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

package io.questdb.test.cutlass.http.websocket;

import io.questdb.cutlass.ilpv4.server.IlpV4WebSocketProcessorState;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for ILP v4 WebSocket processor state management.
 */
public class IlpV4WebSocketProcessorStateTest extends AbstractWebSocketTest {

    // ==================== CREATION TESTS ====================

    @Test
    public void testStateCreation() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            Assert.assertNotNull(state);
            Assert.assertTrue(state.isOk());
        } finally {
            state.close();
        }
    }

    @Test
    public void testStateCreationWithDifferentBufferSizes() {
        int[] sizes = {256, 1024, 4096, 65536};
        for (int size : sizes) {
            IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(size);
            try {
                Assert.assertNotNull(state);
                Assert.assertEquals(size, state.getBufferCapacity());
            } finally {
                state.close();
            }
        }
    }

    // ==================== BUFFER MANAGEMENT TESTS ====================

    @Test
    public void testAddDataToBuffer() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[]{1, 2, 3, 4, 5};
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(data.length, state.getBufferPosition());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testAddMultipleDataChunks() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data1 = new byte[]{1, 2, 3};
            byte[] data2 = new byte[]{4, 5, 6, 7};
            byte[] data3 = new byte[]{8, 9};

            long ptr1 = allocateAndWrite(data1);
            long ptr2 = allocateAndWrite(data2);
            long ptr3 = allocateAndWrite(data3);

            try {
                state.addData(ptr1, ptr1 + data1.length);
                Assert.assertEquals(3, state.getBufferPosition());

                state.addData(ptr2, ptr2 + data2.length);
                Assert.assertEquals(7, state.getBufferPosition());

                state.addData(ptr3, ptr3 + data3.length);
                Assert.assertEquals(9, state.getBufferPosition());
            } finally {
                freeBuffer(ptr1, data1.length);
                freeBuffer(ptr2, data2.length);
                freeBuffer(ptr3, data3.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testBufferGrowsWhenNeeded() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(64);
        try {
            // Add data that exceeds initial buffer size
            byte[] data = new byte[128];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }

            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(data.length, state.getBufferPosition());
                Assert.assertTrue(state.getBufferCapacity() >= data.length);
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testEmptyDataChunk() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            // Add empty chunk (lo == hi)
            state.addData(0, 0);
            Assert.assertEquals(0, state.getBufferPosition());
        } finally {
            state.close();
        }
    }

    // ==================== STATE LIFECYCLE TESTS ====================

    @Test
    public void testClearState() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[]{1, 2, 3, 4, 5};
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(5, state.getBufferPosition());

                state.clear();
                Assert.assertEquals(0, state.getBufferPosition());
                Assert.assertTrue(state.isOk());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testCloseState() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        byte[] data = new byte[]{1, 2, 3};
        long ptr = allocateAndWrite(data);
        try {
            state.addData(ptr, ptr + data.length);
        } finally {
            freeBuffer(ptr, data.length);
        }

        state.close();
        // After close, buffer capacity should be 0
        Assert.assertEquals(0, state.getBufferCapacity());
    }

    @Test
    public void testDoubleClose() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        state.close();
        state.close(); // Should not throw
    }

    @Test
    public void testClearAfterClose() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        state.close();
        state.clear(); // Should not throw
    }

    // ==================== STATUS TESTS ====================

    @Test
    public void testInitialStatusIsOk() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            Assert.assertTrue(state.isOk());
            Assert.assertNull(state.getErrorMessage());
        } finally {
            state.close();
        }
    }

    @Test
    public void testSetError() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setError("Test error message");
            Assert.assertFalse(state.isOk());
            Assert.assertEquals("Test error message", state.getErrorMessage());
        } finally {
            state.close();
        }
    }

    @Test
    public void testClearResetsError() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setError("Test error");
            Assert.assertFalse(state.isOk());

            state.clear();
            Assert.assertTrue(state.isOk());
            Assert.assertNull(state.getErrorMessage());
        } finally {
            state.close();
        }
    }

    @Test
    public void testAddDataWhenInError() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setError("Error occurred");

            byte[] data = new byte[]{1, 2, 3};
            long ptr = allocateAndWrite(data);
            try {
                // When in error state, data should be ignored
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(0, state.getBufferPosition());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    // ==================== BUFFER CONTENT TESTS ====================

    @Test
    public void testGetBufferData() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[]{10, 20, 30, 40, 50};
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);

                // Read data back from buffer
                long bufAddr = state.getBufferAddress();
                for (int i = 0; i < data.length; i++) {
                    byte actual = Unsafe.getUnsafe().getByte(bufAddr + i);
                    Assert.assertEquals(data[i], actual);
                }
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testMultipleChunksPreserveOrder() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] chunk1 = new byte[]{1, 2, 3};
            byte[] chunk2 = new byte[]{4, 5};
            byte[] chunk3 = new byte[]{6, 7, 8, 9};

            long ptr1 = allocateAndWrite(chunk1);
            long ptr2 = allocateAndWrite(chunk2);
            long ptr3 = allocateAndWrite(chunk3);

            try {
                state.addData(ptr1, ptr1 + chunk1.length);
                state.addData(ptr2, ptr2 + chunk2.length);
                state.addData(ptr3, ptr3 + chunk3.length);

                // Verify all data in order
                byte[] expected = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
                long bufAddr = state.getBufferAddress();
                for (int i = 0; i < expected.length; i++) {
                    byte actual = Unsafe.getUnsafe().getByte(bufAddr + i);
                    Assert.assertEquals("Mismatch at index " + i, expected[i], actual);
                }
            } finally {
                freeBuffer(ptr1, chunk1.length);
                freeBuffer(ptr2, chunk2.length);
                freeBuffer(ptr3, chunk3.length);
            }
        } finally {
            state.close();
        }
    }

    // ==================== LARGE DATA TESTS ====================

    @Test
    public void testLargeDataChunk() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            // 64KB data
            byte[] data = new byte[65536];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i % 256);
            }

            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(data.length, state.getBufferPosition());

                // Verify data integrity
                long bufAddr = state.getBufferAddress();
                for (int i = 0; i < data.length; i++) {
                    byte actual = Unsafe.getUnsafe().getByte(bufAddr + i);
                    Assert.assertEquals("Mismatch at index " + i, data[i], actual);
                }
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testManySmallChunks() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            // Add 100 small chunks
            int totalBytes = 0;
            for (int i = 0; i < 100; i++) {
                byte[] data = new byte[]{(byte) i};
                long ptr = allocateAndWrite(data);
                try {
                    state.addData(ptr, ptr + data.length);
                    totalBytes += data.length;
                } finally {
                    freeBuffer(ptr, data.length);
                }
            }

            Assert.assertEquals(100, state.getBufferPosition());

            // Verify data
            long bufAddr = state.getBufferAddress();
            for (int i = 0; i < 100; i++) {
                byte actual = Unsafe.getUnsafe().getByte(bufAddr + i);
                Assert.assertEquals((byte) i, actual);
            }
        } finally {
            state.close();
        }
    }

    // ==================== MESSAGE PROCESSING TESTS ====================

    @Test
    public void testProcessMessageWhenEmpty() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            // Processing empty buffer should succeed (no-op)
            state.processMessage();
            Assert.assertTrue(state.isOk());
        } finally {
            state.close();
        }
    }

    @Test
    public void testProcessMessageClearsBuffer() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[]{1, 2, 3, 4, 5};
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                Assert.assertEquals(5, state.getBufferPosition());

                state.processMessage();

                // After processing, buffer position should be reset
                Assert.assertEquals(0, state.getBufferPosition());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testProcessMessageWhenInError() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setError("Previous error");

            // Processing when in error state should be a no-op
            state.processMessage();
            Assert.assertFalse(state.isOk());
        } finally {
            state.close();
        }
    }

    // ==================== RESPONSE TESTS ====================

    @Test
    public void testHasResponseInitiallyFalse() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            Assert.assertFalse(state.hasResponse());
        } finally {
            state.close();
        }
    }

    @Test
    public void testSetSuccessResponse() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setSuccessResponse();
            Assert.assertTrue(state.hasResponse());
            Assert.assertTrue(state.isResponseSuccess());
        } finally {
            state.close();
        }
    }

    @Test
    public void testSetErrorResponse() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setErrorResponse(1007, "Invalid data");
            Assert.assertTrue(state.hasResponse());
            Assert.assertFalse(state.isResponseSuccess());
            Assert.assertEquals(1007, state.getResponseErrorCode());
            Assert.assertEquals("Invalid data", state.getResponseErrorMessage());
        } finally {
            state.close();
        }
    }

    @Test
    public void testClearResetsResponse() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setSuccessResponse();
            Assert.assertTrue(state.hasResponse());

            state.clear();
            Assert.assertFalse(state.hasResponse());
        } finally {
            state.close();
        }
    }

    @Test
    public void testConsumeResponse() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            state.setSuccessResponse();
            Assert.assertTrue(state.hasResponse());

            state.consumeResponse();
            Assert.assertFalse(state.hasResponse());
        } finally {
            state.close();
        }
    }

    // ==================== BYTES PROCESSED TRACKING TESTS ====================

    @Test
    public void testBytesProcessedInitiallyZero() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            Assert.assertEquals(0, state.getBytesProcessed());
        } finally {
            state.close();
        }
    }

    @Test
    public void testBytesProcessedAfterProcessing() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[100];
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                state.processMessage();

                Assert.assertEquals(100, state.getBytesProcessed());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testBytesProcessedAccumulates() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data1 = new byte[50];
            byte[] data2 = new byte[75];

            long ptr1 = allocateAndWrite(data1);
            long ptr2 = allocateAndWrite(data2);

            try {
                state.addData(ptr1, ptr1 + data1.length);
                state.processMessage();
                Assert.assertEquals(50, state.getBytesProcessed());

                state.addData(ptr2, ptr2 + data2.length);
                state.processMessage();
                Assert.assertEquals(125, state.getBytesProcessed());
            } finally {
                freeBuffer(ptr1, data1.length);
                freeBuffer(ptr2, data2.length);
            }
        } finally {
            state.close();
        }
    }

    @Test
    public void testClearDoesNotResetBytesProcessed() {
        IlpV4WebSocketProcessorState state = new IlpV4WebSocketProcessorState(1024);
        try {
            byte[] data = new byte[100];
            long ptr = allocateAndWrite(data);
            try {
                state.addData(ptr, ptr + data.length);
                state.processMessage();
                Assert.assertEquals(100, state.getBytesProcessed());

                state.clear();
                // Bytes processed should persist across clear
                Assert.assertEquals(100, state.getBytesProcessed());
            } finally {
                freeBuffer(ptr, data.length);
            }
        } finally {
            state.close();
        }
    }

    // ==================== HELPER METHODS ====================

    private long allocateAndWrite(byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, data[i]);
        }
        return ptr;
    }
}
