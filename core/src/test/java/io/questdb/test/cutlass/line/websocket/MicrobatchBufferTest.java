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

import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for MicrobatchBuffer.
 */
public class MicrobatchBufferTest {

    // ==================== CONSTRUCTION TESTS ====================

    @Test
    public void testConstructionWithDefaultThresholds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(1024, buffer.getBufferCapacity());
                Assert.assertEquals(0, buffer.getBufferPos());
                Assert.assertEquals(0, buffer.getRowCount());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.hasData());
            }
        });
    }

    @Test
    public void testConstructionWithCustomThresholds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024, 100, 4096, 1_000_000_000L)) {
                Assert.assertEquals(1024, buffer.getBufferCapacity());
                Assert.assertTrue(buffer.isFilling());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithZeroCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer ignored = new MicrobatchBuffer(0)) {
                Assert.fail("Should have thrown");
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithNegativeCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer ignored = new MicrobatchBuffer(-1)) {
                Assert.fail("Should have thrown");
            }
        });
    }

    // ==================== WRITE OPERATIONS TESTS ====================

    @Test
    public void testWriteByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 0x42);
                Assert.assertEquals(1, buffer.getBufferPos());
                Assert.assertTrue(buffer.hasData());

                byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr());
                Assert.assertEquals((byte) 0x42, read);
            }
        });
    }

    @Test
    public void testWriteMultipleBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                for (int i = 0; i < 100; i++) {
                    buffer.writeByte((byte) i);
                }
                Assert.assertEquals(100, buffer.getBufferPos());

                // Verify data
                for (int i = 0; i < 100; i++) {
                    byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                    Assert.assertEquals((byte) i, read);
                }
            }
        });
    }

    @Test
    public void testWriteFromNativeMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long src = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                // Fill source with test data
                for (int i = 0; i < 10; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) (i + 100));
                }

                try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                    buffer.write(src, 10);
                    Assert.assertEquals(10, buffer.getBufferPos());

                    // Verify data
                    for (int i = 0; i < 10; i++) {
                        byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                        Assert.assertEquals((byte) (i + 100), read);
                    }
                }
            } finally {
                Unsafe.free(src, 10, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSetBufferPos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(100);
                Assert.assertEquals(100, buffer.getBufferPos());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferPosOutOfBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(2000);
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferPosNegative() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(-1);
            }
        });
    }

    // ==================== CAPACITY MANAGEMENT TESTS ====================

    @Test
    public void testEnsureCapacityNoGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.ensureCapacity(512);
                Assert.assertEquals(1024, buffer.getBufferCapacity()); // No change
            }
        });
    }

    @Test
    public void testEnsureCapacityGrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.ensureCapacity(2000);
                Assert.assertTrue(buffer.getBufferCapacity() >= 2000);
            }
        });
    }

    @Test
    public void testWriteBeyondInitialCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(16)) {
                // Write more than initial capacity
                for (int i = 0; i < 100; i++) {
                    buffer.writeByte((byte) i);
                }
                Assert.assertEquals(100, buffer.getBufferPos());
                Assert.assertTrue(buffer.getBufferCapacity() >= 100);

                // Verify data integrity after growth
                for (int i = 0; i < 100; i++) {
                    byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                    Assert.assertEquals((byte) i, read);
                }
            }
        });
    }

    // ==================== ROW COUNT TESTS ====================

    @Test
    public void testIncrementRowCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(0, buffer.getRowCount());
                buffer.incrementRowCount();
                Assert.assertEquals(1, buffer.getRowCount());
                buffer.incrementRowCount();
                Assert.assertEquals(2, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testFirstRowTimeIsRecorded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(0, buffer.getAgeNanos());

                buffer.incrementRowCount();
                long age1 = buffer.getAgeNanos();
                Assert.assertTrue(age1 >= 0);

                Thread.sleep(10);

                long age2 = buffer.getAgeNanos();
                Assert.assertTrue(age2 > age1);
            }
        });
    }

    // ==================== FLUSH THRESHOLD TESTS ====================

    @Test
    public void testShouldFlushWithNoThresholds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();
                Assert.assertFalse(buffer.shouldFlush()); // No thresholds set
            }
        });
    }

    @Test
    public void testShouldFlushRowLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024, 5, 0, 0)) {
                for (int i = 0; i < 4; i++) {
                    buffer.writeByte((byte) i);
                    buffer.incrementRowCount();
                    Assert.assertFalse(buffer.shouldFlush());
                }
                buffer.writeByte((byte) 4);
                buffer.incrementRowCount();
                Assert.assertTrue(buffer.shouldFlush());
                Assert.assertTrue(buffer.isRowLimitExceeded());
            }
        });
    }

    @Test
    public void testShouldFlushByteLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024, 0, 10, 0)) {
                for (int i = 0; i < 9; i++) {
                    buffer.writeByte((byte) i);
                    buffer.incrementRowCount();
                    Assert.assertFalse(buffer.shouldFlush());
                }
                buffer.writeByte((byte) 9);
                buffer.incrementRowCount();
                Assert.assertTrue(buffer.shouldFlush());
                Assert.assertTrue(buffer.isByteLimitExceeded());
            }
        });
    }

    @Test
    public void testShouldFlushAgeLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 50ms timeout
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024, 0, 0, 50_000_000L)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();
                Assert.assertFalse(buffer.shouldFlush());

                Thread.sleep(60);

                Assert.assertTrue(buffer.shouldFlush());
                Assert.assertTrue(buffer.isAgeLimitExceeded());
            }
        });
    }

    @Test
    public void testShouldFlushEmptyBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024, 1, 1, 1)) {
                Assert.assertFalse(buffer.shouldFlush()); // Empty buffer never flushes
            }
        });
    }

    // ==================== STATE MACHINE TESTS ====================

    @Test
    public void testInitialState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(MicrobatchBuffer.STATE_FILLING, buffer.getState());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.isSealed());
                Assert.assertFalse(buffer.isSending());
                Assert.assertFalse(buffer.isRecycled());
                Assert.assertFalse(buffer.isInUse());
            }
        });
    }

    @Test
    public void testSealTransition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.seal();

                Assert.assertEquals(MicrobatchBuffer.STATE_SEALED, buffer.getState());
                Assert.assertFalse(buffer.isFilling());
                Assert.assertTrue(buffer.isSealed());
                Assert.assertTrue(buffer.isInUse());
            }
        });
    }

    @Test
    public void testMarkSendingTransition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                Assert.assertEquals(MicrobatchBuffer.STATE_SENDING, buffer.getState());
                Assert.assertTrue(buffer.isSending());
                Assert.assertTrue(buffer.isInUse());
            }
        });
    }

    @Test
    public void testMarkRecycledTransition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();

                Assert.assertEquals(MicrobatchBuffer.STATE_RECYCLED, buffer.getState());
                Assert.assertTrue(buffer.isRecycled());
                Assert.assertFalse(buffer.isInUse());
            }
        });
    }

    @Test
    public void testResetFromRecycled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();
                long oldBatchId = buffer.getBatchId();

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                Assert.assertTrue(buffer.isFilling());
                Assert.assertEquals(0, buffer.getBufferPos());
                Assert.assertEquals(0, buffer.getRowCount());
                Assert.assertNotEquals(oldBatchId, buffer.getBatchId());
            }
        });
    }

    @Test
    public void testFullStateLifecycle() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                // FILLING
                Assert.assertTrue(buffer.isFilling());
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                // FILLING -> SEALED
                buffer.seal();
                Assert.assertTrue(buffer.isSealed());

                // SEALED -> SENDING
                buffer.markSending();
                Assert.assertTrue(buffer.isSending());

                // SENDING -> RECYCLED
                buffer.markRecycled();
                Assert.assertTrue(buffer.isRecycled());

                // RECYCLED -> FILLING (reset)
                buffer.reset();
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.hasData());
            }
        });
    }

    @Test
    public void testRollbackSealForRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                buffer.seal();
                Assert.assertTrue(buffer.isSealed());

                buffer.rollbackSealForRetry();
                Assert.assertTrue(buffer.isFilling());

                // Verify the same batch remains writable after rollback.
                buffer.writeByte((byte) 2);
                buffer.incrementRowCount();
                Assert.assertEquals(2, buffer.getBufferPos());
                Assert.assertEquals(2, buffer.getRowCount());
            }
        });
    }

    // ==================== INVALID STATE TRANSITION TESTS ====================

    @Test(expected = IllegalStateException.class)
    public void testSealWhenNotFilling() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.seal(); // Should throw
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testMarkSendingWhenNotSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.markSending(); // Should throw - not sealed
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testRollbackSealWhenNotSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.rollbackSealForRetry(); // Should throw - not sealed
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testMarkRecycledWhenNotSending() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markRecycled(); // Should throw - not sending
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testResetWhenSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.reset(); // Should throw
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testResetWhenSending() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();
                buffer.reset(); // Should throw
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteWhenSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.writeByte((byte) 1); // Should throw
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testIncrementRowCountWhenSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.incrementRowCount(); // Should throw
            }
        });
    }

    // ==================== CONCURRENCY TESTS ====================

    @Test
    public void testAwaitRecycled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                AtomicBoolean recycled = new AtomicBoolean(false);
                CountDownLatch started = new CountDownLatch(1);

                Thread waiter = new Thread(() -> {
                    started.countDown();
                    buffer.awaitRecycled();
                    recycled.set(true);
                });
                waiter.start();

                started.await();
                Thread.sleep(50); // Give waiter time to start waiting
                Assert.assertFalse(recycled.get());

                buffer.markRecycled();
                waiter.join(1000);

                Assert.assertTrue(recycled.get());
            }
        });
    }

    @Test
    public void testAwaitRecycledWithTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                // Should timeout
                boolean result = buffer.awaitRecycled(50, TimeUnit.MILLISECONDS);
                Assert.assertFalse(result);

                buffer.markRecycled();

                // Should succeed immediately now
                result = buffer.awaitRecycled(50, TimeUnit.MILLISECONDS);
                Assert.assertTrue(result);
            }
        });
    }

    @Test
    public void testConcurrentStateTransitions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                AtomicReference<Throwable> error = new AtomicReference<>();
                CountDownLatch userDone = new CountDownLatch(1);
                CountDownLatch ioDone = new CountDownLatch(1);

                // Simulate user thread
                Thread userThread = new Thread(() -> {
                    try {
                        buffer.writeByte((byte) 1);
                        buffer.incrementRowCount();
                        buffer.seal();
                        userDone.countDown();

                        // Wait for I/O thread to recycle
                        buffer.awaitRecycled();

                        // Reset and write again
                        buffer.reset();
                        buffer.writeByte((byte) 2);
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });

                // Simulate I/O thread
                Thread ioThread = new Thread(() -> {
                    try {
                        userDone.await();
                        buffer.markSending();

                        // Simulate sending
                        Thread.sleep(10);

                        buffer.markRecycled();
                        ioDone.countDown();
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });

                userThread.start();
                ioThread.start();

                userThread.join(1000);
                ioThread.join(1000);

                Assert.assertNull(error.get());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertEquals(1, buffer.getBufferPos());
            }
        });
    }

    // ==================== BATCH ID TESTS ====================

    @Test
    public void testBatchIdIncrementsOnReset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                long id1 = buffer.getBatchId();

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                long id2 = buffer.getBatchId();
                Assert.assertNotEquals(id1, id2);

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                long id3 = buffer.getBatchId();
                Assert.assertNotEquals(id2, id3);
            }
        });
    }

    // ==================== UTILITY TESTS ====================

    @Test
    public void testStateName() {
        Assert.assertEquals("FILLING", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_FILLING));
        Assert.assertEquals("SEALED", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_SEALED));
        Assert.assertEquals("SENDING", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_SENDING));
        Assert.assertEquals("RECYCLED", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_RECYCLED));
        Assert.assertEquals("UNKNOWN(99)", MicrobatchBuffer.stateName(99));
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                String str = buffer.toString();
                Assert.assertTrue(str.contains("MicrobatchBuffer"));
                Assert.assertTrue(str.contains("state=FILLING"));
                Assert.assertTrue(str.contains("rows=1"));
                Assert.assertTrue(str.contains("bytes=1"));
            }
        });
    }
}
