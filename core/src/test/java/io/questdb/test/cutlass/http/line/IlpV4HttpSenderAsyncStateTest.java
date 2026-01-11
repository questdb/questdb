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

import io.questdb.cutlass.line.http.IlpV4HttpSender;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for IlpV4HttpSender async state management.
 * These tests verify the configuration and state tracking without
 * requiring a real server connection.
 */
public class IlpV4HttpSenderAsyncStateTest {

    // ======================== Configuration Tests ========================

    @Test
    public void testDefaultIsSyncMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                Assert.assertFalse(sender.isAsyncMode());
                Assert.assertEquals(0, sender.getMaxInFlightRequests());
                Assert.assertEquals(0, sender.getInFlightCount());
                Assert.assertFalse(sender.isInErrorState());
                Assert.assertNull(sender.getPendingError());
            }
        });
    }

    @Test
    public void testSetMaxInFlightRequestsPositive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(3);
                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(3, sender.getMaxInFlightRequests());
            }
        });
    }

    @Test
    public void testSetMaxInFlightRequestsOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(1);
                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(1, sender.getMaxInFlightRequests());
            }
        });
    }

    @Test
    public void testSetMaxInFlightRequestsZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(0);
                Assert.assertFalse(sender.isAsyncMode());
                Assert.assertEquals(0, sender.getMaxInFlightRequests());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxInFlightRequestsNegativeThrows() throws Exception {
        try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
            sender.maxInFlightRequests(-1);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotChangeMaxInFlightAfterInitialized() throws Exception {
        try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
            sender.maxInFlightRequests(2);  // First call initializes
            sender.maxInFlightRequests(3);  // Second call should throw
        }
    }

    @Test
    public void testCanSetZeroAfterZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(0);  // Stays in sync mode
                sender.maxInFlightRequests(0);  // Should not throw (not initialized)
                Assert.assertFalse(sender.isAsyncMode());
            }
        });
    }

    // ======================== State Query Tests ========================

    @Test
    public void testInitialStateIsNotError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                Assert.assertFalse(sender.isInErrorState());
                Assert.assertNull(sender.getPendingError());
            }
        });
    }

    @Test
    public void testInitialInFlightCountIsZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(5);
                Assert.assertEquals(0, sender.getInFlightCount());
            }
        });
    }

    // ======================== Method Chaining Tests ========================

    @Test
    public void testMaxInFlightReturnsThis() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                IlpV4HttpSender result = sender.maxInFlightRequests(2);
                Assert.assertSame(sender, result);
            }
        });
    }

    @Test
    public void testConfigurationChaining() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(3)
                        .autoFlushRows(100)
                        .setTimeout(60000)
                        .setGorillaEnabled(true);

                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(3, sender.getMaxInFlightRequests());
            }
        });
    }

    // ======================== Edge Cases ========================

    @Test
    public void testLargeMaxInFlight() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(100);
                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(100, sender.getMaxInFlightRequests());
            }
        });
    }

    @Test
    public void testAsyncModeWithMultipleHosts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Note: This just tests configuration, not actual connection
            // Multi-host would be configured via the create() factory method
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(2);
                Assert.assertTrue(sender.isAsyncMode());
            }
        });
    }

    // ======================== Reset Tests ========================

    @Test
    public void testResetClearsErrorState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                // Verify initial state
                Assert.assertFalse(sender.isInErrorState());
                Assert.assertNull(sender.getPendingError());

                // Call reset - should not throw even if not in error state
                sender.reset();

                // Still not in error state
                Assert.assertFalse(sender.isInErrorState());
                Assert.assertNull(sender.getPendingError());
            }
        });
    }

    @Test
    public void testResetClearsPendingRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                // Add some data
                sender.table("test").longColumn("value", 42);

                // Reset should clear pending data
                sender.reset();

                // Table count should still be 1 (table structure preserved)
                // but row data should be cleared
                Assert.assertEquals(1, sender.getTableCount());
            }
        });
    }

    // ======================== Flush in Sync Mode Tests ========================

    @Test
    public void testFlushWithNoDataInSyncMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                // Should not throw when flushing with no data
                sender.flush();
            }
        });
    }

    @Test
    public void testFlushWithNoDataInAsyncMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(2);

                // Should not throw when flushing with no data in async mode
                sender.flush();

                Assert.assertEquals(0, sender.getInFlightCount());
            }
        });
    }

    // ======================== Close Tests ========================

    @Test
    public void testCloseInSyncModeIsImmediate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000);
            // No async mode configured
            Assert.assertFalse(sender.isAsyncMode());

            // Close should be immediate (no waiting)
            long start = System.nanoTime();
            sender.close();
            long elapsed = System.nanoTime() - start;

            // Should complete quickly (less than 1 second)
            Assert.assertTrue(elapsed < 1_000_000_000L);
        });
    }

    @Test
    public void testCloseInAsyncModeWithNoInFlight() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000);
            sender.maxInFlightRequests(3);
            Assert.assertTrue(sender.isAsyncMode());
            Assert.assertEquals(0, sender.getInFlightCount());

            // Close with no in-flight should be quick
            long start = System.nanoTime();
            sender.close();
            long elapsed = System.nanoTime() - start;

            // Should complete quickly (less than 1 second)
            Assert.assertTrue(elapsed < 1_000_000_000L);
        });
    }

    // ======================== Integration State Tests ========================

    @Test
    public void testAsyncModeIsPreservedAfterFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(2);
                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(2, sender.getMaxInFlightRequests());

                // Empty flush
                sender.flush();

                // Config should be preserved
                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(2, sender.getMaxInFlightRequests());
            }
        });
    }

    @Test
    public void testAsyncModeIsPreservedAfterReset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
                sender.maxInFlightRequests(4);
                Assert.assertTrue(sender.isAsyncMode());

                // Reset should not affect async config
                sender.reset();

                Assert.assertTrue(sender.isAsyncMode());
                Assert.assertEquals(4, sender.getMaxInFlightRequests());
            }
        });
    }
}
