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

import io.questdb.cutlass.line.websocket.ConnectionSymbolState;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConnectionSymbolStateTest {

    @Test
    public void testFreshState_confirmedMaxIdIsMinusOne() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        assertEquals(-1, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
        assertEquals(0, state.getPendingBatchCount());
    }

    @Test
    public void testBatchSent_tracksPendingMaxId() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);
        state.onBatchSent(2, 7);

        assertEquals(3, state.getPendingBatchCount());
        assertFalse(state.isEmpty());

        assertEquals(5, state.getMaxIdForBatch(0));
        assertEquals(10, state.getMaxIdForBatch(1));
        assertEquals(7, state.getMaxIdForBatch(2));
        assertEquals(-1, state.getMaxIdForBatch(3)); // Not found
    }

    @Test
    public void testBatchAcked_updatesConfirmedMaxId() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Batch 0: maxId = 5
        state.onBatchSent(0, 5);
        assertEquals(-1, state.getConfirmedMaxId());

        // ACK batch 0
        state.onBatchesAcked(0);
        assertEquals(5, state.getConfirmedMaxId());
        assertEquals(0, state.getPendingBatchCount());
    }

    @Test
    public void testBatchAcked_cumulativeAckUpdatesWatermark() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Send batches with different maxIds
        state.onBatchSent(0, 3);   // maxId = 3
        state.onBatchSent(1, 7);   // maxId = 7
        state.onBatchSent(2, 5);   // maxId = 5
        state.onBatchSent(3, 10);  // maxId = 10

        assertEquals(4, state.getPendingBatchCount());
        assertEquals(-1, state.getConfirmedMaxId());

        // Cumulative ACK up to seq 2 (acknowledges 0, 1, 2)
        // Max across those batches: max(3, 7, 5) = 7
        state.onBatchesAcked(2);

        assertEquals(7, state.getConfirmedMaxId());
        assertEquals(1, state.getPendingBatchCount()); // Only batch 3 remains

        // ACK the remaining batch
        state.onBatchesAcked(3);
        assertEquals(10, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testBatchAcked_updatesToHighestPendingNotJustAcked() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Batches sent out of order of maxId values
        state.onBatchSent(0, 100);  // Highest maxId is early
        state.onBatchSent(1, 50);
        state.onBatchSent(2, 75);

        // ACK up to 1 (includes 0 with maxId=100 and 1 with maxId=50)
        state.onBatchesAcked(1);

        // Watermark should be 100 (highest across acked batches)
        assertEquals(100, state.getConfirmedMaxId());
        assertEquals(1, state.getPendingBatchCount());
    }

    @Test
    public void testBatchFailed_doesNotAdvanceWatermark() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);  // This one will fail
        state.onBatchSent(2, 15);

        // ACK batch 0
        state.onBatchesAcked(0);
        assertEquals(5, state.getConfirmedMaxId());

        // Fail batch 1 - should NOT advance watermark
        state.onBatchFailed(1);
        assertEquals(5, state.getConfirmedMaxId());  // Still 5, not 10
        assertEquals(1, state.getPendingBatchCount()); // Only batch 2 remains

        // ACK batch 2
        state.onBatchesAcked(2);
        assertEquals(15, state.getConfirmedMaxId());
    }

    @Test
    public void testBatchFailed_removesBatchFromPending() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);
        state.onBatchSent(2, 15);

        assertEquals(3, state.getPendingBatchCount());
        assertEquals(10, state.getMaxIdForBatch(1));

        state.onBatchFailed(1);

        assertEquals(2, state.getPendingBatchCount());
        assertEquals(-1, state.getMaxIdForBatch(1)); // No longer found
        assertEquals(5, state.getMaxIdForBatch(0));  // Still there
        assertEquals(15, state.getMaxIdForBatch(2)); // Still there
    }

    @Test
    public void testReset_clearsAllState() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);
        state.onBatchesAcked(0);

        assertEquals(5, state.getConfirmedMaxId());
        assertEquals(1, state.getPendingBatchCount());

        state.reset();

        assertEquals(-1, state.getConfirmedMaxId());
        assertEquals(0, state.getPendingBatchCount());
        assertTrue(state.isEmpty());
        assertEquals(-1, state.getMaxIdForBatch(1)); // No longer found
    }

    @Test
    public void testDuplicateAck_isIdempotent() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);

        state.onBatchesAcked(1);
        assertEquals(10, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());

        // Duplicate ACK - should be no-op
        state.onBatchesAcked(1);
        assertEquals(10, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());

        // Lower ACK - should also be no-op
        state.onBatchesAcked(0);
        assertEquals(10, state.getConfirmedMaxId());
    }

    @Test
    public void testAckOnEmptyState_isNoOp() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchesAcked(100);
        assertEquals(-1, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testBatchWithNoSymbols() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Batch with no symbols (maxId = -1)
        state.onBatchSent(0, -1);
        state.onBatchSent(1, 5);
        state.onBatchSent(2, -1);

        // ACK all
        state.onBatchesAcked(2);

        // Max should be 5 (ignoring -1 values since they don't affect watermark)
        assertEquals(5, state.getConfirmedMaxId());
    }

    @Test
    public void testDeltaCalculationScenario() {
        // Simulate real-world delta dictionary scenario
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Fresh connection: confirmedMaxId = -1
        assertEquals(-1, state.getConfirmedMaxId());

        // Batch 0: Uses symbols 0, 1, 2 (maxId = 2)
        // Delta sent: symbols [0, 2] (all of them since confirmedMaxId = -1)
        state.onBatchSent(0, 2);

        // Server ACKs batch 0
        state.onBatchesAcked(0);
        assertEquals(2, state.getConfirmedMaxId());

        // Batch 1: Uses symbols 0, 3, 4 (maxId = 4)
        // Delta to send: symbols [3, 4] (confirmedMaxId+1=3 to maxId=4)
        state.onBatchSent(1, 4);

        // Server ACKs batch 1
        state.onBatchesAcked(1);
        assertEquals(4, state.getConfirmedMaxId());

        // Batch 2: Uses only existing symbols 1, 2 (maxId = 2)
        // No delta needed since 2 <= confirmedMaxId (4)
        state.onBatchSent(2, 2);

        // Server ACKs batch 2
        state.onBatchesAcked(2);
        // Watermark stays at 4 (doesn't decrease)
        assertEquals(4, state.getConfirmedMaxId());
    }

    @Test
    public void testLargeNumberOfBatches() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Send 1000 batches
        for (int i = 0; i < 1000; i++) {
            state.onBatchSent(i, i * 2);
        }

        assertEquals(1000, state.getPendingBatchCount());

        // ACK all at once
        state.onBatchesAcked(999);

        assertEquals(999 * 2, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testPartialAcks() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Send 10 batches
        for (int i = 0; i < 10; i++) {
            state.onBatchSent(i, i);
        }

        // ACK in chunks
        state.onBatchesAcked(2);
        assertEquals(2, state.getConfirmedMaxId());
        assertEquals(7, state.getPendingBatchCount());

        state.onBatchesAcked(5);
        assertEquals(5, state.getConfirmedMaxId());
        assertEquals(4, state.getPendingBatchCount());

        state.onBatchesAcked(9);
        assertEquals(9, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testNonSequentialBatchIds() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        // Batches with gaps in sequence numbers
        state.onBatchSent(10, 5);
        state.onBatchSent(20, 10);
        state.onBatchSent(30, 15);

        // ACK up to 20
        state.onBatchesAcked(20);

        assertEquals(10, state.getConfirmedMaxId());
        assertEquals(1, state.getPendingBatchCount());
        assertEquals(15, state.getMaxIdForBatch(30));
    }

    @Test
    public void testFailMiddleBatch() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);
        state.onBatchSent(1, 10);
        state.onBatchSent(2, 15);
        state.onBatchSent(3, 20);

        // Fail batch 1
        state.onBatchFailed(1);

        // ACK up to 3 - should include 0, 2, 3 (batch 1 was removed)
        state.onBatchesAcked(3);

        // Max should be max(5, 15, 20) = 20
        assertEquals(20, state.getConfirmedMaxId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testFailNonExistentBatch() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 5);

        // Fail non-existent batch - should be no-op
        state.onBatchFailed(999);

        assertEquals(1, state.getPendingBatchCount());
        assertEquals(5, state.getMaxIdForBatch(0));
    }

    @Test
    public void testWatermarkNeverDecreases() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 100);
        state.onBatchesAcked(0);
        assertEquals(100, state.getConfirmedMaxId());

        // New batch with lower maxId
        state.onBatchSent(1, 50);
        state.onBatchesAcked(1);

        // Watermark should NOT decrease
        assertEquals(100, state.getConfirmedMaxId());
    }

    @Test
    public void testResetThenReuse() {
        ConnectionSymbolState state = new ConnectionSymbolState();

        state.onBatchSent(0, 100);
        state.onBatchesAcked(0);
        assertEquals(100, state.getConfirmedMaxId());

        // Reset (simulates reconnection)
        state.reset();
        assertEquals(-1, state.getConfirmedMaxId());

        // Start fresh
        state.onBatchSent(0, 5);
        state.onBatchesAcked(0);
        assertEquals(5, state.getConfirmedMaxId());
    }
}
