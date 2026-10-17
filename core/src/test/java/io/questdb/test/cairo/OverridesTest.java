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

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class OverridesTest extends AbstractCairoTest {
    private static final int MAX_UNCOMMITTED_ROWS = 12_345;
    private static final int PART_COUNT = 16;
    private static final long WINDOW_US = 5_000;

    /**
     * A {@code setProperty} call that leaves the stored properties as they are must not cancel a change made
     * before it: the next configuration read shows every change. Runs each way to make a change, followed by
     * each kind of call that changes nothing, and reports every combination that serves a stale configuration.
     */
    @Test
    public void testUnchangingSetPropertyKeepsAPendingChange() {
        final Overrides overrides = node1.getConfigurationOverrides();
        overrides.reset();
        final int defaultPartCount = configuration.getDefaultSeqPartTxnCount();
        final long defaultWindowUs = configuration.getAdaptiveCommitGroupWindowUs();
        final int defaultMaxUncommittedRows = configuration.getMaxUncommittedRows();
        Assert.assertNotEquals(PART_COUNT, defaultPartCount);
        Assert.assertNotEquals(2 * PART_COUNT, defaultPartCount);
        Assert.assertNotEquals(WINDOW_US, defaultWindowUs);
        Assert.assertNotEquals(MAX_UNCOMMITTED_ROWS, defaultMaxUncommittedRows);

        final StringSink lost = new StringSink();
        for (Change change : Change.values()) {
            for (Unchanging unchanging : Unchanging.values()) {
                overrides.reset();
                overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_COUNT);
                overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                // builds the configuration that a lost change would keep serving
                Assert.assertEquals(PART_COUNT, configuration.getDefaultSeqPartTxnCount());

                int expectedPartCount = PART_COUNT;
                int expectedMaxUncommittedRows = defaultMaxUncommittedRows;
                switch (change) {
                    case SET_NEW_KEY -> {
                        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, MAX_UNCOMMITTED_ROWS);
                        expectedMaxUncommittedRows = MAX_UNCOMMITTED_ROWS;
                    }
                    case CHANGE_VALUE -> {
                        overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 2 * PART_COUNT);
                        expectedPartCount = 2 * PART_COUNT;
                    }
                    case REMOVE_KEY -> {
                        overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, (String) null);
                        expectedPartCount = defaultPartCount;
                    }
                    case RESET -> {
                        overrides.reset();
                        overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                        expectedPartCount = defaultPartCount;
                    }
                }

                switch (unchanging) {
                    case SAME_VALUE -> overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                    case REMOVE_ABSENT_KEY -> overrides.setProperty(PropertyKey.QUERY_TIMEOUT, (String) null);
                    case DEFAULT_OF_ABSENT_KEY -> overrides.setProperty(PropertyKey.QUERY_TIMEOUT, 0);
                }

                final int partCount = configuration.getDefaultSeqPartTxnCount();
                final long windowUs = configuration.getAdaptiveCommitGroupWindowUs();
                final int maxUncommittedRows = configuration.getMaxUncommittedRows();
                if (partCount != expectedPartCount || windowUs != WINDOW_US || maxUncommittedRows != expectedMaxUncommittedRows) {
                    lost.put(change.name()).put(" then ").put(unchanging.name())
                            .put(": partCount=").put(partCount).put(" (expected ").put(expectedPartCount)
                            .put("), windowUs=").put(windowUs).put(" (expected ").put(WINDOW_US)
                            .put("), maxUncommittedRows=").put(maxUncommittedRows).put(" (expected ").put(expectedMaxUncommittedRows)
                            .put(")\n");
                }
            }
        }
        Assert.assertEquals("a stale configuration was served after:\n" + lost, 0, lost.length());
    }

    private enum Change {
        SET_NEW_KEY, CHANGE_VALUE, REMOVE_KEY, RESET
    }

    private enum Unchanging {
        SAME_VALUE, REMOVE_ABSENT_KEY, DEFAULT_OF_ABSENT_KEY
    }
}
