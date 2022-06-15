/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class WalWriterRollStrategyTest {

    @Test
    public void testDefault() {
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategy() {
        };

        assertFalse(rollStrategy.isMaxRowCountSet());
        assertFalse(rollStrategy.isMaxSegmentSizeSet());
        assertFalse(rollStrategy.isRollIntervalSet());

        rollStrategy.setMaxSegmentSize(1L);
        rollStrategy.setMaxRowCount(1L);
        rollStrategy.setRollInterval(1L);
        assertFalse(rollStrategy.shouldRoll(0L, 0L, 0L));
        assertFalse(rollStrategy.shouldRoll(1L, 1L, 1L));
        assertFalse(rollStrategy.shouldRoll(100L, 1L, 1L));
        assertFalse(rollStrategy.shouldRoll(1L, 100L, 1L));
        assertFalse(rollStrategy.shouldRoll(1L, 1L, 100L));
        assertFalse(rollStrategy.shouldRoll(100L, 100L, 100L));
    }

    @Test
    public void testSetMaxSegmentSize() {
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
        rollStrategy.setMaxSegmentSize(100L);
        rollStrategy.setMaxRowCount(Long.MAX_VALUE);
        rollStrategy.setRollInterval(Long.MAX_VALUE);

        assertTrue(rollStrategy.isMaxSegmentSizeSet());
        assertFalse(rollStrategy.isMaxRowCountSet());
        assertFalse(rollStrategy.isRollIntervalSet());

        assertFalse(rollStrategy.shouldRoll(99L, 0L, 0L));
        assertTrue(rollStrategy.shouldRoll(100L, 0L, 0L));
        assertTrue(rollStrategy.shouldRoll(101L, 0L, 0L));

        try {
            rollStrategy.setMaxSegmentSize(0);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Max segment size cannot be less than 1 byte, maxSegmentSize=0", e.getMessage());
        }

        try {
            rollStrategy.setMaxSegmentSize(-1);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Max segment size cannot be less than 1 byte, maxSegmentSize=-1", e.getMessage());
        }
    }

    @Test
    public void testSetMaxRowCount() {
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
        rollStrategy.setMaxSegmentSize(Long.MAX_VALUE);
        rollStrategy.setMaxRowCount(100L);
        rollStrategy.setRollInterval(Long.MAX_VALUE);

        assertFalse(rollStrategy.isMaxSegmentSizeSet());
        assertTrue(rollStrategy.isMaxRowCountSet());
        assertFalse(rollStrategy.isRollIntervalSet());

        assertFalse(rollStrategy.shouldRoll(0L, 99L, 0L));
        assertTrue(rollStrategy.shouldRoll(0L, 100L, 0L));
        assertTrue(rollStrategy.shouldRoll(0L, 101L, 0L));

        try {
            rollStrategy.setMaxRowCount(0);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Max number of rows cannot be less than 1, maxRowCount=0", e.getMessage());
        }

        try {
            rollStrategy.setMaxRowCount(-1);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Max number of rows cannot be less than 1, maxRowCount=-1", e.getMessage());
        }
    }

    @Test
    public void testSetRollInterval() {
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
        rollStrategy.setMaxSegmentSize(Long.MAX_VALUE);
        rollStrategy.setMaxRowCount(Long.MAX_VALUE);
        rollStrategy.setRollInterval(100L);

        assertFalse(rollStrategy.isMaxSegmentSizeSet());
        assertFalse(rollStrategy.isMaxRowCountSet());
        assertTrue(rollStrategy.isRollIntervalSet());

        assertFalse(rollStrategy.shouldRoll(0L, 0L, 99L));
        assertTrue(rollStrategy.shouldRoll(0L, 0L, 100L));
        assertTrue(rollStrategy.shouldRoll(0L, 0L, 101L));

        try {
            rollStrategy.setRollInterval(0);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Roll interval cannot be less than 1 millisecond, rollInterval=0", e.getMessage());
        }

        try {
            rollStrategy.setRollInterval(-1);
            Assert.fail("Exception expected");
        } catch(CairoException e) {
            assertEquals("[-100] Roll interval cannot be less than 1 millisecond, rollInterval=-1", e.getMessage());
        }
    }
}