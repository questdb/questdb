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

        rollStrategy.setMaxSegmentSize(1);
        rollStrategy.setMaxRowCount(Long.MAX_VALUE);
        assertFalse(rollStrategy.shouldRoll(100, 1));
        assertFalse(rollStrategy.shouldRoll(100, 100));

        rollStrategy.setMaxSegmentSize(Long.MAX_VALUE);
        rollStrategy.setMaxRowCount(1);
        assertFalse(rollStrategy.shouldRoll(1, 100));
        assertFalse(rollStrategy.shouldRoll(100, 100));
    }

    @Test
    public void testSetMaxSegmentSize() {
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl(1, 1);

        rollStrategy.setMaxSegmentSize(100);
        rollStrategy.setMaxRowCount(Long.MAX_VALUE);

        assertTrue(rollStrategy.isMaxSegmentSizeSet());
        assertFalse(rollStrategy.isMaxRowCountSet());

        assertFalse(rollStrategy.shouldRoll(99, 1));
        assertFalse(rollStrategy.shouldRoll(99, 100));
        assertTrue(rollStrategy.shouldRoll(100, 1));
        assertTrue(rollStrategy.shouldRoll(100, 100));
        assertTrue(rollStrategy.shouldRoll(101, 1));
        assertTrue(rollStrategy.shouldRoll(101, 100));

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
        final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl(1, 1);

        rollStrategy.setMaxSegmentSize(Long.MAX_VALUE);
        rollStrategy.setMaxRowCount(100);

        assertFalse(rollStrategy.isMaxSegmentSizeSet());
        assertTrue(rollStrategy.isMaxRowCountSet());

        assertFalse(rollStrategy.shouldRoll(1, 99));
        assertFalse(rollStrategy.shouldRoll(100, 99));
        assertTrue(rollStrategy.shouldRoll(1, 100));
        assertTrue(rollStrategy.shouldRoll(100, 100));
        assertTrue(rollStrategy.shouldRoll(1, 101));
        assertTrue(rollStrategy.shouldRoll(100, 101));

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
}