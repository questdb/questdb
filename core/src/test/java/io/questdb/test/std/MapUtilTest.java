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

package io.questdb.test.std;


import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MapUtil;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapUtilTest {
    private static final Log LOG = LogFactory.getLog(MapUtilTest.class);
    private static final Rnd rnd = TestUtils.generateRandom(LOG);

    @Test
    public void shouldMoveWhenGapIsIdealPosition() {
        int ideal = rnd.nextPositiveInt();
        int element = rnd.nextPositiveInt();
        assertTrue(MapUtil.shouldMoveToFillGap(element, ideal, ideal));
    }

    @Test
    public void shouldMoveWhenGapBetweenIdealAndElement() {
        // [ IDEAL, GAP, ELEMENT ]
        long ideal = rnd.nextPositiveInt();
        long gap = ideal + rnd.nextPositiveInt();
        long element = gap + rnd.nextPositiveInt();
        assertTrue(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }

    @Test
    public void shouldNotMoveWhenGapBeforeIdeal() {
        // [ GAP, IDEAL, ELEMENT ]
        long gap = rnd.nextPositiveInt();
        long ideal = gap + rnd.nextPositiveInt();
        long element = ideal + rnd.nextPositiveInt();
        assertFalse(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }

    @Test
    public void shouldNotMoveWhenGapAfterElement() {
        // [ IDEAL, ELEMENT, GAP ]
        long ideal = rnd.nextPositiveInt();
        long element = ideal + rnd.nextPositiveInt();
        long gap = element + rnd.nextPositiveInt();
        assertFalse(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }

    @Test
    public void shouldMoveWhenIdealBeforeGap() {
        // [ ELEMENT, IDEAL, GAP ]  (wrap)
        long element = rnd.nextPositiveInt();
        long ideal = element + rnd.nextPositiveInt();
        long gap = ideal + rnd.nextPositiveInt();
        assertTrue(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }

    @Test
    public void shouldMoveWhenGapBeforeElement() {
        // [ GAP, ELEMENT, IDEAL ] (wrap)
        long gap = rnd.nextPositiveInt();
        long element = gap + rnd.nextPositiveInt();
        long ideal = element + rnd.nextPositiveInt();
        assertTrue(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }

    @Test
    public void shouldNotMoveWhenGapBetweenElementAndIdeal() {
        // [ ELEMENT, GAP, IDEAL ] (wrap)
        long element = rnd.nextPositiveInt();
        long gap = element + rnd.nextPositiveInt();
        long ideal = gap + rnd.nextPositiveInt();
        assertFalse(MapUtil.shouldMoveToFillGap(element, ideal, gap));
    }
}