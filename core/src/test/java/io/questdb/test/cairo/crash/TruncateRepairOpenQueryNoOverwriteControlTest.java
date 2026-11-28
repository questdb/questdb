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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * {@link TruncateRepairOpenQueryTest} with the server default for {@code cairo.o3.partition.overwrite.control.enabled}.
 * The test suite turns that check on (see Overrides), and it fails a writer that writes into rows a reader sees.
 * Production leaves it off, so nothing but the repair itself stands between the open query and the new rows.
 */
public class TruncateRepairOpenQueryNoOverwriteControlTest extends TruncateRepairOpenQueryTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // the engine reads the setting once, when the class creates it
        setProperty(PropertyKey.CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED, "false");
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testOverwriteControlIsOff() {
        Assert.assertFalse(engine.getPartitionOverwriteControl().isEnabled());
    }
}
