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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.PartitionSpec;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionSpecTest extends AbstractCairoTest {

    @Test
    public void testCompositeSpecShape() {
        PartitionSpec s = new PartitionSpec();
        s.setTimeUnit(PartitionBy.DAY);
        s.setNamingMode(PartitionSpec.MODE_HIVE);
        s.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
        s.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null));
        s.addClusterColumn(2);

        Assert.assertTrue(s.isComposite());
        Assert.assertEquals(PartitionBy.DAY, s.getTimeUnit());
        Assert.assertEquals(2, s.getDimensionCount());
        Assert.assertEquals(PartitionDimension.KIND_HASH, s.getDimension(1).getKind());
        Assert.assertEquals(32, s.getDimension(1).getParam());
        Assert.assertEquals("symbol_hash", s.getDimension(1).getAlias());
        Assert.assertEquals(1, s.getClusterColumnCount());
        Assert.assertEquals(2, s.getClusterColumn(0));
    }

    @Test
    public void testEmptySpecIsNotComposite() {
        PartitionSpec s = new PartitionSpec();
        s.setTimeUnit(PartitionBy.DAY);
        Assert.assertFalse(s.isComposite());
        Assert.assertEquals(0, s.getDimensionCount());
    }
}
