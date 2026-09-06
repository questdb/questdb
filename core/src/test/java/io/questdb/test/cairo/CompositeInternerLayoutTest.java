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

import io.questdb.cairo.CompositeInternerLayout;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.PartitionSpec;
import io.questdb.cairo.TableUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompositeInternerLayoutTest {
    @Test
    public void testLayoutOrdersDedicatedDictsThenRegistry() {
        // spec: identity(exchange) [no dict], hash(symbol,32) [no dict], truncate(symbol,3) [dict]
        PartitionSpec spec = new PartitionSpec();
        spec.setTimeUnit(PartitionBy.DAY);
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null));
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_TRUNCATE, 2, 3, "symbol_trunc", null));
        CompositeInternerLayout l = CompositeInternerLayout.of(spec);
        Assert.assertEquals(1, l.dedicatedCount());            // only the truncate dim
        Assert.assertFalse(l.needsDedicatedDict(0));
        Assert.assertFalse(l.needsDedicatedDict(1));
        Assert.assertTrue(l.needsDedicatedDict(2));
        Assert.assertEquals(0, l.dedicatedDictSlot(2));        // first (and only) dedicated dict
        Assert.assertEquals(1, l.registrySlot());              // registry after the 1 dedicated dict
        Assert.assertEquals("symbol_trunc", l.dictName(2).toString());
        Assert.assertTrue(l.dictColumnNameTxn(2) > TableUtils.COLUMN_NAME_TXN_NONE);
    }

    @Test
    public void testClusterOnlyTableHasNoInterners() {
        // spec: DAY partitioning + cluster column, zero dimensions
        PartitionSpec spec = new PartitionSpec();
        spec.setTimeUnit(PartitionBy.DAY);
        spec.addClusterColumn(0);
        Assert.assertTrue(spec.isComposite()); // cluster-only is composite but needs no interners
        CompositeInternerLayout l = CompositeInternerLayout.of(spec);
        Assert.assertFalse(l.hasInterners());
        Assert.assertEquals(-1, l.registrySlot());
        Assert.assertEquals(0, l.dedicatedCount());
    }

    @Test
    public void testEmptySpecHasNoInterners() {
        // spec: completely empty, no partitioning, no clustering
        PartitionSpec spec = new PartitionSpec();
        CompositeInternerLayout l = CompositeInternerLayout.of(spec);
        Assert.assertFalse(l.hasInterners());
        Assert.assertEquals(-1, l.registrySlot());
    }

    @Test
    public void testDedicatedDictSlotsSkipNonDedicatedDims() {
        // spec: DAY + truncate(colA) + hash(colB) + truncate(colC)
        // Only truncate dims (0, 2) need dedicated dicts; hash dim (1) does not.
        // Dedicated slots should be 0 and 1 for the two truncate dims, but hash remains -1.
        PartitionSpec spec = new PartitionSpec();
        spec.setTimeUnit(PartitionBy.DAY);
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_TRUNCATE, 1, 3, "a_trunc", null));
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "b_hash", null));
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_TRUNCATE, 3, 4, "c_trunc", null));
        CompositeInternerLayout l = CompositeInternerLayout.of(spec);
        Assert.assertEquals(2, l.dedicatedCount());
        Assert.assertTrue(l.needsDedicatedDict(0));
        Assert.assertFalse(l.needsDedicatedDict(1));
        Assert.assertTrue(l.needsDedicatedDict(2));
        Assert.assertEquals(0, l.dedicatedDictSlot(0));        // first dedicated dict
        Assert.assertEquals(-1, l.dedicatedDictSlot(1));       // hash needs no dict
        Assert.assertEquals(1, l.dedicatedDictSlot(2));        // second dedicated dict (not 2!)
        Assert.assertEquals(2, l.registrySlot());              // registry after the 2 dedicated dicts
        Assert.assertNotEquals(l.dictColumnNameTxn(0), l.dictColumnNameTxn(2)); // unique per dimension
        Assert.assertTrue(l.dictColumnNameTxn(0) > TableUtils.COLUMN_NAME_TXN_NONE);
        Assert.assertTrue(l.dictColumnNameTxn(2) > TableUtils.COLUMN_NAME_TXN_NONE);
    }

    @Test
    public void testExpressionDimNeedsDedicatedDict() {
        // spec: DAY + expression dimension
        PartitionSpec spec = new PartitionSpec();
        spec.setTimeUnit(PartitionBy.DAY);
        spec.addDimension(new PartitionDimension(PartitionDimension.KIND_EXPRESSION, -1, 0, "myexpr", "col + 1"));
        CompositeInternerLayout l = CompositeInternerLayout.of(spec);
        Assert.assertTrue(l.needsDedicatedDict(0));
        Assert.assertEquals(0, l.dedicatedDictSlot(0));
        Assert.assertTrue(l.hasInterners());
    }
}
