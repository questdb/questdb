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
    public void testClearResetsSpec() {
        PartitionSpec s = new PartitionSpec();
        s.setTimeUnit(PartitionBy.DAY);
        s.setNamingMode(PartitionSpec.MODE_PLAIN);
        s.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
        s.addClusterColumn(2);
        Assert.assertTrue(s.isComposite());

        s.clear();

        Assert.assertEquals(0, s.getDimensionCount());
        Assert.assertEquals(0, s.getClusterColumnCount());
        Assert.assertFalse(s.isComposite());
        Assert.assertEquals(PartitionBy.NONE, s.getTimeUnit());
        Assert.assertEquals(PartitionSpec.MODE_HIVE, s.getNamingMode());
    }

    @Test
    public void testDimensionEqualsHashCode() {
        PartitionDimension a = new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null);
        PartitionDimension b = new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());

        PartitionDimension c = new PartitionDimension(PartitionDimension.KIND_HASH, 2, 16, "symbol_hash", null);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testEmptySpecIsNotComposite() {
        PartitionSpec s = new PartitionSpec();
        s.setTimeUnit(PartitionBy.DAY);
        Assert.assertFalse(s.isComposite());
        Assert.assertEquals(0, s.getDimensionCount());
    }

    @Test
    public void testCopyFromDeepCopiesContent() {
        PartitionSpec src = new PartitionSpec();
        src.setTimeUnit(PartitionBy.DAY);
        src.setNamingMode(PartitionSpec.MODE_PLAIN);
        src.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
        src.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null));
        src.addClusterColumn(2);

        PartitionSpec dest = new PartitionSpec();
        dest.copyFrom(src);

        Assert.assertTrue(dest.isComposite());
        Assert.assertEquals(PartitionBy.DAY, dest.getTimeUnit());
        Assert.assertEquals(PartitionSpec.MODE_PLAIN, dest.getNamingMode());
        Assert.assertEquals(2, dest.getDimensionCount());
        Assert.assertEquals(PartitionDimension.KIND_HASH, dest.getDimension(1).getKind());
        Assert.assertEquals(32, dest.getDimension(1).getParam());
        Assert.assertEquals("symbol_hash", dest.getDimension(1).getAlias());
        Assert.assertEquals(1, dest.getClusterColumnCount());
        Assert.assertEquals(2, dest.getClusterColumn(0));

        // PartitionDimension is immutable, so copyFrom is expected to reference-share dimension
        // instances rather than clone them -- only the PartitionSpec container itself must be
        // independent.
        Assert.assertSame(src.getDimension(0), dest.getDimension(0));
        Assert.assertSame(src.getDimension(1), dest.getDimension(1));
    }

    @Test
    public void testCopyFromIsIndependentOfLaterSourceMutation() {
        // Guards the exact aliasing hazard copyFrom exists to avoid: a caller (e.g.
        // MetadataCache's in-memory cache push) copies from a source that is later cleared and
        // repopulated in place -- as TableWriterMetadata's single PartitionSpec field is on every
        // structural ALTER. The destination must not observe any of that later mutation.
        PartitionSpec src = new PartitionSpec();
        src.setTimeUnit(PartitionBy.DAY);
        src.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));

        PartitionSpec dest = new PartitionSpec();
        dest.copyFrom(src);

        // Simulate the source being reused for a later, unrelated reload (e.g. TableWriterMetadata
        // reload() clearing and repopulating its one long-lived PartitionSpec instance).
        src.clear();
        src.setTimeUnit(PartitionBy.HOUR);
        src.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 5, 16, "other", null));
        src.addClusterColumn(9);

        Assert.assertTrue("dest must remain composite after src was cleared", dest.isComposite());
        Assert.assertEquals(PartitionBy.DAY, dest.getTimeUnit());
        Assert.assertEquals(1, dest.getDimensionCount());
        Assert.assertEquals("exchange", dest.getDimension(0).getAlias());
        Assert.assertEquals(0, dest.getClusterColumnCount());

        // And the reverse: mutating dest after the copy must not reach back into src either. src
        // already has its own cluster column (9, added above); dest's addition must not appear there.
        dest.addClusterColumn(7);
        Assert.assertEquals(1, src.getClusterColumnCount());
        Assert.assertEquals(9, src.getClusterColumn(0));
    }

    @Test
    public void testCopyFromEmptySourceLeavesDestNonComposite() {
        PartitionSpec dest = new PartitionSpec();
        dest.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
        Assert.assertTrue(dest.isComposite());

        dest.copyFrom(PartitionSpec.EMPTY);

        Assert.assertFalse(dest.isComposite());
        Assert.assertEquals(0, dest.getDimensionCount());
        Assert.assertEquals(0, dest.getClusterColumnCount());
        Assert.assertEquals(PartitionBy.NONE, dest.getTimeUnit());
        Assert.assertEquals(PartitionSpec.MODE_HIVE, dest.getNamingMode());
    }
}
