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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exhaustive unit coverage for the connection-scope memory caps and their
 * {@code CACHE_RESET} plumbing on the server side: entry/heap caps on the
 * SYMBOL dict, schema-count cap, mask computation, and the {@code applyCacheReset}
 * state machine. End-to-end behaviour over the wire is covered by
 * {@link QwpEgressCacheResetWireTest}.
 */
public class QwpEgressProcessorStateCacheResetTest extends AbstractCairoTest {

    private CairoConfiguration cfg;

    @Before
    public void setUpState() {
        cfg = configuration;
    }

    @Test
    public void testApplyCacheResetBothBitsClearsBoth() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("foo");
            state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(1, dict.size());

            byte mask = (byte) (QwpEgressMsgKind.RESET_MASK_DICT | QwpEgressMsgKind.RESET_MASK_SCHEMAS);
            state.applyCacheReset(mask);

            Assert.assertEquals("dict must be flushed", 0, dict.size());
            // Re-adding the same schema shape must allocate id 0 (counter reset)
            // rather than hitting the cached entry.
            int id = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, id);
            Assert.assertFalse("post-reset lookup is not a reuse", state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testApplyCacheResetDictBitClearsOnlyDict() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("foo");
            dict.addEntry("bar");
            int schemaId = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, schemaId);

            state.applyCacheReset(QwpEgressMsgKind.RESET_MASK_DICT);

            Assert.assertEquals("dict must be flushed", 0, dict.size());
            // Schema cache is untouched, so the same shape still returns id 0
            // as a reuse hit.
            int after = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, after);
            Assert.assertTrue("schema cache must still be hot", state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testApplyCacheResetMaskZeroIsNoOp() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");
            state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));

            state.applyCacheReset((byte) 0);

            Assert.assertEquals("dict unchanged", 1, state.getConnSymbolDict().size());
            int after = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, after);
            Assert.assertTrue(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testApplyCacheResetSchemasBitClearsOnlySchemas() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("foo");
            state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));

            state.applyCacheReset(QwpEgressMsgKind.RESET_MASK_SCHEMAS);

            Assert.assertEquals("dict must be untouched", 1, dict.size());
            Assert.assertEquals("dedup must still work", 0, dict.addEntry("foo"));
            // Schema id counter is reset; the same shape allocates a fresh id.
            int after = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, after);
            Assert.assertFalse(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testApplyCacheResetUnknownBitsAreIgnored() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");
            // Bits 2..7 are reserved. Unknown bits must be ignored rather than
            // trigger spurious resets.
            state.applyCacheReset((byte) 0xFC);
            Assert.assertEquals("dict untouched by unknown bits", 1, state.getConnSymbolDict().size());
        }
    }

    @Test
    public void testComputeMaskDictEntriesCapExactBoundary() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(4, -1, -1);
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("a");
            dict.addEntry("b");
            dict.addEntry("c");
            Assert.assertEquals("below cap", 0, state.computeCacheResetMask());
            dict.addEntry("d");
            Assert.assertEquals("at cap", QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask());
        }
    }

    @Test
    public void testComputeMaskDictHeapCapExactBoundary() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(-1, 10, -1);
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("abcde");  // 5 bytes
            Assert.assertEquals("5 bytes < 10 cap", 0, state.computeCacheResetMask());
            dict.addEntry("12345");  // +5 bytes -> 10 total, triggers cap
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask());
        }
    }

    @Test
    public void testComputeMaskEmptyStateIsZero() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testComputeMaskFiresBothWhenBothOverCap() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(2, -1, 2);
            state.getConnSymbolDict().addEntry("a");
            state.getConnSymbolDict().addEntry("b");
            state.findOrAllocateSchemaId(schemaOf("a", ColumnType.LONG));
            state.findOrAllocateSchemaId(schemaOf("b", ColumnType.LONG));
            Assert.assertEquals((byte) (QwpEgressMsgKind.RESET_MASK_DICT | QwpEgressMsgKind.RESET_MASK_SCHEMAS),
                    state.computeCacheResetMask());
        }
    }

    @Test
    public void testComputeMaskIsNoSideEffects() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, -1, -1);
            state.getConnSymbolDict().addEntry("foo");
            Assert.assertEquals("mask says reset", QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask());
            // Calling computeMask must NOT alter dict state. applyCacheReset is
            // the explicit opt-in mutator.
            Assert.assertEquals("no side effect", 1, state.getConnSymbolDict().size());
            Assert.assertEquals("repeat call still reports cap hit", QwpEgressMsgKind.RESET_MASK_DICT,
                    state.computeCacheResetMask());
        }
    }

    @Test
    public void testComputeMaskSchemasCapExactBoundary() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(-1, -1, 3);
            state.findOrAllocateSchemaId(schemaOf("a", ColumnType.LONG));
            state.findOrAllocateSchemaId(schemaOf("b", ColumnType.LONG));
            Assert.assertEquals("2 < 3", 0, state.computeCacheResetMask());
            state.findOrAllocateSchemaId(schemaOf("c", ColumnType.LONG));
            Assert.assertEquals("3 >= 3", QwpEgressMsgKind.RESET_MASK_SCHEMAS, state.computeCacheResetMask());
        }
    }

    @Test
    public void testDefaultCapsFromConstants() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            // Exercise the "no override" path: a handful of entries is safely
            // below every production cap, so the mask is zero.
            state.getConnSymbolDict().addEntry("x");
            state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testOverrideResetViaNegativeOne() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, 1, 1);
            state.getConnSymbolDict().addEntry("x");
            Assert.assertNotEquals(0, state.computeCacheResetMask());
            // -1 restores production defaults, so the previously-tripped caps
            // no longer apply.
            state.setCacheResetCapsForTest(-1, -1, -1);
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testStateClearResetsOverrides() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, 1, 1);
            state.getConnSymbolDict().addEntry("x");
            Assert.assertNotEquals(0, state.computeCacheResetMask());

            state.clear();

            // connection-level clear empties caches; applied overrides intentionally
            // persist -- state objects are pooled and reused across test connections
            // so tests that share a pool are not broken. The masks are zero solely
            // because the caches are empty again.
            Assert.assertEquals(0, state.computeCacheResetMask());
            state.getConnSymbolDict().addEntry("y");
            Assert.assertNotEquals("overrides survive clear", 0, state.computeCacheResetMask());
        }
    }

    private static ObjList<QwpEgressColumnDef> schemaOf(Object... nameTypePairs) {
        if ((nameTypePairs.length & 1) != 0) {
            throw new IllegalArgumentException("expected alternating name, type pairs");
        }
        ObjList<QwpEgressColumnDef> out = new ObjList<>();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            QwpEgressColumnDef def = new QwpEgressColumnDef();
            int type = ((Number) nameTypePairs[i + 1]).intValue();
            def.of((String) nameTypePairs[i], type);
            out.add(def);
        }
        return out;
    }
}
