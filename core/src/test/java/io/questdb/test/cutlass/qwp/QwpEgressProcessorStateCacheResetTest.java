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
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exhaustive unit coverage for the connection-scope memory caps and their
 * {@code CACHE_RESET} plumbing on the server side: entry/heap caps on the
 * SYMBOL dict, mask computation, and the {@code applyCacheReset} state machine.
 * End-to-end behaviour over the wire is covered by
 * {@link QwpEgressCacheResetWireTest}.
 */
public class QwpEgressProcessorStateCacheResetTest extends AbstractCairoTest {

    private CairoConfiguration cfg;

    @Before
    public void setUpState() {
        cfg = configuration;
    }

    @Test
    public void testApplyCacheResetDictBitClearsDict() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("foo");
            dict.addEntry("bar");
            Assert.assertEquals(2, dict.size());

            state.applyCacheReset(QwpEgressMsgKind.RESET_MASK_DICT);

            Assert.assertEquals("dict must be flushed", 0, dict.size());
        }
    }

    @Test
    public void testApplyCacheResetMaskZeroIsNoOp() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");

            state.applyCacheReset((byte) 0);

            Assert.assertEquals("dict unchanged", 1, state.getConnSymbolDict().size());
        }
    }

    @Test
    public void testApplyCacheResetUnknownBitsAreIgnored() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");
            // Bits 1..7 are reserved. Unknown bits must be ignored rather than
            // trigger spurious resets.
            state.applyCacheReset((byte) 0xFC);
            Assert.assertEquals("dict untouched by unknown bits", 1, state.getConnSymbolDict().size());
        }
    }

    @Test
    public void testComputeMaskDictEntriesCapExactBoundary() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(4, -1);
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
            state.setCacheResetCapsForTest(-1, 10);
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
    public void testComputeMaskIsNoSideEffects() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, -1);
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
    public void testDefaultCapsFromConstants() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            // Exercise the "no override" path: a handful of entries is safely
            // below every production cap, so the mask is zero.
            state.getConnSymbolDict().addEntry("x");
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testForceDictResetEmptyDictIsZero() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            // An empty dict needs no CACHE_RESET frame even when a query forces
            // the reset: there is nothing to clear, and emitting the frame would
            // be pure overhead. This is the empty-dict guard.
            Assert.assertEquals(0, state.computeCacheResetMask(true));
        }
    }

    @Test
    public void testForceDictResetIdempotentWithCap() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, -1);
            state.getConnSymbolDict().addEntry("x");
            // The entry cap is already tripped, so the unforced mask already
            // carries the DICT bit. Forcing must not change or double it.
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask(false));
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask(true));
        }
    }

    @Test
    public void testForceDictResetNoSideEffects() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");
            state.getConnSymbolDict().addEntry("bar");
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask(true));
            // computeCacheResetMask is a pure query; applyCacheReset is the only
            // mutator. A forced compute must not flush the dict.
            Assert.assertEquals("force-compute must not clear the dict", 2, state.getConnSymbolDict().size());
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask(true));
        }
    }

    @Test
    public void testForceDictResetNonEmptyDictSetsMask() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.getConnSymbolDict().addEntry("foo");
            // Below every production cap, so the unforced mask is zero...
            Assert.assertEquals(0, state.computeCacheResetMask(false));
            // ...but a forced reset on a non-empty dict requests the DICT bit so
            // the connection-scoped dict gets scoped to the upcoming query.
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.computeCacheResetMask(true));
        }
    }

    @Test
    public void testForceDictResetThenApplyClearsDict() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("alpha");
            dict.addEntry("beta");
            byte mask = state.computeCacheResetMask(true);
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, mask);
            state.applyCacheReset(mask);
            Assert.assertEquals("forced reset must flush the dict", 0, dict.size());
        }
    }

    @Test
    public void testMergePendingCacheResetMaskIsIdempotent() {
        // Re-staging the same bit must leave the mask unchanged. Covers the
        // benign case where a non-SELECT trips the dict cap, then a follow-up
        // non-SELECT recomputes the same DICT bit before the first one's
        // CACHE_RESET has gone out.
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.mergePendingCacheResetMask(QwpEgressMsgKind.RESET_MASK_DICT);
            state.mergePendingCacheResetMask(QwpEgressMsgKind.RESET_MASK_DICT);
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.getPendingCacheResetMask());
        }
    }

    @Test
    public void testMergePendingCacheResetMaskZeroIsNoOp() {
        // The processor guards against zero before calling merge, but the
        // state method must still be safe to call with zero -- a regression
        // here would mask a guard-bypass with silent corruption.
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.mergePendingCacheResetMask(QwpEgressMsgKind.RESET_MASK_DICT);
            state.mergePendingCacheResetMask((byte) 0);
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, state.getPendingCacheResetMask());
        }
    }

    @Test
    public void testNoArgMaskDelegatesToUnforced() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            // The no-arg overload must behave exactly like forceDictReset=false,
            // both when the dict is empty and when it is non-empty below cap.
            Assert.assertEquals(state.computeCacheResetMask(false), state.computeCacheResetMask());
            state.getConnSymbolDict().addEntry("foo");
            Assert.assertEquals(state.computeCacheResetMask(false), state.computeCacheResetMask());
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testOverrideResetViaNegativeOne() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, 1);
            state.getConnSymbolDict().addEntry("x");
            Assert.assertNotEquals(0, state.computeCacheResetMask());
            // -1 restores production defaults, so the previously-tripped caps
            // no longer apply.
            state.setCacheResetCapsForTest(-1, -1);
            Assert.assertEquals(0, state.computeCacheResetMask());
        }
    }

    @Test
    public void testStateClearResetsOverrides() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            state.setCacheResetCapsForTest(1, 1);
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
}
