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

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressCompressionNegotiator;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pure unit tests for {@link QwpEgressCompressionNegotiator}. These pin the
 * server-side defaults and clamp ranges so a future bump (e.g., changing the
 * default level back to 3, or moving the clamp ceiling) fails loudly here
 * before it ships to clients on the wire.
 */
public class QwpEgressCompressionNegotiatorTest {

    @Test
    public void testBareZstdDefaultsToLevelOne() {
        // "zstd" with no parameter -- the level must default to 1, the
        // cheapest server-side CPU. Bumping this default silently would
        // double or more the per-connection CPU cost for opt-in clients
        // that don't pin a level explicitly.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd"));
        Assert.assertEquals("codec must be ZSTD",
                QwpConstants.COMPRESSION_ZSTD, QwpEgressCompressionNegotiator.codec(result));
        Assert.assertEquals("bare 'zstd' must default to level 1",
                1, QwpEgressCompressionNegotiator.level(result));
    }

    @Test
    public void testEmptyLevelParameterDefaultsToLevelOne() {
        // "zstd;level=" with an empty value -- same default path as bare
        // "zstd". Pinned separately because the parser's two unparseable
        // branches converge on the same default and we want either drift
        // to fail loudly.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd;level="));
        Assert.assertEquals("codec must be ZSTD",
                QwpConstants.COMPRESSION_ZSTD, QwpEgressCompressionNegotiator.codec(result));
        Assert.assertEquals("empty level value must default to level 1",
                1, QwpEgressCompressionNegotiator.level(result));
    }

    @Test
    public void testUnparseableLevelDefaultsToLevelOne() {
        // "zstd;level=foo" -- the parser falls through to the same default
        // as the empty / bare cases. Drift here would mean a typo'd client
        // header silently lands on a different level than the documented
        // default.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd;level=foo"));
        Assert.assertEquals("codec must be ZSTD",
                QwpConstants.COMPRESSION_ZSTD, QwpEgressCompressionNegotiator.codec(result));
        Assert.assertEquals("unparseable level value must default to level 1",
                1, QwpEgressCompressionNegotiator.level(result));
    }

    @Test
    public void testExplicitClientLevelIsHonoredWithinClampRange() {
        // Sanity-check the happy path so the default tests above can't
        // accidentally pass by always returning 1 regardless of input.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd;level=5"));
        Assert.assertEquals(5, QwpEgressCompressionNegotiator.level(result));
    }

    @Test
    public void testLevelAboveMaxClampedDown() {
        // Levels above COMPRESSION_ZSTD_MAX_LEVEL must be clamped down --
        // pins the upper bound at 9.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd;level=22"));
        Assert.assertEquals("level above max must clamp to COMPRESSION_ZSTD_MAX_LEVEL",
                QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL,
                QwpEgressCompressionNegotiator.level(result));
    }

    @Test
    public void testLevelBelowMinClampedUp() {
        // Levels below COMPRESSION_ZSTD_MIN_LEVEL must be clamped up --
        // pins the lower bound at 1. zstd accepts negative levels, but
        // they are disallowed on the QWP wire to bound the codec's worst
        // case behavior.
        long result = QwpEgressCompressionNegotiator.negotiate(new Utf8String("zstd;level=0"));
        Assert.assertEquals("level below min must clamp to COMPRESSION_ZSTD_MIN_LEVEL",
                QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL,
                QwpEgressCompressionNegotiator.level(result));
    }

    // --- resolveEffectiveZstdLevel (operator force-level override) ---

    @Test
    public void testForceLevelZeroIsNoOp() {
        // Default config (forced=0): the client-negotiated level passes
        // through unchanged. Pins the "off" sentinel so a future refactor
        // can't silently flip the default behavior.
        byte effective = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_ZSTD, (byte) 5, 0);
        Assert.assertEquals(5, effective);
    }

    @Test
    public void testForceLevelIgnoredWhenCodecIsNotZstd() {
        // The override only governs ZSTD-negotiated connections. If the
        // server picked raw transport, forcing a zstd level must not flip
        // the codec or write a misleading content-encoding header.
        byte effective = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_NONE, (byte) 0, 9);
        Assert.assertEquals(0, effective);
    }

    @Test
    public void testForceLevelOverridesClientChoiceWhenZstdNegotiated() {
        // The operator override deliberately ignores the client's request:
        // a client asking for level 9 from a server forced at level 1 lands
        // on level 1. That's the whole point of the knob -- bound server
        // CPU regardless of what clients ask for.
        byte effective = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_ZSTD, (byte) 9, 1);
        Assert.assertEquals(1, effective);
    }

    @Test
    public void testForceLevelCanRaiseLevelToo() {
        // Symmetric: operator forcing level 9 must override a client that
        // asked for level 1. Less useful in practice (operators usually
        // want a ceiling not a floor) but the contract is "force, not cap".
        byte effective = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_ZSTD, (byte) 1, 9);
        Assert.assertEquals(9, effective);
    }

    @Test
    public void testForceLevelOutOfRangeClampedAtOverrideSite() {
        // Defense-in-depth: even if a future caller bypasses the config
        // parser's [1, 9] validation, the resolver must clamp into the
        // wire-allowed range so a malformed forced value can't produce a
        // header value the static byte[][] table can't render.
        byte high = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_ZSTD, (byte) 3, 22);
        Assert.assertEquals(QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL, high);

        byte low = QwpEgressCompressionNegotiator.resolveEffectiveZstdLevel(
                QwpConstants.COMPRESSION_ZSTD, (byte) 3, -1);
        Assert.assertEquals(QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL, low);
    }
}
