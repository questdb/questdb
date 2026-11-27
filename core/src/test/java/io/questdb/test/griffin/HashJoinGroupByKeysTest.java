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


package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class HashJoinGroupByKeysTest {

    @Test
    public void testCompositeKeysKeepTheirOrder() {
        HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(7, ColumnType.LONG, 3, ColumnType.LONG));
        Assert.assertTrue(keys.add(1, ColumnType.INT, 2, ColumnType.INT));
        Assert.assertEquals(2, keys.size());
        Assert.assertEquals("7=3:LONG 1=2:INT", describe(keys));
        Assert.assertEquals(7, keys.getProbeColumn(0));
        Assert.assertEquals(3, keys.getBuildColumn(0));
        Assert.assertEquals(ColumnType.INT, keys.getProbeType(1));
        Assert.assertEquals(ColumnType.INT, keys.getBuildType(1));
        Assert.assertFalse(keys.isIntKeyed());
        Assert.assertFalse(keys.isSymbolKey());
    }

    @Test
    public void testMismatchedPairs() {
        // Exactly the pairs the ordinary plan rejects with "join column type mismatch".
        assertRejected(ColumnType.INT, ColumnType.LONG);
        assertRejected(ColumnType.LONG, ColumnType.DOUBLE);
        assertRejected(ColumnType.DATE, ColumnType.TIMESTAMP);
        assertRejected(ColumnType.SYMBOL, ColumnType.INT);
        assertRejected(ColumnType.STRING, ColumnType.CHAR);
        assertRejected(ColumnType.getGeoHashTypeWithBits(10), ColumnType.getGeoHashTypeWithBits(15));
        assertRejected(ColumnType.getDecimalType(10, 2), ColumnType.getDecimalType(10, 3));
    }

    @Test
    public void testNonKeyTypes() {
        // Nothing stages a variable-size key of these types into a map.
        assertRejected(ColumnType.BINARY, ColumnType.BINARY);
        assertRejected(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        assertRejected(ColumnType.INTERVAL, ColumnType.INTERVAL);
    }

    @Test
    public void testRepeatedColumnEncoding() {
        // One column reaches its sink through one encoding, so two keys must agree on it.
        HashJoinGroupByKeys shared = new HashJoinGroupByKeys();
        Assert.assertTrue(shared.add(4, ColumnType.INT, 1, ColumnType.INT));
        Assert.assertTrue(shared.add(4, ColumnType.INT, 2, ColumnType.INT));
        Assert.assertEquals("4=1:INT 4=2:INT", describe(shared));

        HashJoinGroupByKeys probeConflict = new HashJoinGroupByKeys();
        Assert.assertTrue(probeConflict.add(4, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
        Assert.assertFalse(probeConflict.add(4, ColumnType.SYMBOL, 2, ColumnType.VARCHAR));

        HashJoinGroupByKeys buildConflict = new HashJoinGroupByKeys();
        Assert.assertTrue(buildConflict.add(1, ColumnType.TIMESTAMP, 4, ColumnType.TIMESTAMP));
        Assert.assertFalse(buildConflict.add(2, ColumnType.TIMESTAMP_NANO, 4, ColumnType.TIMESTAMP));

        // One probe SYMBOL column translates once: the translating record the probe sink reads
        // indexes its views by probe column, so a second key over that column conflicts.
        HashJoinGroupByKeys twoDictionaries = new HashJoinGroupByKeys();
        Assert.assertTrue(twoDictionaries.add(4, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
        Assert.assertFalse(twoDictionaries.add(4, ColumnType.SYMBOL, 2, ColumnType.SYMBOL));
        // Even the identical pair twice, which no statement produces and no view could carry.
        HashJoinGroupByKeys repeatedPair = new HashJoinGroupByKeys();
        Assert.assertTrue(repeatedPair.add(4, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
        Assert.assertFalse(repeatedPair.add(4, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
        // Two probe columns may share one build column: each translates through its own view.
        HashJoinGroupByKeys sharedBuild = new HashJoinGroupByKeys();
        Assert.assertTrue(sharedBuild.add(4, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
        Assert.assertTrue(sharedBuild.add(5, ColumnType.SYMBOL, 1, ColumnType.SYMBOL));
    }

    @Test
    public void testSingleKeyRouting() {
        // The INT layout takes a lone INT pair and a lone SYMBOL pair; everything else stages a key.
        assertRoute(ColumnType.INT, ColumnType.INT, "0=1:INT", true, false);
        assertRoute(ColumnType.SYMBOL, ColumnType.SYMBOL, "0=1:SYMBOL", true, true);
        assertRoute(ColumnType.LONG, ColumnType.LONG, "0=1:LONG", false, false);
        assertRoute(ColumnType.IPv4, ColumnType.IPv4, "0=1:IPv4", false, false);
    }

    @Test
    public void testSymbolPairTranslatesInEveryPosition() {
        // A SYMBOL pair reconciles to SYMBOL wherever it sits: the probe translates its key into
        // the build's domain, so neither side writes its text and the pair compares as ints.
        HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(0, ColumnType.INT, 1, ColumnType.INT));
        Assert.assertTrue(keys.add(2, ColumnType.SYMBOL, 3, ColumnType.SYMBOL));
        Assert.assertEquals("0=1:INT 2=3:SYMBOL", describe(keys));
        Assert.assertFalse(keys.isIntKeyed());
        Assert.assertFalse(keys.isSymbolKey());
        Assert.assertFalse(keys.isTranslatedSymbol(0));
        Assert.assertTrue(keys.isTranslatedSymbol(1));
        // A SYMBOL against text still compares as text, so only the pair of dictionaries translates.
        HashJoinGroupByKeys text = new HashJoinGroupByKeys();
        Assert.assertTrue(text.add(0, ColumnType.SYMBOL, 1, ColumnType.STRING));
        Assert.assertFalse(text.isTranslatedSymbol(0));
    }

    @Test
    public void testTextPairs() {
        assertKey(ColumnType.STRING, ColumnType.STRING, "0=1:STRING");
        assertKey(ColumnType.VARCHAR, ColumnType.VARCHAR, "0=1:VARCHAR");
        assertKey(ColumnType.SYMBOL, ColumnType.STRING, "0=1:STRING,symbolAsString");
        assertKey(ColumnType.STRING, ColumnType.SYMBOL, "0=1:STRING,symbolAsString");
        assertKey(ColumnType.SYMBOL, ColumnType.VARCHAR, "0=1:VARCHAR,symbolAsString,probeStringAsVarchar");
        assertKey(ColumnType.VARCHAR, ColumnType.SYMBOL, "0=1:VARCHAR,symbolAsString,buildStringAsVarchar");
        assertKey(ColumnType.STRING, ColumnType.VARCHAR, "0=1:VARCHAR,probeStringAsVarchar");
        assertKey(ColumnType.VARCHAR, ColumnType.STRING, "0=1:VARCHAR,buildStringAsVarchar");
    }

    @Test
    public void testTimestampPairs() {
        assertKey(ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, "0=1:TIMESTAMP");
        assertKey(ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, "0=1:TIMESTAMP_NS");
        assertKey(ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO, "0=1:TIMESTAMP_NS,probeTimestampAsNanos");
        assertKey(ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP, "0=1:TIMESTAMP_NS,buildTimestampAsNanos");
    }

    @Test
    public void testUniformPairs() {
        for (int type : new int[]{
                ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.INT,
                ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.DATE, ColumnType.IPv4,
                ColumnType.UUID, ColumnType.LONG128, ColumnType.LONG256, ColumnType.getGeoHashTypeWithBits(20),
                ColumnType.getDecimalType(10, 2)
        }) {
            HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
            Assert.assertTrue(ColumnType.nameOf(type), keys.add(0, type, 1, type));
            Assert.assertEquals(ColumnType.nameOf(type), type, keys.getType(0));
            Assert.assertEquals(ColumnType.nameOf(type), "0=1:" + ColumnType.nameOf(type), describe(keys));
        }
    }

    private static void assertKey(int probeType, int buildType, String expected) {
        HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(expected, keys.add(0, probeType, 1, buildType));
        Assert.assertEquals(expected, describe(keys));
    }

    private static void assertRejected(int probeType, int buildType) {
        String pair = ColumnType.nameOf(probeType) + " against " + ColumnType.nameOf(buildType);
        Assert.assertFalse(pair, new HashJoinGroupByKeys().add(0, probeType, 1, buildType));
        Assert.assertFalse(pair, new HashJoinGroupByKeys().add(0, buildType, 1, probeType));
    }

    private static void assertRoute(int probeType, int buildType, String expected, boolean isIntKeyed, boolean isSymbolKey) {
        HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(expected, keys.add(0, probeType, 1, buildType));
        Assert.assertEquals(expected, describe(keys));
        Assert.assertEquals(expected, isIntKeyed, keys.isIntKeyed());
        Assert.assertEquals(expected, isSymbolKey, keys.isSymbolKey());
    }

    private static String describe(HashJoinGroupByKeys keys) {
        StringSink sink = new StringSink();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(' ');
            }
            sink.put(keys.getProbeColumn(i)).putAscii('=').put(keys.getBuildColumn(i))
                    .putAscii(':').put(ColumnType.nameOf(keys.getType(i)));
            if (keys.isSymbolAsString(i)) {
                sink.putAscii(",symbolAsString");
            }
            if (keys.isProbeStringAsVarchar(i)) {
                sink.putAscii(",probeStringAsVarchar");
            }
            if (keys.isBuildStringAsVarchar(i)) {
                sink.putAscii(",buildStringAsVarchar");
            }
            if (keys.isProbeTimestampAsNanos(i)) {
                sink.putAscii(",probeTimestampAsNanos");
            }
            if (keys.isBuildTimestampAsNanos(i)) {
                sink.putAscii(",buildTimestampAsNanos");
            }
        }
        return sink.toString();
    }
}
