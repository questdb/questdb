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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import java.util.function.Supplier;

abstract class LineUdpInsertGeoHashTest extends LineUdpInsertTest {
    static final String tableName = "tracking";
    static final String targetColumnName = "geohash";

    @Test
    public abstract void testExcessivelyLongGeoHashesAreTruncated() throws Exception;

    @Test
    public abstract void testGeoHashes() throws Exception;

    @Test
    public abstract void testGeoHashesNotEnoughPrecision() throws Exception;

    @Test
    public abstract void testGeoHashesTruncating() throws Exception;

    @Test
    public abstract void testNullGeoHash() throws Exception;

    @Test
    public abstract void testTableHasGeoHashMessageDoesNot() throws Exception;

    @Test
    public abstract void testWrongCharGeoHashes() throws Exception;

    private static Supplier<String> randomGeoHashGenerator(int chars) {
        final Rnd rnd = new Rnd();
        return () -> {
            StringSink sink = Misc.getThreadLocalSink();
            GeoHashes.appendChars(rnd.nextGeoHash(chars * 5), chars, sink);
            return sink.toString();
        };
    }

    protected static void assertGeoHash(int columnBits, int lineGeoSizeChars, int numLines, String expected) throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(columnBits),
                expected,
                sender -> {
                    Supplier<String> rnd = randomGeoHashGenerator(lineGeoSizeChars);
                    for (int i = 0; i < numLines; i++) {
                        sender.metric(tableName).field(targetColumnName, rnd.get()).$((long) ((i + 1) * 1e9));
                    }
                });
    }
}