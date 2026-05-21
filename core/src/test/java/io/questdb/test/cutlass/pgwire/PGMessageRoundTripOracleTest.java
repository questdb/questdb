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

package io.questdb.test.cutlass.pgwire;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PGMessageRoundTripOracleTest {

    @Test
    public void testBindRoundTrip() {
        final byte[] frame = new byte[]{
                'B',
                0, 0, 0, 23,
                0,
                0,
                0, 1,
                0, 0,
                0, 1,
                0, 0, 0, 3,
                'a', 'b', 'c',
                0, 1,
                0, 0
        };
        Assert.assertTrue(PGMessageRoundTripOracle.assertRoundTripIfSupported(frame));
    }

    @Test
    public void testExecuteRoundTrip() {
        final byte[] frame = new byte[]{
                'E',
                0, 0, 0, 9,
                0,
                0, 0, 0, 0
        };
        Assert.assertTrue(PGMessageRoundTripOracle.assertRoundTripIfSupported(frame));
    }

    @Test
    public void testParseRoundTrip() {
        final byte[] sql = "select 1".getBytes(StandardCharsets.UTF_8);
        final byte[] frame = new byte[5 + 1 + sql.length + 1 + Short.BYTES];
        frame[0] = 'P';
        putInt(frame, 1, frame.length - 1);
        int p = 5;
        frame[p++] = 0;
        System.arraycopy(sql, 0, frame, p, sql.length);
        p += sql.length;
        frame[p++] = 0;
        frame[p++] = 0;
        frame[p] = 0;
        Assert.assertTrue(PGMessageRoundTripOracle.assertRoundTripIfSupported(frame));
    }

    @Test
    public void testSkipsUnsupportedMessageType() {
        Assert.assertFalse(PGMessageRoundTripOracle.assertRoundTripIfSupported(new byte[]{
                'S', 0, 0, 0, 4
        }));
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }
}
