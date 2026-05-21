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

import java.io.ByteArrayOutputStream;

final class PGMessageRoundTripOracle {

    private PGMessageRoundTripOracle() {
    }

    static boolean assertRoundTripIfSupported(byte[] input) {
        if (input == null || input.length < 5) {
            return false;
        }

        final int type = input[0] & 0xff;
        if (type != 'P' && type != 'B' && type != 'E') {
            return false;
        }

        final int messageLength = getInt(input, 1);
        if (messageLength < 4 || messageLength != input.length - 1) {
            return false;
        }

        final Cursor cursor = new Cursor(5);
        final ByteArrayOutputStream out = new ByteArrayOutputStream(input.length);
        out.write(type);
        writeInt(out, messageLength);

        switch (type) {
            case 'P':
                copyCString(input, cursor, out);
                copyCString(input, cursor, out);
                copyShortCountedInts(input, cursor, out);
                break;
            case 'B':
                copyCString(input, cursor, out);
                copyCString(input, cursor, out);
                copyShortCountedShorts(input, cursor, out);
                copyShortCountedValues(input, cursor, out);
                copyShortCountedShorts(input, cursor, out);
                break;
            case 'E':
                copyCString(input, cursor, out);
                copyBytes(input, cursor, out, Integer.BYTES);
                break;
            default:
                return false;
        }

        if (cursor.position != input.length) {
            throw new AssertionError("round-trip oracle left trailing bytes [type=" + (char) type + ", trailing=" + (input.length - cursor.position) + "]");
        }
        final byte[] encoded = out.toByteArray();
        Assert.assertArrayEquals("pgwire round-trip mismatch for message type " + (char) type, input, encoded);
        return true;
    }

    private static void checkAvailable(byte[] input, Cursor cursor, int length) {
        if (length < 0 || cursor.position > input.length - length) {
            throw new AssertionError("malformed pgwire message accepted by parser [offset=" + cursor.position + ", need=" + length + ", len=" + input.length + "]");
        }
    }

    private static void copyBytes(byte[] input, Cursor cursor, ByteArrayOutputStream out, int length) {
        checkAvailable(input, cursor, length);
        out.write(input, cursor.position, length);
        cursor.position += length;
    }

    private static void copyCString(byte[] input, Cursor cursor, ByteArrayOutputStream out) {
        int end = cursor.position;
        while (end < input.length && input[end] != 0) {
            end++;
        }
        if (end == input.length) {
            throw new AssertionError("malformed pgwire message accepted by parser [missing nul at offset=" + cursor.position + "]");
        }
        copyBytes(input, cursor, out, end - cursor.position + 1);
    }

    private static void copyShortCountedInts(byte[] input, Cursor cursor, ByteArrayOutputStream out) {
        final int count = getUnsignedShort(input, cursor.position);
        copyBytes(input, cursor, out, Short.BYTES);
        copyBytes(input, cursor, out, count * Integer.BYTES);
    }

    private static void copyShortCountedShorts(byte[] input, Cursor cursor, ByteArrayOutputStream out) {
        final int count = getUnsignedShort(input, cursor.position);
        copyBytes(input, cursor, out, Short.BYTES);
        copyBytes(input, cursor, out, count * Short.BYTES);
    }

    private static void copyShortCountedValues(byte[] input, Cursor cursor, ByteArrayOutputStream out) {
        final int count = getUnsignedShort(input, cursor.position);
        copyBytes(input, cursor, out, Short.BYTES);
        for (int i = 0; i < count; i++) {
            final int length = getInt(input, cursor.position);
            copyBytes(input, cursor, out, Integer.BYTES);
            if (length >= 0) {
                copyBytes(input, cursor, out, length);
            } else if (length != -1) {
                throw new AssertionError("malformed pgwire Bind accepted by parser [parameterLength=" + length + "]");
            }
        }
    }

    private static int getInt(byte[] input, int offset) {
        if (offset > input.length - Integer.BYTES) {
            throw new AssertionError("malformed pgwire message accepted by parser [offset=" + offset + ", need=4, len=" + input.length + "]");
        }
        return ((input[offset] & 0xff) << 24)
                | ((input[offset + 1] & 0xff) << 16)
                | ((input[offset + 2] & 0xff) << 8)
                | (input[offset + 3] & 0xff);
    }

    private static int getUnsignedShort(byte[] input, int offset) {
        if (offset > input.length - Short.BYTES) {
            throw new AssertionError("malformed pgwire message accepted by parser [offset=" + offset + ", need=2, len=" + input.length + "]");
        }
        return ((input[offset] & 0xff) << 8) | (input[offset + 1] & 0xff);
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }

    private static final class Cursor {
        private int position;

        private Cursor(int position) {
            this.position = position;
        }
    }
}
