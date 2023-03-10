/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.CharSink;

public final class Uuid implements Sinkable {
    public static final int FIRST_DASH_POS = 8;
    public static final int FOURTH_DASH_POS = 23;
    public static final int SECOND_DASH_POS = 13;
    public static final int THIRD_DASH_POS = 18;
    public static final int UUID_LENGTH = 36;

    private long hi = Numbers.LONG_NaN;
    private long lo = Numbers.LONG_NaN;

    public Uuid(long lo, long hi) {
        of(lo, hi);
    }

    public Uuid() {

    }

    /**
     * Check UUID string has the right length and dashes in the right places.
     * It does not perform full validation of the UUID string.
     * Call this method before calling {@link #parseHi(CharSequence)} or {@link #parseLo(CharSequence)}.
     *
     * @param uuid UUID string
     * @throws NumericException if UUID string has wrong length or dashes in wrong places
     */
    public static void checkDashesAndLength(CharSequence uuid) throws NumericException {
        checkDashesAndLength(uuid, 0, uuid.length());
    }

    public static void checkDashesAndLength(CharSequence uuid, int lo, int hi) throws NumericException {
        if (lo < 0 || hi < lo || hi > uuid.length()) {
            throw NumericException.INSTANCE;
        }

        if (hi - lo != UUID_LENGTH) {
            throw NumericException.INSTANCE;
        }
        if (uuid.charAt(lo + FIRST_DASH_POS) != '-'
                || uuid.charAt(lo + SECOND_DASH_POS) != '-'
                || uuid.charAt(lo + THIRD_DASH_POS) != '-'
                || uuid.charAt(lo + FOURTH_DASH_POS) != '-') {
            throw NumericException.INSTANCE;
        }
    }

    /**
     * Check if UUID is null.
     *
     * @param lo low 64 bits of UUID
     * @param hi high 64 bits of UUID
     * @return true if UUID is null
     */
    public static boolean isNull(long lo, long hi) {
        return hi == Numbers.LONG_NaN && lo == Numbers.LONG_NaN;
    }

    /**
     * Returns highest 64 bits of UUID.
     * <p>
     * This method assumes that UUID has correct length and dashes in correct positions
     * Use {@link #checkDashesAndLength(CharSequence)} to validate that before calling this method.
     * <p>
     * Returned bits are in little-endian order.
     *
     * @param uuid uuid string
     * @return high UUID bits
     * @throws NumericException if UUID is not valid
     */
    public static long parseHi(CharSequence uuid) throws NumericException {
        return parseHi(uuid, 0);
    }

    public static long parseHi(CharSequence uuid, int lo) throws NumericException {
        assert lo >= 0;
        long hi1;
        long hi2;
        long hi3;
        hi1 = Numbers.parseHexLong(uuid, lo, lo + FIRST_DASH_POS);
        hi2 = Numbers.parseHexLong(uuid, lo + FIRST_DASH_POS + 1, lo + SECOND_DASH_POS);
        hi3 = Numbers.parseHexLong(uuid, lo + SECOND_DASH_POS + 1, lo + THIRD_DASH_POS);
        return (hi1 << 32) | (hi2 << 16) | hi3;
    }

    /**
     * Returns lowest 64 bits of UUID.
     * <p>
     * This method assumes that UUID has correct length and dashes in correct positions
     * Use {@link #checkDashesAndLength(CharSequence)} to validate that before calling this method.
     * <p>
     * Returned bits are in little-endian order.
     *
     * @param uuid uuid string
     * @return low UUID bits
     * @throws NumericException if UUID is not valid
     */
    public static long parseLo(CharSequence uuid) throws NumericException {
        return parseLo(uuid, 0);
    }

    public static long parseLo(CharSequence uuid, int lo) throws NumericException {
        assert lo >= 0;
        long lo1;
        long lo2;
        lo1 = Numbers.parseHexLong(uuid, lo + THIRD_DASH_POS + 1, lo + FOURTH_DASH_POS);
        lo2 = Numbers.parseHexLong(uuid, lo + FOURTH_DASH_POS + 1, lo + UUID_LENGTH);
        return (lo1 << 48) | lo2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != Uuid.class) {
            return false;
        }
        Uuid that = (Uuid) o;
        return lo == that.lo && hi == that.hi;
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int hashCode() {
        return Hash.hash(lo, hi);
    }

    public void of(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    public void of(CharSequence uuid) throws NumericException {
        checkDashesAndLength(uuid);
        this.lo = parseLo(uuid);
        this.hi = parseHi(uuid);
    }

    public void of(CharSequence uuid, int lo, int hi) throws NumericException {
        checkDashesAndLength(uuid, lo, hi);
        this.lo = parseLo(uuid, lo);
        this.hi = parseHi(uuid, lo);
    }

    public void ofNull() {
        this.lo = Numbers.LONG_NaN;
        this.hi = Numbers.LONG_NaN;
    }

    @Override
    public void toSink(CharSink sink) {
        Numbers.appendUuid(lo, hi, sink);
    }
}
