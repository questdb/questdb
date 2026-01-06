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

package io.questdb.std;

/**
 * A collection of SWAR utilities inspired by <a href="https://github.com/ada-url/ada">Ada URL parser</a>.
 */
public final class SwarUtils {

    private SwarUtils() {
    }

    /**
     * Broadcasts the given byte to a long.
     */
    public static long broadcast(byte b) {
        return 0x101010101010101L * (b & 0xffL);
    }

    /**
     * Returns index of lowest (LE) non-zero byte in the input number
     * or 8 in case if the number is zero.
     */
    public static int indexOfFirstMarkedByte(long w) {
        return Long.numberOfTrailingZeros(w) >>> 3;
    }

    /**
     * Returns non-zero result in case if the input contains a zero byte.
     * <p>
     * For the technique, see:
     * <a href="http://graphics.stanford.edu/~seander/bithacks.html">Bit Twiddling Hacks</a>
     * (Determine if a word has a byte equal to n).
     * <p>
     * <strong>Caveat</strong>:
     * there are false positives, but they only occur if there is a real match.
     * The false positives occur only to the left of the correct match, and only for
     * a 0x01 byte.
     * <p>
     * Make sure to handle false positives gracefully by subsequent checks in code.
     */
    public static long markZeroBytes(long w) {
        return ((w - 0x0101010101010101L) & (~w) & 0x8080808080808080L);
    }
}
