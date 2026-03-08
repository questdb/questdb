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

package io.questdb.std.fastdouble;

/**
 * Abstract base class for parsers that parse a {@code FloatingPointLiteral} from a
 * character sequence ({@code str}).
 * <p>
 * This is a C++ to Java port of Daniel Lemire's fast_double_parser.
 * <p>
 * References:
 * <dl>
 *     <dt>Daniel Lemire, fast_double_parser, 4x faster than strtod.
 *     Apache License 2.0 or Boost Software License.</dt>
 *     <dd><a href="https://github.com/lemire/fast_double_parser">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, fast_float number parsing library: 4x faster than strtod.
 *     Apache License 2.0.</dt>
 *     <dd><a href="https://github.com/fastfloat/fast_float">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, Number Parsing at a Gigabyte per Second,
 *     Software: Practice and Experience 51 (8), 202    1.
 *     arXiv.2101.11408v3 [cs.DS] 24 Feb 2021</dt>
 *     <dd><a href="https://arxiv.org/pdf/2101.11408.pdf">arxiv.org</a></dd>
 * </dl>
 */
final class FastDoubleUtils {
    /**
     * Includes all non-negative values of a {@code byte}, so that we only have
     * to check for byte values {@literal <} 0 before accessing this array.
     */
    static final byte[] CHAR_TO_HEX_MAP = new byte[128];
    /**
     * Special value in {@link #CHAR_TO_HEX_MAP} for
     * the decimal point character.
     */
    static final byte DECIMAL_POINT_CLASS = -4;
    /**
     * The decimal exponent of a double has a range of -324 to +308.
     * The hexadecimal exponent of a double has a range of -1022 to +1023.
     */
    final static int MAX_EXPONENT_NUMBER = 1024;
    final static long MINIMAL_NINETEEN_DIGIT_INTEGER = 1000_00000_00000_00000L;
    /**
     * Special value in {@link #CHAR_TO_HEX_MAP} for
     * characters that are neither a hex digit nor
     * a decimal point character.
     */
    static final byte OTHER_CLASS = -1;

    static {
        for (char ch = 0; ch < FastDoubleUtils.CHAR_TO_HEX_MAP.length; ch++) {
            FastDoubleUtils.CHAR_TO_HEX_MAP[ch] = FastDoubleUtils.OTHER_CLASS;
        }
        for (char ch = '0'; ch <= '9'; ch++) {
            FastDoubleUtils.CHAR_TO_HEX_MAP[ch] = (byte) (ch - '0');
        }
        for (char ch = 'A'; ch <= 'F'; ch++) {
            FastDoubleUtils.CHAR_TO_HEX_MAP[ch] = (byte) (ch - 'A' + 10);
        }
        for (char ch = 'a'; ch <= 'f'; ch++) {
            FastDoubleUtils.CHAR_TO_HEX_MAP[ch] = (byte) (ch - 'a' + 10);
        }
        FastDoubleUtils.CHAR_TO_HEX_MAP['.'] = FastDoubleUtils.DECIMAL_POINT_CLASS;
    }
}
