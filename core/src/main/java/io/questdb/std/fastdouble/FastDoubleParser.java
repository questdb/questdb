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

import io.questdb.std.NumericException;

/**
 * Provides static method for parsing a {@code double} from a
 * {@link CharSequence}, {@code char} array or {@code byte} array.
 */
public final class FastDoubleParser {

    /**
     * Don't let anyone instantiate this class.
     */
    private FastDoubleParser() {
    }

    /**
     * Convenience method for calling {@link #parseDouble(CharSequence, int, int)}.
     *
     * @param str            the string to be parsed
     * @param rejectOverflow numbers that overflow double type will be considered illegal
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(CharSequence str, boolean rejectOverflow) throws NumericException {
        return parseDouble(str, 0, str.length(), rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a {@code double} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str    the string to be parsed
     * @param offset the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length the length of {@code FloatingPointLiteral} in {@code str}
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(CharSequence str, int offset, int length) throws NumericException {
        return parseDouble(str, offset, length, false);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a {@code double} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed
     * @param offset         the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length         the length of {@code FloatingPointLiteral} in {@code str}
     * @param rejectOverflow numbers that overflow double type will be considered illegal
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(CharSequence str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastDouble.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

    /**
     * Convenience method for calling {@link #parseDouble(byte[], int, int, boolean)}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(byte[] str, boolean rejectOverflow) throws NumericException {
        return parseDouble(str, 0, str.length, rejectOverflow);
    }

    /**
     * Convenience method for calling {@link #parseDouble(long, int, int, boolean)}.
     *
     * @param str            the string to be parsed, a memory pointer to array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param len            length of the string
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(long str, int len, boolean rejectOverflow) throws NumericException {
        return parseDouble(str, 0, len, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code double} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first byte to parse
     * @param length         The number of bytes to parse
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(byte[] str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastDoubleByteArray.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a bytes in native memory and converts it
     * into a {@code double} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a memory pointer to array of characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first byte to parse
     * @param length         The number of bytes to parse
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(long str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastDoubleMem.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

    /**
     * Convenience method for calling {@link #parseDouble(char[], int, int, boolean)}.
     *
     * @param str            the string to be parsed
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(char[] str, boolean rejectOverflow) throws NumericException {
        return parseDouble(str, 0, str.length, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code double} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first character to parse
     * @param length         The number of characters to parse
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed double value
     * @throws NumericException if the string can not be parsed
     */
    public static double parseDouble(char[] str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastDoubleCharArray.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }
}
