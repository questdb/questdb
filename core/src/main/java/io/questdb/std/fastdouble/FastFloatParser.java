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
 * Provides static method for parsing a {@code float} from a
 * {@link CharSequence}, {@code char} array or {@code byte} array.
 */
public final class FastFloatParser {

    /**
     * Don't let anyone instantiate this class.
     */
    private FastFloatParser() {
    }

    /**
     * Convenience method for calling {@link #parseFloat(CharSequence, int, int, boolean)}.
     *
     * @param str            the string to be parsed
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str, boolean rejectOverflow) throws NumericException {
        return parseFloat(str, 0, str.length(), rejectOverflow);
    }

    /**
     * Convenience method for calling {@link #parseFloat(long, int, int, boolean)}.
     *
     * @param str            the string to be parsed, a memory pointer to array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param len            length of the string
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(long str, int len, boolean rejectOverflow) throws NumericException {
        return parseFloat(str, 0, len, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a bytes in native memory and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a memory pointer to array of characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first byte to parse
     * @param length         The number of bytes to parse
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(long str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloatMem.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed
     * @param offset         the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length         the length of {@code FloatingPointLiteral} in {@code str}
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloat.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

    /**
     * Convenience method for calling {@link #parseFloat(byte[], int, int, boolean)}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(byte[] str, boolean rejectOverflow) throws NumericException {
        return parseFloat(str, 0, str.length, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first byte to parse
     * @param length         The number of bytes to parse
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(byte[] str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloatByteArray.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }


    /**
     * Convenience method for calling {@link #parseFloat(char[], int, int, boolean)}.
     *
     * @param str            the string to be parsed
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(char[] str, boolean rejectOverflow) throws NumericException {
        return parseFloat(str, 0, str.length, rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed, a byte array with characters
     *                       in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset         The index of the first character to parse
     * @param length         The number of characters to parse
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(char[] str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloatCharArray.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }
}
