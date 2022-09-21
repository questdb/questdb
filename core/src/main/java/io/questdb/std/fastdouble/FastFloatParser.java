/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
     * Convenience method for calling {@link #parseFloat(CharSequence, int, int)}.
     *
     * @param str the string to be parsed
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str) throws NumericException {
        return parseFloat(str, 0, str.length());
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str    the string to be parsed
     * @param offset the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length the length of {@code FloatingPointLiteral} in {@code str}
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str, int offset, int length) throws NumericException {
        return FastFloat.parseFloatingPointLiteral(str, offset, length);
    }

    /**
     * Convenience method for calling {@link #parseFloat(byte[], int, int)}.
     *
     * @param str the string to be parsed, a byte array with characters
     *            in ISO-8859-1, ASCII or UTF-8 encoding
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(byte[] str) throws NumericException {
        return parseFloat(str, 0, str.length);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str    the string to be parsed, a byte array with characters
     *               in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset The index of the first byte to parse
     * @param length The number of bytes to parse
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(byte[] str, int offset, int length) throws NumericException {
        return FastFloatByteArray.parseFloatingPointLiteral(str, offset, length);
    }


    /**
     * Convenience method for calling {@link #parseFloat(char[], int, int)}.
     *
     * @param str the string to be parsed
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(char[] str) throws NumericException {
        return parseFloat(str, 0, str.length);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str    the string to be parsed, a byte array with characters
     *               in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset The index of the first character to parse
     * @param length The number of characters to parse
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(char[] str, int offset, int length) throws NumericException {
        return FastFloatCharArray.parseFloatingPointLiteral(str, offset, length);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a bit pattern that encodes a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     * <p>
     * Usage example:
     * <pre>
     *     long bitPattern = parseFloatBits("3.14", 0, 4);
     *     if (bitPattern == -1L) {
     *         ...handle parse error...
     *     } else {
     *         double d = Double.longBitsToDouble(bitPattern);
     *     }
     * </pre>
     *
     * @param str    the string to be parsed
     * @param offset the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length the length of {@code FloatingPointLiteral} in {@code str}
     * @return the bit pattern of the parsed value, if the input is legal;
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloatBits(CharSequence str, int offset, int length) throws NumericException {
        return FastFloat.parseFloatingPointLiteral(str, offset, length);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a bit pattern that encodes a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     * <p>
     * See {@link #parseFloatBits(CharSequence, int, int)} for a usage example.
     *
     * @param str    the string to be parsed, a byte array with characters
     *               in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset The index of the first byte to parse
     * @param length The number of bytes to parse
     * @return the bit pattern of the parsed value, if the input is legal;
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloatBits(byte[] str, int offset, int length) throws NumericException {
        return FastFloatByteArray.parseFloatingPointLiteral(str, offset, length);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@code byte}-Array and converts it
     * into a bit pattern that encodes a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     * <p>
     * See {@link #parseFloatBits(CharSequence, int, int)} for a usage example.
     *
     * @param str    the string to be parsed, a byte array with characters
     *               in ISO-8859-1, ASCII or UTF-8 encoding
     * @param offset The index of the first character to parse
     * @param length The number of characters to parse
     * @return the bit pattern of the parsed value, if the input is legal;
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloatBits(char[] str, int offset, int length) throws NumericException {
        return FastFloatCharArray.parseFloatingPointLiteral(str, offset, length);
    }
}