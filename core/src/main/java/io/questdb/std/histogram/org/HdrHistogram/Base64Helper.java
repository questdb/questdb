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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.std.histogram.org.HdrHistogram;

import java.lang.reflect.Method;

/**
 * Base64Helper exists to bridge inconsistencies in Java SE support of Base64 encoding and decoding.
 * Earlier Java SE platforms (up to and including Java SE 8) supported base64 encode/decode via the
 * javax.xml.bind.DatatypeConverter class, which was deprecated and eventually removed in Java SE 9.
 * Later Java SE platforms (Java SE 8 and later) support base64 encode/decode via the
 * java.util.Base64 class (first introduced in Java SE 8, and not available on e.g. Java SE 6 or 7).
 * <p>
 * This makes it "hard" to write a single piece of source code that deals with base64 encodings and
 * will compile and run on e.g. Java SE 7 AND Java SE 9. And such common source is a common need for
 * libraries. This class is intended to encapsulate this "hard"-ness and hide the ugly pretzle-twising
 * needed under the covers.
 * <p>
 * Base64Helper provides a common API that works across Java SE 6..9 (and beyond hopefully), and
 * uses late binding (Reflection) internally to avoid javac-compile-time dependencies on a specific
 * Java SE version (e.g. beyond 7 or before 9).
 */
public class Base64Helper {

    private static Method decodeMethod;
    // encoderObj and decoderObj are used in non-static method forms, and
    // irrelevant for static method forms:
    private static Object decoderObj;
    private static Method encodeMethod;
    private static Object encoderObj;

    /**
     * Converts a Base64 encoded String to a byte array
     *
     * @param base64input A base64-encoded input String
     * @return a byte array containing the binary representation equivalent of the Base64 encoded input
     */
    public static byte[] parseBase64Binary(String base64input) {
        try {
            return (byte[]) decodeMethod.invoke(decoderObj, base64input);
        } catch (Throwable e) {
            throw new UnsupportedOperationException("Failed to use platform's base64 decode method");
        }
    }

    /**
     * Converts an array of bytes into a Base64 string.
     *
     * @param binaryArray A binary encoded input array
     * @return a String containing the Base64 encoded equivalent of the binary input
     */
    static String printBase64Binary(byte[] binaryArray) {
        try {
            return (String) encodeMethod.invoke(encoderObj, binaryArray);
        } catch (Throwable e) {
            throw new UnsupportedOperationException("Failed to use platform's base64 encode method");
        }
    }

    static {
        try {
            Class<?> javaUtilBase64Class = Class.forName("java.util.Base64");

            Method getDecoderMethod = javaUtilBase64Class.getMethod("getDecoder");
            decoderObj = getDecoderMethod.invoke(null);
            decodeMethod = decoderObj.getClass().getMethod("decode", String.class);

            Method getEncoderMethod = javaUtilBase64Class.getMethod("getEncoder");
            encoderObj = getEncoderMethod.invoke(null);
            encodeMethod = encoderObj.getClass().getMethod("encodeToString", byte[].class);
        } catch (Throwable e) {
            decodeMethod = null;
            encodeMethod = null;
        }

        if (encodeMethod == null) {
            decoderObj = null;
            encoderObj = null;
            try {
                Class<?> javaxXmlBindDatatypeConverterClass = Class.forName("javax.xml.bind.DatatypeConverter");
                decodeMethod = javaxXmlBindDatatypeConverterClass.getMethod("parseBase64Binary", String.class);
                encodeMethod = javaxXmlBindDatatypeConverterClass.getMethod("printBase64Binary", byte[].class);
            } catch (Throwable e) {
                decodeMethod = null;
                encodeMethod = null;
            }
        }
    }
}
