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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.std.IntShortHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class FunctionFactoryDescriptor {
    /**
     * Returned by {@link #signatureChar(ColumnTypeTag)} for a tag no signature can name.
     */
    public static final char NO_SIGNATURE_CHAR = 0;
    private static final int ARRAY_MASK = 1 << 31;
    private static final int CONST_MASK = 1 << 30;
    // lower-case signature character -> tag code; -1 (the map's no-entry value) for any other character
    private static final IntShortHashMap TAG_BY_SIGNATURE_CHAR = new IntShortHashMap();
    private static final int TYPE_MASK = ~(ARRAY_MASK | CONST_MASK);
    private final long[] argTypes;
    private final FunctionFactory factory;
    private final int openParenIndex;
    private final int sigArgCount;

    public FunctionFactoryDescriptor(FunctionFactory factory) throws SqlException {
        this.factory = factory;

        final String sig = factory.getSignature();
        this.openParenIndex = validateSignatureAndGetNameSeparator(sig);
        // validate data types
        int typeCount = 0;
        for (
                int i = openParenIndex + 1, n = sig.length() - 1;
                i < n; typeCount++
        ) {
            char cc = sig.charAt(i);
            if (FunctionFactoryDescriptor.getArgTypeTag(cc) == -1) {
                throw SqlException.position(0).put("illegal argument type: ").put('`').put(cc).put('`');
            }
            // check if this is an array
            i++;
            if (i < n && sig.charAt(i) == '[') {
                i++;
                if (i >= n || sig.charAt(i) != ']') {
                    throw SqlException.position(0).put("invalid array declaration: " + sig);
                }
                i++;
            }
        }

        // second loop, less paranoid
        long[] types = new long[typeCount % 2 == 0 ? typeCount / 2 : typeCount / 2 + 1];
        for (int i = openParenIndex + 1, n = sig.length() - 1, typeIndex = 0; i < n; ) {
            final char c = sig.charAt(i);
            int type = FunctionFactoryDescriptor.getArgTypeTag(c);
            final int arrayIndex = typeIndex / 2;
            final int arrayValueOffset = (typeIndex % 2) * 32;
            // check if this is an array
            i++;

            // array bit
            if (i < n && sig.charAt(i) == '[') {
                type |= ARRAY_MASK;
                i += 2;
            }
            // constant bit
            if ((c & 32) != 0) {
                type |= CONST_MASK;
            }
            types[arrayIndex] |= (toUnsignedLong(type) << (32 - arrayValueOffset));
            typeIndex++;
        }
        this.argTypes = types;
        this.sigArgCount = typeCount;
    }

    /**
     * The tag a signature character stands for, in either case; -1 for a character that is
     * not a signature character. The inverse of {@link #signatureChar(ColumnTypeTag)}.
     */
    public static short getArgTypeTag(char c) {
        return TAG_BY_SIGNATURE_CHAR.get(c | 32);
    }

    public static boolean isArray(int mask) {
        return (mask & ARRAY_MASK) != 0;
    }

    public static boolean isConstant(int mask) {
        return (mask & CONST_MASK) != 0;
    }

    public static String replaceSignatureName(String name, String signature) throws SqlException {
        int openParenIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put(signature, openParenIndex, signature.length());
        return signatureBuilder.toString();
    }

    /**
     * The lower-case signature character that names {@code tag} in a
     * {@link FunctionFactory#getSignature() factory signature}; the upper-case form of the same
     * character is the constant-argument variant, so every character here must differ from its
     * upper-case form in bit 5 only. {@link #NO_SIGNATURE_CHAR} for a tag no signature can name:
     * the geohash and decimal families are named by their pseudo tag, an array by its element
     * character followed by {@code []}, and the rest never appear in a signature.
     * <p>
     * This switch owns the character table; a new tag must take a position here, and the tests
     * in {@code FunctionFactoryDescriptorTest} pin the table and the bit-5 rule.
     */
    public static char signatureChar(ColumnTypeTag tag) {
        return switch (tag) {
            case CHAR -> 'a';
            case BYTE -> 'b';
            case CURSOR -> 'c';
            case DOUBLE -> 'd';
            case SHORT -> 'e';
            case FLOAT -> 'f';
            case GEOHASH -> 'g';
            case LONG256 -> 'h';
            case INT -> 'i';
            case LONG128 -> 'j';
            case SYMBOL -> 'k';
            case LONG -> 'l';
            case DATE -> 'm';
            case TIMESTAMP -> 'n';
            case NULL -> 'o';
            case REGCLASS -> 'p';
            case REGPROCEDURE -> 'q';
            case RECORD -> 'r';
            case STRING -> 's';
            case BOOLEAN -> 't';
            case BINARY -> 'u';
            case VAR_ARG -> 'v';
            case ARRAY_STRING -> 'w';
            case IPv4 -> 'x';
            case UUID -> 'z';
            case VARCHAR -> 'ø';
            case INTERVAL -> 'δ';
            case DECIMAL -> 'ξ';
            case UNDEFINED, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, ARRAY, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64,
                 DECIMAL128, DECIMAL256, PARAMETER, VARCHAR_SLICE, UNKNOWN -> NO_SIGNATURE_CHAR;
        };
    }

    /**
     * The type name {@link #translateSignature(CharSequence, String, StringSink)} prints for a
     * signature character; the {@code functions()} catalogue shows it. Defined only for tags that
     * have a {@link #signatureChar(ColumnTypeTag) signature character}.
     */
    public static String signatureTypeName(ColumnTypeTag tag) {
        return switch (tag) {
            case CHAR -> "char";
            case BYTE -> "byte";
            case CURSOR -> "cursor";
            case DOUBLE -> "double";
            case SHORT -> "short";
            case FLOAT -> "float";
            case GEOHASH -> "geohash";
            case LONG256 -> "long256";
            case INT -> "int";
            case LONG128 -> "long128";
            case SYMBOL -> "symbol";
            case LONG -> "long";
            case DATE -> "date";
            case TIMESTAMP -> "timestamp";
            case NULL -> "null";
            case REGCLASS -> "reg_class";
            case REGPROCEDURE -> "reg_procedure";
            case RECORD -> "record";
            case STRING -> "string";
            case BOOLEAN -> "boolean";
            case BINARY -> "binary";
            case VAR_ARG -> "var_arg";
            case ARRAY_STRING -> "array_string";
            case IPv4 -> "ipv4";
            case UUID -> "uuid";
            case VARCHAR -> "varchar";
            case INTERVAL -> "interval";
            case DECIMAL -> "decimal";
            case UNDEFINED, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, ARRAY, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64,
                 DECIMAL128, DECIMAL256, PARAMETER, VARCHAR_SLICE, UNKNOWN ->
                    throw new IllegalArgumentException("tag has no signature character: " + tag);
        };
    }

    public static String replaceSignatureNameAndSwapArgs(String name, String signature) throws SqlException {
        int openParenIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put('(');
        boolean bracket = false;
        for (int i = signature.length() - 2; i > openParenIndex; i--) {
            char curr = signature.charAt(i);
            if (curr == '[') {
                bracket = true;
            } else if (curr != ']') {
                signatureBuilder.put(curr);
                if (bracket) {
                    signatureBuilder.put("[]");
                    bracket = false;
                }
            }
        }
        if (bracket) {
            signatureBuilder.put("[]");
        }
        signatureBuilder.put(')');
        return signatureBuilder.toString();
    }

    public static int toType(int typeWithFlags) {
        return typeWithFlags & TYPE_MASK;
    }

    public static short toTypeTag(int typeWithFlags) {
        return (short) (typeWithFlags & TYPE_MASK);
    }

    public static StringSink translateSignature(CharSequence funcName, String signature, StringSink sink) {
        int openParenIndex;
        try {
            openParenIndex = validateSignatureAndGetNameSeparator(signature);
        } catch (SqlException err) {
            throw new IllegalArgumentException("offending: '" + signature + "', reason: " + err.getMessage());
        }
        sink.put(funcName).put('(');
        for (int i = openParenIndex + 1, n = signature.length() - 1; i < n; i++) {
            char c = signature.charAt(i);
            final String type;
            if (c != '[') {
                final short tag = getArgTypeTag(c);
                if (tag == -1) {
                    throw new IllegalArgumentException("offending: '" + c + '\'');
                }
                type = signatureTypeName(ColumnTypeTag.of(tag));
                if (Character.isLowerCase(c)) {
                    sink.put("const ");
                }
            } else {
                if (i < 3 || i + 2 > n || signature.charAt(i + 1) != ']') {
                    throw new IllegalArgumentException("offending array: '" + c + '\'');
                }
                type = "[]";
                sink.clear(sink.length() - 2); // remove the preceding comma
                i++; // skip closing bracket
            }
            sink.put(type);
            if (i + 1 < n) {
                sink.put(", ");
            }
        }
        sink.put(')');
        return sink;
    }

    public static int validateSignatureAndGetNameSeparator(String sig) throws SqlException {
        if (sig == null) {
            throw SqlException.$(0, "NULL signature");
        }

        int openParenIndex = sig.indexOf('(');
        if (openParenIndex == -1) {
            throw SqlException.$(0, "open brace expected");
        }

        if (openParenIndex == 0) {
            throw SqlException.$(0, "empty function name");
        }

        if (sig.charAt(sig.length() - 1) != ')') {
            throw SqlException.$(0, "close brace expected");
        }

        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            throw SqlException.$(0, "name must not start with digit");
        }

        for (int i = 0; i < openParenIndex; i++) {
            char cc = sig.charAt(i);
            if (FunctionFactoryCache.invalidFunctionNameChars.contains(cc)) {
                throw SqlException.position(0).put("invalid character: ").put(cc);
            }
        }

        if (FunctionFactoryCache.invalidFunctionNames.keyIndex(sig, 0, openParenIndex) < 0) {
            throw SqlException.position(0).put("invalid function name character: ").put(sig);
        }
        return openParenIndex;
    }

    public int getArgTypeWithFlags(int index) {
        int arrayIndex = index / 2;
        long mask = argTypes[arrayIndex];
        return (int) (mask >>> (32 - (index % 2) * 32));
    }

    public FunctionFactory getFactory() {
        return factory;
    }

    public String getName() {
        return factory.getSignature().substring(0, openParenIndex);
    }

    public int getSigArgCount() {
        return sigArgCount;
    }

    private static long toUnsignedLong(int type) {
        return ((long) type) & 0xffffffffL;
    }

    static {
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            final char c = signatureChar(tag);
            if (c != NO_SIGNATURE_CHAR) {
                assert TAG_BY_SIGNATURE_CHAR.get(c) == -1 : "signature character taken twice: " + c;
                TAG_BY_SIGNATURE_CHAR.put(c, tag.code());
            }
        }
    }
}
