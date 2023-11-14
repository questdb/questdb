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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class FunctionFactoryDescriptor {
    private static final int ARRAY_MASK = 1 << 31;
    private static final int CONST_MASK = 1 << 30;
    private static final IntObjHashMap<String> TYPE2NAME = new IntObjHashMap<>();
    private static final int TYPE_MASK = ~(ARRAY_MASK | CONST_MASK);
    private final long[] argTypes;
    private final FunctionFactory factory;
    private final int openBraceIndex;
    private final int sigArgCount;


    public FunctionFactoryDescriptor(FunctionFactory factory) throws SqlException {
        this.factory = factory;

        final String sig = factory.getSignature();
        openBraceIndex = validateSignatureAndGetNameSeparator(sig);
        // validate data types
        int typeCount = 0;
        for (
                int i = openBraceIndex + 1, n = sig.length() - 1;
                i < n; typeCount++
        ) {
            char cc = sig.charAt(i);
            if (FunctionFactoryDescriptor.getArgType(cc) == ColumnType.UNDEFINED) {
                throw SqlException.invalidSignature(0, sig, cc);
            }
            // check if this is an array
            i++;
            if (i < n && sig.charAt(i) == '[') {
                i++;
                if (i >= n || sig.charAt(i) != ']') {
                    throw SqlException.invalidSignature(i, sig, "invalid array declaration");
                }
                i++;
            }
        }

        // second loop, less paranoid
        long[] types = new long[typeCount % 2 == 0 ? typeCount / 2 : typeCount / 2 + 1];
        for (int i = openBraceIndex + 1, n = sig.length() - 1, typeIndex = 0; i < n; ) {
            final char c = sig.charAt(i);
            int type = FunctionFactoryDescriptor.getArgType(c);
            final int arrayIndex = typeIndex / 2;
            final int arrayValueOffset = (typeIndex % 2) * 32;
            // check if this is an array
            i++;

            // array bit
            if (i < n && sig.charAt(i) == '[') {
                type |= (1 << 31);
                i += 2;
            }
            // constant bit
            if ((c | 32) == c) {
                type |= (1 << 30);
            }
            types[arrayIndex] |= (toUnsignedLong(type) << (32 - arrayValueOffset));
            typeIndex++;
        }
        argTypes = types;
        sigArgCount = typeCount;
    }

    public static byte getArgType(char c) {
        switch (c | 32) {
            case 't':
                return ColumnType.BOOLEAN;
            case 'b':
                return ColumnType.BYTE;
            case 'e':
                return ColumnType.SHORT;
            case 'a':
                return ColumnType.CHAR;
            case 'i':
                return ColumnType.INT;
            case 'l':
                return ColumnType.LONG;
            case 'm':
                return ColumnType.DATE;
            case 'n':
                return ColumnType.TIMESTAMP;
            case 'f':
                return ColumnType.FLOAT;
            case 'd':
                return ColumnType.DOUBLE;
            case 's':
                return ColumnType.STRING;
            case 'k':
                return ColumnType.SYMBOL;
            case 'h':
                return ColumnType.LONG256;
            case 'u':
                return ColumnType.BINARY;
            case 'z':
                return ColumnType.UUID;
            case 'c':
                return ColumnType.CURSOR;
            case 'v':
                return ColumnType.VAR_ARG;
            case 'r':
                return ColumnType.RECORD;
            case 'g':
                return ColumnType.GEOHASH;
            case 'j':
                return ColumnType.LONG128;
            case 'x':
                return ColumnType.IPv4;
            case 'p':
                return ColumnType.REGCLASS;
            case 'q':
                return ColumnType.REGPROCEDURE;
            case 'w':
                return ColumnType.ARRAY_STRING;
            case 'o':
                return ColumnType.NULL;
            default:
                return ColumnType.UNDEFINED;
        }
    }

    public static boolean isArray(int mask) {
        return (mask & ARRAY_MASK) != 0;
    }

    public static boolean isConstant(int mask) {
        return (mask & CONST_MASK) != 0;
    }

    public static String replaceSignatureName(String name, String signature) throws SqlException {
        int openBraceIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put(signature, openBraceIndex, signature.length());
        return signatureBuilder.toString();
    }

    public static String replaceSignatureNameAndSwapArgs(String name, String signature) throws SqlException {
        int openBraceIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put('(');
        for (int i = signature.length() - 2; i > openBraceIndex; i--) {
            char curr = signature.charAt(i);
            if (curr == '[') {
                signatureBuilder.put("[]");
            } else if (curr != ']') {
                signatureBuilder.put(curr);
            }
        }
        signatureBuilder.put(')');
        return signatureBuilder.toString();
    }

    public static byte toTypeTag(int mask) {
        return (byte) (mask & TYPE_MASK);
    }

    public static StringSink translateSignature(CharSequence funcName, String signature, StringSink sink) {
        int openBraceIndex;
        try {
            openBraceIndex = validateSignatureAndGetNameSeparator(signature);
        } catch (SqlException err) {
            throw new IllegalArgumentException(err.getMessage());
        }
        sink.put(funcName).put('(');
        for (int i = openBraceIndex + 1, n = signature.length() - 1; i < n; i++) {
            char c = signature.charAt(i);
            String type = TYPE2NAME.get(c | 32);
            if (type == null) {
                throw new IllegalArgumentException("offending: '" + c + '\'');
            }
            if (c != '[') {
                if (Character.isLowerCase(c)) {
                    sink.put("const ");
                }
            } else {
                if (i < 3 || i + 2 > n || signature.charAt(i + 1) != ']') {
                    throw new IllegalArgumentException("offending array: '" + c + '\'');
                }
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
            throw SqlException.invalidSignature(0, sig, "NULL signature");
        }

        int openBraceIndex = sig.indexOf('(');
        if (openBraceIndex == -1) {
            throw SqlException.invalidSignature(0, sig, "open brace expected");
        }

        if (openBraceIndex == 0) {
            throw SqlException.invalidSignature(0, sig, "empty function name");
        }

        if (sig.charAt(sig.length() - 1) != ')') {
            throw SqlException.invalidSignature(sig.length() - 1, sig, "close brace expected");
        }

        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            throw SqlException.invalidSignature(0, sig, "name must not start with digit");
        }

        for (int i = 0; i < openBraceIndex; i++) {
            char cc = sig.charAt(i);
            if (FunctionFactoryCache.invalidFunctionNameChars.contains(cc)) {
                throw SqlException.invalidSignature(i, sig, cc);
            }
        }

        if (FunctionFactoryCache.invalidFunctionNames.keyIndex(sig, 0, openBraceIndex) < 0) {
            throw SqlException.invalidSignature(0, sig, "invalid function name");
        }
        return openBraceIndex;
    }

    public int getArgTypeMask(int index) {
        int arrayIndex = index / 2;
        long mask = argTypes[arrayIndex];
        return (int) (mask >>> (32 - (index % 2) * 32));
    }

    public FunctionFactory getFactory() {
        return factory;
    }

    public String getName() {
        return factory.getSignature().substring(0, openBraceIndex);
    }

    public int getSigArgCount() {
        return sigArgCount;
    }

    private static long toUnsignedLong(int type) {
        return ((long) type) & 0xffffffffL;
    }

    static {
        TYPE2NAME.put('t', "boolean");
        TYPE2NAME.put('b', "byte");
        TYPE2NAME.put('e', "short");
        TYPE2NAME.put('a', "char");
        TYPE2NAME.put('i', "int");
        TYPE2NAME.put('l', "long");
        TYPE2NAME.put('m', "date");
        TYPE2NAME.put('n', "timestamp");
        TYPE2NAME.put('f', "float");
        TYPE2NAME.put('d', "double");
        TYPE2NAME.put('s', "string");
        TYPE2NAME.put('k', "symbol");
        TYPE2NAME.put('h', "long256");
        TYPE2NAME.put('u', "binary");
        TYPE2NAME.put('z', "uuid");
        TYPE2NAME.put('c', "cursor");
        TYPE2NAME.put('v', "var_arg");
        TYPE2NAME.put('r', "record");
        TYPE2NAME.put('g', "geohash");
        TYPE2NAME.put('j', "long128");
        TYPE2NAME.put('x', "ipv4");
        TYPE2NAME.put('p', "reg_class");
        TYPE2NAME.put('q', "reg_procedure");
        TYPE2NAME.put('w', "array_string");
        TYPE2NAME.put('[' | 32, "[]");
        TYPE2NAME.put('o', "null");
    }
}
