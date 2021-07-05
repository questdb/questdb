/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class FunctionFactoryDescriptor {
    private static final int ARRAY_MASK = 1 << 31;
    private static final int CONST_MASK = 1 << 30;
    private static final int TYPE_MASK = ~(ARRAY_MASK | CONST_MASK);
    private final FunctionFactory factory;
    private final long[] argTypes;
    private final int sigArgCount;
    private final int openBraceIndex;

    public FunctionFactoryDescriptor(FunctionFactory factory) throws SqlException {
        this.factory = factory;

        final String sig = factory.getSignature();
        this.openBraceIndex = validateSignatureAndGetNameSeparator(sig);
        // validate data types
        int typeCount = 0;
        for (
                int i = openBraceIndex + 1, n = sig.length() - 1;
                i < n; typeCount++
        ) {
            char cc = sig.charAt(i);
            if (FunctionFactoryDescriptor.getArgType(cc) == -1) {
                throw SqlException.position(0).put("illegal argument type: ").put('`').put(cc).put('`');
            }
            // check if this is an array
            i++;
            if (i < n && sig.charAt(i) == '[') {
                i++;
                if (i >= n || sig.charAt(i) != ']') {
                    throw SqlException.position(0).put("invalid array declaration");
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
        this.argTypes = types;
        this.sigArgCount = typeCount;
    }

    private static long toUnsignedLong(int type) {
        return ((long) type) & 0xffffffffL;
    }

    public static int getArgType(char c) {
        int sigArgType;
        switch (c | 32) {
            case 'd':
                sigArgType = ColumnType.DOUBLE;
                break;
            case 'b':
                sigArgType = ColumnType.BYTE;
                break;
            case 'e':
                sigArgType = ColumnType.SHORT;
                break;
            case 'a':
                sigArgType = ColumnType.CHAR;
                break;
            case 'f':
                sigArgType = ColumnType.FLOAT;
                break;
            case 'i':
                sigArgType = ColumnType.INT;
                break;
            case 'l':
                sigArgType = ColumnType.LONG;
                break;
            case 's':
                sigArgType = ColumnType.STRING;
                break;
            case 't':
                sigArgType = ColumnType.BOOLEAN;
                break;
            case 'k':
                sigArgType = ColumnType.SYMBOL;
                break;
            case 'm':
                sigArgType = ColumnType.DATE;
                break;
            case 'n':
                sigArgType = ColumnType.TIMESTAMP;
                break;
            case 'u':
                sigArgType = ColumnType.BINARY;
                break;
            case 'v':
                sigArgType = ColumnType.VAR_ARG;
                break;
            case 'c':
                sigArgType = ColumnType.CURSOR;
                break;
            case 'r':
                sigArgType = ColumnType.RECORD;
                break;
            case 'h':
                sigArgType = ColumnType.LONG256;
                break;
            case 'o':
                sigArgType = ColumnType.NULL;
                break;
            default:
                sigArgType = -1;
                break;
        }
        return sigArgType;
    }

    public static boolean isArray(int mask) {
        return (mask & ARRAY_MASK) != 0;
    }

    public static boolean isConstant(int mask) {
        return (mask & CONST_MASK) != 0;
    }

    public static int toType(int mask) {
        return mask & TYPE_MASK;
    }

    public static int validateSignatureAndGetNameSeparator(String sig) throws SqlException {
        if (sig == null) {
            throw SqlException.$(0, "NULL signature");
        }

        int openBraceIndex = sig.indexOf('(');
        if (openBraceIndex == -1) {
            throw SqlException.$(0, "open brace expected");
        }

        if (openBraceIndex == 0) {
            throw SqlException.$(0, "empty function name");
        }

        if (sig.charAt(sig.length() - 1) != ')') {
            throw SqlException.$(0, "close brace expected");
        }

        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            throw SqlException.$(0, "name must not start with digit");
        }

        for (int i = 0; i < openBraceIndex; i++) {
            char cc = sig.charAt(i);
            if (FunctionFactoryCache.invalidFunctionNameChars.contains(cc)) {
                throw SqlException.position(0).put("invalid character: ").put(cc);
            }
        }

        if (FunctionFactoryCache.invalidFunctionNames.keyIndex(sig, 0, openBraceIndex) < 0) {
            throw SqlException.position(0).put("invalid function name character: ").put(sig);
        }
        return openBraceIndex;
    }

    public static String replaceSignatureNameAndSwapArgs(String name, String signature) throws SqlException {
        int openBraceIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalBuilder();
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

    public static String replaceSignatureName(String name, String signature) throws SqlException {
        int openBraceIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalBuilder();
        signatureBuilder.put(name);
        signatureBuilder.put(signature, openBraceIndex, signature.length());
        return signatureBuilder.toString();
    }

    public int getArgTypeMask(int index) {
        int arrayIndex = index / 2;
        long mask = argTypes[arrayIndex];
        return (int) ((mask >>> (32 - (index % 2) * 32)));
    }

    public long[] getArgTypes() {
        return argTypes;
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
}
