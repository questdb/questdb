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
import io.questdb.griffin.engine.functions.bool.AndFunctionFactory;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemAvgFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemMaxFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemMinFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemSumFunctionFactory;
import io.questdb.griffin.engine.functions.array.ArrayDimLengthFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayAccessFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySliceFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.CaseFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountGroupByFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class FunctionFactoryDescriptor {
    private static final int ARRAY_MASK = 1 << 31;
    private static final int CONST_MASK = 1 << 30;
    private static final int TYPE_MASK = ~(ARRAY_MASK | CONST_MASK);
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private final long[] argTypes;
    private FunctionFactoryDescriptor commutedEquality;
    private final FunctionFactory factory;
    private final boolean isAnd;
    private final boolean isArrayAccess;
    private final boolean isArrayColumnLayoutSensitive;
    private final boolean isArrayElementWiseScalar;
    private final boolean isCase;
    private final boolean isOr;
    private final boolean isOrderSensitiveAggregate;
    private final boolean isRelocatableScalar;
    private final boolean isRowCount;
    private final boolean isSwitch;
    private final String name;
    private final int openParenIndex;
    private final int sigArgCount;

    public FunctionFactoryDescriptor(FunctionFactory factory) throws SqlException {
        this(factory, RelocatableScalarFactories.contains(factory));
    }

    // Only the cache's audited argument-swapping/negating aliases inherit this contract.
    FunctionFactoryDescriptor(FunctionFactory factory, boolean isRelocatableScalar) throws SqlException {
        this.factory = factory;
        this.isAnd = factory.getClass() == AndFunctionFactory.class;
        this.isArrayAccess = factory.getClass() == DoubleArrayAccessFunctionFactory.class
                || factory.getClass() == DoubleArraySliceFunctionFactory.class;
        this.isArrayColumnLayoutSensitive = isArrayAccess || factory.getClass() == ArrayDimLengthFunctionFactory.class;
        this.isArrayElementWiseScalar = factory.getClass() == DoubleArrayElemAvgFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMaxFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMinFunctionFactory.class
                || factory.getClass() == DoubleArrayElemSumFunctionFactory.class;
        this.isCase = factory.getClass() == CaseFunctionFactory.class;
        this.isOr = factory.getClass() == OrFunctionFactory.class;
        this.isSwitch = factory.getClass() == SwitchFunctionFactory.class;
        this.isRelocatableScalar = isRelocatableScalar;
        this.isRowCount = factory.getClass() == CountGroupByFunctionFactory.class;

        final String sig = factory.getSignature();
        this.openParenIndex = validateSignatureAndGetNameSeparator(sig);
        this.name = sig.substring(0, openParenIndex);
        this.isOrderSensitiveAggregate = factory.isGroupBy() && (Chars.equalsIgnoreCase(name, "array_agg")
                || Chars.equalsIgnoreCase(name, "first") || Chars.equalsIgnoreCase(name, "first_not_null")
                || Chars.equalsIgnoreCase(name, "last") || Chars.equalsIgnoreCase(name, "last_not_null"));
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

    public static short getArgTypeTag(char c) {
        return switch (c | 32) {
            case 'a' -> ColumnType.CHAR;
            case 'b' -> ColumnType.BYTE;
            case 'c' -> ColumnType.CURSOR;
            case 'd' -> ColumnType.DOUBLE;
            case 'e' -> ColumnType.SHORT;
            case 'f' -> ColumnType.FLOAT;
            case 'g' -> ColumnType.GEOHASH;
            case 'h' -> ColumnType.LONG256;
            case 'i' -> ColumnType.INT;
            case 'j' -> ColumnType.LONG128;
            case 'k' -> ColumnType.SYMBOL;
            case 'l' -> ColumnType.LONG;
            case 'm' -> ColumnType.DATE;
            case 'n' -> ColumnType.TIMESTAMP;
            case 'o' -> ColumnType.NULL;
            case 'p' -> ColumnType.REGCLASS;
            case 'q' -> ColumnType.REGPROCEDURE;
            case 'r' -> ColumnType.RECORD;
            case 's' -> ColumnType.STRING;
            case 't' -> ColumnType.BOOLEAN;
            case 'u' -> ColumnType.BINARY;
            case 'v' -> ColumnType.VAR_ARG;
            case 'w' -> ColumnType.ARRAY_STRING;
            case 'x' -> ColumnType.IPv4;
            case 'z' -> ColumnType.UUID;
            case 'ø' -> ColumnType.VARCHAR;
            case 'δ' -> ColumnType.INTERVAL;
            case 'ξ' -> ColumnType.DECIMAL;
            default -> -1;
        };
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
            String type = typeNameMap.get(c | 32);
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

    void setCommutedEquality(FunctionFactoryDescriptor descriptor) {
        commutedEquality = descriptor;
    }

    public int getArgTypeWithFlags(int index) {
        int arrayIndex = index / 2;
        long mask = argTypes[arrayIndex];
        return (int) (mask >>> (32 - (index % 2) * 32));
    }

    public FunctionFactoryDescriptor getCommutedEquality() {
        return commutedEquality;
    }

    public FunctionFactory getFactory() {
        return factory;
    }

    public String getName() {
        return name;
    }

    public int getSigArgCount() {
        return sigArgCount;
    }

    public boolean isAnd() {
        return isAnd;
    }

    public boolean isArrayAccess() {
        return isArrayAccess;
    }

    public boolean isArrayColumnLayoutSensitive() {
        return isArrayColumnLayoutSensitive;
    }

    public boolean isArrayElementWiseScalar() {
        return isArrayElementWiseScalar;
    }

    public boolean isCase() {
        return isCase;
    }

    public boolean isOr() {
        return isOr;
    }

    /**
     * Audited scalar construction depends only on typed arguments: private leaves
     * may relocate before adoption, and selected calls may be constructed again.
     * FunctionBinder additionally enforces branch restrictions for NULL overloads.
     */
    public boolean isRelocatableScalar() {
        return isRelocatableScalar;
    }

    public boolean isOrderSensitiveAggregate() {
        return isOrderSensitiveAggregate;
    }

    public boolean isRowCount() {
        return isRowCount;
    }

    public boolean isSwitch() {
        return isSwitch;
    }

    private static long toUnsignedLong(int type) {
        return ((long) type) & 0xffffffffL;
    }

    static {
        typeNameMap.put('a', "char");
        typeNameMap.put('b', "byte");
        typeNameMap.put('c', "cursor");
        typeNameMap.put('d', "double");
        typeNameMap.put('e', "short");
        typeNameMap.put('f', "float");
        typeNameMap.put('g', "geohash");
        typeNameMap.put('h', "long256");
        typeNameMap.put('i', "int");
        typeNameMap.put('j', "long128");
        typeNameMap.put('k', "symbol");
        typeNameMap.put('l', "long");
        typeNameMap.put('m', "date");
        typeNameMap.put('n', "timestamp");
        typeNameMap.put('o', "null");
        typeNameMap.put('p', "reg_class");
        typeNameMap.put('q', "reg_procedure");
        typeNameMap.put('r', "record");
        typeNameMap.put('s', "string");
        typeNameMap.put('t', "boolean");
        typeNameMap.put('u', "binary");
        typeNameMap.put('v', "var_arg");
        typeNameMap.put('w', "array_string");
        typeNameMap.put('x', "ipv4");
        typeNameMap.put('z', "uuid");
        typeNameMap.put('ø', "varchar");
        typeNameMap.put('δ', "interval");
        typeNameMap.put('ξ', "decimal");
        typeNameMap.put('[' | 32, "[]");
    }
}
