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
package io.questdb.jit;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.*;

import java.util.Arrays;

/**
 * Intermediate representation (IR) serializer for filters (think, WHERE clause)
 * to be used in SQL JIT compiler.
 */
public class FilterExprIRSerializer implements PostOrderTreeTraversalAlgo.Visitor, Mutable {

    // Column type codes
    static final byte MEM_I1 = 0;
    static final byte MEM_I2 = 1;
    static final byte MEM_I4 = 2;
    static final byte MEM_I8 = 3;
    static final byte MEM_F4 = 4;
    static final byte MEM_F8 = 5;
    // Constant type codes
    static final byte IMM_I1 = 6;
    static final byte IMM_I2 = 7;
    static final byte IMM_I4 = 8;
    static final byte IMM_I8 = 9;
    static final byte IMM_F4 = 10;
    static final byte IMM_F8 = 11;
    // Operator codes
    static final byte NEG = 12;               // -a
    static final byte NOT = 13;               // !a
    static final byte AND = 14;               // a && b
    static final byte OR = 15;                // a || b
    static final byte EQ = 16;                // a == b
    static final byte NE = 17;                // a != b
    static final byte LT = 18;                // a <  b
    static final byte LE = 19;                // a <= b
    static final byte GT = 20;                // a >  b
    static final byte GE = 21;                // a >= b
    static final byte ADD = 22;               // a + b
    static final byte SUB = 23;               // a - b
    static final byte MUL = 24;               // a * b
    static final byte DIV = 25;               // a / b
    static final byte MOD = 26;               // a % b
    static final byte JZ = 27;                // if a == 0 jp b
    static final byte JNZ = 28;               // if a != 0 jp b
    static final byte JP = 29;                // jp a
    static final byte RET = 30;               // ret a

    static final byte UNDEFINED_CODE = -1;
    static final int NOT_NULL_COLUMN_MASK = 1 << 31;

    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final ArithmeticExpressionContext arithmeticContext = new ArithmeticExpressionContext();
    // contains <memory_offset, constant_node> pairs for backfilling purposes
    private final LongObjHashMap<ExpressionNode> backfillNodes = new LongObjHashMap<>();

    private MemoryCARW memory;
    private RecordMetadata metadata;

    public FilterExprIRSerializer of(MemoryCARW memory, RecordMetadata metadata) {
        this.memory = memory;
        this.metadata = metadata;
        return this;
    }

    /**
     * Writes IR of the filter described by the given expression tree to memory.
     *
     * @param node   filter expression tree's root node.
     * @param scalar set use only scalar instruction set execution hint in the returned options.
     * @param debug  set enable debug flag in the returned options.
     * @return JIT compiler options stored in a single int in the following way:
     * <ul>
     *     <li>1 LSB - debug flag.</li>
     *     <li>2-3 LSBs - filter's arithmetic type size (widest type size): 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B.</li>
     *     <li>4-5 LSBs - filter's execution hint: 0 - scalar, 1 - single size (SIMD-friendly), 2 - mixed sizes</li>
     * </ul>
     * <br/>
     * Examples:
     * <ul>
     *     <li>00000000 00000000 00000000 00010100 - 4B, mixed types, debug off</li>
     *     <li>00000000 00000000 00000000 00000111 - 8B, scalar, debug on</li>
     * </ul>
     * @throws SqlException thrown when IR serialization failed.
     */
    public int serialize(ExpressionNode node, boolean scalar, boolean debug) throws SqlException {
        traverseAlgo.traverse(node, this);
        memory.putByte(RET);

        TypesObserver typesObserver = arithmeticContext.globalTypesObserver;
        int options = debug ? 1 : 0;
        int typeSize = typesObserver.maxSize();
        if (typeSize > 0) {
            // typeSize is 2^n, so number of trailing zeros is equal to log2
            int log2 = Integer.numberOfTrailingZeros(typeSize);
            options = options | (log2 << 1);
        }
        if (!scalar) {
            int executionHint = typesObserver.hasMixedSizes() ? 2 : 1;
            options = options | (executionHint << 3);
        }
        return options;
    }

    @Override
    public void clear() {
        memory = null;
        metadata = null;
        arithmeticContext.clear();
        backfillNodes.clear();
    }

    @Override
    public boolean descend(ExpressionNode node) throws SqlException {
        if (node.token == null) {
            throw SqlException.position(node.position)
                    .put("non-null token expected: ")
                    .put(node.token);
        }

        // Check if we're at the start of an arithmetic expression
        arithmeticContext.onNodeDescended(node);

        // Look ahead for negative const
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, "-")) {
            ExpressionNode nextNode = node.lhs != null ? node.lhs : node.rhs;
            if (nextNode != null && nextNode.paramCount == 0 && nextNode.type == ExpressionNode.CONSTANT) {
                // Store negation node for later backfilling
                serializeConstantStub(node);
                return false;
            }
        }

        return true;
    }

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExpressionNode.LITERAL:
                    serializeColumn(node.position, node.token);
                    break;
                case ExpressionNode.CONSTANT:
                    // Write stub values to be backfilled later
                    serializeConstantStub(node);
                    break;
                default:
                    throw SqlException.position(node.position)
                            .put("unsupported token: ")
                            .put(node.token);
            }
        } else {
            serializeOperator(node.position, node.token, argCount);
        }

        boolean contextLeft = arithmeticContext.onNodeVisited(node);

        // We're out of arithmetic expression: backfill constants and clean up
        if (contextLeft) {
            try {
                backfillNodes.forEach((key, value) -> {
                    try {
                        backfillConstant(key, value);
                    } catch (SqlException e) {
                        throw new SqlWrapperException(e);
                    }
                });
                backfillNodes.clear();
            } catch (SqlWrapperException e) {
                throw e.wrappedException;
            }
        }
    }

    private void serializeColumn(int position, final CharSequence token) throws SqlException {
        if (!arithmeticContext.isActive()) {
            throw SqlException.position(position)
                    .put("non-boolean column outside of arithmetic expression: ")
                    .put(token);
        }

        int index = metadata.getColumnIndexQuiet(token);
        if (index == -1) {
            throw SqlException.invalidColumn(position, token);
        }

        final int columnType = metadata.getColumnType(index);
        final int columnTypeTag = ColumnType.tagOf(columnType);
        byte typeCode = columnTypeCode(columnTypeTag);
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position)
                    .put("unsupported column type: ")
                    .put(ColumnType.nameOf(columnTypeTag));
        }

        index = metadata.isColumnNullable(index) ? index : index | NOT_NULL_COLUMN_MASK;

        // In case of a top level boolean column, expand it to "boolean_column = true" expression.
        if (arithmeticContext.singleBooleanColumn && columnTypeTag == ColumnType.BOOLEAN) {
            // "true" constant
            memory.putByte(IMM_I1);
            memory.putLong(1);
            // column
            memory.putByte(typeCode);
            memory.putLong(index);
            // =
            memory.putByte(EQ);
            return;
        }

        memory.putByte(typeCode);
        memory.putLong(index);
    }

    private void serializeConstantStub(final ExpressionNode node) throws SqlException {
        if (!arithmeticContext.isActive()) {
            throw SqlException.position(node.position)
                    .put("constant outside of arithmetic expression: ")
                    .put(node.token);
        }

        long offset = memory.getAppendOffset();
        backfillNodes.put(offset, node);
        memory.putByte(UNDEFINED_CODE);
        memory.putLong(0);
    }

    private void backfillConstant(long offset, final ExpressionNode node) throws SqlException {
        int position = node.position;
        CharSequence token = node.token;
        boolean negate = false;
        // Check for negation case
        if (node.type == ExpressionNode.OPERATION) {
            ExpressionNode nextNode = node.lhs != null ? node.lhs : node.rhs;
            if (nextNode != null) {
                position = nextNode.position;
                token = nextNode.token;
                negate = true;
            }
        }

        long originalOffset = memory.getAppendOffset();
        try {
            memory.jumpTo(offset);
            serializeConstant(position, token, negate);
        } finally {
            memory.jumpTo(originalOffset);
        }
    }

    private void serializeConstant(int position, final CharSequence token, boolean negated) throws SqlException {
        final byte typeCode = arithmeticContext.localTypesObserver.constantTypeCode();
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            boolean geoHashExpression = ExpressionType.GEO_HASH == arithmeticContext.expressionType;
            serializeNull(position, typeCode, geoHashExpression);
            return;
        }

        if (Chars.isQuoted(token)) {
            if (ExpressionType.CHAR != arithmeticContext.expressionType) {
                throw SqlException.position(position).put("char constant in non-char expression: ").put(token);
            }
            final int len = token.length();
            if (len == 3) {
                // this is 'x' - char
                memory.putByte(IMM_I2);
                memory.putLong(token.charAt(1));
                return;
            }
            throw SqlException.position(position).put("unsupported char constant: ").put(token);
        }

        if (SqlKeywords.isTrueKeyword(token)) {
            if (ExpressionType.BOOLEAN != arithmeticContext.expressionType) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            if (ExpressionType.BOOLEAN != arithmeticContext.expressionType) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(0);
            return;
        }

        if (ExpressionType.NUMERIC != arithmeticContext.expressionType) {
            throw SqlException.position(position).put("numeric constant in non-numeric expression: ").put(token);
        }
        if (arithmeticContext.localTypesObserver.hasMixedSizes()) {
            serializeUntypedNumber(position, token, negated);
        } else {
            serializeNumber(position, token, typeCode, negated);
        }
    }

    private void serializeNull(int position, byte typeCode, boolean geoHashExpression) throws SqlException {
        memory.putByte(typeCode);
        switch (typeCode) {
            case IMM_I1:
                if (!geoHashExpression) {
                    throw SqlException.position(position).put("byte type is not nullable");
                }
                memory.putLong(GeoHashes.BYTE_NULL);
                break;
            case IMM_I2:
                if (!geoHashExpression) {
                    throw SqlException.position(position).put("short type is not nullable");
                }
                memory.putLong(GeoHashes.SHORT_NULL);
                break;
            case IMM_I4:
                memory.putLong(geoHashExpression ? GeoHashes.INT_NULL : Numbers.INT_NaN);
                break;
            case IMM_I8:
                memory.putLong(geoHashExpression ? GeoHashes.NULL : Numbers.LONG_NaN);
                break;
            case IMM_F4:
                memory.putDouble(Float.NaN);
                break;
            case IMM_F8:
                memory.putDouble(Double.NaN);
                break;
            default:
                throw SqlException.position(position).put("unexpected null type: ").put(typeCode);
        }
    }

    private void serializeNumber(int position, final CharSequence token, byte typeCode, boolean negated) throws SqlException {
        long sign = negated ? -1 : 1;
        try {
            switch (typeCode) {
                case IMM_I1:
                    final byte b = (byte) Numbers.parseInt(token);
                    memory.putByte(IMM_I1);
                    memory.putLong(sign * b);
                    break;
                case IMM_I2:
                    final short s = (short) Numbers.parseInt(token);
                    memory.putByte(IMM_I2);
                    memory.putLong(sign * s);
                    break;
                case IMM_I4:
                case IMM_F4:
                    try {
                        final int i = Numbers.parseInt(token);
                        memory.putByte(IMM_I4);
                        memory.putLong(sign * i);
                    } catch (NumericException e) {
                        final float fi = Numbers.parseFloat(token);
                        memory.putByte(IMM_F4);
                        memory.putDouble(sign * fi);
                    }
                    break;
                case IMM_I8:
                case IMM_F8:
                    try {
                        final long l = Numbers.parseLong(token);
                        memory.putByte(IMM_I8);
                        memory.putLong(sign * l);
                    } catch (NumericException e) {
                        final double dl = Numbers.parseDouble(token);
                        memory.putByte(IMM_F8);
                        memory.putDouble(sign * dl);
                    }
                    break;
                default:
                    throw SqlException.position(position)
                            .put("unexpected non-numeric constant: ").put(token)
                            .put(", expected type: ").put(typeCode);
            }
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("could not parse constant: ").put(token)
                    .put(", expected type: ").put(typeCode);
        }
    }

    private void serializeUntypedNumber(int position, final CharSequence token, boolean negated) throws SqlException {
        long sign = negated ? -1 : 1;

        try {
            final int i = Numbers.parseInt(token);
            memory.putByte(IMM_I4);
            memory.putLong(sign * i);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final long l = Numbers.parseLong(token);
            memory.putByte(IMM_I8);
            memory.putLong(sign * l);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final float f = Numbers.parseFloat(token);
            memory.putByte(IMM_F4);
            memory.putDouble(sign * f);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final double d = Numbers.parseDouble(token);
            memory.putByte(IMM_F8);
            memory.putDouble(sign * d);
            return;
        } catch (NumericException ignore) {
        }

        throw SqlException.position(position).put("unexpected non-numeric constant: ").put(token);
    }

    private void serializeOperator(int position, final CharSequence token, int argCount) throws SqlException {
        if (SqlKeywords.isNotKeyword(token)) {
            memory.putByte(NOT);
            return;
        }
        if (SqlKeywords.isAndKeyword(token)) {
            memory.putByte(AND);
            return;
        }
        if (SqlKeywords.isOrKeyword(token)) {
            memory.putByte(OR);
            return;
        }
        if (Chars.equals(token, "=")) {
            memory.putByte(EQ);
            return;
        }
        if (Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            memory.putByte(NE);
            return;
        }
        if (Chars.equals(token, "<")) {
            memory.putByte(LT);
            return;
        }
        if (Chars.equals(token, "<=")) {
            memory.putByte(LE);
            return;
        }
        if (Chars.equals(token, ">")) {
            memory.putByte(GT);
            return;
        }
        if (Chars.equals(token, ">=")) {
            memory.putByte(GE);
            return;
        }
        if (Chars.equals(token, "+")) {
            if (argCount == 2) {
                memory.putByte(ADD);
            } // ignore unary
            return;
        }
        if (Chars.equals(token, "-")) {
            if (argCount == 2) {
                memory.putByte(SUB);
            } else if (argCount == 1) {
                memory.putByte(NEG);
            }
            return;
        }
        if (Chars.equals(token, "*")) {
            memory.putByte(MUL);
            return;
        }
        if (Chars.equals(token, "/")) {
            memory.putByte(DIV);
            return;
        }
        throw SqlException.position(position).put("invalid operator: ").put(token);
    }

    private static byte columnTypeCode(int columnTypeTag) {
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                return MEM_I1;
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.CHAR:
                return MEM_I2;
            case ColumnType.INT:
            case ColumnType.GEOINT:
            case ColumnType.SYMBOL:
                return MEM_I4;
            case ColumnType.FLOAT:
                return MEM_F4;
            case ColumnType.LONG:
            case ColumnType.GEOLONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return MEM_I8;
            case ColumnType.DOUBLE:
                return MEM_F8;
            default:
                return UNDEFINED_CODE;
        }
    }

    private static boolean isTopLevelOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (SqlKeywords.isNotKeyword(token)) {
            return true;
        }
        if (node.paramCount < 2) {
            return false;
        }
        if (Chars.equals(token, "=")) {
            return true;
        }
        if (Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            return true;
        }
        if (Chars.equals(token, "<")) {
            return true;
        }
        if (Chars.equals(token, "<=")) {
            return true;
        }
        if (Chars.equals(token, ">")) {
            return true;
        }
        return Chars.equals(token, ">=");
    }

    private boolean isTopLevelBooleanColumn(ExpressionNode node) {
        if (node.type == ExpressionNode.LITERAL && isBooleanColumn(node)) {
            return true;
        }
        // Lookahead for "not boolean_column" case
        final CharSequence token = node.token;
        if (SqlKeywords.isNotKeyword(token)) {
            ExpressionNode columnNode = node.lhs != null ? node.lhs : node.rhs;
            return columnNode != null && isBooleanColumn(columnNode);
        }
        return false;
    }

    private boolean isBooleanColumn(ExpressionNode node) {
        if (node.type != ExpressionNode.LITERAL) {
            return false;
        }
        int index = metadata.getColumnIndexQuiet(node.token);
        if (index == -1) {
            return false;
        }
        final int columnType = metadata.getColumnType(index);
        final int columnTypeTag = ColumnType.tagOf(columnType);
        return columnTypeTag == ColumnType.BOOLEAN;
    }

    private class ArithmeticExpressionContext implements Mutable {

        private ExpressionNode rootNode;
        ExpressionType expressionType;
        boolean singleBooleanColumn;

        final TypesObserver localTypesObserver = new TypesObserver();
        final TypesObserver globalTypesObserver = new TypesObserver();

        @Override
        public void clear() {
            reset();
            globalTypesObserver.clear();
        }

        private void reset() {
            rootNode = null;
            expressionType = null;
            singleBooleanColumn = false;
            localTypesObserver.clear();
        }

        public boolean isActive() {
            return rootNode != null;
        }

        public void onNodeDescended(final ExpressionNode node) {
            if (rootNode == null) {
                boolean topLevelOperation = isTopLevelOperation(node);
                boolean topLevelBooleanColumn = isTopLevelBooleanColumn(node);
                if (topLevelOperation || topLevelBooleanColumn) {
                    reset();
                    rootNode = node;
                }
                if (topLevelBooleanColumn) {
                    expressionType = ExpressionType.BOOLEAN;
                    singleBooleanColumn = true;
                }
            }
        }

        public boolean onNodeVisited(final ExpressionNode node) throws SqlException {
            boolean contextLeft = false;
            if (node == rootNode) {
                rootNode = null;
                contextLeft = true;
            }

            if (node.type == ExpressionNode.LITERAL) {
                final int index = metadata.getColumnIndexQuiet(node.token);
                if (index == -1) {
                    throw SqlException.invalidColumn(node.position, node.token);
                }

                final int columnType = metadata.getColumnType(index);
                final int columnTypeTag = ColumnType.tagOf(columnType);

                updateExpressionType(node.position, columnTypeTag);

                byte code = columnTypeCode(columnTypeTag);
                localTypesObserver.observe(code);
                globalTypesObserver.observe(code);
            }

            return contextLeft;
        }

        private void updateExpressionType(int position, int columnTypeTag) throws SqlException {
            switch (columnTypeTag) {
                case ColumnType.BOOLEAN:
                    if (expressionType != null && expressionType != ExpressionType.BOOLEAN) {
                        throw SqlException.position(position)
                                .put("non-boolean column in boolean expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    expressionType = ExpressionType.BOOLEAN;
                    break;
                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    if (expressionType != null && expressionType != ExpressionType.GEO_HASH) {
                        throw SqlException.position(position)
                                .put("non-geohash column in geohash expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    expressionType = ExpressionType.GEO_HASH;
                    break;
                case ColumnType.CHAR:
                    if (expressionType != null && expressionType != ExpressionType.CHAR) {
                        throw SqlException.position(position)
                                .put("non-char column in char expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    expressionType = ExpressionType.CHAR;
                    break;
                case ColumnType.SYMBOL:
                    if (expressionType != null && expressionType != ExpressionType.SYMBOL) {
                        throw SqlException.position(position)
                                .put("non-symbol column in symbol expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    expressionType = ExpressionType.SYMBOL;
                    break;
                default:
                    if (expressionType != null && expressionType != ExpressionType.NUMERIC) {
                        throw SqlException.position(position)
                                .put("non-numeric column in numeric expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    expressionType = ExpressionType.NUMERIC;
                    break;
            }
        }
    }

    /**
     * Helper class for accumulating column types information.
     */
    private static class TypesObserver implements Mutable {

        private static final int I1_INDEX = 0;
        private static final int I2_INDEX = 1;
        private static final int I4_INDEX = 2;
        private static final int F4_INDEX = 3;
        private static final int I8_INDEX = 4;
        private static final int F8_INDEX = 5;
        private static final int TYPES_COUNT = F8_INDEX + 1;

        private final byte[] sizes = new byte[TYPES_COUNT];

        public void observe(byte code) {
            switch (code) {
                case MEM_I1:
                    sizes[I1_INDEX] = 1;
                    break;
                case MEM_I2:
                    sizes[I2_INDEX] = 2;
                    break;
                case MEM_I4:
                    sizes[I4_INDEX] = 4;
                    break;
                case MEM_F4:
                    sizes[F4_INDEX] = 4;
                    break;
                case MEM_I8:
                    sizes[I8_INDEX] = 8;
                    break;
                case MEM_F8:
                    sizes[F8_INDEX] = 8;
                    break;
            }
        }

        /**
         * Returns the expected constant type calculated based on the "widest" observed column type.
         * The result contains IMM_* value or UNDEFINED_CODE value.
         */
        public byte constantTypeCode() {
            for (int i = sizes.length - 1; i > -1; i--) {
                byte size = sizes[i];
                if (size > 0) {
                    // If floats are present, longs have to be cast to double.
                    if (i == I8_INDEX && sizes[F4_INDEX] > 0) {
                        return IMM_F8;
                    }
                    return indexToTypeCode(i);
                }
            }
            return UNDEFINED_CODE;
        }

        private byte indexToTypeCode(int index) {
            switch (index) {
                case I1_INDEX:
                    return IMM_I1;
                case I2_INDEX:
                    return IMM_I2;
                case I4_INDEX:
                    return IMM_I4;
                case F4_INDEX:
                    return IMM_F4;
                case I8_INDEX:
                    return IMM_I8;
                case F8_INDEX:
                    return IMM_F8;
            }
            return UNDEFINED_CODE;
        }

        /**
         * Returns size in bytes of the "widest" observed column type.
         */
        public int maxSize() {
            for (int i = sizes.length - 1; i > -1; i--) {
                byte size = sizes[i];
                if (size > 0) {
                    return size;
                }
            }
            return 0;
        }

        public boolean hasMixedSizes() {
            byte prevSize = 0;
            for (byte size : sizes) {
                prevSize = prevSize == 0 ? size : prevSize;
                if (prevSize > 0) {
                    if (size > 0 && size != prevSize) {
                        return true;
                    }
                } else {
                    prevSize = size;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            Arrays.fill(sizes, (byte) 0);
        }
    }

    private static class SqlWrapperException extends RuntimeException {

        final SqlException wrappedException;

        SqlWrapperException(SqlException wrappedException) {
            this.wrappedException = wrappedException;
        }
    }

    private enum ExpressionType {
        NUMERIC, CHAR, SYMBOL, BOOLEAN, GEO_HASH
    }
}
