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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.*;

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

    private static final byte UNDEFINED_CODE = -1;

    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final ArithmeticExpressionContext arithmeticContext = new ArithmeticExpressionContext();
    // contains <memory_offset, constant_node> pairs for backfilling purposes
    private final LongObjHashMap<ExpressionNode> backfillNodes = new LongObjHashMap<>();

    private MemoryA memory;
    private RecordMetadata metadata;

    public FilterExprIRSerializer of(MemoryA memory, RecordMetadata metadata) {
        this.memory = memory;
        this.metadata = metadata;
        return this;
    }

    public void serialize(ExpressionNode node) throws SqlException {
        traverseAlgo.traverse(node, this);
        memory.putByte(RET);
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
        // Check if we're at the start of an arithmetic expression
        arithmeticContext.onNodeDescended(node);

        // Check for root expression level constant
        if (arithmeticContext.rootNode == null && node.type == ExpressionNode.CONSTANT) {
            throw SqlException.position(node.position)
                    .put("constant outside of arithmetic expression: ")
                    .put(node.token);
        }

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

        arithmeticContext.onNodeVisited(node);

        // We're out of arithmetic expression: backfill constants and clean up
        if (node == arithmeticContext.rootNode) {
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

            arithmeticContext.clear();
        }
    }

    private void serializeColumn(int position, final CharSequence token) throws SqlException {
        final int index = metadata.getColumnIndexQuiet(token);
        if (index == -1) {
            throw SqlException.invalidColumn(position, token);
        }

        int columnType = metadata.getColumnType(index);
        byte typeCode = columnTypeCode(columnType);
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position)
                    .put("unsupported column type: ")
                    .put(ColumnType.nameOf(columnType));
        }
        memory.putByte(typeCode);
        memory.putLong(index);
    }

    private void serializeConstantStub(final ExpressionNode node) {
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

    private void serializeConstant(int position, final CharSequence token, boolean negate) throws SqlException {
        final byte typeCode = arithmeticContext.constantTypeCode;
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            boolean geoHashExpression = ExpressionType.GEO_HASH == arithmeticContext.type;
            serializeNull(position, typeCode, geoHashExpression);
            return;
        }

        if (Chars.isQuoted(token)) {
            if (ExpressionType.CHAR != arithmeticContext.type) {
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
            if (ExpressionType.BOOLEAN != arithmeticContext.type) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            if (ExpressionType.BOOLEAN != arithmeticContext.type) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(0);
            return;
        }

        if (ExpressionType.NUMERIC != arithmeticContext.type) {
            throw SqlException.position(position).put("numeric constant in non-numeric expression: ").put(token);
        }
        serializeNumber(position, token, typeCode, negate);
    }

    private void serializeNull(int position, byte typeCode, boolean geoHashExpression) throws SqlException {
        memory.putByte(typeCode);
        switch (typeCode) {
            case IMM_I1:
                memory.putLong(geoHashExpression ? GeoHashes.BYTE_NULL : NullConstant.NULL.getByte(null));
                break;
            case IMM_I2:
                memory.putLong(geoHashExpression ? GeoHashes.SHORT_NULL : NullConstant.NULL.getShort(null));
                break;
            case IMM_I4:
                memory.putLong(geoHashExpression ? GeoHashes.INT_NULL : NullConstant.NULL.getInt(null));
                break;
            case IMM_I8:
                memory.putLong(geoHashExpression ? GeoHashes.NULL : NullConstant.NULL.getLong(null));
                break;
            case IMM_F4:
                memory.putDouble(NullConstant.NULL.getFloat(null));
                break;
            case IMM_F8:
                memory.putDouble(NullConstant.NULL.getDouble(null));
                break;
            default:
                throw SqlException.position(position).put("unexpected null type: ").put(typeCode);
        }
    }

    private void serializeNumber(int position, final CharSequence token, byte typeCode, boolean negate) throws SqlException {
        memory.putByte(typeCode);
        long sign = negate ? -1 : 1;
        try {
            switch (typeCode) {
                case IMM_I1:
                    final byte b = (byte) Numbers.parseInt(token);
                    memory.putLong(sign * b);
                    break;
                case IMM_I2:
                    final short s = (short) Numbers.parseInt(token);
                    memory.putLong(sign * s);
                    break;
                case IMM_I4:
                    final int i = Numbers.parseInt(token);
                    memory.putLong(sign * i);
                    break;
                case IMM_I8:
                    final long l = Numbers.parseLong(token);
                    memory.putLong(sign * l);
                    break;
                case IMM_F4:
                    final float f = Numbers.parseFloat(token);
                    memory.putDouble(sign * f);
                    break;
                case IMM_F8:
                    final double d = Numbers.parseDouble(token);
                    memory.putDouble(sign * d);
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

    private static byte columnTypeCode(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
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

    private static boolean isTopLevelArithmeticOperation(ExpressionNode node) {
        if (node.paramCount < 2) {
            return false;
        }
        final CharSequence token = node.token;
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

    private class ArithmeticExpressionContext implements Mutable {

        ExpressionNode rootNode;
        // expected constant type; calculated based on the "widest" column type
        // in the expression (contains IMM_* or UNDEFINED_CODE value)
        byte constantTypeCode;
        ExpressionType type;

        @Override
        public void clear() {
            rootNode = null;
            constantTypeCode = UNDEFINED_CODE;
            type = null;
        }

        public void onNodeDescended(final ExpressionNode node) {
            if (rootNode == null && isTopLevelArithmeticOperation(node)) {
                rootNode = node;
                constantTypeCode = UNDEFINED_CODE;
                type = null;
            }
        }

        public void onNodeVisited(final ExpressionNode node) throws SqlException {
            if (node.type != ExpressionNode.LITERAL) {
                return;
            }

            final int index = metadata.getColumnIndexQuiet(node.token);
            if (index == -1) {
                throw SqlException.invalidColumn(node.position, node.token);
            }

            int columnType = metadata.getColumnType(index);

            updateType(node.position, columnType);
            updateConstantTypeCode(node.position, columnType);
        }

        private void updateType(int position, int columnType) throws SqlException {
            switch (columnType) {
                case ColumnType.BOOLEAN:
                    if (type != null && type != ExpressionType.BOOLEAN) {
                        throw SqlException.position(position)
                                .put("non-boolean column in boolean expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    type = ExpressionType.BOOLEAN;
                    break;
                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    if (type != null && type != ExpressionType.GEO_HASH) {
                        throw SqlException.position(position)
                                .put("non-geohash column in geohash expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    type = ExpressionType.GEO_HASH;
                    break;
                case ColumnType.CHAR:
                    if (type != null && type != ExpressionType.CHAR) {
                        throw SqlException.position(position)
                                .put("non-char column in char expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    type = ExpressionType.CHAR;
                    break;
                default:
                    if (type != null && type != ExpressionType.NUMERIC) {
                        throw SqlException.position(position)
                                .put("non-numeric column in numeric expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    type = ExpressionType.NUMERIC;
                    break;
            }
        }

        private void updateConstantTypeCode(int position, int columnType) throws SqlException {
            byte code = columnTypeCode(columnType);
            switch (code) {
                case MEM_I1:
                    if (constantTypeCode == UNDEFINED_CODE) {
                        constantTypeCode = IMM_I1;
                    }
                    break;
                case MEM_I2:
                    if (constantTypeCode < IMM_I2) {
                        constantTypeCode = IMM_I2;
                    }
                    break;
                case MEM_I4:
                    if (constantTypeCode < IMM_I4) {
                        constantTypeCode = IMM_I4;
                    }
                    break;
                case MEM_I8:
                    if (constantTypeCode < IMM_I8) {
                        constantTypeCode = IMM_I8;
                    }
                    if (constantTypeCode == IMM_F4) {
                        constantTypeCode = IMM_F8;
                    }
                    break;
                case MEM_F4:
                    if (constantTypeCode <= IMM_I4) {
                        constantTypeCode = IMM_F4;
                    }
                    if (constantTypeCode == IMM_I8) {
                        constantTypeCode = IMM_F8;
                    }
                    break;
                case MEM_F8:
                    constantTypeCode = IMM_F8;
                    break;
                default:
                    throw SqlException.position(position)
                            .put("expected numeric column type: ")
                            .put(ColumnType.nameOf(columnType));
            }
        }
    }

    private static class SqlWrapperException extends RuntimeException {

        final SqlException wrappedException;

        SqlWrapperException(SqlException wrappedException) {
            this.wrappedException = wrappedException;
        }
    }

    private enum ExpressionType {
        NUMERIC, CHAR, BOOLEAN, GEO_HASH
    }
}
