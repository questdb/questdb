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
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public class FilterExprIRSerializer implements PostOrderTreeTraversalAlgo.Visitor, Mutable {

    // Column types
    static final byte MEM_I1 = 0;
    static final byte MEM_I2 = 1;
    static final byte MEM_I4 = 2;
    static final byte MEM_I8 = 3;
    static final byte MEM_F4 = 4;
    static final byte MEM_F8 = 5;
    // Constant types
    static final byte IMM_I1 = 6;
    static final byte IMM_I2 = 7;
    static final byte IMM_I4 = 8;
    static final byte IMM_I8 = 9;
    static final byte IMM_F4 = 10;
    static final byte IMM_F8 = 11;
    // Operators
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
    private final ArithmeticExpressionContext exprContext = new ArithmeticExpressionContext();
    private MemoryA memory;
    private RecordMetadata metadata;

    // TODO:
    // +* constant type tagging:
    // afloat + along = 42 => double
    // along / along = 42 => long
    // edge cases:
    // along > 1.5 => we could truncate 1.5 to 1, but for now we're not going to support such filters in JIT
    // afloat > 1 <- this one is fine
    //
    // +* negate constant => negative constant
    // * constant folding??? - nice to have
    // * restricted cast support: CAST(along TO DOUBLE) - nice to have
    // * geohashes have special null values e.g. GeoHashes.BYTE_NULL
    // * EXPLAIN PLAN or at least JIT compilation phase

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
        exprContext.clear();
    }

    @Override
    public boolean descent(ExpressionNode node) throws SqlException {
        // Look ahead for negative const
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, "-")) {
            ExpressionNode nextNode = node.lhs != null ? node.lhs : node.rhs;
            if (nextNode != null && nextNode.paramCount == 0 && nextNode.type == ExpressionNode.CONSTANT) {
                serializeConstant(nextNode.position, nextNode.token, true);
                return false;
            }
        }

        exprContext.onNodeEntered(node);
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
                    serializeConstant(node.position, node.token, false);
                    break;
                default:
                    throw SqlException.position(node.position)
                            .put("unsupported token: ")
                            .put(node.token);
            }
        } else {
            serializeOperator(node.position, node.token, argCount);
        }

        exprContext.onNodeLeft(node);
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

    private void serializeConstant(int position, final CharSequence token, boolean negate) throws SqlException {
        final byte typeCode = exprContext.typeCode;
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            boolean geohashExpression = ExpressionType.GEOHASH == exprContext.expressionType;
            serializeNull(position, typeCode, geohashExpression);
            return;
        }

        if (Chars.isQuoted(token)) {
            if (ExpressionType.CHAR != exprContext.expressionType) {
                throw SqlException.position(position).put("char constant in non-char expression: ").put(token);
            }
            final int len = token.length();
            if (len == 3) {
                // this is 'x' - char
                memory.putByte(IMM_I2);
                memory.putLong(token.charAt(1));
                return;
            }
            throw SqlException.position(position).put("invalid constant: ").put(token);
        }

        if (SqlKeywords.isTrueKeyword(token)) {
            if (ExpressionType.BOOLEAN != exprContext.expressionType) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            if (ExpressionType.BOOLEAN != exprContext.expressionType) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(IMM_I1);
            memory.putLong(0);
            return;
        }

        if (ExpressionType.NUMERIC != exprContext.expressionType) {
            throw SqlException.position(position).put("numeric constant in non-numeric expression: ").put(token);
        }
        try {
            long sign = negate ? -1 : 1;
            switch (typeCode) {
                case IMM_I1:
                    final byte b = (byte) Numbers.parseInt(token);
                    memory.putByte(IMM_I1);
                    memory.putLong(sign * b);
                    return;
                case IMM_I2:
                    final short s = (short) Numbers.parseInt(token);
                    memory.putByte(IMM_I2);
                    memory.putLong(sign * s);
                    return;
                case IMM_I4:
                    final int i = Numbers.parseInt(token);
                    memory.putByte(IMM_I4);
                    memory.putLong(sign * i);
                    return;
                case IMM_I8:
                    final long l = Numbers.parseLong(token);
                    memory.putByte(IMM_I8);
                    memory.putLong(sign * l);
                    return;
                case IMM_F4:
                    final float f = Numbers.parseFloat(token);
                    memory.putByte(IMM_F4);
                    memory.putDouble(sign * f);
                    return;
                case IMM_F8:
                    final double d = Numbers.parseDouble(token);
                    memory.putByte(IMM_F8);
                    memory.putDouble(sign * d);
                    return;
            }
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("could not parse constant: ").put(token)
                    .put(", expected type: ").put(typeCode);
        }

        throw SqlException.position(position).put("invalid constant: ").put(token);
    }

    private void serializeNull(int position, byte typeCode, boolean geohashExpression) throws SqlException {
        memory.putByte(typeCode);
        switch (typeCode) {
            case IMM_I1:
                memory.putLong(geohashExpression ? GeoHashes.BYTE_NULL : NullConstant.NULL.getByte(null));
                break;
            case IMM_I2:
                memory.putLong(geohashExpression ? GeoHashes.SHORT_NULL : NullConstant.NULL.getShort(null));
                break;
            case IMM_I4:
                memory.putLong(geohashExpression ? GeoHashes.INT_NULL : NullConstant.NULL.getInt(null));
                break;
            case IMM_I8:
                memory.putLong(geohashExpression ? GeoHashes.NULL : NullConstant.NULL.getLong(null));
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

    private class ArithmeticExpressionContext implements PostOrderTreeTraversalAlgo.Visitor, Mutable {

        private ExpressionNode rootNode;
        // based on the widest column type in expression (contains IMM_* or UNDEFINED_CODE value)
        byte typeCode;
        ExpressionType expressionType;

        @Override
        public void clear() {
            rootNode = null;
        }

        public void onNodeEntered(ExpressionNode node) throws SqlException {
            if (rootNode == null && isTopLevelArithmeticOperation(node)) {
                rootNode = node;
                typeCode = UNDEFINED_CODE;
                expressionType = null;
                traverseAlgo.traverse(node, this);
            }
        }

        public void onNodeLeft(ExpressionNode node) {
            if (rootNode != null && rootNode == node) {
                rootNode = null;
            }
        }

        @Override
        public void visit(ExpressionNode node) throws SqlException {
            if (node.type != ExpressionNode.LITERAL) {
                return;
            }

            final int index = metadata.getColumnIndexQuiet(node.token);
            if (index == -1) {
                throw SqlException.invalidColumn(node.position, node.token);
            }

            int columnType = metadata.getColumnType(index);

            updateExpressionType(node.position, columnType);
            updateTypeCode(node.position, columnType);
        }

        private boolean isTopLevelArithmeticOperation(ExpressionNode node) {
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

        private void updateExpressionType(int position, int columnType) throws SqlException {
            switch (columnType) {
                case ColumnType.BOOLEAN:
                    if (expressionType != null && expressionType != ExpressionType.BOOLEAN) {
                        throw SqlException.position(position)
                                .put("non-boolean column in boolean expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    expressionType = ExpressionType.BOOLEAN;
                    break;
                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    if (expressionType != null && expressionType != ExpressionType.GEOHASH) {
                        throw SqlException.position(position)
                                .put("non-geohash column in geohash expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    expressionType = ExpressionType.GEOHASH;
                    break;
                case ColumnType.CHAR:
                    if (expressionType != null && expressionType != ExpressionType.CHAR) {
                        throw SqlException.position(position)
                                .put("non-char column in char expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    expressionType = ExpressionType.CHAR;
                    break;
                default:
                    if (expressionType != null && expressionType != ExpressionType.NUMERIC) {
                        throw SqlException.position(position)
                                .put("non-numeric column in numeric expression: ")
                                .put(ColumnType.nameOf(columnType));
                    }
                    expressionType = ExpressionType.NUMERIC;
                    break;
            }
        }

        private void updateTypeCode(int position, int columnType) throws SqlException {
            byte code = columnTypeCode(columnType);
            if (code == UNDEFINED_CODE) {
                throw SqlException.position(position)
                        .put("unsupported column type: ")
                        .put(ColumnType.nameOf(columnType));
            }

            switch (code) {
                case MEM_I1:
                    if (typeCode == UNDEFINED_CODE) {
                        typeCode = IMM_I1;
                    }
                    break;
                case MEM_I2:
                    if (typeCode < IMM_I2) {
                        typeCode = IMM_I2;
                    }
                    break;
                case MEM_I4:
                    if (typeCode < IMM_I4) {
                        typeCode = IMM_I4;
                    }
                    break;
                case MEM_I8:
                    if (typeCode < IMM_I8) {
                        typeCode = IMM_I8;
                    }
                    if (typeCode == IMM_F4) {
                        typeCode = IMM_F8;
                    }
                    break;
                case MEM_F4:
                    if (typeCode <= IMM_I4) {
                        typeCode = IMM_F4;
                    }
                    if (typeCode == IMM_I8) {
                        typeCode = IMM_F8;
                    }
                    break;
                case MEM_F8:
                    typeCode = IMM_F8;
                    break;
                default:
                    throw SqlException.position(position)
                            .put("unexpected numeric type for column: ")
                            .put(ColumnType.nameOf(columnType));
            }
        }
    }

    private enum ExpressionType {
        NUMERIC, CHAR, BOOLEAN, GEOHASH
    }
}
