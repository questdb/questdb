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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public class FilterExprIRSerializer {

    private static final byte MEM_I1 = 0;
    private static final byte MEM_I2 = 1;
    private static final byte MEM_I4 = 2;
    private static final byte MEM_I8 = 3;
    private static final byte MEM_F4 = 4;
    private static final byte MEM_F8 = 5;

    private static final byte IMM_I1 = 6;
    private static final byte IMM_I2 = 7;
    private static final byte IMM_I4 = 8;
    private static final byte IMM_I8 = 9;
    private static final byte IMM_F4 = 10;
    private static final byte IMM_F8 = 11;

    private static final byte NEG = 12;               // -a
    private static final byte NOT = 13;               // !a
    private static final byte AND = 14;               // a && b
    private static final byte OR = 15;                // a || b
    private static final byte EQ = 16;                // a == b
    private static final byte NE = 17;                // a != b
    private static final byte LT = 18;                // a <  b
    private static final byte LE = 19;                // a <= b
    private static final byte GT = 20;                // a >  b
    private static final byte GE = 21;                // a >= b
    private static final byte ADD = 22;               // a + b
    private static final byte SUB = 23;               // a - b
    private static final byte MUL = 24;               // a * b
    private static final byte DIV = 25;               // a / b
    private static final byte MOD = 26;               // a % b
    private static final byte JZ = 27;                // if a == 0 jp b
    private static final byte JNZ = 28;               // if a != 0 jp b
    private static final byte JP = 29;                // jp a
    private static final byte RET = 30;               // ret a
    private static final byte IMM_NULL = 31;          // generic null const

    public static void serialize(MemoryA memory, ExpressionNode node, RecordMetadata metadata) throws SqlException {
        postOrder(memory, node, metadata);
        memory.putByte(RET);
    }

    public static void postOrder(MemoryA memory, ExpressionNode node, RecordMetadata metadata) throws SqlException {
        if (node != null) {

            postOrder(memory, node.lhs, metadata);
            postOrder(memory, node.rhs, metadata);

            int argCount = node.paramCount;
            if (argCount == 0) {
                switch (node.type) {
                    case ExpressionNode.LITERAL:
                        serializeColumn(memory, metadata, node.position, node.token);
                        break;
                    case ExpressionNode.CONSTANT:
                        serializeConstant(memory, node.position, node.token);
                        break;
                    default:
                        throw SqlException.position(node.position)
                                .put("unsupported token")
                                .put(node.token);
                }
            } else {
                serializeOperator(memory, node.position, node.token, argCount);
            }
        }
    }

    private static void serializeColumn(MemoryA memory, RecordMetadata metadata, int position, final CharSequence token) throws SqlException {
        final int index = metadata.getColumnIndexQuiet(token);

        if (index == -1) {
            throw SqlException.invalidColumn(position, token);
        }

        int columnType = metadata.getColumnType(index);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                memory.putByte(MEM_I1);
                memory.putLong(index);
                break;
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.CHAR:
                memory.putByte(MEM_I2);
                memory.putLong(index);
                break;
            case ColumnType.INT:
            case ColumnType.GEOINT:
            case ColumnType.SYMBOL:
                memory.putByte(MEM_I4);
                memory.putLong(index);
                break;
            case ColumnType.FLOAT:
                memory.putByte(MEM_F4);
                memory.putLong(index);
                break;
            case ColumnType.LONG:
            case ColumnType.GEOLONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                memory.putByte(MEM_I8);
                memory.putLong(index);
                break;
            case ColumnType.DOUBLE:
                memory.putByte(MEM_F8);
                memory.putLong(index);
                break;
            default:
                throw SqlException.position(position)
                        .put("unsupported column type ")
                        .put(ColumnType.nameOf(columnType));
        }
    }

    private static void serializeConstant(MemoryA memory, int position, final CharSequence token) throws SqlException {
//        if (SqlKeywords.isNullKeyword(token)) {
//            memory.putByte(IMM_NULL);
//            return;
//        }
//
//        if (Chars.isQuoted(token)) {
//            final int len = token.length();
//            if (len == 3) {
//                // this is 'x' - char
//                memory.putByte(IMM_I2);
//                memory.putChar(token.charAt(1));
//                return;
//            }
//
//            if (len == 2) {
//                // empty
//                memory.putByte(IMM_I2);
//                memory.putChar((char)0);
//                return;
//            }
//            throw SqlException.position(position).put("invalid constant: ").put(token);
//        }
//
        if (SqlKeywords.isTrueKeyword(token)) {
            memory.putByte(IMM_I1);
            memory.putLong(1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            memory.putByte(IMM_I1);
            memory.putLong(0);
            return;
        }

//        try {
//            final int n = Numbers.parseInt(token);
//            memory.putByte(IMM_I4);
//            memory.putLong(n);
//            return;
//        } catch (NumericException ignore) {
//        }

        try {
            final long n = Numbers.parseLong(token);
            memory.putByte(IMM_I8);
            memory.putLong(n);
            return;
        } catch (NumericException ignore) {
        }

//        try {
//            final float n = Numbers.parseFloat(token);
//            memory.putByte(IMM_F4);
//            memory.putDouble(n);
//            return;
//        } catch (NumericException ignore) {
//        }

        try {
            final double n = Numbers.parseDouble(token);
            memory.putByte(IMM_F8);
            memory.putDouble(n);
            return;
        } catch (NumericException ignore) {
        }

        throw SqlException.position(position).put("invalid constant: ").put(token);
    }

    private static void serializeOperator(MemoryA memory, int position, final CharSequence token, int argCount) throws SqlException {
        if(Chars.equals(token, "not")) {
            memory.putByte(NOT);
            return;
        }
        if(Chars.equals(token, "and")) {
            memory.putByte(AND);
            return;
        }
        if(Chars.equals(token, "or")) {
            memory.putByte(OR);
            return;
        }
        if(Chars.equals(token, "=")) {
            memory.putByte(EQ);
            return;
        }
        if(Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            memory.putByte(NE);
            return;
        }
        if(Chars.equals(token, "<")) {
            memory.putByte(LT);
            return;
        }
        if(Chars.equals(token, "<=")) {
            memory.putByte(LE);
            return;
        }
        if(Chars.equals(token, ">")) {
            memory.putByte(GT);
            return;
        }
        if(Chars.equals(token, ">=")) {
            memory.putByte(GE);
            return;
        }
        if(Chars.equals(token, "+")) {
            if(argCount == 2) {
                memory.putByte(ADD);
            } // ignore unary
            return;
        }
        if(Chars.equals(token, "-")) {
            if(argCount == 2) {
                memory.putByte(SUB);
            } else if (argCount == 1) {
                memory.putByte(NEG);
            }
            return;
        }
        if(Chars.equals(token, "*")) {
            memory.putByte(MUL);
            return;
        }
        if(Chars.equals(token, "/")) {
            memory.putByte(DIV);
            return;
        }
        throw SqlException.position(position).put("invalid operator: ").put(token);
    }
}
