/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.ObjList;

public final class OperatorExpression {

    public static final int BINARY = 2;
    public static final int SET = 3;
    public static final int UNARY = 1;
    public static final int DOT_PRECEDENCE = 1;
    static final OperatorExpression UnaryMinus = new OperatorExpression("-", 3, false, UNARY);
    static final OperatorExpression UnaryComplement = new OperatorExpression("~", 3, false, UNARY);
    static final OperatorExpression SetOperationNegation = new OperatorExpression("not", 11, false, UNARY);
    static final ObjList<OperatorExpression> operators = new ObjList<OperatorExpression>() {{
        add(new OperatorExpression(".", DOT_PRECEDENCE, false, BINARY));

        add(new OperatorExpression("::", 2, true, BINARY));
        // arithmetic operators, UnaryMinus and UnaryComplement defined above are strongest from this block
        add(new OperatorExpression("*", 4, true, BINARY));
        add(new OperatorExpression("/", 4, true, BINARY));
        add(new OperatorExpression("%", 4, true, BINARY));

        add(new OperatorExpression("+", 5, true, BINARY));
        add(new OperatorExpression("-", 5, true, BINARY));
        // IP operators
        add(new OperatorExpression("<<", 6, true, BINARY));
        add(new OperatorExpression(">>", 6, true, BINARY));
        add(new OperatorExpression("<<=", 6, true, BINARY));
        add(new OperatorExpression(">>=", 6, true, BINARY));
        // concatenation
        add(new OperatorExpression("||", 7, true, BINARY));
        // bitwise operators
        add(new OperatorExpression("&", 8, true, BINARY));
        add(new OperatorExpression("^", 9, true, BINARY));
        add(new OperatorExpression("|", 10, true, BINARY));
        // set operators
        add(new OperatorExpression("in", 11, false, SET, false));
        add(new OperatorExpression("between", 11, false, SET, false)); // ternary operator
        add(new OperatorExpression("within", 11, false, SET, false));
        // boolean operators
        add(new OperatorExpression("<", 12, true, BINARY));
        add(new OperatorExpression("<=", 12, true, BINARY));
        add(new OperatorExpression(">", 12, true, BINARY));
        add(new OperatorExpression(">=", 12, true, BINARY));
        add(new OperatorExpression("=", 13, true, BINARY));
        add(new OperatorExpression("!=", 13, true, BINARY));
        add(new OperatorExpression("<>", 13, true, BINARY));
        add(new OperatorExpression("~", 13, true, BINARY));
        add(new OperatorExpression("!~", 13, true, BINARY));
        add(new OperatorExpression("like", 13, true, BINARY, false));
        add(new OperatorExpression("ilike", 13, true, BINARY, false));
        // logical operators
        add(new OperatorExpression("not", 14, false, UNARY, false));
        add(new OperatorExpression("and", 15, true, BINARY, false));
        add(new OperatorExpression("or", 16, true, BINARY, false));
    }};

    static final LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression> opMap = new LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            OperatorExpression op = operators.getQuick(i);
            put(op.token, op);
        }
    }};
    final boolean leftAssociative;
    final int precedence;
    final boolean symbol;
    final String token;
    final int type;

    private OperatorExpression(String token, int precedence, boolean leftAssociative, int type, boolean symbol) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = symbol;
    }

    private OperatorExpression(String token, int precedence, boolean leftAssociative, int type) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = true;
    }

    public static int getOperatorType(CharSequence name) {
        int index = opMap.keyIndex(name);
        if (index < 0) {
            return opMap.valueAt(index).type;
        }
        return 0;
    }

    public static boolean isOperator(CharSequence name) {
        return opMap.keyIndex(name) < 0;
    }
}
