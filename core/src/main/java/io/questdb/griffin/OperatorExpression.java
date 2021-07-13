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

import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.ObjList;

public final class OperatorExpression {

    public static final int UNARY = 1;
    public static final int BINARY = 2;
    public static final int SET = 3;
    public static final int DOT_PRECEDENCE = 1;

    static final ObjList<OperatorExpression> operators = new ObjList<OperatorExpression>() {{
        add(new OperatorExpression(".", DOT_PRECEDENCE, false, BINARY));
        add(new OperatorExpression("::", DOT_PRECEDENCE, false, BINARY));
        add(new OperatorExpression("*", 3, true, BINARY));
        add(new OperatorExpression("/", 3, true, BINARY));
        add(new OperatorExpression("%", 3, true, BINARY));
        add(new OperatorExpression("+", 4, true, BINARY));
        add(new OperatorExpression("-", 4, true, BINARY));
        add(new OperatorExpression("||", 5, true, BINARY));
        add(new OperatorExpression("<", 6, true, BINARY));
        add(new OperatorExpression("<=", 6, true, BINARY));
        add(new OperatorExpression(">", 6, true, BINARY));
        add(new OperatorExpression(">=", 6, true, BINARY));
        add(new OperatorExpression("=", 7, true, BINARY));
        add(new OperatorExpression("~", 7, true, BINARY));
        add(new OperatorExpression("!=", 7, true, BINARY));
        add(new OperatorExpression("<>", 7, true, BINARY));
        add(new OperatorExpression("!~", 7, true, BINARY));
        add(new OperatorExpression("in", 7, true, SET, false));
        add(new OperatorExpression("between", 7, true, SET, false));
        add(new OperatorExpression("&", 8, true, BINARY));
        add(new OperatorExpression("^", 9, false, BINARY));
        add(new OperatorExpression("|", 10, true, BINARY));
        add(new OperatorExpression("and", 11, true, BINARY, false));
        add(new OperatorExpression("or", 11, true, BINARY, false));
        add(new OperatorExpression("not", 11, true, UNARY, false));
        add(new OperatorExpression("like", 7, true, BINARY,false));
        add(new OperatorExpression("ilike", 7, true, BINARY,false));
        add(new OperatorExpression("within", 7, true, SET, false));
    }};

    static final LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression> opMap = new LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            OperatorExpression op = operators.getQuick(i);
            put(op.token, op);
        }
    }};

    final String token;
    final int precedence;
    final boolean leftAssociative;
    final int type;
    final boolean symbol;

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
