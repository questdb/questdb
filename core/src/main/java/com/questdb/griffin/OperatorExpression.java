/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import com.questdb.std.ObjList;

public final class OperatorExpression {

    public static final int UNARY = 1;
    public static final int BINARY = 2;
    public static final int SET = 3;
    static final ObjList<OperatorExpression> operators = new ObjList<OperatorExpression>() {{
        add(new OperatorExpression("^", 2, false, BINARY));
        add(new OperatorExpression("*", 3, true, BINARY));
        add(new OperatorExpression("/", 3, true, BINARY));
        add(new OperatorExpression("%", 3, true, BINARY));
        add(new OperatorExpression("+", 4, true, BINARY));
        add(new OperatorExpression("-", 4, true, BINARY));
        add(new OperatorExpression("<", 6, true, BINARY));
        add(new OperatorExpression("<=", 6, true, BINARY));
        add(new OperatorExpression(">", 6, true, BINARY));
        add(new OperatorExpression(">=", 6, true, BINARY));
        add(new OperatorExpression("=", 7, true, BINARY));
        add(new OperatorExpression("~=", 7, true, BINARY));
        add(new OperatorExpression("!=", 7, true, BINARY));
        add(new OperatorExpression("<>", 7, true, BINARY));
        add(new OperatorExpression("!~", 7, true, BINARY));
        add(new OperatorExpression("~=", 7, true, BINARY));
        add(new OperatorExpression("in", 7, true, SET, false));
        add(new OperatorExpression("and", 11, true, BINARY, false));
        add(new OperatorExpression("or", 11, true, BINARY, false));
        add(new OperatorExpression("not", 11, true, UNARY, false));
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
