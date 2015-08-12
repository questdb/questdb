/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
final class ExprOperator {

    static final ObjList<ExprOperator> operators = new ObjList<ExprOperator>() {{
        add(new ExprOperator("^", 2, false, OperatorType.BINARY));
        add(new ExprOperator("*", 3, true, OperatorType.BINARY));
        add(new ExprOperator("/", 3, true, OperatorType.BINARY));
        add(new ExprOperator("+", 4, true, OperatorType.BINARY));
        add(new ExprOperator("-", 4, true, OperatorType.BINARY));
        add(new ExprOperator("<", 6, true, OperatorType.BINARY));
        add(new ExprOperator("<=", 6, true, OperatorType.BINARY));
        add(new ExprOperator(">", 6, true, OperatorType.BINARY));
        add(new ExprOperator(">=", 6, true, OperatorType.BINARY));
        add(new ExprOperator("=", 7, true, OperatorType.BINARY));
        add(new ExprOperator("~", 7, true, OperatorType.BINARY));
        add(new ExprOperator("!=", 7, true, OperatorType.BINARY));
        add(new ExprOperator("in", 7, true, OperatorType.SET, false));
        add(new ExprOperator("and", 11, true, OperatorType.BINARY, false));
        add(new ExprOperator("or", 11, true, OperatorType.BINARY, false));
    }};

    final String token;
    static final CharSequenceObjHashMap<ExprOperator> opMap = new CharSequenceObjHashMap<ExprOperator>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            ExprOperator op = operators.getQuick(i);
            put(op.token, op);
        }
    }};
    final int precedence;
    final boolean leftAssociative;
    final OperatorType type;
    final boolean symbol;

    private ExprOperator(String token, int precedence, boolean leftAssociative, OperatorType type, boolean symbol) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = symbol;
    }

    private ExprOperator(String token, int precedence, boolean leftAssociative, OperatorType type) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = true;
    }

    enum OperatorType {
        UNARY, BINARY, SET
    }
}
