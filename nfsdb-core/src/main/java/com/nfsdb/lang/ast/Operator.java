/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.ast;

import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjObjHashMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class Operator {

    public static final ObjList<Operator> operators = new ObjList<Operator>() {{
        add(new Operator("^", 2, false, OperatorType.BINARY));
        add(new Operator("*", 3, true, OperatorType.BINARY));
        add(new Operator("/", 3, true, OperatorType.BINARY));
        add(new Operator("+", 4, true, OperatorType.BINARY));
        add(new Operator("-", 4, true, OperatorType.BINARY));
        add(new Operator("<", 6, true, OperatorType.BINARY));
        add(new Operator("<=", 6, true, OperatorType.BINARY));
        add(new Operator(">", 6, true, OperatorType.BINARY));
        add(new Operator(">=", 6, true, OperatorType.BINARY));
        add(new Operator("=", 7, true, OperatorType.BINARY));
        add(new Operator("!=", 7, true, OperatorType.BINARY));
        add(new Operator("in", 7, true, OperatorType.BINARY, false));
        add(new Operator("&&", 11, true, OperatorType.BINARY));
        add(new Operator("||", 11, true, OperatorType.BINARY));
    }};

    public final String token;
    public static final ObjObjHashMap<CharSequence, Operator> opMap = new ObjObjHashMap<CharSequence, Operator>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            Operator op = operators.get(i);
            put(op.token, op);
        }
    }};
    public final int precedence;
    public final boolean leftAssociative;
    public final OperatorType type;
    public final boolean symbol;

    public Operator(String token, int precedence, boolean leftAssociative, OperatorType type, boolean symbol) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = symbol;
    }

    public Operator(String token, int precedence, boolean leftAssociative, OperatorType type) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = true;
    }

    public enum OperatorType {
        UNARY, BINARY
    }
}
