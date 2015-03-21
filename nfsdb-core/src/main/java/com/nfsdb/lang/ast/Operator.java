/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Operator {

    public static final List<Operator> operators = new ArrayList<Operator>() {{
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
        add(new Operator("&&", 11, true, OperatorType.BINARY));
        add(new Operator("||", 11, true, OperatorType.BINARY));
    }};
    public String token;
    public static final HashMap<CharSequence, Operator> opMap = new HashMap<CharSequence, Operator>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            Operator op = operators.get(i);
            put(op.token, op);
        }
    }};
    public int precedence;
    public boolean leftAssociative;
    public OperatorType type;

    public Operator(String token, int precedence, boolean leftAssociative, OperatorType type) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
    }

    public static enum OperatorType {
        UNARY, BINARY
    }
}
