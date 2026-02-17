/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.ObjList;

public final class OperatorExpression {
    public static final int BINARY = 2;
    public static final int SET = 3;
    public static final int UNARY = 1;
    private static final OperatorRegistry legacyRegistry = new OperatorRegistry(
            new ObjList<>() {{
                add(new OperatorExpression(Operator.UnaryMinus, 3, false, UNARY));
                add(new OperatorExpression(Operator.UnaryComplement, 3, false, UNARY));
                add(new OperatorExpression(Operator.UnarySetNegation, 11, false, UNARY));
                add(new OperatorExpression(Operator.Dot, 1, false, BINARY));
                add(new OperatorExpression(Operator.DoubleColon, 2, true, BINARY));
                // arithmetic operators, UnaryMinus and UnaryComplement defined above are strongest from this block
                add(new OperatorExpression(Operator.Multiplication, 3, true, BINARY));
                add(new OperatorExpression(Operator.Division, 3, true, BINARY));
                add(new OperatorExpression(Operator.Modulo, 3, true, BINARY));

                add(new OperatorExpression(Operator.Plus, 4, true, BINARY));
                add(new OperatorExpression(Operator.Minus, 4, true, BINARY));
                // IP operators
                add(new OperatorExpression(Operator.IpContainsStrictLeft, 4, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsStrictRight, 4, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsLeft, 4, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsRight, 4, true, BINARY));
                // concatenation
                add(new OperatorExpression(Operator.Concatenation, 5, true, BINARY));
                // bitwise operators
                add(new OperatorExpression(Operator.BitAnd, 8, true, BINARY));
                add(new OperatorExpression(Operator.BitXor, 9, true, BINARY));
                add(new OperatorExpression(Operator.BitOr, 10, true, BINARY));
                // set operators
                add(new OperatorExpression(Operator.In, 7, true, SET, false));
                add(new OperatorExpression(Operator.Between, 7, true, SET, false)); // ternary operator
                add(new OperatorExpression(Operator.Within, 7, true, SET, false));
                // boolean operators
                add(new OperatorExpression(Operator.Less, 6, true, BINARY));
                add(new OperatorExpression(Operator.LessOrEqual, 6, true, BINARY));
                add(new OperatorExpression(Operator.Greater, 6, true, BINARY));
                add(new OperatorExpression(Operator.GreaterOrEqual, 6, true, BINARY));
                add(new OperatorExpression(Operator.Equal, 7, true, BINARY));
                add(new OperatorExpression(Operator.NotEqual, 7, true, BINARY));
                add(new OperatorExpression(Operator.NotEqualSqlStyle, 7, true, BINARY));
                add(new OperatorExpression(Operator.LikeRegex, 7, true, BINARY));
                add(new OperatorExpression(Operator.NotLikeRegex, 7, true, BINARY));
                add(new OperatorExpression(Operator.LikeSqlStyle, 7, true, BINARY, false));
                add(new OperatorExpression(Operator.ILikeSqlStyle, 7, true, BINARY, false));
                // logical operators
                add(new OperatorExpression(Operator.BinaryNot, 11, true, UNARY, false));
                add(new OperatorExpression(Operator.BinaryAnd, 11, true, BINARY, false));
                add(new OperatorExpression(Operator.BinaryOr, 11, true, BINARY, false));
            }});
    private static final OperatorRegistry registry = new OperatorRegistry(
            new ObjList<>() {{
                add(new OperatorExpression(Operator.DeclareVariableAssignment, 100, false, BINARY));
                add(new OperatorExpression(Operator.UnaryMinus, 3, false, UNARY));
                add(new OperatorExpression(Operator.UnaryComplement, 3, false, UNARY));
                add(new OperatorExpression(Operator.UnarySetNegation, 11, false, UNARY));
                add(new OperatorExpression(Operator.Dot, 1, false, BINARY));
                add(new OperatorExpression(Operator.DoubleColon, 2, true, BINARY));
                // arithmetic operators, UnaryMinus and UnaryComplement defined above are strongest from this block
                add(new OperatorExpression(Operator.Multiplication, 4, true, BINARY));
                add(new OperatorExpression(Operator.Division, 4, true, BINARY));
                add(new OperatorExpression(Operator.Modulo, 4, true, BINARY));
                add(new OperatorExpression(Operator.Plus, 5, true, BINARY));
                add(new OperatorExpression(Operator.Minus, 5, true, BINARY));
                // IP operators
                add(new OperatorExpression(Operator.IpContainsStrictLeft, 6, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsStrictRight, 6, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsLeft, 6, true, BINARY));
                add(new OperatorExpression(Operator.IpContainsRight, 6, true, BINARY));
                // concatenation
                add(new OperatorExpression(Operator.Concatenation, 7, true, BINARY));
                // bitwise operators
                add(new OperatorExpression(Operator.BitAnd, 8, true, BINARY));
                add(new OperatorExpression(Operator.BitXor, 9, true, BINARY));
                add(new OperatorExpression(Operator.BitOr, 10, true, BINARY));
                // set operators
                add(new OperatorExpression(Operator.In, 11, false, SET, false));
                add(new OperatorExpression(Operator.Between, 11, false, SET, false)); // ternary operator
                add(new OperatorExpression(Operator.Within, 11, false, SET, false));
                // boolean operators
                add(new OperatorExpression(Operator.Less, 12, true, BINARY));
                add(new OperatorExpression(Operator.LessOrEqual, 12, true, BINARY));
                add(new OperatorExpression(Operator.Greater, 12, true, BINARY));
                add(new OperatorExpression(Operator.GreaterOrEqual, 12, true, BINARY));
                add(new OperatorExpression(Operator.Equal, 13, true, BINARY));
                add(new OperatorExpression(Operator.NotEqual, 13, true, BINARY));
                add(new OperatorExpression(Operator.NotEqualSqlStyle, 13, true, BINARY));
                add(new OperatorExpression(Operator.LikeRegex, 13, true, BINARY));
                add(new OperatorExpression(Operator.NotLikeRegex, 13, true, BINARY));
                add(new OperatorExpression(Operator.LikeSqlStyle, 13, true, BINARY, false));
                add(new OperatorExpression(Operator.ILikeSqlStyle, 13, true, BINARY, false));
                // logical operators
                add(new OperatorExpression(Operator.BinaryNot, 14, false, UNARY, false));
                add(new OperatorExpression(Operator.BinaryAnd, 15, true, BINARY, false));
                add(new OperatorExpression(Operator.BinaryOr, 16, true, BINARY, false));
                add(new OperatorExpression(Operator.Colon, 17, false, BINARY));
            }});
    final boolean leftAssociative;
    final OperatorExpression.Operator operator;
    final int precedence;
    final boolean symbol;
    final int type;

    private OperatorExpression(OperatorExpression.Operator operator, int precedence, boolean leftAssociative, int type, boolean symbol) {
        this.operator = operator;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = symbol;
    }

    private OperatorExpression(OperatorExpression.Operator operator, int precedence, boolean leftAssociative, int type) {
        this.operator = operator;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = true;
    }

    public static OperatorRegistry chooseRegistry(boolean cairoSqlLegacyOperatorPrecedence) {
        return cairoSqlLegacyOperatorPrecedence ? legacyRegistry : registry;
    }

    public static OperatorRegistry getLegacyRegistry() {
        return legacyRegistry;
    }

    public static OperatorRegistry getRegistry() {
        return registry;
    }

    public int getType() {
        return type;
    }

    public String getToken() {
        return operator.token;
    }

    public boolean greaterPrecedence(int otherPrecedence) {
        return (leftAssociative && precedence >= otherPrecedence) || (!leftAssociative && precedence > otherPrecedence);
    }

    public enum Operator {
        DeclareVariableAssignment(":="),
        UnaryMinus("-"),
        UnaryComplement("~"),
        UnarySetNegation("not"),
        Dot("."),
        Colon(":"),
        DoubleColon("::"),
        Multiplication("*"),
        Division("/"),
        Modulo("%"),
        Plus("+"),
        Minus("-"),
        IpContainsStrictLeft("<<"),
        IpContainsStrictRight(">>"),
        IpContainsLeft("<<="),
        IpContainsRight(">>="),
        Concatenation("||"),
        BitAnd("&"),
        BitXor("^"),
        BitOr("|"),
        In("in"),
        Between("between"),
        Within("within"),
        Less("<"),
        LessOrEqual("<="),
        Greater(">"),
        GreaterOrEqual(">="),
        Equal("="),
        NotEqual("!="),
        NotEqualSqlStyle("<>"),
        LikeRegex("~"),
        NotLikeRegex("!~"),
        LikeSqlStyle("like"),
        ILikeSqlStyle("ilike"),
        BinaryNot("not"),
        BinaryAnd("and"),
        BinaryOr("or");

        public final String token;

        Operator(String token) {
            this.token = token;
        }
    }
}
