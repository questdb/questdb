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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ExpressionParserFuzzTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(ExpressionParserFuzzTest.class);
    // fixed operators with precedence (we better to fix this independently of precedence in OperatorExpression in order to test fresh implementation against fixed ordering)
    // syntax like ${operator}'${arity} used for disambiguation (e.g. unary/binary minus)
    public static String[][] operators = {
            {"."},
            {"::"},
            {"-'1", "~'1"},
            {"*", "/", "%"},
            {"+", "-'2"},
            {"<<", ">>", "<<=", ">>="},
            {"||"},
            {"&"},
            {"^"},
            {"|"},
            {"in", "not in", "within", "not within", "between", "not between"},
            {"<", "<=", ">", ">="},
            {"=", "~'2", "!=", "<>", "!~", "like", "ilike", "is null", "is not null"},
            {"not"},
            {"and"},
            {"or"}
    };

    @Test
    public void fuzzTestValidExpressions() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.addAll(numberOperators());
        operators.addAll(logicalOperators());
        operators.addAll(ipOperators());
        operators.addAll(stringOperators());
        operators.addAll(simpleComparisonOperators());
        operators.addAll(complexComparisonOperators());
        final int length = rnd.nextInt(20) + 2;
        fuzzTestValidExpressionAgainstOperators(rnd, engine, operators, length);
    }

    private ArrayList<ExpressionElement> complexComparisonOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("in", 2, false, true));
        operators.add(ExpressionElement.operator("not in", 2, false, true).withWrap(
                (c, x) -> c == ExpressionContext.Query ?
                        String.format("%s not in %s", x.get(0), x.get(1)) :
                        String.format("not (%s in %s)", x.get(0), x.get(1))
        ));
        operators.add(ExpressionElement.operator("within", 2, false, true));
        operators.add(ExpressionElement.operator("not within", 2, false, true).withWrap(
                (c, x) -> c == ExpressionContext.Query ?
                        String.format("%s not within %s", x.get(0), x.get(1)) :
                        String.format("not (%s within %s)", x.get(0), x.get(1))
        ));
        operators.add(ExpressionElement.operator("between", 3, false, true).withWrap(
                (c, x) -> String.format("%s between %s and %s", x.get(0), x.get(1), x.get(2))
        ));
        operators.add(ExpressionElement.operator("not between", 3, false, true).withWrap(
                (c, x) -> c == ExpressionContext.Query ?
                        String.format("%s not between %s and %s", x.get(0), x.get(1), x.get(2)) :
                        String.format("not (%s between %s and %s)", x.get(0), x.get(1), x.get(2))
        ));
        return operators;
    }

    /**
     * The idea of the fuzz test is the following:
     * 1. Generate random expression in postfix notation
     * 2. Convert this expression to infix notation omitting braces in places where precedence rules should disambiguate evaluation (infix)
     * 3. Convert this expression to infix notation with all braces in place (infixNoAmbiguities)
     * 4. Run SqlParser over infix representation with custom ExpressionParserListener which just creates infix notation with all braces in place (similar to infixNoAmbiguities)
     * 5. Compare infixNoAmbiguities with final result from SqlParser
     */
    private void fuzzTestValidExpressionAgainstOperators(Rnd rnd, CairoEngine engine, List<ExpressionElement> operators, int literals) {
        for (int attempt = 0; attempt < 1024; attempt++) {
            List<ExpressionElement> expressions = randomExpression(rnd, operators, literals);
            String infix = "";
            try {
                infix = tryPostfixToInfix(ExpressionContext.Query, expressions, false);
                if (infix == null) {
                    continue;
                }
                String infixNoAmbiguities = tryPostfixToInfix(ExpressionContext.Internal, expressions, true);
                Stack<String> representations = new Stack<>();
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    compiler.testParseExpression(infix, node -> {
                        if (node.paramCount == 0) {
                            representations.push(node.token.toString());
                            return;
                        }
                        ArrayList<String> arguments = new ArrayList<>();
                        for (int argIndex = node.paramCount; argIndex >= 1; argIndex--) {
                            arguments.add("(" + representations.get(representations.size() - argIndex) + ")");
                        }
                        for (int argIndex = 1; argIndex <= node.paramCount; argIndex++) {
                            representations.pop();
                        }
                        representations.push(operators.stream()
                                .filter(x -> x.token.contentEquals(node.token.toString()) && x.arity == node.paramCount)
                                .findFirst()
                                .orElseThrow(() -> new RuntimeException(String.format("unexpected node: token=%s, params=%d", node.token, node.paramCount)))
                                .wrap(ExpressionContext.Internal, arguments)
                        );
                    });
                }
                TestUtils.assertEquals(
                        String.format("%d: unexpected parsing for expression `%s`", attempt, infix),
                        infixNoAmbiguities,
                        representations.peek()
                );
            } catch (Exception e) {
                throw new RuntimeException(String.format("failed for expression `%s` (postfix `%s`)", infix, expressions.stream().map(x -> x.token).collect(Collectors.toList())), e);
            }
        }
    }

    private ArrayList<ExpressionElement> ipOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("<<", 2, true, false));
        operators.add(ExpressionElement.operator(">>", 2, true, false));
        operators.add(ExpressionElement.operator("<<=", 2, true, false));
        operators.add(ExpressionElement.operator(">>=", 2, true, false));
        return operators;
    }

    private ArrayList<ExpressionElement> logicalOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("not", 1, true, false));
        operators.add(ExpressionElement.operator("and", 2, true, false));
        operators.add(ExpressionElement.operator("or", 2, true, false));
        return operators;
    }

    private ArrayList<ExpressionElement> numberOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("-", 1, false, false));
        operators.add(ExpressionElement.operator("~", 1, false, false));

        operators.add(ExpressionElement.operator("*", 2, true, false));
        operators.add(ExpressionElement.operator("/", 2, true, false));
        operators.add(ExpressionElement.operator("%", 2, true, false));

        operators.add(ExpressionElement.operator("+", 2, true, false));
        operators.add(ExpressionElement.operator("-", 2, true, false));

        operators.add(ExpressionElement.operator("&", 2, true, false));
        operators.add(ExpressionElement.operator("^", 2, true, false));
        operators.add(ExpressionElement.operator("|", 2, true, false));
        return operators;
    }

    private ExpressionElement randomBoolLiteral(Rnd rnd) {
        return ExpressionElement.literal(rnd.nextBoolean() ? "true" : "false");
    }

    /**
     * Algorithm, which generates random expression in postfix notation using following approach:
     * First, it generates N random literals;
     * Then, it generates random operators such that sum(arity_i - 1) == N (so we can arrange these operators in single expression tree over N literals);
     * After that, we shuffle all literals and operators in random order;
     * Random order may not be correct postfix order, but it can be proven that exactly one cyclic shift will form valid postfix notation. So, after shuffle we just need to find this position and rotate expression array.
     * (this fact is an easy extension of similar statement about bracket (+1/-1) sequences - the only difference here is that we can have arbitrary negative numbers - not only -1)
     */
    private ArrayList<ExpressionElement> randomExpression(
            Rnd rnd,
            List<ExpressionElement> allowedOperators,
            int literalsCount
    ) {
        ArrayList<ExpressionElement> elements = new ArrayList<>();
        for (int i = 0; i < literalsCount; i++) {
            elements.add(randomLiteral(rnd));
        }
        int roots = literalsCount;

        int applicableOperators = allowedOperators.size();
        allowedOperators.sort(Comparator.comparingInt(a -> a.arity));
        while (roots > 1) {
            while (allowedOperators.get(applicableOperators - 1).arity > roots) {
                applicableOperators--;
            }
            int operatorIndex = rnd.nextInt(applicableOperators);
            elements.add(allowedOperators.get(operatorIndex));
            roots -= allowedOperators.get(operatorIndex).arity - 1;
        }
        rnd.shuffle(elements);
        int balance = 0, minBalance = 0, minBalanceIndex = 0;
        for (int i = 0, n = elements.size(); i < n; i++) {
            balance += 1 - elements.get(i).arity;
            if (balance <= minBalance) {
                minBalance = balance;
                minBalanceIndex = i + 1;
            }
        }
        Collections.rotate(elements, elements.size() - minBalanceIndex);
        return elements;
    }

    private ExpressionElement randomFloatLiteral(Rnd rnd) {
        return ExpressionElement.literal(String.valueOf(rnd.nextFloat() * Math.exp(rnd.nextInt(100))));
    }

    private ExpressionElement randomLiteral(Rnd rnd) {
        int choice = rnd.nextInt(4);
        return switch (choice) {
            case 0 -> randomNumberLiteral(rnd);
            case 1 -> randomStringLiteral(rnd);
            case 2 -> randomFloatLiteral(rnd);
            default -> randomBoolLiteral(rnd);
        };
    }

    private ExpressionElement randomNumberLiteral(Rnd rnd) {
        return ExpressionElement.literal(String.valueOf(rnd.nextPositiveInt()));
    }

    private ExpressionElement randomStringLiteral(Rnd rnd) {
        return ExpressionElement.literal('\'' + rnd.nextString(rnd.nextInt(32) + 1) + '\'');
    }

    private ArrayList<ExpressionElement> simpleComparisonOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("||", 2, true, false));
        operators.add(ExpressionElement.operator("<", 2, true, false));
        operators.add(ExpressionElement.operator("<=", 2, true, false));
        operators.add(ExpressionElement.operator(">", 2, true, false));
        operators.add(ExpressionElement.operator(">=", 2, true, false));

        operators.add(ExpressionElement.operator("=", 2, true, false));
        operators.add(ExpressionElement.operator("!=", 2, true, false));
        operators.add(ExpressionElement.operator("<>", 2, true, false));

        operators.add(ExpressionElement.operator("~", 2, true, false));
        operators.add(ExpressionElement.operator("!~", 2, true, false));
        operators.add(ExpressionElement.operator("like", 2, true, false));
        operators.add(ExpressionElement.operator("ilike", 2, true, false));

        operators.add(ExpressionElement.operator("is null", 1, false, false).withWrap(
                (c, x) -> c == ExpressionContext.Query ? String.format("%s is null", x.get(0)) : String.format("%s = (null)", x.get(0))
        ));
        operators.add(ExpressionElement.operator("is not null", 1, false, false).withWrap(
                (c, x) -> c == ExpressionContext.Query ? String.format("%s is not null", x.get(0)) : String.format("%s != (null)", x.get(0))
        ));
        return operators;
    }

    private ArrayList<ExpressionElement> stringOperators() {
        ArrayList<ExpressionElement> operators = new ArrayList<>();
        operators.add(ExpressionElement.operator("||", 2, true, false));
        return operators;
    }

    private String tryPostfixToInfix(ExpressionContext context, List<ExpressionElement> expression, boolean noAmbiguities) {
        Stack<ExpressionElement> elements = new Stack<>();
        Stack<String> representations = new Stack<>();
        // we want to forbid usage of nested BETWEEN operators and usage of AND operator inside BETWEEN operands
        Stack<Boolean> hasBetweenAndInTree = new Stack<>();
        for (ExpressionElement root : expression) {
            if (root.arity == 0) {
                elements.add(root);
                representations.add(root.token);
                hasBetweenAndInTree.add(false);
                continue;
            }
            ArrayList<String> arguments = new ArrayList<>();
            boolean hasBetweenAnd = false;
            for (int argIndex = 1; argIndex <= root.arity; argIndex++) {
                ExpressionElement arg = elements.get(elements.size() - (root.arity - argIndex + 1));
                String representation = representations.get(elements.size() - (root.arity - argIndex + 1));
                hasBetweenAnd |= hasBetweenAndInTree.get(elements.size() - (root.arity - argIndex + 1));
                boolean wrapInParenthesis = noAmbiguities;
                // note, that set operators can consume single value (without parenthesis, e.g. x in '2022-02-02') - due to this they have "different" precedence for left & right arguments
                // this change done in fuzzy-tests in order to simulate more expected behaviour of parser (because x in '2022-02' || '-02' should be equivalent to x in ('2022-02') || '-02'
                if (root.precedence < arg.precedence) {
                    wrapInParenthesis = true;
                } else if (root.arity == 2 && argIndex == 2 && root.setOperator) { // set operations weird behaviour is here
                    wrapInParenthesis = true;
                } else if (root.arity == 2 && root.leftAssociative && argIndex == 2 && root.precedence == arg.precedence) {
                    wrapInParenthesis = true;
                } else if (root.arity == 2 && !root.leftAssociative && argIndex == 1 && root.precedence == arg.precedence) {
                    wrapInParenthesis = true;
                } else if (root.arity != 2 && root.precedence == arg.precedence) {
                    wrapInParenthesis = true;
                }
                if (wrapInParenthesis) {
                    arguments.add("(" + representation + ")");
                } else {
                    arguments.add(representation);
                }
            }
            for (int argIndex = 1; argIndex <= root.arity; argIndex++) {
                elements.pop();
                representations.pop();
                hasBetweenAndInTree.pop();
            }
            if (root.token.contains("between") && hasBetweenAnd) {
                return null;
            }
            elements.add(root);
            representations.add(root.wrap(context, arguments));
            hasBetweenAndInTree.add(hasBetweenAnd || root.token.contains("between") || root.token.equals("and"));
        }
        return representations.peek();
    }

    enum ExpressionContext {
        Query,
        Internal
    }

    static class ExpressionElement {
        int arity;
        boolean leftAssociative;
        int precedence;
        boolean setOperator;
        String token;
        private BiFunction<ExpressionContext, List<String>, String> wrapFunc;

        public static ExpressionElement literal(String literal) {
            ExpressionElement element = new ExpressionElement();
            element.token = literal;
            return element;
        }

        public static ExpressionElement operator(
                String token,
                int arity,
                boolean leftAssociative,
                boolean setOperator
        ) {
            ExpressionElement element = new ExpressionElement();
            element.token = token;
            element.arity = arity;
            element.leftAssociative = leftAssociative;
            element.setOperator = setOperator;
            int matching = 0;
            for (int precedence = 0; precedence < operators.length; precedence++) {
                for (String operator : operators[precedence]) {
                    String[] tokens = operator.split("'");
                    if (tokens[0].equals(token) && (tokens.length == 1 || tokens[1].equals(String.valueOf(arity)))) {
                        element.precedence = precedence;
                        matching++;
                    }
                }
            }
            if (matching < 1) {
                throw new RuntimeException(String.format("operator not found: %s", token));
            }
            if (matching > 1) {
                throw new RuntimeException(String.format("found ambiguous definition of operator: %s", token));
            }
            return element;
        }

        public ExpressionElement withWrap(BiFunction<ExpressionContext, List<String>, String> wrap) {
            this.wrapFunc = wrap;
            return this;
        }

        public String wrap(ExpressionContext context, List<String> arguments) {
            if (this.wrapFunc == null) {
                return switch (this.arity) {
                    case 1 -> token.length() == 1 ? token + arguments.get(0) : token + " " + arguments.get(0);
                    case 2 -> arguments.get(0) + " " + token + " " + arguments.get(1);
                    default -> throw new RuntimeException("no default wrap method for expression element " + token);
                };
            }
            return this.wrapFunc.apply(context, arguments);
        }
    }
}
