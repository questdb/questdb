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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.std.*;

import java.util.ArrayDeque;
import java.util.Deque;

public class ExprParser {

    private static final IntHashSet nonLiteralBranches = new IntHashSet();
    private static final int BRANCH_NONE = 0;
    private static final int BRANCH_COMMA = 1;
    private static final int BRANCH_LEFT_BRACE = 2;
    private static final int BRANCH_RIGHT_BRACE = 3;
    private static final int BRANCH_CONSTANT = 4;
    private static final int BRANCH_OPERATOR = 5;
    private static final int BRANCH_LITERAL = 6;
    private static final int BRANCH_LAMBDA = 7;
    private final Deque<ExprNode> opStack = new ArrayDeque<>();
    private final IntStack paramCountStack = new IntStack();
    private final ObjectPool<ExprNode> exprNodePool;

    public ExprParser(ObjectPool<ExprNode> exprNodePool) {
        this.exprNodePool = exprNodePool;
    }

    public static void configureLexer(Lexer lexer) {
        lexer.defineSymbol("(");
        lexer.defineSymbol(")");
        lexer.defineSymbol(",");
        lexer.defineSymbol("/*");
        lexer.defineSymbol("*/");
        lexer.defineSymbol("--");
        for (int i = 0, k = ExprOperator.operators.size(); i < k; i++) {
            ExprOperator op = ExprOperator.operators.getQuick(i);
            if (op.symbol) {
                lexer.defineSymbol(op.token);
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void parseExpr(Lexer lexer, ExprListener listener) throws ParserException {

        opStack.clear();
        paramCountStack.clear();

        int paramCount = 0;
        int braceCount = 0;

        ExprNode node;
        CharSequence tok;
        char thisChar = 0, prevChar;
        int prevBranch;
        int thisBranch = BRANCH_NONE;

        OUT:
        while ((tok = lexer.optionTok()) != null) {
            prevChar = thisChar;
            thisChar = tok.charAt(0);
            prevBranch = thisBranch;

            switch (thisChar) {
                case ',':
                    thisBranch = BRANCH_COMMA;
                    if (prevChar == ',') {
                        throw QueryError.$(lexer.position(), "Missing argument");
                    }

                    if (braceCount == 0) {
                        // comma outside of braces
                        lexer.unparse();
                        break OUT;
                    }

                    // If the token is a function argument separator (e.g., a comma):
                    // Until the token at the top of the stack is a left parenthesis,
                    // pop operators off the stack onto the output queue. If no left
                    // parentheses are encountered, either the separator was misplaced or
                    // parentheses were mismatched.
                    while ((node = opStack.poll()) != null && node.token.charAt(0) != '(') {
                        listener.onNode(node);
                    }

                    if (node != null) {
                        opStack.push(node);
                    }

                    paramCount++;
                    break;

                case '(':
                    thisBranch = BRANCH_LEFT_BRACE;
                    braceCount++;
                    // If the token is a left parenthesis, then push it onto the stack.
                    paramCountStack.push(paramCount);
                    paramCount = 0;
                    opStack.push(exprNodePool.next().of(ExprNode.CONTROL, "(", Integer.MAX_VALUE, lexer.position()));
                    break;

                case ')':
                    if (prevChar == ',') {
                        throw QueryError.$(lexer.position(), "Missing argument");
                    }
                    if (braceCount == 0) {
                        lexer.unparse();
                        break OUT;
                    }

                    thisBranch = BRANCH_RIGHT_BRACE;
                    braceCount--;
                    // If the token is a right parenthesis:
                    // Until the token at the top of the stack is a left parenthesis, pop operators off the stack onto the output queue.
                    // Pop the left parenthesis from the stack, but not onto the output queue.
                    //        If the token at the top of the stack is a function token, pop it onto the output queue.
                    //        If the stack runs out without finding a left parenthesis, then there are mismatched parentheses.
                    while ((node = opStack.poll()) != null && node.token.charAt(0) != '(') {
                        listener.onNode(node);
                    }

                    // enable operation or literal absorb parameters
                    if ((node = opStack.peek()) != null && (node.type == ExprNode.LITERAL || (node.type == ExprNode.SET_OPERATION))) {
                        node.paramCount = (prevChar == '(' ? 0 : paramCount + 1) + (node.paramCount == 2 ? 1 : 0);
                        node.type = ExprNode.FUNCTION;
                        listener.onNode(node);
                        opStack.poll();
                        if (paramCountStack.notEmpty()) {
                            paramCount = paramCountStack.pop();
                        }
                    }
                    break;
                case '`':
                    thisBranch = BRANCH_LAMBDA;
                    // If the token is a number, then add it to the output queue.
                    listener.onNode(exprNodePool.next().of(ExprNode.LAMBDA, tok.toString(), 0, lexer.position()));
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case '"':
                case '\'':
                case 'N':
                case 'n':
                    if ((thisChar != 'N' && thisChar != 'n') || Chars.equals("NaN", tok) || Chars.equals("null", tok)) {
                        thisBranch = BRANCH_CONSTANT;
                        // If the token is a number, then add it to the output queue.
                        listener.onNode(exprNodePool.next().of(ExprNode.CONSTANT, tok.toString(), 0, lexer.position()));
                        break;
                    }
                default:
                    ExprOperator op;
                    if ((op = ExprOperator.opMap.get(tok)) != null) {

                        thisBranch = BRANCH_OPERATOR;

                        // If the token is an operator, o1, then:
                        // while there is an operator token, o2, at the top of the operator stack, and either
                        // o1 is left-associative and its precedence is less than or equal to that of o2, or
                        // o1 is right associative, and has precedence less than that of o2,
                        //        then pop o2 off the operator stack, onto the output queue;
                        // push o1 onto the operator stack.

                        int operatorType = op.type;


                        if (thisChar == '-') {
                            switch (prevBranch) {
                                case BRANCH_OPERATOR:
                                case BRANCH_LEFT_BRACE:
                                case BRANCH_COMMA:
                                case BRANCH_NONE:
                                    // we have unary minus
                                    operatorType = ExprOperator.UNARY;
                                    break;
                                default:
                                    break;
                            }
                        }

                        ExprNode other;
                        // UNARY operators must never pop BINARY ones regardless of precedence
                        // this is to maintain correctness of -a^b
                        while ((other = opStack.peek()) != null) {
                            boolean greaterPrecedence = (op.leftAssociative && op.precedence >= other.precedence) || (!op.leftAssociative && op.precedence > other.precedence);
                            if (greaterPrecedence &&
                                    (operatorType != ExprOperator.UNARY || (operatorType == ExprOperator.UNARY && other.paramCount == 1))) {
                                listener.onNode(other);
                                opStack.poll();
                            } else {
                                break;
                            }
                        }
                        node = exprNodePool.next().of(
                                op.type == ExprOperator.SET ? ExprNode.SET_OPERATION : ExprNode.OPERATION,
                                op.token,
                                op.precedence,
                                lexer.position()
                        );
                        if (operatorType == ExprOperator.UNARY) {
                            node.paramCount = 1;
                        } else {
                            node.paramCount = 2;
                        }
                        opStack.push(node);
                    } else if (!nonLiteralBranches.contains(thisBranch)) {
                        thisBranch = BRANCH_LITERAL;
                        // If the token is a function token, then push it onto the stack.
                        opStack.push(exprNodePool.next().of(ExprNode.LITERAL, Chars.toString(tok), Integer.MIN_VALUE, lexer.position()));
                    } else {
                        // literal can be at start of input, after a bracket or part of an operator
                        // all other cases are illegal and will be considered end-of-input
                        lexer.unparse();
                        break OUT;
                    }
            }
        }

        while ((node = opStack.poll()) != null) {
            if (node.token.charAt(0) == '(') {
                throw QueryError.$(node.position, "Unbalanced (");
            }
            listener.onNode(node);
        }
    }

    static {
        nonLiteralBranches.add(BRANCH_RIGHT_BRACE);
        nonLiteralBranches.add(BRANCH_CONSTANT);
        nonLiteralBranches.add(BRANCH_LITERAL);
        nonLiteralBranches.add(BRANCH_LAMBDA);
    }
}
