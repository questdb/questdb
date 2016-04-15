/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.ex.ParserException;
import com.nfsdb.misc.Chars;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.IntStack;
import com.nfsdb.std.ObjectPool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;
import java.util.Deque;

public class ExprParser {

    private static final IntHashSet nonLiteralBranches = new IntHashSet();
    private final Lexer lexer;
    private final Deque<ExprNode> opStack = new ArrayDeque<>();
    private final IntStack paramCountStack = new IntStack();
    private final ObjectPool<ExprNode> exprNodePool;

    public ExprParser(Lexer lexer, ObjectPool<ExprNode> exprNodePool) {
        this.lexer = lexer;
        this.exprNodePool = exprNodePool;
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

    public ExprParser(ObjectPool<ExprNode> exprNodePool) {
        this(new Lexer(), exprNodePool);
    }

    public void parseExpr(CharSequence in, ExprListener listener) throws ParserException {
        lexer.setContent(in);
        parseExpr(listener);
    }

    @SuppressWarnings("ConstantConditions")
    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY"})
    public void parseExpr(ExprListener listener) throws ParserException {

        opStack.clear();
        paramCountStack.clear();

        int paramCount = 0;
        int braceCount = 0;

        ExprNode node;
        CharSequence tok;
        char thisChar = 0, prevChar;
        Branch prevBranch;
        Branch thisBranch = Branch.NONE;

        OUT:
        while ((tok = lexer.optionTok()) != null) {
            prevChar = thisChar;
            thisChar = tok.charAt(0);
            prevBranch = thisBranch;

            switch (thisChar) {
                case ',':
                    thisBranch = Branch.COMMA;
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
                    thisBranch = Branch.LEFT_BRACE;
                    braceCount++;
                    // If the token is a left parenthesis, then push it onto the stack.
                    paramCountStack.push(paramCount);
                    paramCount = 0;
                    opStack.push(exprNodePool.next().of(ExprNode.NodeType.CONTROL, "(", Integer.MAX_VALUE, lexer.position()));
                    break;

                case ')':
                    if (prevChar == ',') {
                        throw QueryError.$(lexer.position(), "Missing argument");
                    }
                    if (braceCount == 0) {
                        lexer.unparse();
                        break OUT;
                    }

                    thisBranch = Branch.RIGHT_BRACE;
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
                    if ((node = opStack.peek()) != null && (node.type == ExprNode.NodeType.LITERAL || (node.type == ExprNode.NodeType.SET_OPERATION))) {
                        node.paramCount = (prevChar == '(' ? 0 : paramCount + 1) + (node.paramCount == 2 ? 1 : 0);
                        node.type = ExprNode.NodeType.FUNCTION;
                        listener.onNode(node);
                        opStack.poll();
                        if (paramCountStack.notEmpty()) {
                            paramCount = paramCountStack.pop();
                        }
                    }
                    break;
                case '`':
                    thisBranch = Branch.LAMBDA;
                    // If the token is a number, then add it to the output queue.
                    listener.onNode(exprNodePool.next().of(ExprNode.NodeType.LAMBDA, tok.toString(), 0, lexer.position()));
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
                        thisBranch = Branch.CONSTANT;
                        // If the token is a number, then add it to the output queue.
                        listener.onNode(exprNodePool.next().of(ExprNode.NodeType.CONSTANT, tok.toString(), 0, lexer.position()));
                        break;
                    }
                default:
                    ExprOperator op;
                    if ((op = ExprOperator.opMap.get(tok)) != null) {

                        thisBranch = Branch.OPERATOR;

                        // If the token is an operator, o1, then:
                        // while there is an operator token, o2, at the top of the operator stack, and either
                        // o1 is left-associative and its precedence is less than or equal to that of o2, or
                        // o1 is right associative, and has precedence less than that of o2,
                        //        then pop o2 off the operator stack, onto the output queue;
                        // push o1 onto the operator stack.

                        ExprOperator.OperatorType type = op.type;


                        switch (thisChar) {
                            case '-':
                                switch (prevBranch) {
                                    case OPERATOR:
                                    case COMMA:
                                    case NONE:
                                        // we have unary minus
                                        type = ExprOperator.OperatorType.UNARY;
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            default:
                                break;
                        }

                        ExprNode other;
                        // UNARY operators must never pop BINARY ones regardless of precedence
                        // this is to maintain correctness of -a^b
                        while ((other = opStack.peek()) != null) {
                            boolean greaterPrecedence = (op.leftAssociative && op.precedence >= other.precedence) || (!op.leftAssociative && op.precedence > other.precedence);
                            if (greaterPrecedence &&
                                    (type != ExprOperator.OperatorType.UNARY || (type == ExprOperator.OperatorType.UNARY && other.paramCount == 1))) {
                                listener.onNode(other);
                                opStack.poll();
                            } else {
                                break;
                            }
                        }
                        node = exprNodePool.next().of(
                                op.type == ExprOperator.OperatorType.SET ? ExprNode.NodeType.SET_OPERATION : ExprNode.NodeType.OPERATION,
                                op.token,
                                op.precedence,
                                lexer.position()
                        );
                        switch (type) {
                            case UNARY:
                                node.paramCount = 1;
                                break;
                            default:
                                node.paramCount = 2;
                                break;
                        }
                        opStack.push(node);
                    } else if (!nonLiteralBranches.contains(thisBranch.ordinal())) {
                        thisBranch = Branch.LITERAL;
                        // If the token is a function token, then push it onto the stack.
                        opStack.push(exprNodePool.next().of(ExprNode.NodeType.LITERAL, Chars.toString(tok), Integer.MIN_VALUE, lexer.position()));
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

    private enum Branch {
        NONE, COMMA, LEFT_BRACE, RIGHT_BRACE, CONSTANT, OPERATOR, LITERAL, LAMBDA
    }

    static {
        nonLiteralBranches.add(Branch.RIGHT_BRACE.ordinal());
        nonLiteralBranches.add(Branch.CONSTANT.ordinal());
        nonLiteralBranches.add(Branch.LITERAL.ordinal());
        nonLiteralBranches.add(Branch.LAMBDA.ordinal());
    }
}
