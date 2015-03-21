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

package com.nfsdb.lang.parser;

import com.nfsdb.PartitionType;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.GenericIntBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StdoutSink;
import com.nfsdb.lang.ast.*;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

public class NQLParser {

    private final TokenStream tokenStream = new TokenStream() {{
        defineSymbol(new Token(" "));
        defineSymbol(new Token("("));
        defineSymbol(new Token(")"));
        defineSymbol(new Token(","));
        defineSymbol(new Token("+"));
    }};

    public NQLParser() {
        for (int i = 0, k = Operator.operators.size(); i < k; i++) {
            tokenStream.defineSymbol(new Token(Operator.operators.get(i).token));
        }
    }

    public static void main(String[] args) throws ParserException, JournalException {
        NQLParser parser = new NQLParser();
        QueryExecutor executor = new QueryExecutor(new JournalFactory("/Users/vlad/dev/data"));
//        executor.execute(parser.parse("create journal xyz (a INT index buckets 250, b DOUBLE)  partition by MONTH"));
        RecordSourcePrinter p = new RecordSourcePrinter(new StdoutSink());
        parser.setContent("select vendorId, medallion, pickupLongitude from trip_data_1.csv");
        RecordSource<? extends Record> src = executor.execute(parser.parse());
        if (src != null) {
            p.print(src);
        }

        System.out.println("ok");
    }

    public Statement parse() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        if (Chars.equals(tok, "select")) {
            return parseQuery();
        }

        throw err("create | select expected");
    }

    public ExprNode parseExpr() throws ParserException {
        Deque<ExprNode> opStack = new ArrayDeque<>();
        Deque<ExprNode> astStack = new ArrayDeque<>();
        Deque<Integer> paramCountStack = new ArrayDeque<>();

        int tokCount = 0;
        int opAt = 0;
        int paramCount = 0;
        int braceCount = 0;

        ExprNode node;
        CharSequence tok;
        char thisChar = 0, prevChar;
        Branch prevBranch;
        Branch thisBranch = Branch.NONE;

        OUT:
        while ((tok = optionTok()) != null) {
            tokCount++;
            prevChar = thisChar;
            thisChar = tok.charAt(0);
            prevBranch = thisBranch;

            switch (thisChar) {
                case ',':
                    thisBranch = Branch.COMMA;
                    if (prevChar == ',') {
                        throw new ParserException(tokenStream.position(), "Missing argument");
                    }

                    if (braceCount == 0) {
                        // comma outside of braces
                        break OUT;
                    }

                    // If the token is a function argument separator (e.g., a comma):
                    // Until the token at the top of the stack is a left parenthesis,
                    // pop operators off the stack onto the output queue. If no left
                    // parentheses are encountered, either the separator was misplaced or
                    // parentheses were mismatched.
                    while ((node = opStack.pollFirst()) != null && node.token.charAt(0) != '(') {
                        addNode(node, astStack);
                    }

                    if (node != null) {
                        opStack.push(node);
                    }

                    opAt = tokCount;
                    paramCount++;
                    break;

                case '(':
                    thisBranch = Branch.LEFT_BRACE;
                    braceCount++;
                    // If the token is a left parenthesis, then push it onto the stack.
                    paramCountStack.push(paramCount);
                    paramCount = 0;
                    opStack.push(new ExprNode(ExprNode.NodeType.CONTROL, "(", Integer.MAX_VALUE, tokenStream.position()));
                    opAt = tokCount;
                    break;

                case ')':
                    if (prevChar == ',') {
                        throw new ParserException(tokenStream.position(), "Missing argument");
                    }
                    if (braceCount == 0) {
                        throw new ParserException(tokenStream.position(), "Unbalanced )");
                    }

                    thisBranch = Branch.RIGHT_BRACE;
                    braceCount--;
                    // If the token is a right parenthesis:
                    // Until the token at the top of the stack is a left parenthesis, pop operators off the stack onto the output queue.
                    // Pop the left parenthesis from the stack, but not onto the output queue.
                    //        If the token at the top of the stack is a function token, pop it onto the output queue.
                    //        If the stack runs out without finding a left parenthesis, then there are mismatched parentheses.
                    while ((node = opStack.pollFirst()) != null && node.token.charAt(0) != '(') {
                        addNode(node, astStack);
                    }

                    if ((node = opStack.peek()) != null && node.type == ExprNode.NodeType.LITERAL) {
                        node.type = ExprNode.NodeType.FUNCTION;
                        node.paramCount = prevChar == '(' ? 0 : paramCount + 1;
                        addNode(node, astStack);
                        opStack.pollFirst();
                        if (!paramCountStack.isEmpty()) {
                            paramCount = paramCountStack.pollFirst();
                        }
                    }
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
                    thisBranch = Branch.CONSTANT;
                    // If the token is a number, then add it to the output queue.
                    astStack.push(new ExprNode(ExprNode.NodeType.CONSTANT, tok.toString(), 0, tokenStream.position()));
                    break;
                default:
                    Operator op;
                    if ((op = Operator.opMap.get(tok)) != null) {

                        thisBranch = Branch.OPERATOR;

                        // If the token is an operator, o1, then:
                        // while there is an operator token, o2, at the top of the operator stack, and either
                        // o1 is left-associative and its precedence is less than or equal to that of o2, or
                        // o1 is right associative, and has precedence less than that of o2,
                        //        then pop o2 off the operator stack, onto the output queue;
                        // push o1 onto the operator stack.

                        Operator.OperatorType type = op.type;


                        switch (thisChar) {
                            case '-':
                                switch (prevBranch) {
                                    case OPERATOR:
                                    case COMMA:
                                    case NONE:
                                        // we have unary minus
                                        type = Operator.OperatorType.UNARY;

                                }
                        }

                        opAt = tokCount;
                        ExprNode other;
                        // UNARY operators must never pop BINARY ones regardless of precedence
                        // this is to maintain correctness of -a^b
                        while ((other = opStack.peek()) != null) {
                            boolean greaterPrecedence = (op.leftAssociative && op.precedence >= other.precedence) || (!op.leftAssociative && op.precedence > other.precedence);
                            if (greaterPrecedence &&
                                    (type == Operator.OperatorType.BINARY || (type == Operator.OperatorType.UNARY && other.paramCount == 1))) {

                                addNode(other, astStack);
                                opStack.pollFirst();
                            } else {
                                break;
                            }
                        }
                        node = new ExprNode(ExprNode.NodeType.OPERATION, op.token, op.precedence, tokenStream.position());
                        switch (type) {
                            case UNARY:
                                node.paramCount = 1;
                                break;
                            default:
                                node.paramCount = 2;
                        }
                        opStack.push(node);
                    } else if (opAt == 0 || opAt == tokCount - 1) {
                        thisBranch = Branch.LITERAL;
                        // If the token is a function token, then push it onto the stack.
                        opStack.push(new ExprNode(ExprNode.NodeType.LITERAL, tok.toString(), Integer.MIN_VALUE, tokenStream.position()));
                    } else {
                        // literal can be at start of input, after a bracket or part of an operator
                        // all other cases are illegal and will be considered end-of-input
                        break OUT;
                    }
            }
        }

        while ((node = opStack.poll()) != null) {
            if (node.token.charAt(0) == '(') {
                throw new ParserException(node.position, "Unbalanced (");
            }
            addNode(node, astStack);
        }
        return astStack.pollFirst();
    }

    public void setContent(CharSequence cs) {
        tokenStream.setContent(cs);
    }

    private static void addNode(ExprNode node, Deque<ExprNode> stack) {
        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = stack.pollFirst();
                break;
            case 2:
                node.rhs = stack.pollFirst();
                node.lhs = stack.pollFirst();
                break;
            default:
                ArrayList<ExprNode> a = new ArrayList<>();
                for (int i = 0; i < node.paramCount; i++) {
                    a.add(stack.pollFirst());
                }
                node.args = a;
        }
        stack.push(node);
    }

    private ParserException err(String msg) {
        return new ParserException(tokenStream.position(), msg);
    }

    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (!Chars.equals(tok, expected)) {
            throw err("\"" + expected + "\" expected");
        }

    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private String notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw err("Invalid column definition");
        }
        return tok.toString();
    }

    private CharSequence optionTok() {
        while (tokenStream.hasNext()) {
            CharSequence cs = tokenStream.next();
            if (!Chars.equals(cs, ' ')) {
                return cs;
            }
        }
        return null;
    }

    private Statement parseCreateJournal() throws ParserException {
        JournalStructure structure = new JournalStructure(tok().toString());
        parseJournalFields(structure);
        CharSequence tok = optionTok();
        if (tok != null) {
            expectTok(tok, "partition");
            expectTok(tok(), "by");
            structure.partitionBy(PartitionType.valueOf(tok().toString()));
        }
        return new Statement(StatementType.CREATE_JOURNAL, structure);
    }

    private Statement parseCreateStatement() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "journal")) {
            return parseCreateJournal();
        }

        throw err("journal expected");
    }

    private CharSequence parseIntDefinition(GenericIntBuilder genericIntBuilder) throws ParserException {
        CharSequence tok = tok();

        if (isFieldTerm(tok)) {
            return tok;
        }

        expectTok(tok, "index");
        genericIntBuilder.index();

        if (isFieldTerm(tok = tok())) {
            return tok;
        }

        expectTok(tok, "buckets");

        try {
            genericIntBuilder.buckets(Numbers.parseInt(tok()));
        } catch (NumberFormatException e) {
            throw err("expected number of buckets (int)");
        }

        return null;
    }

    private void parseJournalFields(JournalStructure struct) throws ParserException {
        if (!Chars.equals(tok(), '(')) {
            throw err("( expected");
        }

        while (true) {
            String name = notTermTok();
            CharSequence tok = null;
            switch (ColumnType.valueOf(notTermTok())) {
                case INT:
                    tok = parseIntDefinition(struct.$int(name));
                    break;
                case DOUBLE:
                    struct.$double(name);
                    break;
                case BOOLEAN:
                    struct.$bool(name);
                    break;
                case FLOAT:
                    struct.$float(name);
                    break;
                case LONG:
                    struct.$long(name);
                    break;
                case SHORT:
                    struct.$short(name);
                    break;
                case STRING:
                    struct.$str(name);
                    break;
                case SYMBOL:
                    struct.$sym(name);
                    break;
                case BINARY:
                    struct.$bin(name);
                    break;
                case DATE:
                    struct.$date(name);
            }

            if (tok == null) {
                tok = tok();
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(", or ) expected");
            }
        }
    }

    private Statement parseQuery() throws ParserException {

        QueryModel model = new QueryModel();
        while (true) {
            model.addColumn(tok().toString());
            CharSequence tok = tok();
            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(", expected");
            }
        }

        model.setJournalName(tok().toString());

        return new Statement(StatementType.QUERY_JOURNAL, model);

    }

    private CharSequence tok() throws ParserException {

        CharSequence tok = optionTok();
        if (tok == null) {
            throw err("Unexpected end of input");
        }
        return tok;
    }

    private enum Branch {
        NONE, COMMA, LEFT_BRACE, RIGHT_BRACE, CONSTANT, OPERATOR, LITERAL
    }
}
