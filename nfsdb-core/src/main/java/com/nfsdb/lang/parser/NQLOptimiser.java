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

import com.nfsdb.Journal;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NoSuchColumnException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.lang.ast.ExprNode;
import com.nfsdb.lang.ast.QueryColumn;
import com.nfsdb.lang.ast.QueryModel;
import com.nfsdb.lang.ast.Signature;
import com.nfsdb.lang.ast.factory.Function;
import com.nfsdb.lang.ast.factory.FunctionFactories;
import com.nfsdb.lang.ast.factory.FunctionFactory;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.lang.cst.impl.rsrc.AllRowSource;
import com.nfsdb.lang.cst.impl.virt.*;
import com.nfsdb.utils.Numbers;

import java.util.ArrayDeque;

public class NQLOptimiser {

    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final ArrayDeque<ExprNode> exprStack = new ArrayDeque<>();
    private final JournalFactory factory;

    public NQLOptimiser(JournalFactory factory) {
        this.factory = factory;
    }

    public RecordSource<? extends Record> compile(QueryModel model) throws ParserException, JournalException {
        RecordSource<? extends Record> rs = getRecordSource(model);

        ObjList<QueryColumn> columns = model.getColumns();
        ObjList<VirtualColumn> virtualColumns = new ObjList<>();
        ObjList<String> selectedColumns = new ObjList<>();
        int columnSequence = 0;

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.get(i);
            ExprNode node = qc.getAst();

            switch (node.type) {
                case LITERAL:
                    selectedColumns.add(node.token);
                    break;
                default:
                    VirtualColumn c = createVirtualColumn(qc.getAst(), rs);
                    String colName = qc.getName() == null ? "col" + columnSequence++ : qc.getName();
                    c.setName(colName);
                    selectedColumns.add(colName);
                    virtualColumns.add(c);
            }
        }


        if (virtualColumns.size() > 0) {
            rs = new VirtualColumnRecordSource(rs, virtualColumns);
        }
        return new SelectedColumnsRecordSource(rs, selectedColumns);
    }

    private void createColumn(ExprNode node, RecordSource<? extends Record> recordSource) throws ParserException {
        Function f;
        Signature sig = new Signature();
        ObjList<VirtualColumn> args = new ObjList<>();

        int argCount = node.paramCount;
        sig.clear();

        switch (argCount) {
            case 0:
                switch (node.type) {
                    case LITERAL:
                        // lookup column
                        stack.addFirst(lookupColumn(node, recordSource));
                        break;
                    case CONSTANT:
                        stack.addFirst(parseConstant(node));
                        break;
                    default:
                        // lookup zero arg function from symbol table
                        stack.addFirst(lookupFunction(node, sig.setName(node.token).setParamCount(0)));
                }
                break;
            default:
                args.ensureCapacity(argCount);
                sig.setName(node.token).setParamCount(argCount);
                for (int n = argCount - 1; n > -1; n--) {
                    VirtualColumn c = stack.pollFirst();
                    sig.paramType(n, c.getType());
                    args.setQuick(n, c);
                }
                f = lookupFunction(node, sig);
                for (int i = 0; i < node.paramCount; i++) {
                    f.setArg(i, args.getQuick(i));
                }
                stack.addFirst(f);
        }
    }

    private VirtualColumn createVirtualColumn(ExprNode node, RecordSource<? extends Record> recordSource) throws ParserException {
        // post-order iterative tree traversal
        // see http://en.wikipedia.org/wiki/Tree_traversal

        ExprNode lastVisited = null;

        while (!exprStack.isEmpty() || node != null) {
            if (node != null) {
                exprStack.push(node);
                node = node.lhs;
            } else {
                ExprNode peek = exprStack.peekFirst();

                if (peek.rhs != null && lastVisited != peek.rhs) {
                    node = peek.rhs;
                } else {
                    createColumn(peek, recordSource);
                    lastVisited = exprStack.pollFirst();
                }
            }
        }

        return stack.pollFirst();
    }

    private RecordSource<? extends Record> getRecordSource(QueryModel model) throws JournalException {
        Journal r = factory.reader(model.getJournalName());
        return new JournalSourceImpl(new JournalPartitionSource(r, true), new AllRowSource());
    }

    private VirtualColumn lookupColumn(ExprNode node, RecordSource<? extends Record> recordSource) throws ParserException {
        try {
            return new RecordSourceColumn(node.token, recordSource.getMetadata().getColumn(node.token).getType());
        } catch (NoSuchColumnException e) {
            throw new ParserException(node.position, "No such column: " + node.token);
        }
    }

    private Function lookupFunction(ExprNode node, Signature sig) throws ParserException {
        FunctionFactory f = FunctionFactories.find(sig);
        if (f == null) {
            throw new ParserException(node.position, "No such function: " + sig);
        }
        return FunctionFactories.find(sig).newInstance();
    }

    private VirtualColumn parseConstant(ExprNode node) throws ParserException {
        try {
            return new ConstIntColumn(Numbers.parseInt(node.token));
        } catch (NumberFormatException ignore) {

        }

        try {
            return new ConstDoubleColumn(Numbers.parseDouble(node.token));
        } catch (NumberFormatException ignore) {

        }

        throw new ParserException(node.position, "Unknown value type: " + node.token);
    }

}
