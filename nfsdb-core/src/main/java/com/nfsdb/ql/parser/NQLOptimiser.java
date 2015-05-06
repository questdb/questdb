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

package com.nfsdb.ql.parser;

import com.nfsdb.Journal;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NoSuchColumnException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.ql.*;
import com.nfsdb.ql.impl.*;
import com.nfsdb.ql.model.*;
import com.nfsdb.ql.ops.*;
import com.nfsdb.ql.ops.fact.FunctionFactories;
import com.nfsdb.ql.ops.fact.FunctionFactory;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;

public class NQLOptimiser {

    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final ArrayDeque<ExprNode> exprStack = new ArrayDeque<>();
    private final IntrinsicExtractor intrinsicExtractor = new IntrinsicExtractor();
    private final JournalFactory factory;

    public NQLOptimiser(JournalFactory factory) {
        this.factory = factory;
    }

    public RecordSource<? extends Record> compile(QueryModel model) throws ParserException, JournalException {
        RecordSource<? extends Record> rs = createRecordSource(model);
        RecordMetadata meta = rs.getMetadata();
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
                    if (meta.invalidColumn(node.token)) {
                        throw new InvalidColumnException(node.position);
                    }
                    selectedColumns.add(node.token);
                    break;
                default:
                    String colName = qc.getName() == null ? "col" + columnSequence++ : qc.getName();
                    VirtualColumn c = createVirtualColumn(qc.getAst(), rs.getMetadata());
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

    private void createColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
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
                        stack.addFirst(lookupColumn(node, metadata));
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
                    if (c == null) {
                        throw new ParserException(node.position, "Too few arguments");
                    }
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

    private RecordSource<? extends Record> createRecordSource(QueryModel model) throws JournalException, ParserException {

        ExprNode readerNode = model.getJournalName();
        if (readerNode.type != ExprNode.NodeType.LITERAL) {
            throw new ParserException(readerNode.position, "Journal name expected");
        }

        if (factory.exists(readerNode.token) == JournalReaderFactory.JournalExistenceCheck.DOES_NOT_EXIST) {
            throw new ParserException(readerNode.position, "Journal does not exist");
        }

        if (factory.exists(readerNode.token) == JournalReaderFactory.JournalExistenceCheck.EXISTS_FOREIGN) {
            throw new ParserException(readerNode.position, "Journal directory is of unknown format");
        }

        Journal reader = factory.reader(readerNode.token);

        PartitionSource ps = new JournalPartitionSource(reader, true);
        RowSource rs = null;

        String latestByCol = null;

        if (model.getLatestBy() != null) {
            ExprNode l = model.getLatestBy();
            if (l.type != ExprNode.NodeType.LITERAL) {
                throw new ParserException(l.position, "Column name expected");
            }

            if (reader.getMetadata().invalidColumn(l.token)) {
                throw new InvalidColumnException(l.position);
            }

            ColumnMetadata m = reader.getMetadata().getColumn(l.token);

            if (m.type != ColumnType.SYMBOL) {
                throw new ParserException(l.position, "Expected symbol column, found: " + m.type);
            }

            if (!m.indexed) {
                throw new ParserException(l.position, "Column is not indexed");
            }

            latestByCol = l.token;
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = intrinsicExtractor.extract(where, reader, latestByCol);

            if (im.intrinsicValue == IntrinsicValue.FALSE) {
                ps = new NoOpJournalPartitionSource(reader);
            } else {

                if (im.intervalHi < Long.MAX_VALUE || im.intervalLo > Long.MIN_VALUE) {

                    ps = new MultiIntervalPartitionSource(ps,
                            new SingleIntervalSource(
                                    new Interval(im.intervalLo, im.intervalHi
                                    )
                            )
                    );
                }

                if (im.intervalSource != null) {
                    ps = new MultiIntervalPartitionSource(ps, im.intervalSource);
                }

                if (latestByCol == null) {
                    if (im.keyColumn != null) {
                        rs = new KvIndexRowSource(im.keyColumn, new PartialSymbolKeySource(im.keyColumn, im.keyValues));
                    }

                    if (im.filter != null) {
                        rs = new FilteredRowSource(rs == null ? new AllRowSource() : rs, createVirtualColumn(im.filter, reader.getMetadata()));
                    }
                } else {

                    if (im.keyColumn != null && im.filter != null) {
                        rs = new KvIndexHeadRowSource(latestByCol, new PartialSymbolKeySource(latestByCol, im.keyValues), 1, 0, createVirtualColumn(im.filter, reader.getMetadata()));
                    } else if (im.keyColumn != null) {
                        rs = new KvIndexHeadRowSource(latestByCol, new PartialSymbolKeySource(latestByCol, im.keyValues), 1, 0, null);
                    } else {
                        rs = new KvIndexHeadRowSource(latestByCol, new SymbolKeySource(latestByCol), 1, 0, null);
                    }
                }
            }
        } else if (latestByCol != null) {
            rs = new KvIndexHeadRowSource(latestByCol, new SymbolKeySource(latestByCol), 1, 0, null);
        }

        return new JournalSource(ps, rs == null ? new AllRowSource() : rs);
    }

    private VirtualColumn createVirtualColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        // post-order iterative tree traversal
        // see http://en.wikipedia.org/wiki/Tree_traversal

        ExprNode lastVisited = null;

        while (!exprStack.isEmpty() || node != null) {
            if (node != null) {
                exprStack.addFirst(node);
                node = node.lhs;
            } else {
                ExprNode peek = exprStack.peekFirst();

                if (peek.rhs != null && lastVisited != peek.rhs) {
                    node = peek.rhs;
                } else {
                    createColumn(peek, metadata);
                    lastVisited = exprStack.pollFirst();
                }
            }
        }
        return stack.pollFirst();
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private VirtualColumn lookupColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        try {
            return new RecordSourceColumn(node.token, metadata);
        } catch (NoSuchColumnException e) {
            throw new InvalidColumnException(node.position);
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

        String s = Chars.stripQuotes(node.token);

        // by ref comparison
        if (s != node.token) {
            return new StringConstant(s);
        }

        try {
            return new IntConstant(Numbers.parseInt(node.token));
        } catch (NumberFormatException ignore) {

        }

        try {
            return new DoubleConstant(Numbers.parseDouble(node.token));
        } catch (NumberFormatException ignore) {

        }

        throw new ParserException(node.position, "Unknown value type: " + node.token);
    }

}
