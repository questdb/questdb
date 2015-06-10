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

package com.nfsdb.ql.parser;

import com.nfsdb.JournalKey;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NoSuchColumnException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.io.sink.StringSink;
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

public class Optimiser {

    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final IntrinsicExtractor intrinsicExtractor = new IntrinsicExtractor();
    private final VirtualColumnBuilder virtualColumnBuilderVisitor = new VirtualColumnBuilder();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final Signature mutableSig = new Signature();
    private final StringSink concatenator = new StringSink();

    public Optimiser() {
        // seed concatenator with default column prefix, which we will reuse
        concatenator.put("col");
    }

    public JournalRecordSource<? extends Record> compile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        if (model.getJournalName() != null) {
            return selectColumns(createRecordSource(model, factory), model.getColumns());
        } else {
            JournalRecordSource<? extends Record> rs = compile(model.getNestedQuery(), factory);
            if (model.getWhereClause() == null) {
                return selectColumns(rs, model.getColumns());
            }

            RecordMetadata m = rs.getMetadata();
            IntrinsicModel im = intrinsicExtractor.extract(model.getWhereClause(), m, null);

            switch (im.intrinsicValue) {
                case FALSE:
                    return selectColumns(new NoOpJournalRecordSource(rs), model.getColumns());
                default:
                    if (im.intervalSource != null) {
                        rs = new IntervalJournalRecordSource(rs, im.intervalSource);
                    }
                    if (im.filter != null) {
                        VirtualColumn vc = createVirtualColumn(im.filter, m);
                        if (vc.isConstant()) {
                            if (vc.getBool(null)) {
                                return selectColumns(rs, model.getColumns());
                            } else {
                                return selectColumns(new NoOpJournalRecordSource(rs), model.getColumns());
                            }
                        }
                        return selectColumns(new FilteredJournalRecordSource(rs, vc), model.getColumns());
                    } else {
                        return selectColumns(rs, model.getColumns());
                    }
            }
        }
    }

    private void createColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        ObjList<VirtualColumn> args = new ObjList<>();

        int argCount = node.paramCount;
        mutableSig.clear();

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
                        stack.addFirst(lookupFunction(node, mutableSig.setName(node.token).setParamCount(0), null));
                }
                break;
            default:
                args.ensureCapacity(argCount);
                mutableSig.setName(node.token).setParamCount(argCount);
                for (int n = 0; n < argCount; n++) {
                    VirtualColumn c = stack.pollFirst();
                    if (c == null) {
                        throw new ParserException(node.position, "Too few arguments");
                    }
                    mutableSig.paramType(n, c.getType(), c.isConstant());
                    args.setQuick(n, c);
                }
                stack.addFirst(lookupFunction(node, mutableSig, args));
        }
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT", "CC_CYCLOMATIC_COMPLEXITY"})
    private JournalRecordSource<? extends Record> createRecordSource(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        ExprNode readerNode = model.getJournalName();
        if (readerNode.type != ExprNode.NodeType.LITERAL && readerNode.type != ExprNode.NodeType.CONSTANT) {
            throw new ParserException(readerNode.position, "Journal name must be either literal or string constant");
        }

        JournalConfiguration configuration = factory.getConfiguration();

        String reader = Chars.stripQuotes(readerNode.token);
        if (configuration.exists(reader) == JournalConfiguration.JournalExistenceCheck.DOES_NOT_EXIST) {
            throw new ParserException(readerNode.position, "Journal does not exist");
        }

        if (configuration.exists(reader) == JournalConfiguration.JournalExistenceCheck.EXISTS_FOREIGN) {
            throw new ParserException(readerNode.position, "Journal directory is of unknown format");
        }

        JournalMetadata metadata = factory.getOrCreateMetadata(new JournalKey<>(reader));

        PartitionSource ps = new JournalPartitionSource(metadata, true);
        RowSource rs = null;

        String latestByCol = null;
        ColumnMetadata latestByMetadata = null;
        ExprNode latestByNode = null;

        if (model.getLatestBy() != null) {
            latestByNode = model.getLatestBy();
            if (latestByNode.type != ExprNode.NodeType.LITERAL) {
                throw new ParserException(latestByNode.position, "Column name expected");
            }

            if (metadata.invalidColumn(latestByNode.token)) {
                throw new InvalidColumnException(latestByNode.position);
            }

            latestByMetadata = metadata.getColumn(latestByNode.token);

            if (latestByMetadata.type != ColumnType.SYMBOL && latestByMetadata.type != ColumnType.STRING) {
                throw new ParserException(latestByNode.position, "Expected symbol or string column, found: " + latestByMetadata.type);
            }

            if (!latestByMetadata.indexed) {
                throw new ParserException(latestByNode.position, "Column is not indexed");
            }

            latestByCol = latestByNode.token;
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = intrinsicExtractor.extract(where, metadata, latestByCol);

            VirtualColumn filter = im.filter != null ? createVirtualColumn(im.filter, metadata) : null;

            if (filter != null) {
                if (filter.getType() != ColumnType.BOOLEAN) {
                    throw new ParserException(im.filter.position, "Boolean expression expected");
                }

                if (filter.isConstant()) {
                    if (filter.getBool(null)) {
                        // constant TRUE, no filtering needed
                        filter = null;
                    } else {
                        im.intrinsicValue = IntrinsicValue.FALSE;
                    }
                }
            }

            if (im.intrinsicValue == IntrinsicValue.FALSE) {
                ps = new NoOpJournalPartitionSource(metadata);
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
                        switch (metadata.getColumn(im.keyColumn).type) {
                            case SYMBOL:
                                rs = createRecordSourceForSym(im);
                                break;
                            case STRING:
                                rs = createRecordSourceForStr(im);
                                break;
                        }
                    }

                    if (filter != null) {
                        rs = new FilteredRowSource(rs == null ? new AllRowSource() : rs, filter);
                    }
                } else {
                    switch (latestByMetadata.type) {
                        case SYMBOL:
                            if (im.keyColumn != null) {
                                rs = new KvIndexSymListHeadRowSource(latestByCol, im.keyValues, filter);
                            } else {
                                rs = new KvIndexAllSymHeadRowSource(latestByCol, filter);
                            }
                            break;
                        case STRING:
                            if (im.keyColumn != null) {
                                rs = new KvIndexStrListHeadRowSource(latestByCol, im.keyValues, filter);
                            } else {
                                throw new ParserException(latestByNode.position, "Filter on string column expected");
                            }
                            break;
                    }
                }
            }
        } else if (latestByCol != null) {
            switch (latestByMetadata.type) {
                case SYMBOL:
                    rs = new KvIndexAllSymHeadRowSource(latestByCol, null);
                    break;
                case STRING:
                    throw new ParserException(latestByNode.position, "Filter on string column expected");
            }
        }

        return new JournalSource(ps, rs == null ? new AllRowSource() : rs);
    }

    private RowSource createRecordSourceForStr(IntrinsicModel im) {
        if (im.keyValues.size() == 1) {
            return new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.getLast()));
        } else {
            RowSource src = null;
            for (int i = 0, k = im.keyValues.size(); i < k; i++) {
                if (src == null) {
                    src = new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true);
                } else {
                    src = new MergingRowSource(src, new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true));
                }
            }
            return src;
        }
    }

    private RowSource createRecordSourceForSym(IntrinsicModel im) {
        if (im.keyValues.size() == 1) {
            return new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.getLast()));
        } else {
            RowSource src = null;
            for (int i = 0, k = im.keyValues.size(); i < k; i++) {
                if (src == null) {
                    src = new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true);
                } else {
                    src = new MergingRowSource(src, new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true));
                }
            }
            return src;
        }
    }

    private VirtualColumn createVirtualColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        virtualColumnBuilderVisitor.metadata = metadata;
        traversalAlgo.traverse(node, virtualColumnBuilderVisitor);
        return stack.pollFirst();
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private VirtualColumn lookupColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        try {
            int index = metadata.getColumnIndex(node.token);
            switch (metadata.getColumn(index).getType()) {
                case DOUBLE:
                    return new DoubleRecordSourceColumn(index);
                case INT:
                    return new IntRecordSourceColumn(index);
                case LONG:
                    return new LongRecordSourceColumn(index);
                case STRING:
                    return new StrRecordSourceColumn(index);
                case SYMBOL:
                    return new SymRecordSourceColumn(index);
                default:
                    throw new ParserException(node.position, "Not yet supported type");
            }
        } catch (NoSuchColumnException e) {
            throw new InvalidColumnException(node.position);
        }
    }

    private VirtualColumn lookupFunction(ExprNode node, Signature sig, ObjList<VirtualColumn> args) throws ParserException {
        FunctionFactory factory = FunctionFactories.find(sig, args);
        if (factory == null) {
            throw new ParserException(node.position, "No such function: " + sig.userReadable());
        }

        Function f = factory.newInstance(args);
        if (args != null) {
            int n = node.paramCount;
            for (int i = 0; i < n; i++) {
                f.setArg(i, args.getQuick(i));
            }
        }
        return f.isConstant() ? processConstantExpression(f) : f;
    }

    private VirtualColumn parseConstant(ExprNode node) throws ParserException {

        if ("null".equals(node.token)) {
            return new NullConstant();
        }

        String s = Chars.stripQuotes(node.token);

        // by ref comparison
        //noinspection StringEquality
        if (s != node.token) {
            return new StrConstant(s);
        }

        try {
            return new IntConstant(Numbers.parseInt(node.token));
        } catch (NumberFormatException ignore) {

        }

        try {
            return new LongConstant(Numbers.parseLong(node.token));
        } catch (NumberFormatException ignore) {

        }

        try {
            return new DoubleConstant(Numbers.parseDouble(node.token));
        } catch (NumberFormatException ignore) {

        }

        throw new ParserException(node.position, "Unknown value type: " + node.token);
    }

    private VirtualColumn processConstantExpression(Function f) {
        switch (f.getType()) {
            case INT:
                return new IntConstant(f.getInt(null));
            case DOUBLE:
                return new DoubleConstant(f.getDouble(null));
            case BOOLEAN:
                return new BooleanConstant(f.getBool(null));
            default:
                return f;
        }
    }

    private JournalRecordSource<? extends Record> selectColumns(JournalRecordSource<? extends Record> rs, ObjList<QueryColumn> columns) throws ParserException {
        if (columns.size() == 0) {
            return rs;
        }

        ObjList<VirtualColumn> virtualColumns = null;
        ObjList<String> selectedColumns = new ObjList<>();
        int columnSequence = 0;
        final RecordMetadata meta = rs.getMetadata();

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            ExprNode node = qc.getAst();

            switch (node.type) {
                case LITERAL:
                    if (meta.invalidColumn(node.token)) {
                        throw new InvalidColumnException(node.position);
                    }
                    selectedColumns.add(node.token);
                    break;
                default:

                    String colName = qc.getName();
                    if (colName == null) {
                        concatenator.clear(3);
                        Numbers.append(concatenator, columnSequence++);
                        colName = concatenator.toString();
                    }
                    VirtualColumn c = createVirtualColumn(qc.getAst(), rs.getMetadata());
                    c.setName(colName);
                    selectedColumns.add(colName);
                    if (virtualColumns == null) {
                        virtualColumns = new ObjList<>();
                    }
                    virtualColumns.add(c);
            }
        }

        if (virtualColumns != null) {
            rs = new VirtualColumnJournalRecordSource(rs, virtualColumns);
        }
        return new SelectedColumnsJournalRecordSource(rs, selectedColumns);
    }

    private class VirtualColumnBuilder implements PostOrderTreeTraversalAlgo.Visitor {
        private RecordMetadata metadata;

        @Override
        public void visit(ExprNode node) throws ParserException {
            createColumn(node, metadata);
        }
    }

}
