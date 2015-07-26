/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.JournalKey;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NoSuchColumnException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
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
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;

public class Optimiser {

    private final static NullConstant nullConstant = new NullConstant();

    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final IntrinsicExtractor intrinsicExtractor = new IntrinsicExtractor();
    private final VirtualColumnBuilder virtualColumnBuilderVisitor = new VirtualColumnBuilder();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final Signature mutableSig = new Signature();
    private final ObjList<VirtualColumn> mutableArgs = new ObjList<>();
    private final StringSink columnNameAssembly = new StringSink();
    private final int columnNamePrefixLen;

    public Optimiser() {
        // seed column name assembly with default column prefix, which we will reuse
        columnNameAssembly.put("col");
        columnNamePrefixLen = 3;
    }

    public RecordSource<? extends Record> compile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        return selectColumns(compile0(model, factory), model.getColumns());
    }

    void collectJournalMetadata(QueryModel model, JournalReaderFactory factory) throws ParserException, JournalException {
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

        model.setMetadata(factory.getOrCreateMetadata(new JournalKey<>(reader)));
    }

    private RecordSource<? extends Record> compile0(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        if (model.getJournalName() != null) {
            return createRecordSource(model, factory);
        } else {
            RecordSource<? extends Record> rs = compile(model.getNestedModel(), factory);
            if (model.getWhereClause() == null) {
                return rs;
            }

            RecordMetadata m = rs.getMetadata();
            IntrinsicModel im = intrinsicExtractor.extract(model.getWhereClause(), m, null);

            switch (im.intrinsicValue) {
                case FALSE:
                    return new NoOpJournalRecordSource(rs);
                default:
                    if (im.intervalSource != null) {
                        rs = new IntervalJournalRecordSource(rs, im.intervalSource);
                    }
                    if (im.filter != null) {
                        VirtualColumn vc = createVirtualColumn(im.filter, m);
                        if (vc.isConstant()) {
                            if (vc.getBool(null)) {
                                return rs;
                            } else {
                                return new NoOpJournalRecordSource(rs);
                            }
                        }
                        return new FilteredJournalRecordSource(rs, vc);
                    } else {
                        return rs;
                    }
            }
        }
    }

    private void createColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        mutableArgs.clear();
        mutableSig.clear();

        int argCount = node.paramCount;
        switch (argCount) {
            case 0:
                switch (node.type) {
                    case LITERAL:
                        // lookup column
                        stack.push(lookupColumn(node, metadata));
                        break;
                    case CONSTANT:
                        stack.push(parseConstant(node));
                        break;
                    default:
                        // lookup zero arg function from symbol table
                        stack.push(lookupFunction(node, mutableSig.setName(node.token).setParamCount(0), null));
                }
                break;
            default:
                mutableArgs.ensureCapacity(argCount);
                mutableSig.setName(node.token).setParamCount(argCount);
                for (int n = 0; n < argCount; n++) {
                    VirtualColumn c = stack.poll();
                    if (c == null) {
                        throw new ParserException(node.position, "Too few arguments");
                    }
                    mutableSig.paramType(n, c.getType(), c.isConstant());
                    mutableArgs.setQuick(n, c);
                }
                stack.push(lookupFunction(node, mutableSig, mutableArgs));
        }
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT", "CC_CYCLOMATIC_COMPLEXITY"})
    private RecordSource<? extends Record> createRecordSource(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        JournalMetadata metadata = model.getMetadata();

        if (metadata == null) {
            collectJournalMetadata(model, factory);
            metadata = model.getMetadata();
        }

        PartitionSource ps = new JournalPartitionSource(metadata, true);
        RowSource rs = null;

        String latestByCol = null;
        RecordColumnMetadata latestByMetadata = null;
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

            ColumnType type = latestByMetadata.getType();
            if (type != ColumnType.SYMBOL && type != ColumnType.STRING) {
                throw new ParserException(latestByNode.position, "Expected symbol or string column, found: " + type);
            }

            if (!latestByMetadata.isIndexed()) {
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
                    switch (latestByMetadata.getType()) {
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
            switch (latestByMetadata.getType()) {
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
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(0)), true),
                        new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexStrLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private RowSource createRecordSourceForSym(IntrinsicModel im) {
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(0)), true),
                        new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexSymLookupRowSource(im.keyColumn, new StrConstant(im.keyValues.get(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private VirtualColumn createVirtualColumn(ExprNode node, RecordMetadata metadata) throws ParserException {
        virtualColumnBuilderVisitor.metadata = metadata;
        traversalAlgo.traverse(node, virtualColumnBuilderVisitor);
        return stack.poll();
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

    @SuppressFBWarnings({"ES_COMPARING_STRINGS_WITH_EQ"})
    private VirtualColumn parseConstant(ExprNode node) throws ParserException {

        if ("null".equals(node.token)) {
            return nullConstant;
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

    private RecordSource<? extends Record> selectColumns(RecordSource<? extends Record> rs, ObjList<QueryColumn> columns) throws ParserException {
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
                        columnNameAssembly.clear(columnNamePrefixLen);
                        Numbers.append(columnNameAssembly, columnSequence++);
                        colName = columnNameAssembly.toString();
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
            rs = new VirtualColumnRecordSource(rs, virtualColumns);
        }
        return new SelectedColumnsRecordSource(rs, selectedColumns);
    }

    private class VirtualColumnBuilder implements Visitor {
        private RecordMetadata metadata;

        @Override
        public void visit(ExprNode node) throws ParserException {
            createColumn(node, metadata);
        }
    }
}
