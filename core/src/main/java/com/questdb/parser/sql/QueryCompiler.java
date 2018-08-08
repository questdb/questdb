/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.*;
import com.questdb.ql.*;
import com.questdb.ql.aggregation.*;
import com.questdb.ql.analytic.*;
import com.questdb.ql.interval.IntervalRecordSource;
import com.questdb.ql.interval.MultiIntervalPartitionSource;
import com.questdb.ql.join.AsOfJoinRecordSource;
import com.questdb.ql.join.AsOfPartitionedJoinRecordSource;
import com.questdb.ql.join.CrossJoinRecordSource;
import com.questdb.ql.join.HashJoinRecordSource;
import com.questdb.ql.lambda.*;
import com.questdb.ql.latest.*;
import com.questdb.ql.map.RecordKeyCopierCompiler;
import com.questdb.ql.ops.FunctionFactories;
import com.questdb.ql.ops.Parameter;
import com.questdb.ql.ops.Signature;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.constant.LongConstant;
import com.questdb.ql.select.SelectedColumnsRecordSource;
import com.questdb.ql.sort.ComparatorCompiler;
import com.questdb.ql.sort.RBTreeSortedRecordSource;
import com.questdb.ql.sort.RecordComparator;
import com.questdb.ql.sys.SysFactories;
import com.questdb.ql.sys.SystemViewFactory;
import com.questdb.ql.virtual.VirtualColumnRecordSource;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.FlyweightCharSequence;
import com.questdb.std.str.StringSink;
import com.questdb.store.*;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.factory.configuration.JournalStructure;

import java.util.ArrayDeque;

public class QueryCompiler {

    private static final CharSequenceObjHashMap<Parameter> EMPTY_PARAMS = new CharSequenceObjHashMap<>();
    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private final static ObjObjHashMap<Signature, LatestByLambdaRowSourceFactory> LAMBDA_ROW_SOURCE_FACTORIES = new ObjObjHashMap<>();
    private final static LongConstant LONG_ZERO_CONST = new LongConstant(0L, 0);
    private final static IntHashSet joinBarriers;
    private static final int ORDER_BY_UNKNOWN = 0;
    private static final int ORDER_BY_REQUIRED = 1;
    private static final int ORDER_BY_INVARIANT = 2;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final QueryParser parser = new QueryParser();
    private final QueryFilterAnalyser queryFilterAnalyser = new QueryFilterAnalyser();
    private final StringSink columnNameAssembly = new StringSink();
    private final int columnNamePrefixLen;
    private final ObjectPool<FlyweightCharSequence> csPool = new ObjectPool<>(FlyweightCharSequence.FACTORY, 64);
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 16);
    private final IntHashSet deletedContexts = new IntHashSet();
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final ObjectPool<JoinContext> contextPool = new ObjectPool<>(JoinContext.FACTORY, 16);
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final VirtualColumnBuilder virtualColumnBuilder;
    private final IntList clausesToSteal = new IntList();
    private final IntList literalCollectorAIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final IntList literalCollectorBIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final IntPriorityQueue orderingStack = new IntPriorityQueue();
    private final IntList tempCrosses = new IntList();
    private final IntList tempCrossIndexes = new IntList();
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final IntHashSet journalsSoFar = new IntHashSet();
    private final ObjList<IntList> postFilterJournalRefs = new ObjList<>();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(IntList::new, 16);
    private final ArrayDeque<ExprNode> exprNodeStack = new ArrayDeque<>();
    private final IntList nullCounts = new IntList();
    private final ObjList<CharSequence> selectedColumns = new ObjList<>();
    private final CharSequenceHashSet selectedColumnAliases = new CharSequenceHashSet();
    private final CharSequenceIntHashMap constNameToIndex = new CharSequenceIntHashMap();
    private final CharSequenceObjHashMap<ExprNode> constNameToNode = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<String> constNameToToken = new CharSequenceObjHashMap<>();
    private final Signature mutableSig = new Signature();
    private final ObjectPool<QueryColumn> aggregateColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 8);
    private final ObjList<QueryColumn> aggregators = new ObjList<>();
    private final ObjList<QueryColumn> outerVirtualColumns = new ObjList<>();
    private final ObjList<QueryColumn> innerVirtualColumn = new ObjList<>();
    private final ObjList<AnalyticColumn> analyticColumns = new ObjList<>();
    private final ObjHashSet<String> groupKeyColumns = new ObjHashSet<>();
    private final ComparatorCompiler cc = new ComparatorCompiler(asm);
    private final LiteralMatcher literalMatcher = new LiteralMatcher(traversalAlgo);
    private final ServerConfiguration configuration;
    private final ObjObjHashMap<IntList, ObjList<AnalyticFunction>> grouppedAnalytic = new ObjObjHashMap<>();
    private final CopyHelperCompiler copyHelperCompiler = new CopyHelperCompiler(asm);
    private final RecordKeyCopierCompiler recordKeyCopierCompiler = new RecordKeyCopierCompiler(asm);
    private final BootstrapEnv env;
    private ObjList<JoinContext> emittedJoinClauses;
    private int aggregateColumnSequence;

    public QueryCompiler() {
        this(new BootstrapEnv() {{
            configuration = new ServerConfiguration();
        }});
    }

    public QueryCompiler(BootstrapEnv env) {
        // seed column name assembly with default column prefix, which we will reuse
        this.env = env;
        this.configuration = env.configuration;
        this.virtualColumnBuilder = new VirtualColumnBuilder(traversalAlgo, env);
        columnNameAssembly.put("col");
        columnNamePrefixLen = 3;
    }

    public RecordSource compile(Factory factory, CharSequence query) throws ParserException {
        return compile(factory, parse(query));
    }

    public RecordSource compile(Factory factory, ParsedModel model) throws ParserException {
        if (model.getModelType() == ParsedModel.QUERY) {
            clearState();
            final QueryModel qm = (QueryModel) model;
            qm.setParameterMap(EMPTY_PARAMS);
            RecordSource rs = compile(qm, factory);
            rs.setParameterMap(EMPTY_PARAMS);
            return rs;
        }
        throw new IllegalArgumentException("QueryModel expected");
    }

    public JournalWriter createWriter(Factory factory, ParsedModel model) throws ParserException, JournalException {
        if (model.getModelType() != ParsedModel.CREATE_JOURNAL) {
            throw new IllegalArgumentException("create table statement expected");
        }
        clearState();
        CreateJournalModel cm = (CreateJournalModel) model;

        final String name = cm.getName().token;
        switch (factory.getConfiguration().exists(name)) {
            case JournalConfiguration.EXISTS:
                throw QueryError.$(cm.getName().position, "Journal already exists");
            case JournalConfiguration.EXISTS_FOREIGN:
                throw QueryError.$(cm.getName().position, "Name is reserved");
            default:
                break;
        }

        JournalStructure struct = cm.getStruct();

        RecordSource rs;
        QueryModel queryModel = cm.getQueryModel();
        if (queryModel != null) {
            rs = compile(queryModel, factory);
        } else {
            rs = null;
        }

        if (struct == null) {
            assert rs != null;
            RecordMetadata metadata = rs.getMetadata();
            CharSequenceObjHashMap<ColumnCastModel> castModels = cm.getColumnCastModels();

            // validate cast models
            for (int i = 0, n = castModels.size(); i < n; i++) {
                ColumnCastModel castModel = castModels.valueQuick(i);
                if (metadata.getColumnIndexQuiet(castModel.getName().token) == -1) {
                    throw QueryError.invalidColumn(castModel.getName().position, castModel.getName().token);
                }
            }
            struct = createStructure(name, metadata, castModels);
        }

        try {

            validateAndSetTimestamp(struct, cm.getTimestamp());
            validateAndSetPartitionBy(struct, cm.getPartitionBy());

            ExprNode recordHint = cm.getRecordHint();
            if (recordHint != null) {
                try {
                    struct.recordCountHint(Numbers.parseInt(recordHint.token));
                } catch (NumericException e) {
                    throw QueryError.$(recordHint.position, "Bad int");
                }
            }

            ObjList<ColumnIndexModel> columnIndexModels = cm.getColumnIndexModels();
            for (int i = 0, n = columnIndexModels.size(); i < n; i++) {
                ColumnIndexModel cim = columnIndexModels.getQuick(i);

                ExprNode nn = cim.getName();
                ColumnMetadata m = struct.getColumnMetadata(nn.token);

                if (m == null) {
                    throw QueryError.invalidColumn(nn.position, nn.token);
                }

                switch (m.getType()) {
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                        m.indexed = true;
                        m.distinctCountHint = cim.getBuckets();
                        break;
                    default:
                        throw QueryError.$(nn.position, "Type index not supported");
                }
            }

            JournalWriter w = factory.writer(struct);
            w.setSequentialAccess(true);
            if (rs != null) {
                try {
                    copy(factory, rs, w);
                } catch (Throwable e) {
                    w.close();
                    throw e;
                }
            }

            return w;

        } catch (Throwable e) {
            Misc.free(rs);
            throw e;
        }
    }

    public JournalWriter createWriter(Factory factory, CharSequence statement) throws ParserException, JournalException {
        return createWriter(factory, parse(statement));
    }

    public void execute(Factory factory, CharSequence statement) throws ParserException, JournalException {
        execute(factory, parse(statement));
    }

    public void execute(Factory factory, ParsedModel model) throws ParserException, JournalException {
        switch (model.getModelType()) {
            case ParsedModel.CREATE_JOURNAL:
                createWriter(factory, model).close();
                break;
            case ParsedModel.QUERY:
                throw new IllegalArgumentException("Statement expected");
            case ParsedModel.RENAME_JOURNAL:
                renameJournal(factory, (RenameJournalModel) model);
                break;
            default:
                throw new IllegalArgumentException("Unknown statement");
        }
    }

    public ParsedModel parse(CharSequence statement) throws ParserException {
        return parser.parse(statement);
    }

    private static void validateAndSetPartitionBy(JournalStructure struct, ExprNode partitionBy) throws ParserException {
        if (partitionBy == null) {
            return;
        }

        if (struct.hasTimestamp()) {
            int p = PartitionBy.fromString(partitionBy.token);
            if (p == -1) {
                throw QueryError.$(partitionBy.position, "Invalid partition type");
            }
            struct.partitionBy(p);
        } else {
            throw QueryError.$(partitionBy.position, "No timestamp");
        }
    }

    private static void validateAndSetTimestamp(JournalStructure struct, ExprNode timestamp) throws ParserException {

        if (timestamp == null) {
            return;
        }

        int index = struct.getColumnIndex(timestamp.token);
        if (index == -1) {
            throw QueryError.invalidColumn(timestamp.position, timestamp.token);
        }

        if (struct.getColumnMetadata(index).getType() != ColumnType.DATE) {
            throw QueryError.$(timestamp.position, "Not a DATE");
        }

        struct.$ts(index);
    }

    private static Signature lbs(int masterType, int lambdaType) {
        return new Signature().setName("").setParamCount(2).paramType(0, masterType, true).paramType(1, lambdaType, false);
    }

    private static IntHashSet toIntHashSet(IntrinsicModel im) throws ParserException {
        IntHashSet set = null;
        for (int i = 0, n = im.keyValues.size(); i < n; i++) {
            try {
                int v = Numbers.parseInt(im.keyValues.get(i));
                if (set == null) {
                    set = new IntHashSet(n);
                }
                set.add(v);
            } catch (NumericException e) {
                throw QueryError.$(im.keyValuePositions.get(i), "int value expected");
            }
        }
        return set;
    }

    private static LongHashSet toLongHashSet(IntrinsicModel im) throws ParserException {
        LongHashSet set = null;
        for (int i = 0, n = im.keyValues.size(); i < n; i++) {
            try {
                long v = Numbers.parseLong(im.keyValues.get(i));
                if (set == null) {
                    set = new LongHashSet(n);
                }
                set.add(v);
            } catch (NumericException e) {
                throw QueryError.$(im.keyValuePositions.get(i), "int value expected");
            }
        }
        return set;
    }

    private static void assertNotNull(ExprNode node, int position, String message) throws ParserException {
        if (node == null) {
            throw QueryError.$(position, message);
        }
    }

    private void addAlias(int position, String alias) throws ParserException {
        if (selectedColumnAliases.add(alias)) {
            return;
        }
        throw QueryError.$(position, "Duplicate alias");
    }

    private void addFilterOrEmitJoin(QueryModel parent, int index, int ai, CharSequence an, ExprNode ao, int bi, CharSequence bn, ExprNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(index);
            return;
        }

        if (ai == bi) {
            // (same journal)
            ExprNode node = exprNodePool.next().of(ExprNode.OPERATION, "=", 0, 0);
            node.paramCount = 2;
            node.lhs = ao;
            node.rhs = bo;
            addWhereClause(parent, ai, node);
        } else {
            // (different journals)
            JoinContext jc = contextPool.next();
            jc.aIndexes.add(ai);
            jc.aNames.add(an);
            jc.aNodes.add(ao);
            jc.bIndexes.add(bi);
            jc.bNames.add(bn);
            jc.bNodes.add(bo);
            jc.slaveIndex = ai > bi ? ai : bi;
            jc.parents.add(ai < bi ? ai : bi);
            emittedJoinClauses.add(jc);
        }

        deletedContexts.add(index);
    }

    private void addJoinContext(QueryModel parent, JoinContext context) {
        QueryModel jm = parent.getJoinModels().getQuick(context.slaveIndex);
        JoinContext other = jm.getContext();
        if (other == null) {
            jm.setContext(context);
        } else {
            jm.setContext(mergeContexts(parent, other, context));
        }
    }

    /**
     * Adds filters derived from transitivity of equals operation, for example
     * if there is filter:
     * <p>
     * a.x = b.x and b.x = 10
     * <p>
     * derived filter would be:
     * <p>
     * a.x = 10
     * <p>
     * this filter is not explicitly mentioned but it might help pre-filtering record sources
     * before hashing.
     */
    private void addTransitiveFilters(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getContext();
            if (jc != null) {
                for (int k = 0, kn = jc.bNames.size(); k < kn; k++) {
                    CharSequence name = jc.bNames.getQuick(k);
                    if (constNameToIndex.get(name) == jc.bIndexes.getQuick(k)) {
                        ExprNode node = exprNodePool.next().of(ExprNode.OPERATION, constNameToToken.get(name), 0, 0);
                        node.lhs = jc.aNodes.getQuick(k);
                        node.rhs = constNameToNode.get(name);
                        node.paramCount = 2;
                        addWhereClause(parent, jc.slaveIndex, node);
                    }
                }
            }
        }
    }

    private void addWhereClause(QueryModel parent, int index, ExprNode filter) {
        QueryModel m = parent.getJoinModels().getQuick(index);
        m.setWhereClause(concatFilters(m.getWhereClause(), filter));
    }

    /**
     * Move fields that belong to slave journal to left and parent fields
     * to right of equals operator.
     */
    private void alignJoinClauses(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getContext();
            if (jc != null) {
                int index = jc.slaveIndex;
                for (int k = 0, kc = jc.aIndexes.size(); k < kc; k++) {
                    if (jc.aIndexes.getQuick(k) != index) {
                        int idx = jc.aIndexes.getQuick(k);
                        CharSequence name = jc.aNames.getQuick(k);
                        ExprNode node = jc.aNodes.getQuick(k);

                        jc.aIndexes.setQuick(k, jc.bIndexes.getQuick(k));
                        jc.aNames.setQuick(k, jc.bNames.getQuick(k));
                        jc.aNodes.setQuick(k, jc.bNodes.getQuick(k));

                        jc.bIndexes.setQuick(k, idx);
                        jc.bNames.setQuick(k, name);
                        jc.bNodes.setQuick(k, node);
                    }
                }
            }

        }
    }

    private RecordSource analyseAndCompileOrderBy(QueryModel model, RecordSource recordSource) throws ParserException {
        literalCollectorANames.clear();
        literalCollectorAIndexes.clear();
        literalCollector.withParent(model);
        literalCollector.resetNullCount();

        // create hash map keyed by column name for fast lookup access
        CharSequenceObjHashMap<QueryColumn> columnHashMap = new CharSequenceObjHashMap<>();
        ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn c = columns.getQuick(i);
            columnHashMap.put(c.getAlias() == null ? c.getName() : c.getAlias(), c);
        }

        ObjList<ExprNode> orderBy = model.getOrderBy();

        // determine where order by columns belong to
        innerVirtualColumn.clear();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            QueryColumn col = columnHashMap.get(orderBy.getQuick(i).token);
            if (col != null) {
                traversalAlgo.traverse(col.getAst(), literalCollector.lhs());
                if (col.getAst().type != ExprNode.LITERAL) {
                    innerVirtualColumn.add(col);
                }
            } else {
                traversalAlgo.traverse(orderBy.getQuick(i), literalCollector.lhs());
            }
        }

        // we interested in columns that belong to journal 0 only
        // abort if we find references to anywhere else
        boolean canUseOrderByHere = true;
        for (int i = 0, n = literalCollectorAIndexes.size(); i < n; i++) {
            if (literalCollectorAIndexes.getQuick(i) != 0) {
                canUseOrderByHere = false;
                break;
            }
        }

        // we may need to create virtual column record source
        // if order by refers to an expression
        if (canUseOrderByHere) {

            RecordSource rs = recordSource;
            if (innerVirtualColumn.size() > 0) {
                ObjList<VirtualColumn> virtualColumns = new ObjList<>();
                for (int i = 0, n = innerVirtualColumn.size(); i < n; i++) {
                    QueryColumn qc = innerVirtualColumn.getQuick(i);
                    VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), rs.getMetadata());
                    vc.setName(qc.getAlias());
                    virtualColumns.add(vc);
                    // make query column a literal to prevent another virtual column being created
                    // further down the pipeline
                    ExprNode expr = qc.getAst();
                    expr.type = ExprNode.LITERAL;
                    expr.token = qc.getAlias();
                    expr.args.clear();
                    expr.lhs = null;
                    expr.rhs = null;
                    expr.position = qc.getAliasPosition();
                    expr.paramCount = 0;
                }
                rs = new VirtualColumnRecordSource(rs, virtualColumns);
            }

            for (int i = 0, n = orderBy.size(); i < n; i++) {
                QueryColumn col = columnHashMap.get(orderBy.getQuick(i).token);
                if (col != null && col.getAst().type == ExprNode.LITERAL && col.getAlias() != null) {
                    orderBy.getQuick(i).token = col.getAst().token;
                }
            }

            rs = order(rs, model);
            model.getOrderBy().clear();
            return rs;
        }
        return recordSource;
    }

    private void analyseEquals(QueryModel parent, ExprNode node) throws ParserException {
        traverseNamesAndIndices(parent, node);

        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        JoinContext jc;

        switch (aSize) {
            case 0:
                if (bSize == 1
                        && literalCollector.nullCount == 0
                        // journal must not be OUTER or ASOF joined
                        && !joinBarriers.contains(parent.getJoinModels().get(literalCollectorBIndexes.getQuick(0)).getJoinType())) {
                    // single journal reference + constant
                    jc = contextPool.next();
                    jc.slaveIndex = literalCollectorBIndexes.getQuick(0);

                    addWhereClause(parent, jc.slaveIndex, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorBNames.getQuick(0);
                    constNameToIndex.put(cs, jc.slaveIndex);
                    constNameToNode.put(cs, node.lhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node);
                }
                break;
            case 1:
                jc = contextPool.next();
                int lhi = literalCollectorAIndexes.getQuick(0);
                if (bSize == 1) {
                    int rhi = literalCollectorBIndexes.getQuick(0);
                    if (lhi == rhi) {
                        // single journal reference
                        jc.slaveIndex = lhi;
                        addWhereClause(parent, lhi, node);
                    } else {
                        jc.aNodes.add(node.lhs);
                        jc.bNodes.add(node.rhs);
                        jc.aNames.add(literalCollectorANames.getQuick(0));
                        jc.bNames.add(literalCollectorBNames.getQuick(0));
                        jc.aIndexes.add(lhi);
                        jc.bIndexes.add(rhi);
                        int max = lhi > rhi ? lhi : rhi;
                        int min = lhi < rhi ? lhi : rhi;
                        jc.slaveIndex = max;
                        jc.parents.add(min);
                        linkDependencies(parent, min, max);
                    }
                    addJoinContext(parent, jc);
                } else if (bSize == 0
                        && literalCollector.nullCount == 0
                        && !joinBarriers.contains(parent.getJoinModels().get(literalCollectorAIndexes.getQuick(0)).getJoinType())) {
                    // single journal reference + constant
                    jc.slaveIndex = lhi;
                    addWhereClause(parent, lhi, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorANames.getQuick(0);
                    constNameToIndex.put(cs, lhi);
                    constNameToNode.put(cs, node.rhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node);
                }
                break;
            default:
                parent.addParsedWhereNode(node);
                break;
        }
    }

    private void analyseRegex(QueryModel parent, ExprNode node) throws ParserException {
        traverseNamesAndIndices(parent, node);

        if (literalCollector.nullCount == 0) {
            int aSize = literalCollectorAIndexes.size();
            int bSize = literalCollectorBIndexes.size();
            if (aSize == 1 && bSize == 0) {
                CharSequence name = literalCollectorANames.getQuick(0);
                constNameToIndex.put(name, literalCollectorAIndexes.getQuick(0));
                constNameToNode.put(name, node.rhs);
                constNameToToken.put(name, node.token);
            }
        }
    }

    private void applyLimit(QueryModel model) throws ParserException {
        // analyse limit first as it is easy win
        if (model.getLimitLo() != null || model.getLimitHi() != null) {
            ExprNode lo = model.getLimitLo();
            ExprNode hi = model.getLimitHi();
            if (hi == null) {
                model.setLimitVc(LONG_ZERO_CONST, limitToVirtualColumn(model, lo));
            } else {
                model.setLimitVc(limitToVirtualColumn(model, lo), limitToVirtualColumn(model, hi));
            }
        }
    }

    private void assignFilters(QueryModel parent) throws ParserException {

        journalsSoFar.clear();
        postFilterRemoved.clear();
        postFilterJournalRefs.clear();
        nullCounts.clear();

        literalCollector.withParent(parent);
        ObjList<ExprNode> filterNodes = parent.getParsedWhere();
        // collect journal indexes from each part of global filter
        int pc = filterNodes.size();
        for (int i = 0; i < pc; i++) {
            IntList indexes = intListPool.next();
            literalCollector.resetNullCount();
            traversalAlgo.traverse(filterNodes.getQuick(i), literalCollector.to(indexes));
            postFilterJournalRefs.add(indexes);
            nullCounts.add(literalCollector.nullCount);
        }

        IntList ordered = parent.getOrderedJoinModels();
        // match journal references to set of journals in join order
        for (int i = 0, n = ordered.size(); i < n; i++) {
            int index = ordered.getQuick(i);
            journalsSoFar.add(index);

            for (int k = 0; k < pc; k++) {
                if (postFilterRemoved.contains(k)) {
                    continue;
                }

                IntList refs = postFilterJournalRefs.getQuick(k);
                int rs = refs.size();
                if (rs == 0) {
                    // condition has no journal references
                    // must evaluate as constant
                    postFilterRemoved.add(k);
                    parent.addParsedWhereConst(k);
                } else if (rs == 1
                        && nullCounts.getQuick(k) == 0
                        // single journal reference and this journal is not joined via OUTER or ASOF
                        && !joinBarriers.contains(parent.getJoinModels().getQuick(refs.getQuick(0)).getJoinType())) {
                    // get single journal reference out of the way right away
                    // we don't have to wait until "our" journal comes along
                    addWhereClause(parent, refs.getQuick(0), filterNodes.getQuick(k));
                    postFilterRemoved.add(k);
                } else {
                    boolean qualifies = true;
                    // check if filter references journals processed so far
                    for (int y = 0; y < rs; y++) {
                        if (!journalsSoFar.contains(refs.getQuick(y))) {
                            qualifies = false;
                            break;
                        }
                    }
                    if (qualifies) {
                        postFilterRemoved.add(k);
                        QueryModel m = parent.getJoinModels().getQuick(index);
                        m.setPostJoinWhereClause(concatFilters(m.getPostJoinWhereClause(), filterNodes.getQuick(k)));
                    }
                }
            }
        }

        assert postFilterRemoved.size() == pc;
    }

    private RowSource buildRowSourceForInt(IntrinsicModel im) throws ParserException {
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexIntLookupRowSource(im.keyColumn, toInt(im.keyValues.getLast(), im.keyValuePositions.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexIntLookupRowSource(im.keyColumn, toInt(im.keyValues.get(0), im.keyValuePositions.getQuick(0)), true),
                        new KvIndexIntLookupRowSource(im.keyColumn, toInt(im.keyValues.get(1), im.keyValuePositions.getQuick(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexIntLookupRowSource(im.keyColumn, toInt(im.keyValues.get(i), im.keyValuePositions.getQuick(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private RowSource buildRowSourceForLong(IntrinsicModel im) throws ParserException {
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexLongLookupRowSource(im.keyColumn, toLong(im.keyValues.getLast(), im.keyValuePositions.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexLongLookupRowSource(im.keyColumn, toLong(im.keyValues.get(0), im.keyValuePositions.getQuick(0)), true),
                        new KvIndexLongLookupRowSource(im.keyColumn, toLong(im.keyValues.get(1), im.keyValuePositions.getQuick(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexLongLookupRowSource(im.keyColumn, toLong(im.keyValues.get(i), im.keyValuePositions.getQuick(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private RowSource buildRowSourceForStr(IntrinsicModel im) {
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexStrLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexStrLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(0)), true),
                        new KvIndexStrLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexStrLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private RowSource buildRowSourceForSym(IntrinsicModel im) {
        int nSrc = im.keyValues.size();
        switch (nSrc) {
            case 1:
                return new KvIndexSymLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.getLast()));
            case 2:
                return new MergingRowSource(
                        new KvIndexSymLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(0)), true),
                        new KvIndexSymLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(1)), true)
                );
            default:
                RowSource sources[] = new RowSource[nSrc];
                for (int i = 0; i < nSrc; i++) {
                    Unsafe.arrayPut(sources, i, new KvIndexSymLookupRowSource(im.keyColumn, Chars.toString(im.keyValues.get(i)), true));
                }
                return new HeapMergingRowSource(sources);
        }
    }

    private void clearState() {
        csPool.clear();
        exprNodePool.clear();
        contextPool.clear();
        intListPool.clear();
        joinClausesSwap1.clear();
        joinClausesSwap2.clear();
        queryFilterAnalyser.reset();
        constNameToIndex.clear();
        constNameToNode.clear();
        constNameToToken.clear();
    }

    private RecordSource compile(QueryModel model, Factory factory) throws ParserException {
        optimiseInvertedBooleans(model);
        optimiseOrderBy(model, ORDER_BY_UNKNOWN);
        optimiseSubQueries(model, factory);
        createOrderHash(model);
        return compileNoOptimise(model, factory);
    }

    private RecordSource compileAggregates(RecordSource rs, QueryModel model) throws ParserException {
        final int n = aggregators.size();
        final ExprNode sampleBy = model.getSampleBy();
        ObjList<AggregatorFunction> af = new ObjList<>(n);
        // create virtual columns
        for (int i = 0; i < n; i++) {
            QueryColumn qc = aggregators.get(i);
            VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), rs.getMetadata());
            if (vc instanceof AggregatorFunction) {
                vc.setName(qc.getAlias());
                af.add((AggregatorFunction) vc);
            } else {
                throw QueryError.$(qc.getAst().position, "Internal configuration error. Not an aggregate");
            }
        }

        RecordSource out;
        if (sampleBy == null) {
            out = new AggregatedRecordSource(rs, groupKeyColumns, af, configuration.getDbAggregatePage(), recordKeyCopierCompiler);
        } else {
            TimestampSampler sampler = SamplerFactory.from(sampleBy.token);
            if (sampler == null) {
                throw QueryError.$(sampleBy.position, "Invalid sample");
            }
            out = new ResampledRecordSource(rs,
                    getTimestampIndex(model, model.getTimestamp(), rs.getMetadata()),
                    groupKeyColumns,
                    af,
                    sampler,
                    configuration.getDbAggregatePage(),
                    recordKeyCopierCompiler);
        }
        return out;
    }

    private RecordSource compileAnalytic(RecordSource rs, QueryModel model) throws ParserException {
        final int n = analyticColumns.size();
        grouppedAnalytic.clear();
        final RecordMetadata metadata = rs.getMetadata();
        ObjList<AnalyticFunction> naturalOrderFunctions = null;
        boolean needCache = false;

        for (int i = 0; i < n; i++) {
            AnalyticColumn col = analyticColumns.getQuick(i);

            ObjList<VirtualColumn> partitionBy = null;
            int psz = col.getPartitionBy().size();
            if (psz > 0) {
                partitionBy = new ObjList<>(psz);
                for (int j = 0; j < psz; j++) {
                    partitionBy.add(virtualColumnBuilder.createVirtualColumn(model, col.getPartitionBy().getQuick(j), metadata));
                }
            }

            ExprNode ast = col.getAst();

            if (ast.paramCount > 1) {
                throw QueryError.$(col.getAst().position, "Too many arguments");
            }

            VirtualColumn valueColumn;
            if (ast.paramCount == 1) {
                valueColumn = virtualColumnBuilder.createVirtualColumn(model, col.getAst().rhs, metadata);
                valueColumn.setName(col.getAlias());
            } else {
                valueColumn = null;
            }

            final int osz = col.getOrderBy().size();
            AnalyticFunction f = AnalyticFunctionFactories.newInstance(
                    configuration,
                    ast.token,
                    valueColumn,
                    col.getAlias(),
                    partitionBy,
                    rs.supportsRowIdAccess(),
                    osz > 0
            );

            if (f == null) {
                Misc.free(rs);
                throw QueryError.$(col.getAst().position, "Unknown function");
            }
            CharSequenceIntHashMap orderHash = model.getOrderHash();

            boolean dismissOrder;
            if (osz > 0 && orderHash.size() > 0) {
                dismissOrder = true;
                for (int j = 0; j < osz; j++) {
                    ExprNode node = col.getOrderBy().getQuick(j);
                    int direction = col.getOrderByDirection().getQuick(j);
                    if (orderHash.get(node.token) != direction) {
                        dismissOrder = false;
                    }
                }
            } else {
                dismissOrder = false;
            }

            if (osz > 0 && !dismissOrder) {
                IntList order = toOrderIndices(metadata, col.getOrderBy(), col.getOrderByDirection());
                ObjList<AnalyticFunction> funcs = grouppedAnalytic.get(order);
                if (funcs == null) {
                    grouppedAnalytic.put(order, funcs = new ObjList<>());
                }
                funcs.add(f);
                needCache = true;
            } else {
                if (naturalOrderFunctions == null) {
                    naturalOrderFunctions = new ObjList<>();
                }
                needCache = needCache || f.getType() != AnalyticFunction.STREAM;
                naturalOrderFunctions.add(f);
            }
        }


        if (needCache) {
            final ObjList<RecordComparator> analyticComparators = new ObjList<>(grouppedAnalytic.size());
            final ObjList<ObjList<AnalyticFunction>> functionGroups = new ObjList<>(grouppedAnalytic.size());
            for (ObjObjHashMap.Entry<IntList, ObjList<AnalyticFunction>> e : grouppedAnalytic) {
                analyticComparators.add(cc.compile(metadata, e.key));
                functionGroups.add(e.value);
            }

            if (naturalOrderFunctions != null) {
                analyticComparators.add(null);
                functionGroups.add(naturalOrderFunctions);
            }

            if (rs.supportsRowIdAccess()) {
                return new CachedRowAnalyticRecordSource(
                        configuration.getDbAnalyticWindowPage(),
                        rs,
                        analyticComparators,
                        functionGroups);
            }

            return new CachedAnalyticRecordSource(
                    configuration.getDbAnalyticWindowPage(),
                    configuration.getDbSortKeyPage(),
                    rs,
                    analyticComparators,
                    functionGroups
            );
        } else {
            assert naturalOrderFunctions != null;
            return new AnalyticRecordSource(rs, naturalOrderFunctions);
        }
    }

    private RecordSource compileJoins(QueryModel model, Factory factory) throws ParserException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        IntList ordered = model.getOrderedJoinModels();
        RecordSource master = null;

        try {
            boolean needColumnNameHistogram = model.getColumns().size() > 0;

            model.getColumnNameHistogram().clear();

            for (int i = 0, n = ordered.size(); i < n; i++) {
                int index = ordered.getQuick(i);
                QueryModel m = joinModels.getQuick(index);

                // compile
                RecordSource slave = m.getRecordSource();

                if (slave == null) {
                    slave = compileJournal(m, factory);
                } else {
                    slave = filter(m, slave);
                }

                if (m.getAlias() != null) {
                    slave.getMetadata().setAlias(m.getAlias().token);
                }

                if (needColumnNameHistogram) {
                    model.createColumnNameHistogram(slave);
                }

                // check if this is the root of joins
                if (master == null) {
                    // This is an opportunistic check of order by clause
                    // to determine if we can get away ordering main record source only
                    // Ordering main record source could benefit from rowid access thus
                    // making it faster compared to ordering of join record source that
                    // doesn't allow rowid access.
                    master = analyseAndCompileOrderBy(model, slave);
                } else {
                    // not the root, join to "master"
                    switch (m.getJoinType()) {
                        case QueryModel.JOIN_CROSS:
                            // there are fields to analyse
                            master = new CrossJoinRecordSource(master, slave);
                            break;
                        case QueryModel.JOIN_ASOF:
                            master = createAsOfJoin(model.getTimestamp(), m, master, slave);
                            break;
                        default:
                            master = createHashJoin(m, master, slave);
                            break;
                    }
                }

                // check if there are post-filters
                ExprNode filter = m.getPostJoinWhereClause();
                if (filter != null) {
                    master = new FilteredRecordSource(master, virtualColumnBuilder.createVirtualColumn(model, filter, master.getMetadata()), filter);
                }
            }

            if (joinModelIsFalse(model)) {
                return new NoOpJournalRecordSource(master);
            }
            return master;
        } catch (ParserException e) {
            Misc.free(master);
            throw e;
        }
    }

    private RecordSource compileJournal(QueryModel model, Factory factory) throws ParserException {
        SystemViewFactory systemViewFactory = SysFactories.getFactory(model.getJournalName().token);
        if (systemViewFactory != null) {
            return compileSysView(model, factory, systemViewFactory);
        } else {
            return compileJournal0(model, factory);
        }
    }

    @SuppressWarnings("ConstantConditions")
    private RecordSource compileJournal0(QueryModel model, Factory factory) throws ParserException {

        applyLimit(model);

        RecordMetadata metadata = model.getMetadata();
        JournalMetadata journalMetadata;

        if (metadata == null) {
            metadata = model.collectJournalMetadata(factory);
        }

        if (metadata instanceof JournalMetadata) {
            journalMetadata = (JournalMetadata) metadata;
        } else {
            throw QueryError.$(0, "Internal error: invalid metadata");
        }

        if (model.getAlias() != null) {
            journalMetadata.setAlias(model.getAlias().token);
        } else {
            journalMetadata.setAlias(QueryModel.stripMarker(model.getJournalName().token));
        }

        PartitionSource ps = new JournalPartitionSource(journalMetadata, true);
        RowSource rs = null;

        String latestByCol = null;
        RecordColumnMetadata latestByMetadata = null;
        ExprNode latestByNode = null;

        if (model.getLatestBy() != null) {
            latestByNode = model.getLatestBy();
            if (latestByNode.type != ExprNode.LITERAL) {
                throw QueryError.$(latestByNode.position, "Column name expected");
            }

            latestByCol = model.translateAlias(latestByNode.token).toString();

            int colIndex = journalMetadata.getColumnIndexQuiet(latestByCol);
            if (colIndex == -1) {
                throw QueryError.invalidColumn(latestByNode.position, latestByNode.token);
            }

            latestByMetadata = journalMetadata.getColumnQuick(colIndex);

            int type = latestByMetadata.getType();
            if (type != ColumnType.SYMBOL && type != ColumnType.STRING && type != ColumnType.INT && type != ColumnType.LONG) {
                throw QueryError.position(latestByNode.position).$("Expected symbol, string, int or long column, found: ").$(ColumnType.nameOf(type)).$();
            }

            if (!latestByMetadata.isIndexed()) {
                throw QueryError.position(latestByNode.position).$("Column is not indexed").$();
            }
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = queryFilterAnalyser.extract(
                    model,
                    where,
                    journalMetadata,
                    latestByCol,
                    getTimestampIndexQuiet(model.getTimestamp(), journalMetadata)
            );

            VirtualColumn filter = im.filter != null ? virtualColumnBuilder.createVirtualColumn(model, im.filter, journalMetadata) : null;

            if (filter != null) {
                if (filter.getType() != ColumnType.BOOLEAN) {
                    throw QueryError.$(im.filter.position, "Boolean expression expected");
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
                ps = new NoOpJournalPartitionSource(journalMetadata);
            } else {

                if (im.intervals != null) {
                    ps = new MultiIntervalPartitionSource(ps, im.intervals);
                }

                if (latestByCol == null) {
                    if (im.keyColumn != null) {
                        switch (journalMetadata.getColumn(im.keyColumn).getType()) {
                            case ColumnType.SYMBOL:
                                rs = buildRowSourceForSym(im);
                                break;
                            case ColumnType.STRING:
                                rs = buildRowSourceForStr(im);
                                break;
                            case ColumnType.INT:
                                rs = buildRowSourceForInt(im);
                                break;
                            case ColumnType.LONG:
                                rs = buildRowSourceForLong(im);
                                break;
                            default:
                                break;
                        }
                    }

                    if (filter != null) {
                        rs = new FilteredRowSource(rs == null ? new AllRowSource() : rs, filter);
                    }
                } else {
                    if (im.keyColumn != null && im.keyValuesIsLambda) {
                        int lambdaColIndex;
                        RecordSource lambda = compileSourceInternal(factory, im.keyValues.get(0));
                        RecordMetadata m = lambda.getMetadata();

                        switch (m.getColumnCount()) {
                            case 0:
                                Misc.free(lambda);
                                throw QueryError.$(im.keyValuePositions.getQuick(0), "Query must select at least one column");
                            case 1:
                                lambdaColIndex = 0;
                                break;
                            default:
                                lambdaColIndex = m.getColumnIndexQuiet(latestByCol);
                                if (lambdaColIndex == -1) {
                                    Misc.free(lambda);
                                    throw QueryError.$(im.keyValuePositions.getQuick(0), "Ambiguous column names in lambda query. Specify select clause");
                                }
                                break;
                        }

                        int lambdaColType = m.getColumnQuick(lambdaColIndex).getType();
                        mutableSig.setParamCount(2).setName("").paramType(0, latestByMetadata.getType(), true).paramType(1, lambdaColType, false);
                        LatestByLambdaRowSourceFactory fact = LAMBDA_ROW_SOURCE_FACTORIES.get(mutableSig);
                        if (fact != null) {
                            rs = fact.newInstance(latestByCol, lambda, lambdaColIndex, filter);
                        } else {
                            Misc.free(lambda);
                            throw QueryError.$(im.keyValuePositions.getQuick(0), "Mismatched types");
                        }
                    } else {
                        switch (latestByMetadata.getType()) {
                            case ColumnType.SYMBOL:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexSymListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    rs = new KvIndexSymAllHeadRowSource(latestByCol, filter);
                                }
                                break;
                            case ColumnType.STRING:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexStrListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on string column expected");
                                }
                                break;
                            case ColumnType.LONG:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexLongListHeadRowSource(latestByCol, toLongHashSet(im), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on long column expected");
                                }
                                break;
                            case ColumnType.INT:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexIntListHeadRowSource(latestByCol, toIntHashSet(im), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on int column expected");
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        } else if (latestByCol != null) {
            switch (latestByMetadata.getType()) {
                case ColumnType.SYMBOL:
                    rs = new KvIndexSymAllHeadRowSource(latestByCol, null);
                    break;
                default:
                    Misc.free(rs);
                    throw QueryError.$(latestByNode.position, "Only SYM columns can be used here without filter");
            }
        }

        // check for case of simple "select count() from tab"
        if (rs == null && model.getColumns().size() == 1) {
            QueryColumn qc = model.getColumns().getQuick(0);
            if ("count".equals(qc.getAst().token) && qc.getAst().paramCount == 0) {
                // remove order clause
                model.getOrderBy().clear();
                // remove columns so that there is no wrapping of result source
                model.getColumns().clear();
                return new CountRecordSource(qc.getAlias() == null ? "count" : qc.getAlias(), ps);
            }
        }

        RecordSource recordSource = new JournalRecordSource(ps, rs == null ? new AllRowSource() : rs);
        if (QueryModel.hasMarker(model.getJournalName().token)) {
            return new NoRowIdRecordSource().of(recordSource);
        }
        return recordSource;
    }

    private RecordSource compileNoOptimise(QueryModel model, Factory factory) throws ParserException {
        RecordSource rs;
        try {
            if (model.getJoinModels().size() > 1) {
                optimiseJoins(model, factory);
                rs = compileJoins(model, factory);
            } else if (model.getJournalName() != null) {
                rs = compileJournal(model, factory);
            } else {
                rs = compileSubQuery(model, factory);
                QueryModel nm = model.getNestedModel();
                if (nm != null) {
                    nm.setRecordSource(rs);
                }
            }
            return limit(timestamp(order(selectColumns(rs, model), model), model), model);
        } catch (ParserException e) {
            freeModelRecordSources(model);
            throw e;
        }
    }

    private RecordSource compileOuterVirtualColumns(RecordSource rs, QueryModel model) throws ParserException {
        ObjList<VirtualColumn> outer = new ObjList<>(outerVirtualColumns.size());
        for (int i = 0, n = outerVirtualColumns.size(); i < n; i++) {
            QueryColumn qc = outerVirtualColumns.get(i);
            VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), rs.getMetadata());
            vc.setName(qc.getAlias());
            outer.add(vc);
        }
        return new VirtualColumnRecordSource(rs, outer);
    }

    private RecordSource compileSourceInternal(Factory factory, CharSequence query) throws ParserException {
        return compile((QueryModel) parser.parseInternal(query), factory);
    }

    private RecordSource compileSubQuery(QueryModel model, Factory factory) throws ParserException {
        applyLimit(model);
        return filter(model, compileNoOptimise(model.getNestedModel(), factory));
    }

    private RecordSource compileSysView(QueryModel model, Factory factory, SystemViewFactory systemViewFactory) throws ParserException {
        applyLimit(model);
        return filter(model, systemViewFactory.create(factory, env));
    }

    private ExprNode concatFilters(ExprNode old, ExprNode filter) {
        if (filter == null || filter == old) {
            return old;
        }

        if (old == null) {
            return filter;
        } else {
            ExprNode n = exprNodePool.next().of(ExprNode.OPERATION, "and", 0, 0);
            n.paramCount = 2;
            n.lhs = old;
            n.rhs = filter;
            return n;
        }
    }

    private void copy(Factory factory, RecordSource rs, JournalWriter w) throws JournalException {
        final int tsIndex = w.getMetadata().getTimestampIndex();
        RecordCursor cursor = rs.prepareCursor(factory);
        try {
            if (tsIndex == -1) {
                copyNonPartitioned(cursor, w, copyHelperCompiler.compile(rs.getMetadata(), w.getMetadata()));
            } else {
                copyPartitioned(cursor, w, copyHelperCompiler.compile(rs.getMetadata(), w.getMetadata()), tsIndex);
            }
            w.commit();
        } finally {
            cursor.releaseCursor();
        }
    }

    private void copyNonPartitioned(RecordCursor cursor, JournalWriter w, CopyHelper helper) throws JournalException {
        while (cursor.hasNext()) {
            Record r = cursor.next();
            JournalEntryWriter ew = w.entryWriter();
            helper.copy(r, ew);
            ew.append();
        }
    }

    private void copyPartitioned(RecordCursor cursor, JournalWriter w, CopyHelper helper, int tsIndex) throws JournalException {
        while (cursor.hasNext()) {
            Record r = cursor.next();
            JournalEntryWriter ew = w.entryWriter(r.getDate(tsIndex));
            helper.copy(r, ew);
            ew.append();
        }
    }

    private String createAlias(int index) {
        columnNameAssembly.clear(columnNamePrefixLen);
        Numbers.append(columnNameAssembly, index);
        return columnNameAssembly.toString();
    }

    private RecordSource createAsOfJoin(
            ExprNode masterTimestampNode,
            QueryModel model,
            RecordSource master,
            RecordSource slave) throws ParserException {
        JoinContext jc = model.getContext();

        ExprNode slaveTimestampNode = model.getTimestamp();
        RecordMetadata masterMetadata = master.getMetadata();
        RecordMetadata slaveMetadata = slave.getMetadata();
        int slaveTimestampIndex = getTimestampIndex(model, slaveTimestampNode, slaveMetadata);
        int masterTimestampIndex = getTimestampIndex(model, masterTimestampNode, masterMetadata);

        if (jc.bIndexes.size() == 0) {
//            if (jc == null) {
            return new AsOfJoinRecordSource(master, masterTimestampIndex, slave, slaveTimestampIndex);
        } else {
            int sz = jc.aNames.size();
            CharSequenceHashSet slaveKeys = new CharSequenceHashSet();
            CharSequenceHashSet masterKeys = new CharSequenceHashSet();

            for (int i = 0; i < sz; i++) {
                slaveKeys.add(jc.aNames.getQuick(i));
                masterKeys.add(jc.bNames.getQuick(i));
            }

            return new AsOfPartitionedJoinRecordSource(
                    master,
                    masterTimestampIndex,
                    slave,
                    slaveTimestampIndex,
                    masterKeys,
                    slaveKeys,
                    configuration.getDbAsOfDataPage(),
                    configuration.getDbAsOfIndexPage(),
                    configuration.getDbAsOfRowPage(),
                    recordKeyCopierCompiler

            );
        }
    }

    private RecordSource createHashJoin(QueryModel model, RecordSource master, RecordSource slave) throws ParserException {
        JoinContext jc = model.getContext();
        RecordMetadata bm = master.getMetadata();
        RecordMetadata am = slave.getMetadata();

        IntList masterColIndices = null;
        IntList slaveColIndices = null;

        for (int k = 0, kn = jc.aIndexes.size(); k < kn; k++) {

            CharSequence ca = jc.aNames.getQuick(k);
            CharSequence cb = jc.bNames.getQuick(k);

            int ia = am.getColumnIndex(ca);
            int ib = bm.getColumnIndex(cb);

            if (am.getColumnQuick(ia).getType() != bm.getColumnQuick(ib).getType()) {
                Misc.free(master);
                Misc.free(slave);
                throw QueryError.$(jc.aNodes.getQuick(k).position, "Column type mismatch");
            }

            if (masterColIndices == null) {
                // these go together, so checking one for null is enough
                masterColIndices = new IntList();
                slaveColIndices = new IntList();
            }

            masterColIndices.add(ib);
            slaveColIndices.add(ia);
        }
        return new HashJoinRecordSource(master,
                masterColIndices,
                slave,
                slaveColIndices,
                model.getJoinType() == QueryModel.JOIN_OUTER,
                configuration.getDbHashKeyPage(),
                configuration.getDbHashDataPage(),
                configuration.getDbHashRowPage(),
                new RecordKeyCopierCompiler(new BytecodeAssembler())
        );
    }

    /**
     * Creates dependencies via implied columns, typically timestamp.
     * Dependencies like that are not explicitly expressed in SQL query and
     * therefore are not created by analyzing "where" clause.
     * <p>
     * Explicit dependencies however are required for journal ordering.
     *
     * @param parent the parent model
     */
    private void createImpliedDependencies(QueryModel parent) {
        ObjList<QueryModel> models = parent.getJoinModels();
        JoinContext jc;
        for (int i = 0, n = models.size(); i < n; i++) {
            QueryModel m = models.getQuick(i);
            if (m.getJoinType() == QueryModel.JOIN_ASOF) {
                linkDependencies(parent, 0, i);
                if (m.getContext() == null) {
                    m.setContext(jc = contextPool.next());
                    jc.parents.add(0);
                    jc.slaveIndex = i;
                }
            }
        }
    }

    // order hash is used to determine redundant order by clauses
    private void createOrderHash(QueryModel model) {
        CharSequenceIntHashMap hash = model.getOrderHash();
        hash.clear();

        final ObjList<ExprNode> orderBy = model.getOrderBy();
        final int n = orderBy.size();
        final ObjList<QueryColumn> columns = model.getColumns();
        final int m = columns.size();
        final QueryModel nestedModel = model.getNestedModel();

        if (n > 0) {
            final IntList orderByDirection = model.getOrderByDirection();
            for (int i = 0; i < n; i++) {
                hash.put(orderBy.getQuick(i).token, orderByDirection.getQuick(i));
            }
        } else if (nestedModel != null && m > 0) {
            createOrderHash(nestedModel);
            CharSequenceIntHashMap thatHash = nestedModel.getOrderHash();
            if (thatHash.size() > 0) {
                for (int i = 0; i < m; i++) {
                    QueryColumn column = columns.getQuick(i);
                    ExprNode node = column.getAst();
                    if (node.type == ExprNode.LITERAL) {
                        int direction = thatHash.get(node.token);
                        if (direction != -1) {
                            hash.put(column.getName(), direction);
                        }
                    }
                }
            }
        }
    }

    private JournalStructure createStructure(String location, RecordMetadata rm, CharSequenceObjHashMap<ColumnCastModel> castModels) throws ParserException {
        int n = rm.getColumnCount();
        ObjList<ColumnMetadata> m = new ObjList<>(n);
        for (int i = 0; i < n; i++) {
            ColumnMetadata cm = new ColumnMetadata();
            RecordColumnMetadata im = rm.getColumnQuick(i);
            cm.name = im.getName();

            int srcType = im.getType();
            ColumnCastModel castModel = castModels.get(cm.name);
            if (castModel != null) {
                validateTypeCastCompatibility(srcType, castModel);
                cm.type = castModel.getColumnType();
                if (cm.type == ColumnType.SYMBOL) {
                    cm.distinctCountHint = Numbers.ceilPow2(castModel.getCount()) - 1;
                }
            } else {
                cm.type = srcType;
            }

            switch (cm.type) {
                case ColumnType.STRING:
                    cm.size = cm.avgSize + 4;
                    break;
                default:
                    cm.size = ColumnType.sizeOf(cm.type);
                    break;
            }
            m.add(cm);
        }
        return new JournalStructure(location, m).$ts(rm.getTimestampIndex());
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : csPool.next().of(token, dot + 1, token.length() - dot - 1);
    }

    private RecordSource filter(QueryModel model, RecordSource rs) throws ParserException {
        try {
            if (model.getWhereClause() == null) {
                return rs;
            }

            RecordMetadata m = rs.getMetadata();
            if (model.getAlias() != null) {
                m.setAlias(model.getAlias().token);
            }

            int timestampIndex = getTimestampIndexQuiet(model.getTimestamp(), m);
            IntrinsicModel im = queryFilterAnalyser.extract(model, model.getWhereClause(), m, null, timestampIndex);

            if (im.intrinsicValue == IntrinsicValue.FALSE) {
                return new NoOpJournalRecordSource(rs);
            }

            if (im.intervals != null) {
                rs = new IntervalRecordSource(rs, im.intervals, timestampIndex);
            }

            if (im.filter != null) {
                VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, im.filter, m);
                if (vc.isConstant()) {
                    // todo: not hit by test
                    if (vc.getBool(null)) {
                        return rs;
                    } else {
                        return new NoOpJournalRecordSource(rs);
                    }
                }
                return new FilteredRecordSource(rs, vc, im.filter);
            } else {
                return rs;
            }
        } catch (ParserException e) {
            Misc.free(rs);
            throw e;
        }
    }

    private void freeModelRecordSources(QueryModel model) {
        if (model == null) {
            return;
        }
        Misc.free(model.getRecordSource());
        freeModelRecordSources(model.getNestedModel());

        ObjList<QueryModel> joinModels = model.getJoinModels();
        int n = joinModels.size();
        if (n > 1) {
            for (int i = 1; i < n; i++) {
                freeModelRecordSources(joinModels.getQuick(i));
            }
        }
    }

    private int getTimestampIndex(QueryModel model, ExprNode node, RecordMetadata m) throws ParserException {
        int index = getTimestampIndexQuiet(node, m);
        int pos = model.getJournalName() != null ? model.getJournalName().position : 0;
        switch (index) {
            case -1:
                throw QueryError.position(pos).$("Missing timestamp").$();
            case -2:
                throw QueryError.position(pos).$("Ambiguous timestamp column").$();
            default:
                return index;
        }
    }

    private int getTimestampIndexQuiet(ExprNode node, RecordMetadata m) throws ParserException {
        if (node != null) {
            if (node.type != ExprNode.LITERAL) {
                throw QueryError.position(node.position).$("Literal expression expected").$();
            }

            int index = m.getColumnIndexQuiet(node.token);
            if (index == -1) {
                throw QueryError.position(node.position).$("Invalid column: ").$(node.token).$();
            }
            return index;
        } else {
            return m.getTimestampIndex();
//            int index = m.getTimestampIndex();
//            if (index > -1) {
//                return index;
//            }
//
//            for (int i = 0, n = m.getColumnCount(); i < n; i++) {
//                if (m.getColumnQuick(i).getType() == ColumnType.DATE) {
//                    if (index == -1) {
//                        index = i;
//                    } else {
//                        return -2;
//                    }
//                }
//            }
//            return index;
        }
    }

    private boolean hasAggregates(ExprNode node) {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case ExprNode.LITERAL:
                        node = null;
                        continue;
                    case ExprNode.FUNCTION:
                        if (FunctionFactories.isAggregate(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        if (node.rhs != null) {
                            this.exprNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = this.exprNodeStack.poll();
            }
        }
        return false;
    }

    private void homogenizeCrossJoins(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            JoinContext c = m.getContext();

            if (m.getJoinType() == QueryModel.JOIN_CROSS) {
                if (c != null && c.parents.size() > 0) {
                    m.setJoinType(QueryModel.JOIN_INNER);
                }
            } else if (m.getJoinType() != QueryModel.JOIN_ASOF && (c == null || c.parents.size() == 0)) {
                m.setJoinType(QueryModel.JOIN_CROSS);
            }
        }
    }

    private boolean joinModelIsFalse(QueryModel model) throws ParserException {
        ExprNode current = null;
        IntHashSet constants = model.getParsedWhereConsts();
        ObjList<ExprNode> whereNodes = model.getParsedWhere();

        for (int i = 0, n = constants.size(); i < n; i++) {
            current = concatFilters(current, whereNodes.getQuick(constants.get(i)));
        }

        if (current == null) {
            return false;
        }

        VirtualColumn col = virtualColumnBuilder.createVirtualColumn(model, current, null);
        if (col.isConstant()) {
            if (col.getType() != ColumnType.BOOLEAN) {
                throw QueryError.$(current.position, "Boolean expression expected");
            }
            return !col.getBool(null);
        } else {
            throw QueryError.$(0, "Internal error: expected constant");
        }
    }

    private RecordSource limit(RecordSource rs, QueryModel model) {
        if (model.getLimitLoVc() == null || model.getLimitHiVc() == null) {
            return rs;
        } else {
            return new TopRecordSource(rs, model.getLimitLoVc(), model.getLimitHiVc());
        }
    }

    private VirtualColumn limitToVirtualColumn(QueryModel model, ExprNode node) throws ParserException {
        switch (node.type) {
            case ExprNode.LITERAL:
                if (Chars.startsWith(node.token, ':')) {
                    return Parameter.getOrCreate(node, model.getParameterMap());
                }
                break;
            case ExprNode.CONSTANT:
                try {
                    return new LongConstant(Numbers.parseLong(node.token), node.position);
                } catch (NumericException e) {
                    // todo: not hit by test
                    throw QueryError.$(node.position, "Long number expected");
                }
            default:
                break;
        }
        // todo: not hit by test
        throw QueryError.$(node.position, "Constant expected");
    }

    private void linkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).addDependency(child);
    }

    private JoinContext mergeContexts(QueryModel parent, JoinContext a, JoinContext b) {
        assert a.slaveIndex == b.slaveIndex;

        deletedContexts.clear();
        JoinContext r = contextPool.next();
        // check if we merging a.x = b.x to a.y = b.y
        // or a.x = b.x to a.x = b.y, e.g. one of columns in the same
        for (int i = 0, n = b.aNames.size(); i < n; i++) {

            CharSequence ban = b.aNames.getQuick(i);
            int bai = b.aIndexes.getQuick(i);
            ExprNode bao = b.aNodes.getQuick(i);

            CharSequence bbn = b.bNames.getQuick(i);
            int bbi = b.bIndexes.getQuick(i);
            ExprNode bbo = b.bNodes.getQuick(i);

            for (int k = 0, z = a.aNames.size(); k < z; k++) {

                if (deletedContexts.contains(k)) {
                    // todo: not hit by test
                    continue;
                }

                CharSequence aan = a.aNames.getQuick(k);
                int aai = a.aIndexes.getQuick(k);
                ExprNode aao = a.aNodes.getQuick(k);
                CharSequence abn = a.bNames.getQuick(k);
                int abi = a.bIndexes.getQuick(k);
                ExprNode abo = a.bNodes.getQuick(k);

                if (aai == bai && Chars.equals(aan, ban)) {
                    // a.x = ?.x
                    //  |     ?
                    // a.x = ?.y
                    addFilterOrEmitJoin(parent, k, abi, abn, abo, bbi, bbn, bbo);
                    break;
                } else if (abi == bai && Chars.equals(abn, ban)) {
                    // a.y = b.x
                    //    /
                    // b.x = a.x
                    addFilterOrEmitJoin(parent, k, aai, aan, aao, bbi, bbn, bbo);
                    break;
                } else if (aai == bbi && Chars.equals(aan, bbn)) {
                    // a.x = b.x
                    //     \
                    // b.y = a.x
                    addFilterOrEmitJoin(parent, k, abi, abn, abo, bai, ban, bao);
                    break;
                } else if (abi == bbi && Chars.equals(abn, bbn)) {
                    // a.x = b.x
                    //        |
                    // a.y = b.x
                    addFilterOrEmitJoin(parent, k, aai, aan, aao, bai, ban, bao);
                    break;
                }
            }
            r.aIndexes.add(bai);
            r.aNames.add(ban);
            r.aNodes.add(bao);
            r.bIndexes.add(bbi);
            r.bNames.add(bbn);
            r.bNodes.add(bbo);
            int max = bai > bbi ? bai : bbi;
            int min = bai < bbi ? bai : bbi;
            r.slaveIndex = max;
            r.parents.add(min);
            linkDependencies(parent, min, max);
        }

        // add remaining a nodes
        for (int i = 0, n = a.aNames.size(); i < n; i++) {
            int aai, abi, min, max;

            aai = a.aIndexes.getQuick(i);
            abi = a.bIndexes.getQuick(i);

            if (aai < abi) {
                min = aai;
                max = abi;
            } else {
                min = abi;
                max = aai;
            }

            if (deletedContexts.contains(i)) {
                if (!r.parents.contains(min)) {
                    unlinkDependencies(parent, min, max);
                }
            } else {
                r.aNames.add(a.aNames.getQuick(i));
                r.bNames.add(a.bNames.getQuick(i));
                r.aIndexes.add(aai);
                r.bIndexes.add(abi);
                r.aNodes.add(a.aNodes.getQuick(i));
                r.bNodes.add(a.bNodes.getQuick(i));

                r.parents.add(min);
                linkDependencies(parent, min, max);
            }
        }

        return r;
    }

    private JoinContext moveClauses(QueryModel parent, JoinContext from, JoinContext to, IntList positions) {
        int p = 0;
        int m = positions.size();

        if (m == 0) {
            // todo: not hit by test
            return from;
        }

        JoinContext result = contextPool.next();
        result.slaveIndex = from.slaveIndex;

        for (int i = 0, n = from.aIndexes.size(); i < n; i++) {
            // logically those clauses we move away from "from" context
            // should not longer exist in "from", but instead of implementing
            // "delete" function, which would be manipulating underlying array
            // on every invocation, we copy retained clauses to new context,
            // which is "result".
            // hence whenever exists in "positions" we copy clause to "to"
            // otherwise copy to "result"
            JoinContext t = p < m && i == positions.getQuick(p) ? to : result;
            int ai = from.aIndexes.getQuick(i);
            int bi = from.bIndexes.getQuick(i);
            t.aIndexes.add(ai);
            t.aNames.add(from.aNames.getQuick(i));
            t.aNodes.add(from.aNodes.getQuick(i));
            t.bIndexes.add(bi);
            t.bNames.add(from.bNames.getQuick(i));
            t.bNodes.add(from.bNodes.getQuick(i));

            // either ai or bi is definitely belongs to this context
            if (ai != t.slaveIndex) {
                t.parents.add(ai);
                linkDependencies(parent, ai, bi);
            } else {
                t.parents.add(bi);
                linkDependencies(parent, bi, ai);
            }
        }

        return result;
    }

    ExprNode optimiseInvertedBooleans(final ExprNode node, boolean reverse) throws ParserException {
        switch (node.token) {
            case "not":
                if (reverse) {
                    return optimiseInvertedBooleans(node.rhs, false);
                } else {
                    switch (node.rhs.type) {
                        case ExprNode.LITERAL:
                        case ExprNode.CONSTANT:
                            return node;
                        default:
                            assertNotNull(node.rhs, node.position, "Missing right argument");
                            return optimiseInvertedBooleans(node.rhs, true);
                    }
                }
            case "and":
                if (reverse) {
                    node.token = "or";
                }
                assertNotNull(node.lhs, node.position, "Missing left argument");
                assertNotNull(node.rhs, node.position, "Missing right argument");
                node.lhs = optimiseInvertedBooleans(node.lhs, reverse);
                node.rhs = optimiseInvertedBooleans(node.rhs, reverse);
                return node;
            case "or":
                if (reverse) {
                    node.token = "and";
                }
                assertNotNull(node.lhs, node.position, "Missing left argument");
                assertNotNull(node.rhs, node.position, "Missing right argument");
                node.lhs = optimiseInvertedBooleans(node.lhs, reverse);
                node.rhs = optimiseInvertedBooleans(node.rhs, reverse);
                return node;
            case ">":
                if (reverse) {
                    node.token = "<=";
                }
                return node;
            case ">=":
                if (reverse) {
                    node.token = "<";
                }
                return node;

            case "<":
                if (reverse) {
                    node.token = ">=";
                }
                return node;
            case "<=":
                if (reverse) {
                    node.token = ">";
                }
                return node;
            case "=":
                if (reverse) {
                    node.token = "!=";
                }
                return node;
            case "!=":
                if (reverse) {
                    node.token = "=";
                }
                return node;
            default:
                if (reverse) {
                    ExprNode n = exprNodePool.next();
                    n.token = "not";
                    n.paramCount = 1;
                    n.rhs = node;
                    n.type = ExprNode.OPERATION;
                    return n;
                }
                return node;
        }
    }

    private void optimiseInvertedBooleans(QueryModel model) throws ParserException {
        ExprNode where = model.getWhereClause();
        if (where != null) {
            model.setWhereClause(optimiseInvertedBooleans(where, false));
        }

        if (model.getNestedModel() != null) {
            optimiseInvertedBooleans(model.getNestedModel());
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            if (m != model) {
                optimiseInvertedBooleans(joinModels.getQuick(i));
            }
        }
    }

    private void optimiseJoins(QueryModel parent, Factory factory) throws ParserException {
        ObjList<QueryModel> joinModels = parent.getJoinModels();

        int n = joinModels.size();
        if (n > 1) {

            // create metadata for all journals and sub-queries involved in this query
            for (int i = 0; i < n; i++) {
                resolveJoinMetadata(parent, i, factory);
            }

            emittedJoinClauses = joinClausesSwap1;

            // for sake of clarity, "parent" model is the first in the list of
            // joinModels, e.g. joinModels.get(0) == parent
            // only parent model is allowed to have "where" clause
            // so we can assume that "where" clauses of joinModel elements are all null (except for element 0).
            // in case one of joinModels is subquery, its entire query model will be set as
            // nestedModel, e.g. "where" clause is still null there as well

            ExprNode where = parent.getWhereClause();

            // clear where clause of parent so that
            // optimiser can assign there correct nodes

            parent.setWhereClause(null);
            processJoinConditions(parent, where);

            for (int i = 1; i < n; i++) {
                processJoinConditions(parent, joinModels.getQuick(i).getJoinCriteria());
            }

            if (emittedJoinClauses.size() > 0) {
                processEmittedJoinClauses(parent);
            }

            createImpliedDependencies(parent);
            reorderJournals(parent);
            homogenizeCrossJoins(parent);
            assignFilters(parent);
            alignJoinClauses(parent);
            addTransitiveFilters(parent);
        }
    }

    private void optimiseOrderBy(QueryModel model, int orderByState) {
        ObjList<QueryColumn> columns = model.getColumns();
        int subQueryOrderByState;

        int n = columns.size();
        // determine if ordering is required
        switch (orderByState) {
            case ORDER_BY_UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                subQueryOrderByState = ORDER_BY_REQUIRED;
                if (model.getSampleBy() == null) {
                    for (int i = 0; i < n; i++) {
                        QueryColumn col = columns.getQuick(i);
                        if (hasAggregates(col.getAst())) {
                            subQueryOrderByState = ORDER_BY_INVARIANT;
                            break;
                        }
                    }
                }
                break;
            case ORDER_BY_REQUIRED:
                // parent requires order
                // if this model forces ordering - sub-query ordering is not needed
                if (model.getOrderBy().size() > 0) {
                    subQueryOrderByState = ORDER_BY_INVARIANT;
                } else {
                    subQueryOrderByState = ORDER_BY_REQUIRED;
                }
                break;
            default:
                // sub-query ordering is not needed
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    // todo: not hit by test
                    subQueryOrderByState = ORDER_BY_REQUIRED;
                } else {
                    subQueryOrderByState = ORDER_BY_INVARIANT;
                }
                break;
        }

        ObjList<QueryModel> jm = model.getJoinModels();
        for (int i = 0, k = jm.size(); i < k; i++) {
            QueryModel qm = jm.getQuick(i).getNestedModel();
            if (qm != null) {
                optimiseOrderBy(qm, subQueryOrderByState);
            }
        }
    }

    /**
     * Objective of this method is to move filters inside of sub-queries where
     * possible. This should reduce data flow into heavier algos such as
     * sorting and hashing. Method achieves this by analysing columns returned by
     * sub-queries and matching them to components of "where" clause.
     * <p/>
     *
     * @param model   query model to analyse
     * @param factory factory is used to collect journal columns.
     * @throws ParserException usually syntax exceptions
     */
    private void optimiseSubQueries(QueryModel model, Factory factory) throws ParserException {
        QueryModel nm;
        ObjList<QueryModel> jm = model.getJoinModels();
        ObjList<ExprNode> where = model.parseWhereClause();
        ExprNode thisWhere = null;

        // create name histograms
        for (int i = 0, n = jm.size(); i < n; i++) {
            if ((nm = jm.getQuick(i).getNestedModel()) != null) {
                nm.createColumnNameHistogram(factory);
            }
        }

        // match each of where conditions to join models
        // if "where" matches two models, we have ambiguous column name
        for (int j = 0, k = where.size(); j < k; j++) {
            ExprNode node = where.getQuick(j);
            int matchModel = -1;
            for (int i = 0, n = jm.size(); i < n; i++) {
                QueryModel qm = jm.getQuick(i);
                nm = qm.getNestedModel();
                if (nm != null && literalMatcher.matches(node, nm.getColumnNameHistogram(), qm.getAlias() != null ? qm.getAlias().token : null)) {
                    if (matchModel > -1) {
                        throw QueryError.ambiguousColumn(node.position);
                    }
                    matchModel = i;
                }
            }

            if (matchModel > -1) {
                nm = jm.getQuick(matchModel).getNestedModel();
                nm.setWhereClause(concatFilters(nm.getWhereClause(), node));
            } else {
                thisWhere = concatFilters(thisWhere, node);
            }
        }

        model.getParsedWhere().clear();
        model.setWhereClause(thisWhere);

        // recursively apply same logic to nested model of each of join model
        for (int i = 0, n = jm.size(); i < n; i++) {
            QueryModel qm = jm.getQuick(i);
            nm = qm.getNestedModel();
            if (nm != null) {
                optimiseSubQueries(nm, factory);
            }
        }
    }

    private RecordSource order(RecordSource rs, QueryModel model) throws ParserException {
        ObjList<ExprNode> orderBy = model.getOrderBy();
        if (orderBy.size() > 0) {
            try {
                RecordMetadata m = rs.getMetadata();
                return new RBTreeSortedRecordSource(rs,
                        cc.compile(
                                m,
                                toOrderIndices(m, orderBy, model.getOrderByDirection())
                        ),
                        configuration.getDbSortKeyPage(),
                        configuration.getDbSortDataPage());
            } catch (ParserException e) {
                Misc.free(rs);
                throw e;
            }
        } else {
            return rs;
        }
    }

    // todo: remove
    CharSequence plan(Factory factory, CharSequence query) throws ParserException {
        QueryModel model = (QueryModel) parser.parse(query);
        resetAndOptimise(model, factory);
        return model.plan();
    }

    private void processEmittedJoinClauses(QueryModel model) {
        // process emitted join conditions
        do {
            ObjList<JoinContext> clauses = emittedJoinClauses;

            if (clauses == joinClausesSwap1) {
                emittedJoinClauses = joinClausesSwap2;
            } else {
                // todo: not hit by test
                emittedJoinClauses = joinClausesSwap1;
            }
            emittedJoinClauses.clear();
            for (int i = 0, k = clauses.size(); i < k; i++) {
                addJoinContext(model, clauses.getQuick(i));
            }
        } while (emittedJoinClauses.size() > 0);

    }

    /**
     * Splits "where" clauses into "and" concatenated list of boolean expressions.
     *
     * @param node expression n
     */
    private void processJoinConditions(QueryModel parent, ExprNode node) throws ParserException {
        ExprNode n = node;
        // pre-order traversal
        exprNodeStack.clear();
        while (!exprNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (n.token) {
                    case "and":
                        if (n.rhs != null) {
                            exprNodeStack.push(n.rhs);
                        }
                        n = n.lhs;
                        break;
                    case "=":
                        analyseEquals(parent, n);
                        n = null;
                        break;
                    case "or":
                        processOrConditions(parent, n);
                        n = null;
                        break;
                    case "~":
                        analyseRegex(parent, n);
                        // intentional fallthrough
                    default:
                        parent.addParsedWhereNode(n);
                        n = null;
                        break;
                }
            } else {
                n = exprNodeStack.poll();
            }
        }
    }

    /**
     * There are two ways "or" conditions can go:
     * - all "or" conditions have at least one fields in common
     * e.g. a.x = b.x or a.x = b.y
     * this can be implemented as a hash join where master table is "b"
     * and slave table is "a" keyed on "a.x" so that
     * if HashTable contains all rows of "a" keyed on "a.x"
     * hash join algorithm can do:
     * rows = HashTable.get(b.x);
     * if (rows == null) {
     * rows = HashTable.get(b.y);
     * }
     * <p>
     * in this case tables can be reordered as long as "b" is processed
     * before "a"
     * <p>
     * - second possibility is where all "or" conditions are random
     * in which case query like this:
     * <p>
     * from a
     * join c on a.x = c.x
     * join b on a.x = b.x or c.y = b.y
     * <p>
     * can be rewritten to:
     * <p>
     * from a
     * join c on a.x = c.x
     * join b on a.x = b.x
     * union
     * from a
     * join c on a.x = c.x
     * join b on c.y = b.y
     */
    private void processOrConditions(QueryModel parent, ExprNode node) {
        // stub: use filter
        parent.addParsedWhereNode(node);
    }

    private void renameJournal(Factory factory, RenameJournalModel model) throws ParserException {

        String from = Chars.stripQuotes(model.getFrom().token);
        String to = Chars.stripQuotes(model.getTo().token);

        try {
            factory.rename(from, to);
        } catch (JournalException e) {
            throw QueryError.position(model.getFrom().position).$(e.getMessage()).$();
        }

    }

    /**
     * Identify joined journals without join clause and try to find other reversible join clauses
     * that may be applied to it. For example when these journals joined"
     * <p>
     * from a
     * join b on c.x = b.x
     * join c on c.y = a.y
     * <p>
     * the system that prefers child table with lowest index will attribute c.x = b.x clause to
     * journal "c" leaving "b" without clauses.
     */
    @SuppressWarnings({"StatementWithEmptyBody", "ConstantConditions"})
    private void reorderJournals(QueryModel parent) throws ParserException {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        int n = joinModels.size();

        tempCrosses.clear();
        // collect crosses
        for (int i = 0; i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getContext() == null || q.getContext().parents.size() == 0) {
                tempCrosses.add(i);
            }
        }

        int cost = Integer.MAX_VALUE;
        int root = -1;

        // analyse state of tree for each set of n-1 crosses
        for (int z = 0, zc = tempCrosses.size(); z < zc; z++) {
            for (int i = 0; i < zc; i++) {
                if (z != i) {
                    int to = tempCrosses.getQuick(i);
                    JoinContext jc = joinModels.getQuick(to).getContext();
                    // look above i up to OUTER join
                    for (int k = i - 1; k > -1 && swapJoinOrder(parent, to, k, jc); k--) ;
                    // look below i for up to OUTER join
                    for (int k = i + 1; k < n && swapJoinOrder(parent, to, k, jc); k++) ;
                }
            }

            IntList ordered = parent.nextOrderedJoinModels();
            int thisCost = reorderJournals0(parent, ordered);
            if (thisCost < cost) {
                root = z;
                cost = thisCost;
                parent.setOrderedJoinModels(ordered);
            }
        }

        if (root == -1) {
            // todo: not hit by test
            throw QueryError.$(0, "Cycle");
        }
    }

    private int reorderJournals0(QueryModel parent, IntList ordered) {
        tempCrossIndexes.clear();
        ordered.clear();
        this.orderingStack.clear();
        ObjList<QueryModel> joinModels = parent.getJoinModels();

        int cost = 0;

        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getJoinType() == QueryModel.JOIN_CROSS || q.getContext() == null || q.getContext().parents.size() == 0) {
                if (q.getDependencies().size() > 0) {
                    orderingStack.push(i);
                } else {
                    tempCrossIndexes.add(i);
                }
            } else {
                q.getContext().inCount = q.getContext().parents.size();
            }
        }

        while (orderingStack.notEmpty()) {
            //remove a node n from orderingStack
            int index = orderingStack.pop();

            //insert n into orderedJournals
            ordered.add(index);

            QueryModel m = joinModels.getQuick(index);

            switch (m.getJoinType()) {
                case QueryModel.JOIN_CROSS:
                    // todo: not hit by test
                    cost += 10;
                    break;
                default:
                    cost += 5;
                    break;
            }

            IntHashSet dependencies = m.getDependencies();

            //for each node m with an edge e from n to m do
            for (int i = 0, k = dependencies.size(); i < k; i++) {
                int depIndex = dependencies.get(i);
                JoinContext jc = joinModels.getQuick(depIndex).getContext();
                if (--jc.inCount == 0) {
                    orderingStack.push(depIndex);
                }
            }
        }

        //Check to see if all edges are removed
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            if (m.getContext() != null && m.getContext().inCount > 0) {
                return Integer.MAX_VALUE;
            }
        }

        // add pure crosses at end of ordered journal list
        for (int i = 0, n = tempCrossIndexes.size(); i < n; i++) {
            ordered.add(tempCrossIndexes.getQuick(i));
        }

        return cost;
    }

    private ExprNode replaceIfAggregate(@Transient ExprNode node, ObjList<QueryColumn> aggregateColumns) {
        if (node != null && FunctionFactories.isAggregate(node.token)) {
            QueryColumn c = aggregateColumnPool.next().of(createAlias(aggregateColumnSequence++), node.position, node);
            aggregateColumns.add(c);
            return exprNodePool.next().of(ExprNode.LITERAL, c.getAlias(), 0, 0);
        }
        return node;
    }

    private void resetAndOptimise(QueryModel model, Factory factory) throws ParserException {
        clearState();
        optimiseJoins(model, factory);
    }

    private void resolveJoinMetadata(QueryModel parent, int index, Factory factory) throws ParserException {
        QueryModel model = parent.getJoinModels().getQuick(index);
        RecordMetadata metadata;
        if (model.getJournalName() != null) {
            model.setMetadata(metadata = model.collectJournalMetadata(factory));
        } else {
            RecordSource rs = compileNoOptimise(model.getNestedModel(), factory);
            model.setMetadata(metadata = rs.getMetadata());
            model.setRecordSource(rs);
        }

        if (model.getAlias() != null) {
            if (!parent.addAliasIndex(model.getAlias(), index)) {
                throw QueryError.$(model.getAlias().position, "Duplicate alias");
            }
            metadata.setAlias(model.getAlias().token);
        } else if (model.getJournalName() != null) {
            parent.addAliasIndex(model.getJournalName(), index);
            metadata.setAlias(model.getJournalName().token);
        }
    }

    private int resolveJournalIndex(QueryModel parent, @Transient CharSequence alias, CharSequence col, int position) throws ParserException {
        CharSequence column = parent.translateAlias(col);
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        int index = -1;
        if (alias == null) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                RecordMetadata m = joinModels.getQuick(i).getMetadata();
                if (m.getColumnIndexQuiet(column) == -1) {
                    continue;
                }

                if (index > -1) {
                    throw QueryError.ambiguousColumn(position);
                }

                index = i;
            }

            if (index == -1) {
                throw QueryError.invalidColumn(position, column);
            }

            return index;
        } else {
            index = parent.getAliasIndex(alias);

            if (index == -1) {
                throw QueryError.$(position, "Invalid journal name/alias");
            }
            RecordMetadata m = joinModels.getQuick(index).getMetadata();

            if (m.getColumnIndexQuiet(column) == -1) {
                // todo: not hit by test
                throw QueryError.invalidColumn(position, column);
            }

            return index;
        }
    }

    private RecordSource selectColumns(RecordSource rs, QueryModel model) throws ParserException {
        return model.getColumns().size() == 0 ? rs : selectColumns0(rs, model);
    }

    private RecordSource selectColumns0(final RecordSource recordSource, QueryModel model) throws ParserException {
        final ObjList<QueryColumn> columns = model.getColumns();
        final CharSequenceIntHashMap columnNameHistogram = model.getColumnNameHistogram();
        final RecordMetadata meta = recordSource.getMetadata();

        this.outerVirtualColumns.clear();
        this.innerVirtualColumn.clear();
        this.aggregators.clear();
        this.selectedColumns.clear();
        this.selectedColumnAliases.clear();
        this.groupKeyColumns.clear();
        this.aggregateColumnSequence = 0;
        this.analyticColumns.clear();

        RecordSource rs = recordSource;

        try {
            // create virtual columns from select list
            for (int i = 0, k = columns.size(); i < k; i++) {
                final QueryColumn qc = columns.getQuick(i);
                final ExprNode node = qc.getAst();
                final boolean analytic = qc instanceof AnalyticColumn;

                if (!analytic && node.type == ExprNode.LITERAL) {
                    // check literal column validity
                    if (meta.getColumnIndexQuiet(node.token) == -1) {
                        throw QueryError.invalidColumn(node.position, node.token);
                    }

                    // check if this column is exists in more than one journal/result set
                    if (columnNameHistogram.get(node.token) > 0) {
                        throw QueryError.ambiguousColumn(node.position);
                    }

                    // add to selected columns
                    selectedColumns.add(node.token);
                    addAlias(node.position, qc.getName());
                    groupKeyColumns.add(node.token);
                    continue;
                }

                // generate missing alias for everything else
                if (qc.getAlias() == null) {
                    qc.of(createAlias(aggregateColumnSequence++), node.position, node);
                }

                selectedColumns.add(qc.getAlias());
                addAlias(node.position, qc.getAlias());

                // outright aggregate
                if (!analytic && node.type == ExprNode.FUNCTION && FunctionFactories.isAggregate(node.token)) {
                    aggregators.add(qc);
                    continue;
                }

                // check if this expression references aggregate function
                if (node.type == ExprNode.OPERATION || node.type == ExprNode.FUNCTION) {
                    int beforeSplit = aggregators.size();
                    splitAggregates(node, aggregators);
                    if (beforeSplit < aggregators.size()) {
                        outerVirtualColumns.add(qc);
                        continue;
                    }
                }


                if (analytic) {
                    if (qc.getAst().type != ExprNode.FUNCTION) {
                        // todo: not hit by test
                        throw QueryError.$(qc.getAst().position, "Analytic function expected");
                    }

                    if (aggregators.size() > 0) {
                        throw QueryError.$(qc.getAst().position, "Analytic function is not allowed in context of aggregation. Use sub-query.");
                    }

                    AnalyticColumn ac = (AnalyticColumn) qc;
                    analyticColumns.add(ac);
                } else {
                    // this is either a constant or non-aggregate expression
                    // either case is a virtual column
                    innerVirtualColumn.add(qc);
                }
            }


            // if virtual columns are present, create record source to calculate them
            if (innerVirtualColumn.size() > 0) {
                // this is either a constant or non-aggregate expression
                // either case is a virtual column
                ObjList<VirtualColumn> virtualColumns = new ObjList<>();

                final String timestampName;
                if (model.getSampleBy() != null) {
                    timestampName = meta.getColumnName(getTimestampIndex(model, model.getTimestamp(), meta));
                } else {
                    timestampName = null;
                }

                for (int i = 0, n = innerVirtualColumn.size(); i < n; i++) {
                    QueryColumn qc = innerVirtualColumn.getQuick(i);
                    if (Chars.equalsNc(qc.getAlias(), timestampName)) {
                        throw QueryError.$(qc.getAliasPosition() == -1 ? qc.getAst().position : qc.getAliasPosition(), "Alias clashes with implicit sample column name");
                    }

                    VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), meta);
                    vc.setName(qc.getAlias());
                    virtualColumns.add(vc);
                    groupKeyColumns.add(qc.getAlias());
                }
                rs = new VirtualColumnRecordSource(rs, virtualColumns);
            }

            // if aggregators present, wrap record source into group-by source
            if (aggregators.size() > 0) {
                rs = compileAggregates(rs, model);
            } else {
                ExprNode sampleBy = model.getSampleBy();
                if (sampleBy != null) {
                    throw QueryError.$(sampleBy.position, "There are no aggregation columns");
                }
            }

            if (outerVirtualColumns.size() > 0) {
                rs = compileOuterVirtualColumns(rs, model);
            }

            if (analyticColumns.size() > 0) {
                rs = compileAnalytic(rs, model);
            }

            if (selectedColumns.size() > 0) {
                // wrap underlying record source into selected columns source.
                rs = new SelectedColumnsRecordSource(rs, selectedColumns, selectedColumnAliases);
            }
            return rs;
        } catch (ParserException e) {
            Misc.free(rs);
            throw e;
        }
    }

    private void splitAggregates(@Transient ExprNode node, ObjList<QueryColumn> aggregateColumns) {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    ExprNode n = replaceIfAggregate(node.rhs, aggregateColumns);
                    if (node.rhs == n) {
                        this.exprNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExprNode n = replaceIfAggregate(node.lhs, aggregateColumns);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = this.exprNodeStack.poll();
            }
        }
    }

    /**
     * Moves reversible join clauses, such as a.x = b.x from journal "from" to journal "to".
     *
     * @param to      target journal index
     * @param from    source journal index
     * @param context context of target journal index
     * @return false if "from" is outer joined journal, otherwise - true
     */
    private boolean swapJoinOrder(QueryModel parent, int to, int from, final JoinContext context) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        QueryModel jm = joinModels.getQuick(from);
        if (joinBarriers.contains(jm.getJoinType())) {
            return false;
        }

        JoinContext jc = context;
        clausesToSteal.clear();

        JoinContext that = jm.getContext();
        if (that != null && that.parents.contains(to)) {
            int zc = that.aIndexes.size();
            for (int z = 0; z < zc; z++) {
                if (that.aIndexes.getQuick(z) == to || that.bIndexes.getQuick(z) == to) {
                    clausesToSteal.add(z);
                }
            }

            if (clausesToSteal.size() < zc) {
                QueryModel target = joinModels.getQuick(to);
                target.getDependencies().clear();
                if (jc == null) {
                    target.setContext(jc = contextPool.next());
                }
                jc.slaveIndex = to;
                jm.setContext(moveClauses(parent, that, jc, clausesToSteal));
                if (target.getJoinType() == QueryModel.JOIN_CROSS) {
                    target.setJoinType(QueryModel.JOIN_INNER);
                }
            }
        }
        return true;
    }

    private RecordSource timestamp(RecordSource rs, QueryModel model) throws ParserException {
        try {
            ExprNode timestamp = model.getTimestamp();
            if (timestamp == null) {
                return rs;
            }

            int index = rs.getMetadata().getColumnIndexQuiet(timestamp.token);
            if (index == -1) {
                throw QueryError.invalidColumn(timestamp.position, timestamp.token);
            }

            return new TimestampRelocatingRecordSource(rs, index);
        } catch (ParserException e) {
            Misc.free(rs);
            throw e;
        }
    }

    private int toInt(CharSequence cs, int pos) throws ParserException {
        try {
            return Numbers.parseInt(cs);
        } catch (NumericException e) {
            // todo: not hit by test
            throw QueryError.$(pos, "int value expected");
        }
    }

    private long toLong(CharSequence cs, int pos) throws ParserException {
        try {
            return Numbers.parseLong(cs);
        } catch (NumericException e) {
            // todo: not hit by test
            throw QueryError.$(pos, "long value expected");
        }
    }

    private IntList toOrderIndices(RecordMetadata m, ObjList<ExprNode> orderBy, IntList orderByDirection) throws ParserException {
        final IntList indices = intListPool.next();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExprNode tok = orderBy.getQuick(i);
            int index = m.getColumnIndexQuiet(tok.token);
            if (index == -1) {
                throw QueryError.invalidColumn(tok.position, tok.token);
            }

            // shift index by 1 to use sign as sort direction
            index++;

            // negative column index means descending order of sort
            if (orderByDirection.getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                index = -index;
            }

            indices.add(index);
        }
        return indices;
    }

    private void traverseNamesAndIndices(QueryModel parent, ExprNode node) throws ParserException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withParent(parent);
        literalCollector.resetNullCount();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());
    }

    private void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    private void validateTypeCastCompatibility(int srcType, ColumnCastModel castModel) throws ParserException {
        int dstType = castModel.getColumnType();
        boolean incompatible;

        switch (srcType) {
            case ColumnType.INT:
            case ColumnType.BYTE:
            case ColumnType.DATE:
            case ColumnType.SHORT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                switch (dstType) {
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                    case ColumnType.BINARY:
                    case ColumnType.BOOLEAN:
                        incompatible = true;
                        break;
                    default:
                        incompatible = false;
                        break;
                }
                break;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                switch (dstType) {
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        incompatible = false;
                        break;
                    default:
                        // todo: not hit by test
                        incompatible = true;
                        break;
                }
                break;
            default:
                incompatible = true;
        }

        if (incompatible) {
            throw QueryError
                    .position(castModel.getColumnTypePos())
                    .$("Incompatible cast: ")
                    .$(ColumnType.nameOf(srcType))
                    .$(" as ")
                    .$(ColumnType.nameOf(dstType))
                    .$();
        }
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private IntList indexes;
        private ObjList<CharSequence> names;
        private int nullCount;
        private QueryModel parent;

        @Override
        public void visit(ExprNode node) throws ParserException {
            switch (node.type) {
                case ExprNode.LITERAL:
                    int dot = node.token.indexOf('.');
                    CharSequence name = extractColumnName(node.token, dot);
                    indexes.add(resolveJournalIndex(parent, dot == -1 ? null : csPool.next().of(node.token, 0, dot), name, node.position));
                    if (names != null) {
                        names.add(name);
                    }
                    break;
                case ExprNode.CONSTANT:
                    if (nullConstants.contains(node.token)) {
                        nullCount++;
                    }
                    break;
                default:
                    break;
            }
        }

        private PostOrderTreeTraversalAlgo.Visitor lhs() {
            indexes = literalCollectorAIndexes;
            names = literalCollectorANames;
            return this;
        }

        private void resetNullCount() {
            nullCount = 0;
        }

        private PostOrderTreeTraversalAlgo.Visitor rhs() {
            indexes = literalCollectorBIndexes;
            names = literalCollectorBNames;
            return this;
        }

        private PostOrderTreeTraversalAlgo.Visitor to(IntList indexes) {
            this.indexes = indexes;
            this.names = null;
            return this;
        }

        private void withParent(QueryModel parent) {
            this.parent = parent;
        }
    }

    static {
        joinBarriers = new IntHashSet();
        joinBarriers.add(QueryModel.JOIN_OUTER);
        joinBarriers.add(QueryModel.JOIN_ASOF);
    }

    static {
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.SYMBOL, ColumnType.SYMBOL), KvIndexSymSymLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.SYMBOL, ColumnType.STRING), KvIndexSymStrLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.INT, ColumnType.INT), KvIndexIntLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.STRING, ColumnType.STRING), KvIndexStrStrLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.STRING, ColumnType.SYMBOL), KvIndexStrSymLambdaHeadRowSource.FACTORY);
    }

    static {
        nullConstants.add("null");
        nullConstants.add("NaN");
    }
}
