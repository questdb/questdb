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
 ******************************************************************************/

package com.questdb.ql.parser;

import com.questdb.ex.JournalException;
import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Chars;
import com.questdb.misc.Interval;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.*;
import com.questdb.ql.impl.*;
import com.questdb.ql.impl.aggregation.*;
import com.questdb.ql.impl.analytic.*;
import com.questdb.ql.impl.interval.IntervalRecordSource;
import com.questdb.ql.impl.interval.MultiIntervalPartitionSource;
import com.questdb.ql.impl.interval.SingleIntervalSource;
import com.questdb.ql.impl.join.AsOfJoinRecordSource;
import com.questdb.ql.impl.join.AsOfPartitionedJoinRecordSource;
import com.questdb.ql.impl.join.CrossJoinRecordSource;
import com.questdb.ql.impl.join.HashJoinRecordSource;
import com.questdb.ql.impl.lambda.*;
import com.questdb.ql.impl.latest.*;
import com.questdb.ql.impl.select.SelectedColumnsRecordSource;
import com.questdb.ql.impl.sort.ComparatorCompiler;
import com.questdb.ql.impl.sort.RBTreeSortedRecordSource;
import com.questdb.ql.impl.virtual.VirtualColumnRecordSource;
import com.questdb.ql.model.*;
import com.questdb.ql.ops.FunctionFactories;
import com.questdb.ql.ops.Parameter;
import com.questdb.ql.ops.Signature;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.constant.LongConstant;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;

@SuppressFBWarnings("LEST_LOST_EXCEPTION_STACK_TRACE")
public class QueryCompiler {

    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private final static ObjObjHashMap<Signature, LatestByLambdaRowSourceFactory> LAMBDA_ROW_SOURCE_FACTORIES = new ObjObjHashMap<>();
    private final static LongConstant LONG_ZERO_CONST = new LongConstant(0L);
    private final static IntHashSet joinBarriers;
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
    private final VirtualColumnBuilder virtualColumnBuilder = new VirtualColumnBuilder(traversalAlgo);
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
    private final ObjList<CharSequence> postFilterThrowawayNames = new ObjList<>();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(new ObjectFactory<IntList>() {
        @Override
        public IntList newInstance() {
            return new IntList();
        }
    }, 16);
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
    private final ObjList<AnalyticColumn> analyticColumns = new ObjList<>();
    private final ObjHashSet<String> groupKeyColumns = new ObjHashSet<>();
    private final ComparatorCompiler cc = new ComparatorCompiler();
    private final LiteralMatcher literalMatcher = new LiteralMatcher(traversalAlgo);
    private final ServerConfiguration configuration;

    private ObjList<JoinContext> emittedJoinClauses;
    private int aggregateColumnSequence;

    public QueryCompiler(ServerConfiguration configuration) {
        // seed column name assembly with default column prefix, which we will reuse
        this.configuration = configuration;
        columnNameAssembly.put("col");
        columnNamePrefixLen = 3;
    }

    public <T> RecordCursor compile(JournalReaderFactory factory, Class<T> clazz) throws JournalException, ParserException {
        return compile(factory, clazz.getName());
    }

    public RecordCursor compile(JournalReaderFactory factory, CharSequence query) throws ParserException, JournalException {
        return compileSource(factory, query).prepareCursor(factory, NoOpCancellationHandler.INSTANCE);
    }

    public RecordSource compileSource(JournalReaderFactory factory, CharSequence query) throws ParserException, JournalException {
        clearState();
        final QueryModel model = parser.parse(query).getQueryModel();
        final CharSequenceObjHashMap<Parameter> map = new CharSequenceObjHashMap<>();
        model.setParameterMap(map);
        RecordSource rs = compile(model, factory);
        rs.setParameterMap(map);
        return rs;
    }

    private static Signature lbs(ColumnType master, boolean indexed, ColumnType lambda) {
        return new Signature().setName("").setParamCount(2).paramType(0, master, indexed).paramType(1, lambda, false);
    }

    private void addAlias(int position, String alias) throws ParserException {
        if (selectedColumnAliases.add(alias)) {
            return;
        }
        throw QueryError.$(position, "Duplicate alias");
    }

    private void addFilterOrEmitJoin(QueryModel parent, int idx, int ai, CharSequence an, ExprNode ao, int bi, CharSequence bn, ExprNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(idx);
            return;
        }

        if (ai == bi) {
            // (same journal)
            ExprNode node = exprNodePool.next().of(ExprNode.NodeType.OPERATION, "=", 0, 0);
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

        deletedContexts.add(idx);
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
                        ExprNode node = exprNodePool.next().of(ExprNode.NodeType.OPERATION, constNameToToken.get(name), 0, 0);
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

    @SuppressFBWarnings("PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS")
    private void analyseEquals(QueryModel parent, ExprNode node) throws ParserException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withParent(parent);
        literalCollector.resetNullCount();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());

        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        JoinContext jc;

        switch (aSize) {
            case 0:
                if (bSize == 1
                        && literalCollector.nullCount == 0
                        // journal must not be OUTER or ASOF joined
                        && !joinBarriers.contains(parent.getJoinModels().get(literalCollectorBIndexes.getQuick(0)).getJoinType().ordinal())) {
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
                        && !joinBarriers.contains(parent.getJoinModels().get(literalCollectorAIndexes.getQuick(0)).getJoinType().ordinal())) {
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
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withParent(parent);
        literalCollector.resetNullCount();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());

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

    @SuppressFBWarnings("PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS")
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
            traversalAlgo.traverse(filterNodes.getQuick(i), literalCollector.to(indexes, postFilterThrowawayNames));
            postFilterJournalRefs.add(indexes);
            nullCounts.add(literalCollector.nullCount);
            postFilterThrowawayNames.clear();
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
                        && !joinBarriers.contains(parent.getJoinModels().getQuick(refs.getQuick(0)).getJoinType().ordinal())) {
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

    private RecordSource compile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        optimiseOrderBy(model, OrderByState.UNKNOWN);
        optimiseSubQueries(model, factory);
        return compileNoOptimise(model, factory);
    }

    private RecordSource compileJoins(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        IntList ordered = model.getOrderedJoinModels();
        RecordSource master = null;

        boolean needColumnNameHistogram = model.getColumns().size() > 0;

        for (int i = 0, n = ordered.size(); i < n; i++) {
            int index = ordered.getQuick(i);
            QueryModel m = joinModels.getQuick(index);

            // compile
            RecordSource slave = m.getRecordSource();

            if (slave == null) {
                // subquery would have been compiled already
                slave = compileJournal(m, factory);
                if (m.getAlias() != null) {
                    slave.getMetadata().setAlias(m.getAlias().token);
                }
            }

            if (needColumnNameHistogram) {
                model.createColumnNameHistogram(slave);
            }

            // check if this is the root of joins
            if (master == null) {
                master = slave;
            } else {
                // not the root, join to "master"
                switch (m.getJoinType()) {
                    case CROSS:
                        // there are fields to analyse
                        master = new CrossJoinRecordSource(master, slave);
                        break;
                    case ASOF:
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
                master = new FilteredJournalRecordSource(master, virtualColumnBuilder.createVirtualColumn(model, filter, master.getMetadata()), filter);
            }
        }

        if (joinModelIsFalse(model)) {
            return new NoOpJournalRecordSource(master);
        }
        return master;
    }

    @SuppressWarnings("ConstantConditions")
    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY"})
    private RecordSource compileJournal(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        applyLimit(model);

        RecordMetadata metadata = model.getMetadata();
        JournalMetadata journalMetadata;

        if (metadata == null) {
            journalMetadata = model.collectJournalMetadata(factory);
        } else if (metadata instanceof JournalMetadata) {
            journalMetadata = (JournalMetadata) metadata;
        } else {
            throw QueryError.$(0, "Internal error: invalid metadata");
        }

        if (model.getAlias() != null) {
            journalMetadata.setAlias(model.getAlias().token);
        } else {
            journalMetadata.setAlias(model.getJournalName().token);
        }

        PartitionSource ps = new JournalPartitionSource(journalMetadata, true);
        RowSource rs = null;

        String latestByCol = null;
        RecordColumnMetadata latestByMetadata = null;
        ExprNode latestByNode = null;

        if (model.getLatestBy() != null) {
            latestByNode = model.getLatestBy();
            if (latestByNode.type != ExprNode.NodeType.LITERAL) {
                throw QueryError.$(latestByNode.position, "Column name expected");
            }

            int colIndex = journalMetadata.getColumnIndexQuiet(latestByNode.token);
            if (colIndex == -1) {
                throw QueryError.invalidColumn(latestByNode.position, latestByNode.token);
            }

            latestByMetadata = journalMetadata.getColumnQuick(colIndex);

            ColumnType type = latestByMetadata.getType();
            if (type != ColumnType.SYMBOL && type != ColumnType.STRING && type != ColumnType.INT) {
                throw QueryError.position(latestByNode.position).$("Expected symbol or string column, found: ").$(type).$();
            }

            if (!latestByMetadata.isIndexed()) {
                throw QueryError.position(latestByNode.position).$("Column is not indexed").$();
            }

            latestByCol = latestByNode.token;
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = queryFilterAnalyser.extract(where, journalMetadata, latestByCol);

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
                        switch (journalMetadata.getColumn(im.keyColumn).getType()) {
                            case SYMBOL:
                                rs = buildRowSourceForSym(im);
                                break;
                            case STRING:
                                rs = buildRowSourceForStr(im);
                                break;
                            case INT:
                                rs = buildRowSourceForInt(im);
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
                                throw QueryError.$(im.keyValuePositions.getQuick(0), "Query must select at least one column");
                            case 1:
                                lambdaColIndex = 0;
                                break;
                            default:
                                lambdaColIndex = m.getColumnIndexQuiet(latestByCol);
                                if (lambdaColIndex == -1) {
                                    throw QueryError.$(im.keyValuePositions.getQuick(0), "Ambiguous column names in lambda query. Specify select clause");
                                }
                                break;
                        }

                        ColumnType lambdaColType = m.getColumn(lambdaColIndex).getType();
                        mutableSig.setParamCount(2).setName("").paramType(0, latestByMetadata.getType(), true).paramType(1, lambdaColType, false);
                        LatestByLambdaRowSourceFactory fact = LAMBDA_ROW_SOURCE_FACTORIES.get(mutableSig);
                        if (fact != null) {
                            rs = fact.newInstance(latestByCol, lambda, lambdaColIndex, filter);
                        } else {
                            throw QueryError.$(im.keyValuePositions.getQuick(0), "Mismatched types");
                        }
                    } else {
                        switch (latestByMetadata.getType()) {
                            case SYMBOL:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexSymListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    rs = new KvIndexSymAllHeadRowSource(latestByCol, filter);
                                }
                                break;
                            case STRING:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexStrListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    throw QueryError.$(latestByNode.position, "Filter on string column expected");
                                }
                                break;
                            case INT:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexIntListHeadRowSource(latestByCol, toIntHashSet(im), filter);
                                } else {
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
                case SYMBOL:
                    rs = new KvIndexSymAllHeadRowSource(latestByCol, null);
                    break;
                default:
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
        return new JournalSource(ps, rs == null ? new AllRowSource() : rs);
    }

    private RecordSource compileNoOptimise(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        RecordSource rs;
        if (model.getJoinModels().size() > 1) {
            optimiseJoins(model, factory);
            rs = compileJoins(model, factory);
        } else if (model.getJournalName() != null) {
            rs = compileJournal(model, factory);
        } else {
            rs = compileSubQuery(model, factory);
        }

        return limit(order(selectColumns(rs, model), model), model);
    }

    private RecordSource compileSourceInternal(JournalReaderFactory factory, CharSequence query) throws ParserException, JournalException {
        return compile(parser.parseInternal(query).getQueryModel(), factory);
    }

    private RecordSource compileSubQuery(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        applyLimit(model);

        RecordSource rs = compileNoOptimise(model.getNestedModel(), factory);
        if (model.getWhereClause() == null) {
            return rs;
        }

        RecordMetadata m = rs.getMetadata();
        if (model.getAlias() != null) {
            m.setAlias(model.getAlias().token);
        }

        IntrinsicModel im = queryFilterAnalyser.extract(model.getWhereClause(), m, null);

        switch (im.intrinsicValue) {
            case FALSE:
                return new NoOpJournalRecordSource(rs);
            default:
                if (im.intervalSource != null) {
                    rs = new IntervalRecordSource(rs, im.intervalSource);
                }
                if (im.filter != null) {
                    VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, im.filter, m);
                    if (vc.isConstant()) {
                        if (vc.getBool(null)) {
                            return rs;
                        } else {
                            return new NoOpJournalRecordSource(rs);
                        }
                    }
                    return new FilteredJournalRecordSource(rs, vc, im.filter);
                } else {
                    return rs;
                }
        }

    }

    private ExprNode concatFilters(ExprNode old, ExprNode filter) {
        if (filter == null || filter == old) {
            return old;
        }

        if (old == null) {
            return filter;
        } else {
            ExprNode n = exprNodePool.next().of(ExprNode.NodeType.OPERATION, "and", 0, 0);
            n.paramCount = 2;
            n.lhs = old;
            n.rhs = filter;
            return n;
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

        if (jc == null) {
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
                    configuration.getDbAsOfRowPage());
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
                model.getJoinType() == QueryModel.JoinType.OUTER,
                configuration.getDbHashKeyPage(),
                configuration.getDbHashDataPage(),
                configuration.getDbHashRowPage()
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
            if (m.getJoinType() == QueryModel.JoinType.ASOF) {
                linkDependencies(parent, 0, i);
                if (m.getContext() == null) {
                    m.setContext(jc = contextPool.next());
                    jc.parents.add(0);
                    jc.slaveIndex = i;
                }
            }
        }
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : csPool.next().of(token, dot + 1, token.length() - dot - 1);
    }

    private int getTimestampIndex(QueryModel model, ExprNode node, RecordMetadata m) throws ParserException {
        int pos = model.getJournalName() != null ? model.getJournalName().position : 0;
        if (node != null) {
            if (node.type != ExprNode.NodeType.LITERAL) {
                throw QueryError.position(node.position).$("Literal expression expected").$();
            }

            int index = m.getColumnIndexQuiet(node.token);
            if (index == -1) {
                throw QueryError.position(node.position).$("Invalid column: ").$(node.token).$();
            }
            return index;
        } else {
            int index = m.getTimestampIndex();
            if (index > -1) {
                return index;
            }

            for (int i = 0, n = m.getColumnCount(); i < n; i++) {
                if (m.getColumnQuick(i).getType() == ColumnType.DATE) {
                    if (index == -1) {
                        index = i;
                    } else {
                        throw QueryError.position(pos).$("Ambiguous timestamp column").$();
                    }
                }
            }

            if (index == -1) {
                throw QueryError.position(pos).$("Missing timestamp").$();
            }
            return index;
        }
    }

    private boolean hasAggregates(ExprNode node) {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case LITERAL:
                        node = null;
                        continue;
                    case FUNCTION:
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

            if (m.getJoinType() == QueryModel.JoinType.CROSS) {
                if (c != null && c.parents.size() > 0) {
                    m.setJoinType(QueryModel.JoinType.INNER);
                }
            } else if (m.getJoinType() != QueryModel.JoinType.ASOF && (c == null || c.parents.size() == 0)) {
                m.setJoinType(QueryModel.JoinType.CROSS);
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
            case LITERAL:
                if (Chars.startsWith(node.token, ':')) {
                    return Parameter.getOrCreate(node, model.getParameterMap());
                }
                break;
            case CONSTANT:
                try {
                    return new LongConstant(Numbers.parseLong(node.token));
                } catch (NumericException e) {
                    throw QueryError.$(node.position, "Long number expected");
                }
            default:
                break;
        }
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

    private void optimiseJoins(QueryModel parent, JournalReaderFactory factory) throws JournalException, ParserException {
        ObjList<QueryModel> joinModels = parent.getJoinModels();

        // todo: remove check, this method must not be called on single journal SQLs
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

    private void optimiseOrderBy(QueryModel model, OrderByState state) {
        ObjList<QueryColumn> columns = model.getColumns();
        OrderByState subQueryState;

        int n = columns.size();
        // determine if ordering is required
        switch (state) {
            case UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                subQueryState = OrderByState.NEED_ORDERED;
                if (model.getSampleBy() == null) {
                    for (int i = 0; i < n; i++) {
                        QueryColumn col = columns.getQuick(i);
                        if (hasAggregates(col.getAst())) {
                            subQueryState = OrderByState.ORDER_INVARIANT;
                            break;
                        }
                    }
                }
                break;
            case NEED_ORDERED:
                // parent requires order
                // if this model forces ordering - sub-query ordering is not needed
                if (model.getOrderBy().size() > 0) {
                    subQueryState = OrderByState.ORDER_INVARIANT;
                } else {
                    subQueryState = OrderByState.NEED_ORDERED;
                }
                break;
            default:
                // sub-query ordering is not needed
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    subQueryState = OrderByState.NEED_ORDERED;
                } else {
                    subQueryState = OrderByState.ORDER_INVARIANT;
                }
                break;
        }

        ObjList<QueryModel> jm = model.getJoinModels();
        for (int i = 0, k = jm.size(); i < k; i++) {
            QueryModel qm = jm.getQuick(i).getNestedModel();
            if (qm != null) {
                optimiseOrderBy(qm, subQueryState);
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
     * @throws JournalException
     * @throws ParserException
     */
    private void optimiseSubQueries(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
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
        int n = orderBy.size();
        if (n > 0) {
            IntList orderByDirection = model.getOrderByDirection();
            IntList indices = model.getOrderColumnIndices();
            RecordMetadata m = rs.getMetadata();
            for (int i = 0; i < n; i++) {
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
            return new RBTreeSortedRecordSource(rs,
                    cc.compile(RBTreeSortedRecordSource.class, m, indices),
                    configuration.getDbSortKeyPage(),
                    configuration.getDbSortDataPage());
        } else {
            return rs;
        }
    }

    // todo: remove
    CharSequence plan(JournalReaderFactory factory, CharSequence query) throws ParserException, JournalException {
        QueryModel model = parser.parse(query).getQueryModel();
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
     * @throws ParserException
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
            if (q.getJoinType() == QueryModel.JoinType.CROSS || q.getContext() == null || q.getContext().parents.size() == 0) {
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
                case CROSS:
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
            QueryColumn c = aggregateColumnPool.next().of(createAlias(aggregateColumnSequence++), node);
            aggregateColumns.add(c);
            return exprNodePool.next().of(ExprNode.NodeType.LITERAL, c.getAlias(), 0, 0);
        }
        return node;
    }

    private void resetAndOptimise(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        clearState();
        optimiseJoins(model, factory);
    }

    private void resolveJoinMetadata(QueryModel parent, int index, JournalReaderFactory factory) throws JournalException, ParserException {
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

    private int resolveJournalIndex(QueryModel parent, @Transient CharSequence alias, CharSequence column, int position) throws ParserException {
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
                throw QueryError.invalidColumn(position, column);
            }

            return index;
        }
    }

    private RecordSource selectColumns(RecordSource rs, QueryModel model) throws ParserException {
        return model.getColumns().size() == 0 ? rs : selectColumns0(rs, model);
    }

    @SuppressFBWarnings("PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS")
    private RecordSource selectColumns0(final RecordSource recordSource, QueryModel model) throws ParserException {
        final ObjList<QueryColumn> columns = model.getColumns();
        final CharSequenceIntHashMap columnNameHistogram = model.getColumnNameHistogram();
        final RecordMetadata meta = recordSource.getMetadata();

        this.outerVirtualColumns.clear();
        this.aggregators.clear();
        this.selectedColumns.clear();
        this.selectedColumnAliases.clear();
        this.groupKeyColumns.clear();
        this.aggregateColumnSequence = 0;
        this.analyticColumns.clear();

        ObjList<VirtualColumn> virtualColumns = null;

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final ExprNode node = qc.getAst();
            final boolean analytic = qc instanceof AnalyticColumn;

            if (!analytic && node.type == ExprNode.NodeType.LITERAL) {
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
                addAlias(node.position, qc.getAlias() == null ? node.token : qc.getAlias());
                groupKeyColumns.add(node.token);
                continue;
            }

            // generate missing alias for everything else
            if (qc.getAlias() == null) {
                qc.of(createAlias(aggregateColumnSequence++), node);
            }

            selectedColumns.add(qc.getAlias());
            addAlias(node.position, qc.getAlias());

            // outright aggregate
            if (!analytic && node.type == ExprNode.NodeType.FUNCTION && FunctionFactories.isAggregate(node.token)) {
                aggregators.add(qc);
                continue;
            }

            // check if this expression references aggregate function
            if (node.type == ExprNode.NodeType.OPERATION || node.type == ExprNode.NodeType.FUNCTION) {
                int beforeSplit = aggregators.size();
                splitAggregates(node, aggregators);
                if (beforeSplit < aggregators.size()) {
                    outerVirtualColumns.add(qc);
                    continue;
                }
            }

            // this is either a constant or non-aggregate expression
            // either case is a virtual column
            if (virtualColumns == null) {
                virtualColumns = new ObjList<>();
            }

            if (analytic) {
                if (qc.getAst().type != ExprNode.NodeType.FUNCTION) {
                    throw QueryError.$(qc.getAst().position, "Analytic function expected");
                }
                analyticColumns.add((AnalyticColumn) qc);
            } else {
                VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), recordSource.getMetadata());
                vc.setName(qc.getAlias());
                virtualColumns.add(vc);
            }
            groupKeyColumns.add(qc.getAlias());
        }

        RecordSource rs = recordSource;

        // if virtual columns are present, create record source to calculate them
        if (virtualColumns != null) {
            rs = new VirtualColumnRecordSource(rs, virtualColumns);
        }

        // if aggregators present, wrap record source into group-by source
        ExprNode sampleBy = model.getSampleBy();
        int asz = aggregators.size();
        if (asz > 0) {
            ObjList<AggregatorFunction> af = new ObjList<>(asz);
            // create virtual columns
            for (int i = 0; i < asz; i++) {
                QueryColumn qc = aggregators.get(i);
                VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), rs.getMetadata());
                if (vc instanceof AggregatorFunction) {
                    vc.setName(qc.getAlias());
                    af.add((AggregatorFunction) vc);
                } else {
                    throw QueryError.$(qc.getAst().position, "Internal configuration error. Not an aggregate");
                }
            }

            if (sampleBy == null) {
                rs = new AggregatedRecordSource(rs, groupKeyColumns, af, configuration.getDbAggregatePage());
            } else {
                TimestampSampler sampler = SamplerFactory.from(sampleBy.token);
                if (sampler == null) {
                    throw QueryError.$(sampleBy.position, "Invalid sample");
                }
                rs = new ResampledRecordSource(rs,
                        getTimestampIndex(model, model.getTimestamp(), rs.getMetadata()),
                        groupKeyColumns,
                        af,
                        sampler,
                        configuration.getDbAggregatePage());
            }
        } else {
            if (sampleBy != null) {
                throw QueryError.$(sampleBy.position, "There are no aggregation columns");
            }
        }

        if (outerVirtualColumns.size() > 0) {
            ObjList<VirtualColumn> outer = new ObjList<>(outerVirtualColumns.size());
            for (int i = 0, n = outerVirtualColumns.size(); i < n; i++) {
                QueryColumn qc = outerVirtualColumns.get(i);
                VirtualColumn vc = virtualColumnBuilder.createVirtualColumn(model, qc.getAst(), rs.getMetadata());
                vc.setName(qc.getAlias());
                outer.add(vc);
            }
            rs = new VirtualColumnRecordSource(rs, outer);
        }

        if (analyticColumns.size() > 0) {
            final int n = analyticColumns.size();
            final ObjList<AnalyticFunction> functions = new ObjList<>(n);
            final RecordMetadata metadata = rs.getMetadata();
            boolean hasTwoPassFunctions = false;

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

                if (col.getAst().paramCount > 1) {
                    throw QueryError.$(col.getAst().position, "Too many arguments");
                }

                if (col.getAst().paramCount < 1) {
                    throw QueryError.$(col.getAst().position, "Expression expected");
                }

                VirtualColumn valueColumn = virtualColumnBuilder.createVirtualColumn(model, col.getAst().rhs, metadata);
                valueColumn.setName(col.getAlias());

                AnalyticFunction f = AnalyticFunctionFactories.newInstance(
                        configuration,
                        col.getAst().token,
                        valueColumn,
                        partitionBy,
                        rs.supportsRowIdAccess()
                );

                if (f == null) {
                    throw QueryError.$(col.getAst().position, "Unknown function");
                }

                if (!hasTwoPassFunctions && (f instanceof TwoPassAnalyticFunction)) {
                    hasTwoPassFunctions = true;
                }
                functions.add(f);
            }

            if (hasTwoPassFunctions) {
                rs = new CachingAnalyticRecordSource(configuration.getDbAnalyticWindowPage(), rs, functions);
            } else {
                rs = new AnalyticRecordSource(rs, functions);
            }
        }

        if (selectedColumns.size() > 0) {
            // wrap underlying record source into selected columns source.
            rs = new SelectedColumnsRecordSource(rs, selectedColumns, selectedColumnAliases);
        }
        return rs;
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
        if (joinBarriers.contains(jm.getJoinType().ordinal())) {
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
                if (target.getJoinType() == QueryModel.JoinType.CROSS) {
                    target.setJoinType(QueryModel.JoinType.INNER);
                }
            }
        }
        return true;
    }

    private int toInt(CharSequence cs, int pos) throws ParserException {
        try {
            return Numbers.parseInt(cs);
        } catch (NumericException e) {
            throw QueryError.$(pos, "int value expected");
        }
    }

    private IntHashSet toIntHashSet(IntrinsicModel im) throws ParserException {
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

    private void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    private enum OrderByState {
        UNKNOWN, NEED_ORDERED, ORDER_INVARIANT
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private IntList indexes;
        private ObjList<CharSequence> names;
        private int nullCount;
        private QueryModel parent;

        @Override
        public void visit(ExprNode node) throws ParserException {
            switch (node.type) {
                case LITERAL:
                    int dot = node.token.indexOf('.');
                    CharSequence name = extractColumnName(node.token, dot);
                    indexes.add(resolveJournalIndex(parent, dot == -1 ? null : csPool.next().of(node.token, 0, dot), name, node.position));
                    names.add(name);
                    break;
                case CONSTANT:
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

        private PostOrderTreeTraversalAlgo.Visitor to(IntList indexes, ObjList<CharSequence> names) {
            this.indexes = indexes;
            this.names = names;
            return this;
        }

        private void withParent(QueryModel parent) {
            this.parent = parent;
        }
    }

    static {
        joinBarriers = new IntHashSet();
        joinBarriers.add(QueryModel.JoinType.OUTER.ordinal());
        joinBarriers.add(QueryModel.JoinType.ASOF.ordinal());
    }

    static {
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.SYMBOL, true, ColumnType.SYMBOL), KvIndexSymSymLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.SYMBOL, true, ColumnType.STRING), KvIndexSymStrLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.INT, true, ColumnType.INT), KvIndexIntLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.STRING, true, ColumnType.STRING), KvIndexStrStrLambdaHeadRowSource.FACTORY);
        LAMBDA_ROW_SOURCE_FACTORIES.put(lbs(ColumnType.STRING, true, ColumnType.SYMBOL), KvIndexStrSymLambdaHeadRowSource.FACTORY);
    }

    static {
        nullConstants.add("null");
        nullConstants.add("NaN");
    }
}
