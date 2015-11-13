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
import com.nfsdb.collections.*;
import com.nfsdb.exceptions.InvalidColumnException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.*;
import com.nfsdb.ql.impl.*;
import com.nfsdb.ql.impl.aggregation.AggregatedRecordSource;
import com.nfsdb.ql.impl.aggregation.ResampledRecordSource;
import com.nfsdb.ql.impl.aggregation.SamplerFactory;
import com.nfsdb.ql.impl.aggregation.TimestampSampler;
import com.nfsdb.ql.impl.interval.IntervalJournalRecordSource;
import com.nfsdb.ql.impl.interval.MultiIntervalPartitionSource;
import com.nfsdb.ql.impl.interval.SingleIntervalSource;
import com.nfsdb.ql.impl.join.AsOfJoinRecordSource;
import com.nfsdb.ql.impl.join.AsOfPartitionedJoinRecordSource;
import com.nfsdb.ql.impl.join.CrossJoinRecordSource;
import com.nfsdb.ql.impl.join.HashJoinRecordSource;
import com.nfsdb.ql.impl.lambda.*;
import com.nfsdb.ql.impl.latest.*;
import com.nfsdb.ql.impl.select.SelectedColumnsRecordSource;
import com.nfsdb.ql.impl.virtual.VirtualColumnRecordSource;
import com.nfsdb.ql.model.*;
import com.nfsdb.ql.ops.FunctionFactories;
import com.nfsdb.ql.ops.Signature;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.ql.ops.constant.LongConstant;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;

public class QueryCompiler {

    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private final static ObjObjHashMap<Signature, LatestByLambdaRowSourceFactory> LAMBDA_ROW_SOURCE_FACTORIES = new ObjObjHashMap<>();
    private final static LongConstant LONG_ZERO_CONST = new LongConstant(0L);
    private final static IntHashSet joinBarriers;
    private final QueryParser parser = new QueryParser();
    private final JournalReaderFactory factory;
    private final AssociativeCache<RecordSource<? extends Record>> cache = new AssociativeCache<>(8, 1024);
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
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(new ObjectPoolFactory<IntList>() {
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
    private final ObjHashSet<String> groupKeyColumns = new ObjHashSet<>();
    private ObjList<JoinContext> emittedJoinClauses;
    private int aggregateColumnSequence;

    public QueryCompiler(JournalReaderFactory factory) {
        this.factory = factory;

        // seed column name assembly with default column prefix, which we will reuse
        columnNameAssembly.put("col");
        columnNamePrefixLen = 3;
    }

    public void clearCache() {
        cache.clear();
    }

    public <T> RecordCursor<? extends Record> compile(Class<T> clazz) throws JournalException, ParserException {
        return compile(clazz.getName());
    }

    public RecordCursor<? extends Record> compile(CharSequence query) throws ParserException, JournalException {
        return compileSource(query).prepareCursor(factory);
    }

    public RecordSource<? extends Record> compileSource(CharSequence query) throws ParserException, JournalException {
        RecordSource<? extends Record> rs = cache.get(query);
        if (rs == null) {
            rs = resetAndCompile(parser.parse(query).getQueryModel(), factory);
            cache.put(query, rs);
        } else {
            rs.reset();
        }
        return rs;
    }

    public CharSequence plan(CharSequence query) throws ParserException, JournalException {
        QueryModel model = parser.parse(query).getQueryModel();
        resetAndOptimise(model, factory);
        return model.plan();
    }

    private static Signature lbs(ColumnType master, boolean indexed, ColumnType lambda) {
        return new Signature().setName("").setParamCount(2).paramType(0, master, indexed).paramType(1, lambda, false);
    }

    private void addAlias(int position, String alias) throws ParserException {
        if (selectedColumnAliases.add(alias)) {
            return;
        }
        throw new ParserException(position, "Duplicate alias");
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
     * <p/>
     * a.x = b.x and b.x = 10
     * <p/>
     * derived filter would be:
     * <p/>
     * a.x = 10
     * <p/>
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

    private void analyseLimit(QueryModel model) throws ParserException {
        ExprNode lo = model.getLimitLo();
        ExprNode hi = model.getLimitHi();
        if (hi == null) {
            model.setLimitVc(LONG_ZERO_CONST, toVirtualColumn(lo));
        } else {
            model.setLimitVc(toVirtualColumn(lo), toVirtualColumn(hi));
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
                        parent.getJoinModels().getQuick(index).setPostJoinWhereClause(filterNodes.getQuick(k));
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
        csPool.reset();
        exprNodePool.reset();
        contextPool.reset();
        intListPool.reset();
        joinClausesSwap1.clear();
        joinClausesSwap2.clear();
        queryFilterAnalyser.reset();
        constNameToIndex.clear();
        constNameToNode.clear();
        constNameToToken.clear();
    }

    private void collectColumnNameFrequency(QueryModel model, RecordSource<? extends Record> rs) {
        CharSequenceIntHashMap map = model.getColumnNameHistogram();
        RecordMetadata m = rs.getMetadata();
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            CharSequence name = m.getColumnName(i);
            map.put(name, map.get(name) + 1);
        }
    }

    private JournalMetadata collectJournalMetadata(QueryModel model, JournalReaderFactory factory) throws ParserException, JournalException {
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

        return factory.getOrCreateMetadata(new JournalKey<>(reader));
    }

    private RecordSource<? extends Record> compile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        return limit(
                selectColumns(
                        model.getJoinModels().size() > 1 ?
                                optimise(model, factory).compileJoins(model, factory) :
                                optimise(model, factory).compileSingleOrSubQuery(model, factory),
                        model
                ), model
        );
    }

    private RecordSource<? extends Record> compileJoins(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        ObjList<QueryModel> joinModels = model.getJoinModels();
        IntList ordered = model.getOrderedJoinModels();
        RecordSource<? extends Record> master = null;

        boolean collectColumnNameFrequency = model.getColumns().size() > 0;

        for (int i = 0, n = ordered.size(); i < n; i++) {
            int index = ordered.getQuick(i);
            QueryModel m = joinModels.getQuick(index);

            // compile
            RecordSource<? extends Record> slave = m.getRecordSource();

            if (slave == null) {
                slave = compileSingleOrSubQuery(m, factory);
                if (m.getAlias() != null) {
                    slave.getMetadata().setAlias(m.getAlias().token);
                }
            }

            if (collectColumnNameFrequency) {
                collectColumnNameFrequency(model, slave);
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
                master = new FilteredJournalRecordSource(master, createVirtualColumn(filter, master.getMetadata(), model.getColumnNameHistogram()));
            }
        }

        if (joinModelIsFalse(model)) {
            return new NoOpJournalRecordSource(master);
        }
        return master;
    }

    @SuppressWarnings("ConstantConditions")
    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT", "CC_CYCLOMATIC_COMPLEXITY"})
    private RecordSource<? extends Record> compileSingleJournal(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        // analyse limit first as it is easy win
        if (model.getLimitLo() != null || model.getLimitHi() != null) {
            analyseLimit(model);
        }

        RecordMetadata metadata = model.getMetadata();
        JournalMetadata journalMetadata;

        if (metadata == null) {
            journalMetadata = collectJournalMetadata(model, factory);
        } else if (metadata instanceof JournalMetadata) {
            journalMetadata = (JournalMetadata) metadata;
        } else {
            throw new ParserException(0, "Internal error: invalid metadata");
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
                throw new ParserException(latestByNode.position, "Column name expected");
            }

            int colIndex = journalMetadata.getColumnIndexQuiet(latestByNode.token);
            if (colIndex == -1) {
                throw new InvalidColumnException(latestByNode.position);
            }

            latestByMetadata = journalMetadata.getColumnQuick(colIndex);

            ColumnType type = latestByMetadata.getType();
            if (type != ColumnType.SYMBOL && type != ColumnType.STRING && type != ColumnType.INT) {
                throw new ParserException(latestByNode.position, "Expected symbol or string column, found: " + type);
            }

            if (!latestByMetadata.isIndexed()) {
                throw new ParserException(latestByNode.position, "Column is not indexed");
            }

            latestByCol = latestByNode.token;
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = queryFilterAnalyser.extract(where, journalMetadata, latestByCol);

            VirtualColumn filter = im.filter != null ? createVirtualColumn(im.filter, journalMetadata, model.getColumnNameHistogram()) : null;

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
                        }
                    }

                    if (filter != null) {
                        rs = new FilteredRowSource(rs == null ? new AllRowSource() : rs, filter);
                    }
                } else {
                    if (im.keyColumn != null && im.keyValuesIsLambda) {
                        int lambdaColIndex;
                        RecordSource<? extends Record> lambda = compileSourceInternal(im.keyValues.get(0));
                        RecordMetadata m = lambda.getMetadata();

                        switch (m.getColumnCount()) {
                            case 0:
                                throw new ParserException(im.keyValuePositions.getQuick(0), "Query must select at least one column");
                            case 1:
                                lambdaColIndex = 0;
                                break;
                            default:
                                lambdaColIndex = m.getColumnIndexQuiet(latestByCol);
                                if (lambdaColIndex == -1) {
                                    throw new ParserException(im.keyValuePositions.getQuick(0), "Ambiguous column names in lambda query. Specify select clause");
                                }
                                break;
                        }

                        ColumnType lambdaColType = m.getColumn(lambdaColIndex).getType();
                        mutableSig.setParamCount(2).setName("").paramType(0, latestByMetadata.getType(), true).paramType(1, lambdaColType, false);
                        LatestByLambdaRowSourceFactory fact = LAMBDA_ROW_SOURCE_FACTORIES.get(mutableSig);
                        if (fact != null) {
                            rs = fact.newInstance(latestByCol, lambda, lambdaColIndex, filter);
                        } else {
                            throw new ParserException(im.keyValuePositions.getQuick(0), "Mismatched types");
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
                                    throw new ParserException(latestByNode.position, "Filter on string column expected");
                                }
                                break;
                            case INT:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexIntListHeadRowSource(latestByCol, toIntHashSet(im), filter);
                                } else {
                                    throw new ParserException(latestByNode.position, "Filter on int column expected");
                                }
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
                    throw new ParserException(latestByNode.position, "Only SYM columns can be used here without filter");
            }
        }

        return new JournalSource(ps, rs == null ? new AllRowSource() : rs);
    }

    private RecordSource<? extends Record> compileSingleOrSubQuery(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        return model.getJournalName() != null ? compileSingleJournal(model, factory) : compileSubQuery(model, factory);
    }

    private RecordSource<? extends Record> compileSourceInternal(CharSequence query) throws ParserException, JournalException {
        RecordSource<? extends Record> rs = cache.get(query);
        if (rs == null) {
            rs = compile(parser.parseInternal(query).getQueryModel(), factory);
            cache.put(query, rs);
        } else {
            rs.reset();
        }
        return rs;
    }

    private RecordSource<? extends Record> compileSubQuery(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {

        RecordSource<? extends Record> rs = compile(model.getNestedModel(), factory);
        if (model.getWhereClause() == null) {
            return rs;
        }

        RecordMetadata m = rs.getMetadata();
        IntrinsicModel im = queryFilterAnalyser.extract(model.getWhereClause(), m, null);

        switch (im.intrinsicValue) {
            case FALSE:
                return new NoOpJournalRecordSource(rs);
            default:
                if (im.intervalSource != null) {
                    rs = new IntervalJournalRecordSource(rs, im.intervalSource);
                }
                if (im.filter != null) {
                    VirtualColumn vc = createVirtualColumn(im.filter, m, model.getColumnNameHistogram());
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

    private RecordSource<? extends Record> createAsOfJoin(
            ExprNode masterTimestampNode,
            QueryModel model,
            RecordSource<? extends Record> master,
            RecordSource<? extends Record> slave) throws ParserException {
        JoinContext jc = model.getContext();

        ExprNode slaveTimestampNode = model.getTimestamp();
        RecordMetadata masterMetadata = master.getMetadata();
        RecordMetadata slaveMetadata = slave.getMetadata();
        int slaveTimestampIndex;
        int masterTimestampIndex;

        // check for explicit timestamp definition
        if (slaveTimestampNode != null) {
            slaveTimestampIndex = slaveMetadata.getColumnIndexQuiet(slaveTimestampNode.token);
            if (slaveTimestampIndex == -1) {
                throw new ParserException(slaveTimestampNode.position, "Invalid column");
            }
        } else if (slaveMetadata.getTimestampIndex() == -1) {
            throw new ParserException(0, "Result set timestamp column is undefined");
        } else {
            slaveTimestampIndex = slaveMetadata.getTimestampIndex();
        }


        if (masterTimestampNode != null) {
            masterTimestampIndex = masterMetadata.getColumnIndexQuiet(masterTimestampNode.token);
            if (masterTimestampIndex == -1) {
                throw new ParserException(masterTimestampNode.position, "Invalid column");
            }
        } else if (masterMetadata.getTimestampIndex() == -1) {
            throw new ParserException(0, "Result set timestamp column is undefined");
        } else {
            masterTimestampIndex = masterMetadata.getTimestampIndex();
        }

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

            return new AsOfPartitionedJoinRecordSource(master, masterTimestampIndex, slave, slaveTimestampIndex, masterKeys, slaveKeys, 4 * 1024 * 1024);
        }
    }

    @NotNull
    private RecordSource<? extends Record> createHashJoin(QueryModel model, RecordSource<? extends Record> master, RecordSource<? extends Record> slave) throws ParserException {
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
                throw new ParserException(jc.aNodes.getQuick(k).position, "Column type mismatch");
            }

            if (masterColIndices == null) {
                // these go together, so checking one for null is enough
                masterColIndices = new IntList();
                slaveColIndices = new IntList();
            }

            masterColIndices.add(ib);
            slaveColIndices.add(ia);
        }
        master = new HashJoinRecordSource(master, masterColIndices, slave, slaveColIndices, model.getJoinType() == QueryModel.JoinType.OUTER);
        return master;
    }

    /**
     * Creates dependencies via implied columns, typically timestamp.
     * Dependencies like that are no explicitly expressed in SQL query and
     * therefore are not created by analyzing "where" clause.
     * <p/>
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

    private VirtualColumn createVirtualColumn(ExprNode node, RecordMetadata metadata, CharSequenceIntHashMap columnNameHistogram) throws ParserException {
        return virtualColumnBuilder.createVirtualColumn(node, metadata, columnNameHistogram);
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : csPool.next().of(token, dot + 1, token.length() - dot - 1);
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
            } else if (m.getJoinType() != QueryModel.JoinType.ASOF) {
                if (c == null || c.parents.size() == 0) {
                    m.setJoinType(QueryModel.JoinType.CROSS);
                }
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

        VirtualColumn col = createVirtualColumn(current, null, model.getColumnNameHistogram());
        if (col.isConstant()) {
            return !col.getBool(null);
        } else {
            throw new ParserException(0, "Internal error: expected constant");
        }
    }

    private RecordSource<? extends Record> limit(RecordSource<? extends Record> rs, QueryModel model) {
        if (model.getLimitLoVc() == null || model.getLimitHiVc() == null) {
            return rs;
        } else {
            return new TopRecordSource(rs, model.getLimitLoVc(), model.getLimitHiVc());
        }
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

    private QueryCompiler optimise(QueryModel parent, JournalReaderFactory factory) throws JournalException, ParserException {
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
            processAndConditions(parent, where);

            for (int i = 1; i < n; i++) {
                processAndConditions(parent, joinModels.getQuick(i).getJoinCriteria());
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
//            rewriteColumnsRemovedByJoins(parent);
        }
        return this;
    }

    /**
     * Splits "where" clauses into "and" concatenated list of boolean expressions.
     *
     * @param node expression node
     * @throws ParserException
     */
    private void processAndConditions(QueryModel parent, ExprNode node) throws ParserException {
        // pre-order traversal
        exprNodeStack.clear();
        while (!exprNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (node.rhs != null) {
                            exprNodeStack.push(node.rhs);
                        }
                        node = node.lhs;
                        break;
                    case "=":
                        analyseEquals(parent, node);
                        node = null;
                        break;
                    case "or":
                        processOrConditions(parent, node);
                        node = null;
                        break;
                    case "~":
                        analyseRegex(parent, node);
                        // intentional fallthrough
                    default:
                        parent.addParsedWhereNode(node);
                        node = null;
                        break;
                }
            } else {
                node = exprNodeStack.poll();
            }
        }
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
     * <p/>
     * in this case tables can be reordered as long as "b" is processed
     * before "a"
     * <p/>
     * - second possibility is where all "or" conditions are random
     * in which case query like this:
     * <p/>
     * from a
     * join c on a.x = c.x
     * join b on a.x = b.x or c.y = b.y
     * <p/>
     * can be rewritten to:
     * <p/>
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
     * <p/>
     * from a
     * join b on c.x = b.x
     * join c on c.y = a.y
     * <p/>
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
            throw new ParserException(0, "Cycle");
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

    private RecordSource<? extends Record> resetAndCompile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        clearState();
        return compile(model, factory);
    }

    private void resetAndOptimise(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        clearState();
        optimise(model, factory);
    }

    private void resolveJoinMetadata(QueryModel parent, int index, JournalReaderFactory factory) throws JournalException, ParserException {
        QueryModel model = parent.getJoinModels().getQuick(index);
        RecordMetadata metadata;
        if (model.getJournalName() != null) {
            model.setMetadata(metadata = collectJournalMetadata(model, factory));
        } else {
            RecordSource<? extends Record> rs = compile(model.getNestedModel(), factory);
            model.setMetadata(metadata = rs.getMetadata());
            model.setRecordSource(rs);
        }

        if (model.getAlias() != null) {
            if (!parent.addAliasIndex(model.getAlias(), index)) {
                throw new ParserException(model.getAlias().position, "Duplicate alias");
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
                    throw new ParserException(position, "Ambiguous column name");
                }

                index = i;
            }

            if (index == -1) {
                throw new InvalidColumnException(position);
            }

            return index;
        } else {
            index = parent.getAliasIndex(alias);

            if (index == -1) {
                throw new ParserException(position, "Invalid journal name/alias");
            }
            RecordMetadata m = joinModels.getQuick(index).getMetadata();

            if (m.getColumnIndexQuiet(column) == -1) {
                throw new InvalidColumnException(position);
            }

            return index;
        }
    }

    private RecordSource<? extends Record> selectColumns(RecordSource<? extends Record> rs, QueryModel model) throws ParserException {
        return model.getColumns().size() == 0 ? rs : selectColumns0(rs, model);
    }

    @NotNull
    private RecordSource<? extends Record> selectColumns0(RecordSource<? extends Record> rs, QueryModel model) throws ParserException {
        final ObjList<QueryColumn> columns = model.getColumns();
        final CharSequenceIntHashMap columnNameHistogram = model.getColumnNameHistogram();
        final RecordMetadata meta = rs.getMetadata();

        this.outerVirtualColumns.clear();
        this.aggregators.clear();
        this.selectedColumns.clear();
        this.selectedColumnAliases.clear();
        this.groupKeyColumns.clear();
        this.aggregateColumnSequence = 0;

        ObjList<VirtualColumn> virtualColumns = null;

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final ExprNode node = qc.getAst();


            if (node.type == ExprNode.NodeType.LITERAL) {
                // check literal column validity
                if (meta.getColumnIndexQuiet(node.token) == -1) {
                    throw new InvalidColumnException(node.position);
                }

                // check if this column is exists in more than one journal/result set
                if (columnNameHistogram.get(node.token) > 0) {
                    throw new ParserException(node.position, "Ambiguous column name");
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
            if (node.type == ExprNode.NodeType.FUNCTION && FunctionFactories.isAggregate(node.token)) {
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

            VirtualColumn vc = createVirtualColumn(qc.getAst(), rs.getMetadata(), model.getColumnNameHistogram());
            vc.setName(qc.getAlias());
            virtualColumns.add(vc);
            groupKeyColumns.add(qc.getAlias());
        }


        // if virtual columns are present, create record source to calculate them
        if (virtualColumns != null) {
            rs = new VirtualColumnRecordSource(rs, virtualColumns);
        }

        // if aggregators present, wrap record source into group-by source
        ExprNode sampleBy = model.getSampleBy();
        int asz = aggregators.size();
        if (asz > 0) {
            ObjList<AggregatorFunction> af = new ObjList<>(aggregators.size());
            // create virtual columns
            for (int i = 0; i < asz; i++) {
                QueryColumn qc = aggregators.get(i);
                VirtualColumn vc = createVirtualColumn(qc.getAst(), rs.getMetadata(), model.getColumnNameHistogram());
                if (vc instanceof AggregatorFunction) {
                    vc.setName(qc.getAlias());
                    af.add((AggregatorFunction) vc);
                } else {
                    throw new ParserException(qc.getAst().position, "Internal configuration error. Not an aggregate");
                }
            }

            if (sampleBy == null) {
                rs = new AggregatedRecordSource(rs, groupKeyColumns, af);
            } else {
                TimestampSampler sampler = SamplerFactory.from(sampleBy.token);
                if (sampler == null) {
                    throw new ParserException(sampleBy.position, "Invalid sample");
                }
                rs = new ResampledRecordSource(rs, groupKeyColumns, af, sampler);
            }
        } else {
            if (sampleBy != null) {
                throw new ParserException(sampleBy.position, "There are no aggregation columns");
            }
        }

        if (outerVirtualColumns.size() > 0) {
            ObjList<VirtualColumn> outer = new ObjList<>(outerVirtualColumns.size());
            for (int i = 0, n = outerVirtualColumns.size(); i < n; i++) {
                QueryColumn qc = outerVirtualColumns.get(i);
                VirtualColumn vc = createVirtualColumn(qc.getAst(), rs.getMetadata(), model.getColumnNameHistogram());
                vc.setName(qc.getAlias());
                outer.add(vc);
            }
            rs = new VirtualColumnRecordSource(rs, outer);
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
     * @param to   target journal index
     * @param from source journal index
     * @param jc   context of target journal index
     * @return false if "from" is outer joined journal, otherwise - true
     */
    private boolean swapJoinOrder(QueryModel parent, int to, int from, JoinContext jc) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        QueryModel jm = joinModels.getQuick(from);
        if (joinBarriers.contains(jm.getJoinType().ordinal())) {
            return false;
        }

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
            throw new ParserException(pos, "int value expected");
        }
    }

    private IntHashSet toIntHashSet(IntrinsicModel im) throws ParserException {
        IntHashSet set = null;
        for (int i = 0, n = im.keyValues.size(); i < n; i++) {
            try {
                int v = Numbers.parseInt(im.keyValues.get(i));
                if (set == null) {
                    set = new IntHashSet(im.keyValues.size());
                }
                set.add(v);
            } catch (NumericException e) {
                throw new ParserException(im.keyValuePositions.get(i), "int value expected");
            }
        }
        return set;
    }

    private VirtualColumn toVirtualColumn(ExprNode node) throws ParserException {
        if (node.type == ExprNode.NodeType.CONSTANT) {
            try {
                return new LongConstant(Numbers.parseLong(node.token));
            } catch (NumericException e) {
                throw new ParserException(node.position, "Long number expected");
            }
        } else {
            throw new ParserException(node.position, "Constant expected");
        }
    }

    private void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
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
