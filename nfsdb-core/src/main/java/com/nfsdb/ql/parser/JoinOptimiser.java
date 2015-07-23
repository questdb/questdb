/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.collections.*;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.JournalRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.JoinContext;
import com.nfsdb.ql.model.QueryModel;
import com.nfsdb.utils.Chars;

public class JoinOptimiser {

    private final ObjectPool<FlyweightCharSequence> csPool = new ObjectPool<>(FlyweightCharSequence.FACTORY, 64);
    private final Optimiser optimiser;
    private final ObjObjHashMap<String, RecordMetadata> namedJoinMetadata = new ObjObjHashMap<>();
    private final ObjList<RecordMetadata> allJoinMetadata = new ObjList<>();
    private final CharSequenceIntHashMap joinMetadataIndexLookup = new CharSequenceIntHashMap();
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 16);
    private final IntHashSet deletedContexts = new IntHashSet();
    private final IntObjHashMap<ExprNode> preFilters = new IntObjHashMap<>();
    private final IntObjHashMap<ExprNode> postFilters = new IntObjHashMap<>();
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final ObjectPool<JoinContext> contextPool = new ObjectPool<>(JoinContext.FACTORY, 16);
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final IntList clausesToSteal = new IntList();
    private final IntList literalCollectorAIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final IntList literalCollectorBIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final StringSink planSink = new StringSink();
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final IntStack orderingStack = new IntStack();
    private final ObjList<ExprNode> globalFilterParts = new ObjList<>();
    private final ObjList<QueryModel> tempCrosses = new ObjList<>();
    private final IntList tempCrossIndexes = new IntList();
    private final IntHashSet constantConditions = new IntHashSet();
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final IntHashSet postFilterAvailable = new IntHashSet();
    private final ObjList<IntList> postFilterJournalRefs = new ObjList<>();
    private final ObjList<CharSequence> postFilterThrowawayNames = new ObjList<>();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(new ObjectPoolFactory<IntList>() {
        @Override
        public IntList newInstance() {
            return new IntList();
        }
    }, 16);

    private IntList orderedJournals;
    private IntList orderedJournals1 = new IntList();
    private IntList orderedJournals2 = new IntList();
    private ObjList<JoinContext> emittedJoinClauses;

    public JoinOptimiser(Optimiser optimiser) {
        this.optimiser = optimiser;
    }

    public void compile(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        clearState();
        joinModels.add(model);
        joinModels.add(model.getJoinModels());
        int n = joinModels.size();

        // create metadata for all journals and sub-queries involved in this query
        for (int i = 0; i < n; i++) {
            resolveJoinMetadata(joinModels.getQuick(i), factory);
        }

        emittedJoinClauses = joinClausesSwap1;

        for (int i = 0; i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            processJoinConditions(m.getWhereClause(), null);
            processJoinConditions(m.getJoinCriteria(), null);
        }

        if (emittedJoinClauses.size() > 0) {
            processEmittedJoinClauses();
        }

        trySwapJoinOrder();
        tryAssignPostJoinFilters();
    }

    public CharSequence plan() {
        planSink.clear();
        for (int i = 0, n = orderedJournals.size(); i < n; i++) {
            final int index = orderedJournals.getQuick(i);
            final QueryModel m = joinModels.getQuick(index);
            final JoinContext jc = m.getContext();

            final boolean cross = jc == null || jc.parents.size() == 0;
            planSink.put('+').put(' ').put(index);

            // join type
            planSink.put('[').put(' ');
            if (m.getJoinType() == QueryModel.JoinType.CROSS || cross) {
                planSink.put("cross");
            } else if (m.getJoinType() == QueryModel.JoinType.INNER) {
                planSink.put("inner");
            } else {
                planSink.put("outer");
            }
            planSink.put(' ').put(']').put(' ');

            // journal name/alias
            if (m.getAlias() != null) {
                planSink.put(m.getAlias());
            } else if (m.getJournalName() != null) {
                planSink.put(m.getJournalName().token);
            } else {
                planSink.put("subquery");
            }

            // pre-filter
            ExprNode filter = preFilters.get(index);
            if (filter != null) {
                planSink.put(" (filter: ");
                filter.toString(planSink);
                planSink.put(')');
            }

            // join clause
            if (!cross && jc.aIndexes.size() > 0) {
                planSink.put(" ON ");
                for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                    if (k > 0) {
                        planSink.put(" and ");
                    }
                    jc.aNodes.getQuick(k).toString(planSink);
                    planSink.put(" = ");
                    jc.bNodes.getQuick(k).toString(planSink);
                }
            }

            // post-filter
            filter = postFilters.get(index);
            if (filter != null) {
                planSink.put(" (post-filter: ");
                filter.toString(planSink);
                planSink.put(')');
            }

            planSink.put('\n');
        }

        planSink.put('\n');

        return planSink;
    }

    private void addFilterOrEmitJoin(int idx, int ai, CharSequence an, ExprNode ao, int bi, CharSequence bn, ExprNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(idx);
            return;
        }

        if (ai == bi) {
            // (same journal)
            ExprNode node = exprNodePool.next().init(ExprNode.NodeType.OPERATION, "=", 0, 0);
            node.paramCount = 2;
            node.lhs = ao;
            node.rhs = bo;
            preFilters.put(ai, mergeFilters(preFilters.get(ai), node));
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

    private void addGlobalFilter(ExprNode node) {
        this.globalFilterParts.add(node);
    }

    private void addJoinContext(JoinContext context) {
        QueryModel jm = joinModels.getQuick(context.slaveIndex);
        JoinContext other = jm.getContext();
        if (other == null) {
            jm.setContext(context);
        } else {
            jm.setContext(mergeContexts(other, context));
        }
    }

    private void analyseEquals(ExprNode node, JoinContext jc) throws ParserException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());

        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        switch (aSize) {
            case 0:
                if (bSize == 1) {
                    // single journal reference
                    jc.slaveIndex = literalCollectorBIndexes.getQuick(0);
                    preFilters.put(jc.slaveIndex, mergeFilters(preFilters.get(jc.slaveIndex), node));
                } else {
                    addGlobalFilter(node);
                }
                break;
            case 1:
                int lhi = literalCollectorAIndexes.getQuick(0);
                if (bSize == 1) {
                    int rhi = literalCollectorBIndexes.getQuick(0);
                    if (lhi == rhi) {
                        // single journal reference
                        jc.slaveIndex = lhi;
                        preFilters.put(lhi, mergeFilters(preFilters.get(lhi), node));
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
                        linkDependencies(min, max);
                    }
                } else {
                    // single journal reference
                    jc.slaveIndex = lhi;
                    preFilters.put(lhi, mergeFilters(preFilters.get(lhi), node));
                }
                break;
            default:
                addGlobalFilter(node);
                break;
        }
    }

    private void clearState() {
        namedJoinMetadata.clear();
        allJoinMetadata.clear();
        joinMetadataIndexLookup.clear();
        csPool.reset();
        exprNodePool.reset();
        joinModels.clear();
        globalFilterParts.clear();
        contextPool.reset();
        preFilters.clear();
        postFilters.clear();
        constantConditions.clear();
        postFilterRemoved.clear();
        postFilterAvailable.clear();
        postFilterJournalRefs.clear();
        intListPool.reset();
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : csPool.next().of(token, dot + 1, token.length() - dot - 1);
    }

    private void linkDependencies(int parent, int child) {
        joinModels.getQuick(parent).addDependency(child);
    }

    private JoinContext mergeContexts(JoinContext a, JoinContext b) {
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
                    addFilterOrEmitJoin(k, abi, abn, abo, bbi, bbn, bbo);
                    break;
                } else if (abi == bai && Chars.equals(abn, ban)) {
                    // a.y = b.x
                    //    /
                    // b.x = a.x
                    addFilterOrEmitJoin(k, aai, aan, aao, bbi, bbn, bbo);
                    break;
                } else if (aai == bbi && Chars.equals(aan, bbn)) {
                    // a.x = b.x
                    //     \
                    // b.y = a.x
                    addFilterOrEmitJoin(k, abi, abn, abo, bai, ban, bao);
                    break;
                } else if (abi == bbi && Chars.equals(abn, bbn)) {
                    // a.x = b.x
                    //        |
                    // a.y = b.x
                    addFilterOrEmitJoin(k, aai, aan, aao, bai, ban, bao);
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
            linkDependencies(min, max);
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
                    unlinkDependencies(min, max);
                }
            } else {
                r.aNames.add(a.aNames.getQuick(i));
                r.bNames.add(a.bNames.getQuick(i));
                r.aIndexes.add(aai);
                r.bIndexes.add(abi);
                r.aNodes.add(a.aNodes.getQuick(i));
                r.bNodes.add(a.bNodes.getQuick(i));

                r.parents.add(min);
                linkDependencies(min, max);
            }
        }

        return r;
    }

    private ExprNode mergeFilters(ExprNode a, ExprNode b) {
        if (a == null && b == null) {
            return null;
        }

        if (a == null) {
            return b;
        }

        if (b == null) {
            return a;
        }

        ExprNode n = exprNodePool.next().init(ExprNode.NodeType.OPERATION, "and", 0, 0);
        n.paramCount = 2;
        n.lhs = a;
        n.rhs = b;
        return n;
    }

    private JoinContext moveClauses(JoinContext from, JoinContext to, IntList positions) {
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
                linkDependencies(ai, bi);
            } else {
                t.parents.add(bi);
                linkDependencies(bi, ai);
            }
        }

        return result;
    }

    private int orderJournals(IntList ordered) {
        ordered.clear();
        this.orderingStack.clear();

        int cost = 0;

        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.isCrossJoin() || 0 == q.getContext().parents.size()) {
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
            if (!m.isCrossJoin() && m.getContext().inCount > 0) {
                return Integer.MAX_VALUE;
            }
        }

        // add pure crosses at end of ordered journal list
        for (int i = 0, n = tempCrossIndexes.size(); i < n; i++) {
            ordered.add(tempCrossIndexes.getQuick(i));
        }

        return cost;
    }

    private void processEmittedJoinClauses() {
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
                addJoinContext(clauses.getQuick(i));
            }
        } while (emittedJoinClauses.size() > 0);

    }

    private void processJoinConditions(ExprNode node, JoinContext c) throws ParserException {

        if (node == null) {
            return;
        }

        JoinContext context = null;

        if (c == null && (node.type == ExprNode.NodeType.LITERAL || (node.type == ExprNode.NodeType.OPERATION && !"and".equals(node.token)))) {
            context = c = contextPool.next();
        }

        boolean descend = true;

        if (c != null) {
            switch (node.type) {
                case OPERATION:
                    switch (node.token) {
                        case "and":
                            break;
                        case "=":
                            analyseEquals(node, c);
                            descend = false;
                            break;
                        default:
                            addGlobalFilter(node);
                            descend = false;
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
        if (descend) {
            if (node.paramCount < 3) {
                processJoinConditions(node.lhs, c);
                processJoinConditions(node.rhs, c);
            } else {
                for (int i = 0; i < node.paramCount; i++) {
                    processJoinConditions(node.args.getQuick(i), c);
                }
            }
        }

        if (context != null && context.slaveIndex > -1) {
            addJoinContext(context);
        }
    }

    private void resolveJoinMetadata(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        RecordMetadata metadata;
        if (model.getJournalName() != null) {
            optimiser.collectJournalMetadata(model, factory);
            metadata = model.getMetadata();
        } else {
            JournalRecordSource<? extends Record> rs = optimiser.compile(model, factory);
            metadata = rs.getMetadata();
            model.setRecordSource(rs);
        }

        int pos = allJoinMetadata.size();

        allJoinMetadata.add(metadata);

        if (model.getAlias() != null) {
            namedJoinMetadata.put(model.getAlias(), metadata);
            joinMetadataIndexLookup.put(model.getAlias(), pos);
        } else if (model.getJournalName() != null) {
            namedJoinMetadata.put(model.getJournalName().token, metadata);
            joinMetadataIndexLookup.put(model.getJournalName().token, pos);
        }
    }

    private int resolveJournalIndex(CharSequence alias, CharSequence column, int position) throws ParserException {

        int index = -1;
        if (alias == null) {
            for (int i = 0, n = allJoinMetadata.size(); i < n; i++) {
                RecordMetadata m = allJoinMetadata.getQuick(i);
                if (m.invalidColumn(column)) {
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
            index = joinMetadataIndexLookup.get(alias);

            if (index == -1) {
                throw new ParserException(position, "Invalid journal name/alias");
            }
            RecordMetadata m = allJoinMetadata.getQuick(index);

            if (m.invalidColumn(column)) {
                throw new InvalidColumnException(position);
            }

            return index;
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
    private boolean swapJoinOrder(int to, int from, JoinContext jc) {
        QueryModel jm = joinModels.getQuick(from);
        if (jm.getJoinType() == QueryModel.JoinType.OUTER) {
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
                jm.setContext(moveClauses(that, jc, clausesToSteal));
                if (target.getJoinType() == QueryModel.JoinType.CROSS) {
                    target.setJoinType(QueryModel.JoinType.INNER);
                }
            }
        }
        return true;
    }

    private void tryAssignPostJoinFilters() throws ParserException {

        // collect journal indexes from each part of global filter
        int pc = globalFilterParts.size();
        for (int i = 0; i < pc; i++) {
            IntList indexes = intListPool.next();
            traversalAlgo.traverse(globalFilterParts.getQuick(i), literalCollector.to(indexes, postFilterThrowawayNames));
            postFilterJournalRefs.add(indexes);
            postFilterThrowawayNames.clear();
        }

        // match journal references to set of journals in join order
        for (int i = 0, n = orderedJournals.size(); i < n; i++) {
            postFilterAvailable.add(orderedJournals.getQuick(i));

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
                    constantConditions.add(k);
                } else {
                    boolean remove = true;
                    for (int y = 0; y < rs; y++) {
                        if (!postFilterAvailable.contains(refs.getQuick(y))) {
                            remove = false;
                            break;
                        }
                    }
                    if (remove) {
                        postFilterRemoved.add(k);
                        postFilters.put(i, globalFilterParts.getQuick(k));
                    }
                }
            }
        }

        assert postFilterRemoved.size() == pc;
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
    @SuppressWarnings("StatementWithEmptyBody")
    private void trySwapJoinOrder() throws ParserException {
        int n = joinModels.size();

        tempCrosses.clear();
        // collect crosses
        for (int i = 0; i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.isCrossJoin()) {
                tempCrosses.add(q);
            }
        }

        int cost = Integer.MAX_VALUE;
        int root = -1;

        // analyse state of tree for each set of n-1 crosses
        for (int z = 0, zc = tempCrosses.size(); z < zc; z++) {
            for (int i = 0; i < zc; i++) {
                if (z != i) {
                    QueryModel q = tempCrosses.getQuick(i);
                    JoinContext jc = q.getContext();
                    // look above i up to OUTER join
                    for (int k = i - 1; k > -1 && swapJoinOrder(i, k, jc); k--) ;
                    // look below i for up to OUTER join
                    for (int k = i + 1; k < n && swapJoinOrder(i, k, jc); k++) ;
                }
            }

            IntList ordered;
            if (orderedJournals == orderedJournals1) {
                ordered = orderedJournals2;
            } else {
                ordered = orderedJournals1;
            }

            ordered.clear();
            int thisCost = orderJournals(ordered);
            if (thisCost < cost) {
                root = z;
                cost = thisCost;
                orderedJournals = ordered;
            }
        }

        if (root == -1) {
            throw new ParserException(0, "Cycle");
        }
    }

    private void unlinkDependencies(int parent, int child) {
        joinModels.getQuick(parent).removeDependency(child);
    }

    private class LiteralCollector implements Visitor {
        private IntList indexes;
        private ObjList<CharSequence> names;

        @Override
        public void visit(ExprNode node) throws ParserException {
            if (node.type == ExprNode.NodeType.LITERAL) {
                int dot = node.token.indexOf('.');
                CharSequence name = extractColumnName(node.token, dot);
                int index = resolveJournalIndex(dot == -1 ? null : csPool.next().of(node.token, 0, dot), name, node.position);
                indexes.add(index);
                names.add(name);
            }
        }

        private Visitor lhs() {
            indexes = literalCollectorAIndexes;
            names = literalCollectorANames;
            return this;
        }

        private Visitor rhs() {
            indexes = literalCollectorBIndexes;
            names = literalCollectorBNames;
            return this;
        }

        private Visitor to(IntList indexes, ObjList<CharSequence> names) {
            this.indexes = indexes;
            this.names = names;
            return this;
        }
    }
}