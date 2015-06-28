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

import com.nfsdb.collections.*;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.JournalRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.JoinContext;
import com.nfsdb.ql.model.JoinModel;
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
    private final ObjList<JoinContext> joinConditionsSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinConditionsSwap2 = new ObjList<>();
    private final ObjectPool<JoinContext> contextPool = new ObjectPool<>(JoinContext.FACTORY, 16);
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final IntList clausesToSteal = new IntList();
    private final IntList literalCollectorAIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final IntList literalCollectorBIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final StringSink planSink = new StringSink();
    private ExprNode globalFilter;
    private ObjList<JoinContext> emittedJoinConditions;
    private ObjList<JoinModel> joinModels;
    private QueryModel current;

    public JoinOptimiser(Optimiser optimiser) {
        this.optimiser = optimiser;
    }

    public void compileJoins(QueryModel model, JournalReaderFactory factory) throws JournalException, ParserException {
        // prepare for new iteration
        namedJoinMetadata.clear();
        allJoinMetadata.clear();
        joinMetadataIndexLookup.clear();
        csPool.reset();
        exprNodePool.reset();

        this.current = model;

        joinModels = model.getJoinModels();
        int n = joinModels.size();

        // create metadata for all journals and sub-queries involved in this query
        resolveJoinMetadata(model, factory);
        for (int i = 0; i < n; i++) {
            resolveJoinMetadata(joinModels.getQuick(i), factory);
        }

        contextPool.reset();
        emittedJoinConditions = joinConditionsSwap1;

        traversePreOrderRecursive(model.getWhereClause(), null);
        for (int i = 0; i < n; i++) {
            traversePreOrderRecursive(joinModels.getQuick(i).getJoinCriteria(), null);
        }

        if (emittedJoinConditions.size() > 0) {
            processEmittedConditions();
        }

        trySwapJoinOrder();
    }

    public CharSequence plan() {
        planSink.clear();

        if (current.getAlias() != null) {
            planSink.put(current.getAlias());
        } else if (current.getJournalName() != null) {
            planSink.put(current.getJournalName().token);
        } else {
            planSink.put("subquery");
        }

        planSink.put('\n');


        for (int i = 0, n = current.getJoinModels().size(); i < n; i++) {
            final JoinModel m = current.getJoinModels().getQuick(i);
            final JoinContext jc = m.getContext();

            final boolean cross = jc == null || jc.parents.size() == 0;
            planSink.put('+').put(' ').put(i);

            // join type
            planSink.put('[').put(' ');
            if (m.getJoinType() == JoinModel.JoinType.CROSS || cross) {
                planSink.put("cross");
            } else if (m.getJoinType() == JoinModel.JoinType.INNER) {
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
            ExprNode filter = preFilters.get(i + 1);
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

            planSink.put('\n');
        }

        // global filter
        if (globalFilter != null) {
            planSink.put("where ");
            globalFilter.toString(planSink);
        }

        planSink.put('\n');

        return planSink;
    }

    private void addJoinContext(JoinContext context) {
        JoinModel jm = joinModels.getQuick(context.slaveIndex - 1);
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
                    globalFilter = mergeFilters(globalFilter, node);
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
                        jc.slaveIndex = lhi > rhi ? lhi : rhi;
                        jc.parents.add(lhi < rhi ? lhi : rhi);
                    }
                } else {
                    // single journal reference
                    jc.slaveIndex = lhi;
                    preFilters.put(lhi, mergeFilters(preFilters.get(lhi), node));
                }
                break;
            default:
                globalFilter = mergeFilters(globalFilter, node);
                break;
        }
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : csPool.next().of(token, dot + 1, token.length() - dot - 1);
    }

    private JoinContext mergeContexts(JoinContext a, JoinContext b) {

        deletedContexts.clear();
        JoinContext r = contextPool.next();

        assert a.slaveIndex == b.slaveIndex;
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
                    processJoinCombo(k, abi, abn, abo, bbi, bbn, bbo);
                    break;
                } else if (abi == bai && Chars.equals(abn, ban)) {
                    // a.y = b.x
                    //    /
                    // b.x = a.x
                    processJoinCombo(k, aai, aan, aao, bbi, bbn, bbo);
                    break;
                } else if (aai == bbi && Chars.equals(aan, bbn)) {
                    // a.x = b.x
                    //     \
                    // b.y = a.x
                    processJoinCombo(k, abi, abn, abo, bai, ban, bao);
                    break;
                } else if (abi == bbi && Chars.equals(abn, bbn)) {
                    // a.x = b.x
                    //        |
                    // a.y = b.x
                    processJoinCombo(k, aai, aan, aao, bai, ban, bao);
                    break;
                }
            }
            r.add(bai, ban, bao, bbi, bbn, bbo);
        }

        // add remaining a nodes
        for (int i = 0, n = a.aNames.size(); i < n; i++) {
            if (deletedContexts.contains(i)) {
                continue;
            }
            r.aNames.add(a.aNames.getQuick(i));
            r.bNames.add(a.bNames.getQuick(i));
            r.aIndexes.add(a.aIndexes.getQuick(i));
            r.bIndexes.add(a.bIndexes.getQuick(i));
            r.aNodes.add(a.aNodes.getQuick(i));
            r.bNodes.add(a.bNodes.getQuick(i));
            r.parents.add(a.parents);
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

    /**
     * Moves reversible join clauses, such as a.x = b.x from journal "from" to journal "to".
     *
     * @param to   target journal index
     * @param from source journal index
     * @param jc   context of target journal index
     * @return false if "from" is outer joined journal, otherwise - true
     */
    private boolean move(int to, int from, JoinContext jc) {
        int _to = to + 1;
        JoinModel jm = joinModels.getQuick(from);
        if (jm.getJoinType() == JoinModel.JoinType.OUTER) {
            return false;
        }

        clausesToSteal.clear();

        JoinContext that = jm.getContext();
        if (that != null && that.parents.contains(_to)) {
            int zc = that.aIndexes.size();
            for (int z = 0; z < zc; z++) {
                if (that.aIndexes.getQuick(z) == _to || that.bIndexes.getQuick(z) == _to) {
                    clausesToSteal.add(z);
                }
            }

            if (clausesToSteal.size() < zc) {
                if (jc == null) {
                    joinModels.getQuick(to).setContext(jc = contextPool.next());
                }
                jm.setContext(moveClauses(that, jc, clausesToSteal));
                jc.slaveIndex = _to;
                that.parents.remove(_to);
            }
        }
        return true;
    }

    private JoinContext moveClauses(JoinContext from, JoinContext to, IntList positions) {
        int p = 0;
        int m = positions.size();

        if (m == 0) {
            return from;
        }

        JoinContext result = contextPool.next();
        for (int i = 0, n = from.aIndexes.size(); i < n; i++) {
            JoinContext t = p < m && i == positions.getQuick(p) ? to : result;
            t.add(from.aIndexes.getQuick(i), from.aNames.getQuick(i), from.aNodes.getQuick(i), from.bIndexes.getQuick(i), from.bNames.getQuick(i), from.bNodes.getQuick(i));
        }

        return result;
    }

    private void processEmittedConditions() {
        // process emitted join conditions
        do {
            ObjList<JoinContext> conditions = emittedJoinConditions;

            if (conditions == joinConditionsSwap1) {
                emittedJoinConditions = joinConditionsSwap2;
            } else {
                emittedJoinConditions = joinConditionsSwap1;
            }
            emittedJoinConditions.clear();
            for (int i = 0, k = conditions.size(); i < k; i++) {
                addJoinContext(conditions.getQuick(i));
            }
        } while (emittedJoinConditions.size() > 0);

    }

    private void processJoinCombo(int idx, int ai, CharSequence an, ExprNode ao, int bi, CharSequence bn, ExprNode bo) {
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
            deletedContexts.add(idx);
        } else {
            // (different journals)
            emittedJoinConditions.add(contextPool.next().add(ai, an, ao, bi, bn, bo));
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

    private void traversePreOrderRecursive(ExprNode node, JoinContext c) throws ParserException {
        if (node == null) {
            return;
        }

        JoinContext context = null;

        if (c == null && (node.type == ExprNode.NodeType.LITERAL || (node.type == ExprNode.NodeType.OPERATION && !node.token.equals("and")))) {
            context = c = contextPool.next();
        }

        boolean descend = true;

        if (c != null) {
            switch (node.type) {
                case LITERAL:
                    break;
                case FUNCTION:
                    break;
                case OPERATION:
                    switch (node.token) {
                        case "and":
                            break;
                        case "or":
                            c.trivial = false;
                            break;
                        case "=":
                            if (c.trivial) {
                                analyseEquals(node, c);
                                descend = false;
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
        if (descend) {
            if (node.paramCount < 3) {
                traversePreOrderRecursive(node.lhs, c);
                traversePreOrderRecursive(node.rhs, c);
            } else {
                for (int i = 0; i < node.paramCount; i++) {
                    traversePreOrderRecursive(node.args.getQuick(i), c);
                }
            }
        }

        if (context != null && context.slaveIndex > -1) {
            addJoinContext(context);
        }
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
    private void trySwapJoinOrder() {
//        int crossCount = 0;
        int n = joinModels.size();

        for (int i = 0; i < n; i++) {
            JoinModel jm = joinModels.getQuick(i);
            if (jm.getJoinType() != JoinModel.JoinType.OUTER) {
                JoinContext jc = joinModels.getQuick(i).getContext();
                if (jc == null || jc.parents.size() == 0) {
                    // look above i up to OUTER join
                    for (int k = i - 1; k > -1 && move(i, k, jc); k--) ;
                    // look below i for up to OUTER join
                    for (int k = i + 1; k < n && move(i, k, jc); k++) ;
                }

//                // get context again to check if anything moved there
//                jc = joinModels.getQuick(i).getContext();
//                if (jc == null || jc.parents.size() == 0) {
//                    crossCount++;
//                }

            }
        }

//        if (crossCount > 0) {
//            // this is a bit of a hack, "move" will not create context if it is already there
//            // if it wasn't, it would crash with bad list index
//            JoinContext jc = contextPool.next();
//            for (int i = 0; i < n && move(-1, i, jc); i++);
//            if (jc.parents.size() > 0) {
//                current.setContext(jc);
//            }
//        }
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

    }
}