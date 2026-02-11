/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.engine.functions.catalogue.AllTablesFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowDateStyleCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowDefaultTransactionReadOnlyCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowMaxIdentifierLengthCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowParametersCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowSearchPathCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowServerVersionCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowServerVersionNumCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowStandardConformingStringsCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTimeZoneFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTransactionIsolationLevelCursorFactory;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.std.BoolList;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.IntSortedList;
import io.questdb.std.LowerCaseAsciiCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Transient;
import io.questdb.std.Uuid;
import io.questdb.std.str.FlyweightCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION;
import static io.questdb.griffin.SqlCodeGenerator.joinsRequiringTimestamp;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.*;
import static io.questdb.griffin.model.QueryModel.*;
import static io.questdb.std.GenericLexer.unquote;
import static io.questdb.std.Numbers.IPv4_NULL;

public class SqlOptimiser implements Mutable {
    public static final int REWRITE_STATUS_FORCE_INNER_MODEL = 64;
    public static final int REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE = 32;
    public static final int REWRITE_STATUS_USE_DISTINCT_MODEL = 16;
    public static final int REWRITE_STATUS_USE_GROUP_BY_MODEL = 4;
    public static final int REWRITE_STATUS_USE_INNER_MODEL = 1;
    public static final int REWRITE_STATUS_USE_OUTER_MODEL = 8;
    public static final int REWRITE_STATUS_USE_WINDOWS_JOIN_MODE = 128;
    public static final int REWRITE_STATUS_USE_WINDOW_MODEL = 2;
    private static final int JOIN_OP_AND = 2;
    private static final int JOIN_OP_EQUAL = 1;
    private static final int JOIN_OP_OR = 3;
    private static final int JOIN_OP_REGEX = 4;
    private static final String LONG_MAX_VALUE_STR = "" + Long.MAX_VALUE;
    // Maximum depth of nested window functions (e.g., sum(sum(row_number() OVER ()) OVER ()) OVER () is 3 levels)
    private static final int MAX_WINDOW_FUNCTION_NESTING_DEPTH = 8;
    private static final int NOT_OP_AND = 2;
    private static final int NOT_OP_EQUAL = 8;
    private static final int NOT_OP_GREATER = 4;
    private static final int NOT_OP_GREATER_EQ = 5;
    private static final int NOT_OP_LESS = 6;
    private static final int NOT_OP_LESS_EQ = 7;
    private static final int NOT_OP_NOT = 1;
    private static final int NOT_OP_NOT_EQ = 9;
    private static final int NOT_OP_OR = 3;
    // these are bit flags
    private static final int SAMPLE_BY_REWRITE_NO_WRAP = 0;
    private static final int SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES = 2;
    private static final int SAMPLE_BY_REWRITE_WRAP_CONVERT_TIME_ZONE = 4;
    private static final int SAMPLE_BY_REWRITE_WRAP_REMOVE_TIMESTAMP = 1;
    private static final IntHashSet flexColumnModelTypes = new IntHashSet();
    // list of join types that don't support all optimisations (e.g., pushing table-specific predicates to both left and right table)
    private static final IntHashSet joinBarriers;
    private static final CharSequenceIntHashMap joinOps = new CharSequenceIntHashMap();
    private static final IntHashSet limitTypes = new IntHashSet();
    private static final CharSequenceIntHashMap notOps = new CharSequenceIntHashMap();
    private static final CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private final static LowerCaseAsciiCharSequenceHashSet orderedGroupByFunctions;
    protected final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final CharacterStore characterStore;
    private final IntList clausesToSteal = new IntList();
    private final ColumnPrefixEraser columnPrefixEraser = new ColumnPrefixEraser();
    private final CairoConfiguration configuration;
    private final CharSequenceIntHashMap constNameToIndex = new CharSequenceIntHashMap();
    private final CharSequenceObjHashMap<ExpressionNode> constNameToNode = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<CharSequence> constNameToToken = new CharSequenceObjHashMap<>();
    private final ObjectPool<JoinContext> contextPool;
    private final IntHashSet deletedContexts = new IntHashSet();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final FunctionParser functionParser;
    // list of group-by-model-level expressions with prefixes
    // we've to use it because group by is likely to contain rewritten/aliased expressions that make matching input expressions by pure AST unreliable
    private final ObjList<CharSequence> groupByAliases = new ObjList<>();
    private final ObjList<ExpressionNode> groupByNodes = new ObjList<>();
    // Collects inner window models for nested window functions (e.g., sum(row_number() OVER ()) OVER ())
    private final ObjList<QueryModel> innerWindowModels = new ObjList<>();
    private final ObjectPool<IntHashSet> intHashSetPool = new ObjectPool<>(IntHashSet::new, 16);
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final LiteralCheckingVisitor literalCheckingVisitor = new LiteralCheckingVisitor();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final IntHashSet literalCollectorAIndexes = new IntHashSet();
    private final IntHashSet literalCollectorBIndexes = new IntHashSet();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralRewritingVisitor literalRewritingVisitor = new LiteralRewritingVisitor();
    private final int maxRecursion;
    private final AtomicInteger nonAggSelectCount = new AtomicInteger(0);
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private final IntSortedList orderingStack = new IntSortedList();
    private final Path path;
    private final LowerCaseCharSequenceHashSet pivotAliasMap = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceIntHashMap pivotAliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final ObjList<IntHashSet> postFilterTableRefs = new ObjList<>();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    // Reusable hash set for collecting referenced column aliases during pass-through optimization
    private final LowerCaseCharSequenceHashSet referencedAliasesSet = new LowerCaseCharSequenceHashSet();
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjList<RecordCursorFactory> tableFactoriesInFlight = new ObjList<>();
    private final FlyweightCharSequence tableLookupSequence = new FlyweightCharSequence();
    private final IntHashSet tablesSoFar = new IntHashSet();
    private final BoolList tempBoolList = new BoolList();
    private final CharSequenceHashSet tempCharSequenceHashSet = new CharSequenceHashSet();
    private final ObjList<QueryColumn> tempColumns = new ObjList<>();
    private final ObjList<QueryColumn> tempColumns2 = new ObjList<>();
    private final IntList tempCrossIndexes = new IntList();
    private final IntList tempCrosses = new IntList();
    private final LowerCaseCharSequenceIntHashMap tempCursorAliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> tempCursorAliases = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<ExpressionNode> tempExprs = new ObjList<>();
    private final IntHashSet tempIntHashSet = new IntHashSet();
    private final IntList tempIntList = new IntList();
    private final StringSink tmpStringSink = new StringSink();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjList<CharSequence> trivialExpressionCandidates = new ObjList<>();
    private final TrivialExpressionVisitor trivialExpressionVisitor = new TrivialExpressionVisitor();
    private final LowerCaseCharSequenceIntHashMap trivialExpressions = new LowerCaseCharSequenceIntHashMap();
    // Pool of ObjList<QueryColumn> for windowFunctionHashMap values (to avoid allocations)
    private final ObjectPool<ObjList<QueryColumn>> windowColumnListPool = new ObjectPool<>(ObjList::new, 16);
    // Hash map for O(1) window function deduplication lookup: hash -> list of QueryColumns with that hash
    private final IntObjHashMap<ObjList<QueryColumn>> windowFunctionHashMap = new IntObjHashMap<>();
    // Second stack for emitWindowFunctions, separate from sqlNodeStack because
    // replaceIfWindowFunction calls emitLiterals which clears and reuses sqlNodeStack.
    private final ArrayDeque<ExpressionNode> windowNodeStack = new ArrayDeque<>();
    private int defaultAliasCount = 0;
    private ObjList<JoinContext> emittedJoinClauses;
    private OperatorExpression opAnd;
    private OperatorExpression opGeq;
    private OperatorExpression opLt;
    private CharSequence tempColumnAlias;
    private QueryModel tempQueryModel;

    public SqlOptimiser(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo,
            FunctionParser functionParser,
            Path path
    ) {
        this.configuration = configuration;
        this.expressionNodePool = expressionNodePool;
        this.characterStore = characterStore;
        this.traversalAlgo = traversalAlgo;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.functionParser = functionParser;
        this.contextPool = new ObjectPool<>(JoinContext.FACTORY, configuration.getSqlJoinContextPoolCapacity());
        this.path = path;
        this.maxRecursion = configuration.getSqlWindowMaxRecursion();
        initialiseOperatorExpressions();
    }

    /**
     * Checks if an alias appears in the columns and function args of a model.
     * <p>
     * Consider a query:<br>
     * (select x1, sum(x1) from (select x as x1 from y))<br>
     * If you were to move lift the column from the RHS to the LHS, you might get:<br>
     * (select x x1, sum(x1) from y)<br>
     * Now x1 is invalid.
     */
    public static boolean aliasAppearsInFuncArgs(QueryModel model, CharSequence alias, ArrayDeque<ExpressionNode> sqlNodeStack) {
        if (model == null) {
            return false;
        }
        boolean appearsInArgs = false;
        ObjList<QueryColumn> modelColumns = model.getColumns();
        for (int i = 0, n = modelColumns.size(); i < n; i++) {
            QueryColumn col = modelColumns.get(i);
            switch (col.getAst().type) {
                case FUNCTION:
                case OPERATION:
                    appearsInArgs |= searchExpressionNodeForAlias(col.getAst(), alias, sqlNodeStack);
                    break;
            }
        }
        return appearsInArgs;
    }

    public static boolean hasGroupByFunc(ArrayDeque<ExpressionNode> sqlNodeStack, FunctionFactoryCache functionFactoryCache, ExpressionNode node) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case LITERAL:
                        node = null;
                        continue;
                    case FUNCTION:
                        if (functionFactoryCache.isGroupBy(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        for (int i = 0, n = node.args.size(); i < n; i++) {
                            sqlNodeStack.add(node.args.getQuick(i));
                        }
                        if (node.rhs != null) {
                            sqlNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = sqlNodeStack.poll();
            }
        }
        return false;
    }

    public void clear() {
        clearForUnionModelInJoin();
        contextPool.clear();
        intHashSetPool.clear();
        joinClausesSwap1.clear();
        joinClausesSwap2.clear();
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();
        literalCollectorANames.clear();
        literalCollectorBNames.clear();
        defaultAliasCount = 0;
        expressionNodePool.clear();
        characterStore.clear();
        tablesSoFar.clear();
        clausesToSteal.clear();
        tempCursorAliases.clear();
        tempCursorAliasSequenceMap.clear();
        tableFactoriesInFlight.clear();
        groupByAliases.clear();
        groupByNodes.clear();
        tempColumnAlias = null;
        tempQueryModel = null;
        tempIntList.clear();
        tempIntHashSet.clear();
        tempBoolList.clear();
        tempColumns.clear();
        tempColumns2.clear();
        tempCharSequenceHashSet.clear();
        pivotAliasMap.clear();
        pivotAliasSequenceMap.clear();
        tmpStringSink.clear();
        clearWindowFunctionHashMap();
    }

    public void clearForUnionModelInJoin() {
        constNameToIndex.clear();
        constNameToNode.clear();
        constNameToToken.clear();
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionParser.getFunctionFactoryCache();
    }

    public boolean hasOrderedGroupByFunc(ExpressionNode node) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case LITERAL:
                        node = null;
                        continue;
                    case FUNCTION:
                        if (node.token != null && orderedGroupByFunctions.contains(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        for (int i = 0, n = node.args.size(); i < n; i++) {
                            sqlNodeStack.add(node.args.getQuick(i));
                        }
                        if (node.rhs != null) {
                            sqlNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = sqlNodeStack.poll();
            }
        }
        return false;
    }

    private static ExpressionNode concatFilters(
            boolean cairoSqlLegacyOperatorPrecedence,
            ObjectPool<ExpressionNode> expressionNodePool,
            ExpressionNode old,
            ExpressionNode filter
    ) {
        if (old == null) {
            return filter;
        } else {
            OperatorExpression andOp = OperatorExpression.chooseRegistry(cairoSqlLegacyOperatorPrecedence).getOperatorDefinition("and");
            ExpressionNode node = expressionNodePool.next().of(OPERATION, andOp.operator.token, andOp.precedence, filter.position);
            node.paramCount = 2;
            node.lhs = old;
            node.rhs = filter;
            return node;
        }
    }

    private static boolean isOrderedByDesignatedTimestamp(QueryModel model) {
        return model.getTimestamp() != null
                && model.getOrderBy().size() == 1
                && Chars.equals(model.getOrderBy().getQuick(0).token, model.getTimestamp().token);
    }

    private static boolean isSymbolColumn(ExpressionNode countDistinctExpr, QueryModel nested) {
        return countDistinctExpr.rhs.type == LITERAL
                && nested.getAliasToColumnMap().get(countDistinctExpr.rhs.token) != null
                && nested.getAliasToColumnMap().get(countDistinctExpr.rhs.token).getColumnType() == ColumnType.SYMBOL;
    }

    private static void linkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).addDependency(child);
    }

    private static boolean modelIsFlex(QueryModel model) {
        return model != null && flexColumnModelTypes.contains(model.getSelectModelType());
    }

    /**
     * Parses a unit character from a dateadd unit token.
     * Handles both quoted ('h') and unquoted (h) formats.
     *
     * @param unitToken the unit token from the expression
     * @return the unit character, or 0 if parsing fails
     */
    private static char parseUnitCharacter(CharSequence unitToken) {
        if (unitToken == null || unitToken.isEmpty()) {
            return 0;
        }
        int len = unitToken.length();
        if (len == 3 && unitToken.charAt(0) == '\'' && unitToken.charAt(2) == '\'') {
            // Quoted single char: 'h'
            return unitToken.charAt(1);
        } else if (len == 1) {
            // Unquoted single char: h
            return unitToken.charAt(0);
        }
        // Invalid format
        return 0;
    }

    private static boolean printRecordColumnOrNull(Record record, RecordMetadata metadata, StringSink sink, int position) throws SqlException {
        final int columnType = metadata.getColumnType(0);
        sink.clear();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.ARRAY_STRING: {
                final CharSequence val = record.getStrA(0);
                if (val == null) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.SYMBOL: {
                final CharSequence val = record.getSymA(0);
                if (val == null) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.VARCHAR: {
                final var val = record.getVarcharA(0);
                if (val == null) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.INT: {
                final int val = record.getInt(0);
                if (val == Numbers.INT_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.LONG: {
                final long val = record.getLong(0);
                if (val == Numbers.LONG_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.SHORT: {
                // short and byte doesn't have null
                final short val = record.getShort(0);
                sink.put(val);
                return false;
            }
            case ColumnType.BYTE: {
                final byte val = record.getByte(0);
                sink.put(val);
                return false;
            }
            case ColumnType.DOUBLE: {
                final double val = record.getDouble(0);
                if (!Numbers.isFinite(val)) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.FLOAT: {
                final float val = record.getFloat(0);
                if (!Numbers.isFinite(val)) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.DATE: {
                final long val = record.getDate(0);
                if (val == Numbers.LONG_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.putISODateMillis(val);
                return false;
            }
            case ColumnType.TIMESTAMP: {
                final long val = record.getTimestamp(0);
                if (val == Numbers.LONG_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.putISODate(ColumnType.getTimestampDriver(columnType), val);
                return false;
            }
            case ColumnType.CHAR: {
                final char val = record.getChar(0);
                if (val == 0) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.BOOLEAN: {
                sink.put(record.getBool(0));
                return false;
            }
            case ColumnType.NULL: {
                sink.put("NULL");
                return true;
            }
            case ColumnType.GEOBYTE: {
                final byte val = record.getGeoByte(0);
                if (val == GeoHashes.BYTE_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.GEOSHORT: {
                final short val = record.getGeoShort(0);
                if (val == GeoHashes.SHORT_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.GEOINT: {
                final int val = record.getGeoInt(0);
                if (val == GeoHashes.INT_NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.GEOLONG: {
                final long val = record.getGeoLong(0);
                if (val == GeoHashes.NULL) {
                    sink.put("NULL");
                    return true;
                }
                sink.put(val);
                return false;
            }
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID: {
                final long hi = record.getLong128Hi(0);
                final long lo = record.getLong128Lo(0);
                if (Uuid.isNull(lo, hi)) {
                    sink.put("NULL");
                    return true;
                }
                Uuid uuid = new Uuid(lo, hi);
                uuid.toSink(sink);
                return false;
            }
            case ColumnType.IPv4: {
                final int val = record.getIPv4(0);
                if (val == IPv4_NULL) {
                    sink.put("NULL");
                    return true;
                }
                Numbers.intToIPv4Sink(sink, val);
                return false;
            }
            case ColumnType.DECIMAL8: {
                final byte val = record.getDecimal8(0);
                if (val == Decimals.DECIMAL8_NULL) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(val, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            case ColumnType.DECIMAL16: {
                final short val = record.getDecimal16(0);
                if (val == Decimals.DECIMAL16_NULL) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(val, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            case ColumnType.DECIMAL32: {
                final int val = record.getDecimal32(0);
                if (val == Decimals.DECIMAL32_NULL) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(val, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            case ColumnType.DECIMAL64: {
                final long val = record.getDecimal64(0);
                if (val == Decimals.DECIMAL64_NULL) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(val, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            case ColumnType.DECIMAL128: {
                final var decimal = Misc.getThreadLocalDecimal128();
                record.getDecimal128(0, decimal);
                if (decimal.isNull()) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(decimal, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            case ColumnType.DECIMAL256: {
                final var decimal = Misc.getThreadLocalDecimal256();
                record.getDecimal256(0, decimal);
                if (decimal.isNull()) {
                    sink.put("NULL");
                    return true;
                }
                Decimals.append(decimal, ColumnType.getDecimalPrecision(columnType), ColumnType.getDecimalScale(columnType), sink);
                return false;
            }
            default:
                throw SqlException.$(position, "unsupported PIVOT FOR column type: ").put(ColumnType.nameOf(columnType));
        }
    }

    private static void pushDownLimitAdvice(QueryModel model, QueryModel nestedModel, boolean useDistinctModel) {
        if ((nestedModel.getOrderBy().size() == 0 || isOrderedByDesignatedTimestamp(nestedModel)) && !useDistinctModel) {
            nestedModel.setLimitAdvice(model.getLimitLo(), model.getLimitHi());
        }
    }

    /**
     * Recurse down expression node tree looking for an alias.
     */
    private static boolean searchExpressionNodeForAlias(ExpressionNode node, CharSequence alias, ArrayDeque<ExpressionNode> sqlNodeStack) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (Chars.equalsIgnoreCase(node.token, alias)) {
                    return true;
                }
                for (int i = 0, n = node.args.size(); i < n; i++) {
                    sqlNodeStack.add(node.args.getQuick(i));
                }
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                node = sqlNodeStack.poll();
            }
        }
        return false;
    }

    private static void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    private void addColumnToSelectModel(QueryModel model, IntList insertColumnIndexes, ObjList<QueryColumn> insertColumnAliases, CharSequence timestampAlias) {
        tempColumns2.clear();
        tempColumns2.addAll(model.getBottomUpColumns());
        model.clearColumnMapStructs();

        // These are merged columns; the assumption is that the insetColumnIndexes are ordered.
        // This loop will fail miserably in indexes are unordered.
        int src1ColumnCount = tempColumns2.size();
        int src2ColumnCount = insertColumnIndexes.size();
        for (int i = 0, k = 0, m = 0; i < src1ColumnCount || k < src2ColumnCount; m++) {
            if (k < src2ColumnCount && insertColumnIndexes.getQuick(k) == m) {
                QueryColumn column = insertColumnAliases.get(k);
                // insert column at this position, this column must reference our timestamp, that
                // comes out of the group-by result set, but with user-provided aliases.
                if (column.getAst().type == LITERAL) {
                    model.addBottomUpColumnIfNotExists(nextColumn(column.getAlias(), timestampAlias));
                } else {
                    model.addBottomUpColumnIfNotExists(column);
                }
                k++;
            } else {
                QueryColumn qcFrom = tempColumns2.getQuick(i);
                model.addBottomUpColumnIfNotExists(nextColumn(qcFrom.getAlias()));
                i++;
            }
        }
    }

    /*
     * Uses validating model to determine if column name exists and non-ambiguous in case of using joins.
     */
    private void addColumnToTranslatingModel(
            QueryColumn column,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel
    ) throws SqlException {
        if (baseModel != null) {
            final CharSequence refColumn = column.getAst().token;
            final int dot = Chars.indexOfLastUnquoted(refColumn, '.');
            validateColumnAndGetModelIndex(baseModel, innerVirtualModel, refColumn, dot, column.getAst().position, false);
            // when we have only one model, e.g. this is not a join,
            // and there is a table alias to lookup column;
            // we will remove this alias as unneeded
            if (dot != -1 && baseModel.getJoinModels().size() == 1) {
                ExpressionNode base = column.getAst();
                column.of(
                        column.getAlias(),
                        expressionNodePool.next().of(
                                base.type,
                                base.token.subSequence(dot + 1, base.token.length()),
                                base.precedence,
                                base.position
                        )
                );
            }
        }
        translatingModel.addBottomUpColumn(column);
    }

    private QueryColumn addCursorFunctionAsCrossJoin(
            ExpressionNode node,
            @Nullable CharSequence alias,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryColumn qc = cursorModel.findBottomUpColumnByAst(node);
        if (qc == null) {
            // we are about to add a new column as a join model to the base model
            // the name of this column must not clash with any name of the base mode
            CharSequence baseAlias;
            if (alias != null) {
                baseAlias = createColumnAlias(alias, baseModel);
            } else {
                baseAlias = createColumnAlias(node, baseModel);
            }

            // add to temp aliases so that two cursors cannot use the same alias!
            baseAlias = SqlUtil.createColumnAlias(characterStore, baseAlias, -1, tempCursorAliases, tempCursorAliasSequenceMap, false);

            final QueryColumn crossColumn = queryColumnPool.next().of(baseAlias, node);

            final QueryModel cross = queryModelPool.next();
            cross.setJoinType(JOIN_CROSS);
            cross.setSelectModelType(SELECT_MODEL_CURSOR);
            cross.setAlias(makeJoinAlias());

            final QueryModel crossInner = queryModelPool.next();
            crossInner.setTableNameExpr(node);
            parseFunctionAndEnumerateColumns(crossInner, sqlExecutionContext, sqlParserCallback);
            cross.setNestedModel(crossInner);

            cross.addBottomUpColumn(crossColumn);
            baseModel.addJoinModel(cross);

            // keep track of duplicates
            tempCursorAliases.put(baseAlias, crossColumn);

            // now we need to make alias in the translating column

            CharSequence translatingAlias;
            translatingAlias = createColumnAlias(baseAlias, translatingModel);

            // add trackable expression to a cursor model
            cursorModel.addBottomUpColumn(queryColumnPool.next().of(baseAlias, node));

            qc = queryColumnPool.next().of(translatingAlias, nextLiteral(baseAlias));
            translatingModel.addBottomUpColumn(qc);

        } else {
            final CharSequence al = translatingModel.getColumnNameToAliasMap().get(qc.getAlias());
            if (alias != null && !Chars.equalsIgnoreCase(al, alias)) {
                QueryColumn existing = translatingModel.getAliasToColumnMap().get(alias);
                if (existing == null) {
                    // create new column
                    qc = nextColumn(alias, al);
                    translatingModel.addBottomUpColumn(qc);

                    if (innerVirtualModel != null) {
                        innerVirtualModel.addBottomUpColumn(qc);
                    }

                    return qc;
                }

                if (compareNodesExact(node, existing.getAst())) {
                    return existing;
                }

                throw SqlException.invalidColumn(node.position, "duplicate alias");
            }

            // check if column is in an inner virtual model as requested
            qc = translatingModel.getAliasToColumnMap().get(al);
        }
        if (innerVirtualModel != null) {
            innerVirtualModel.addBottomUpColumn(qc);
        }
        return qc;
    }

    private void addFilterOrEmitJoin(QueryModel parent, int idx, int ai, CharSequence an, ExpressionNode ao, int bi, CharSequence bn, ExpressionNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(idx);
            return;
        }

        if (ai == bi) {
            // (same table)
            OperatorExpression eqOp = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition("=");
            ExpressionNode node = expressionNodePool.next().of(OPERATION, eqOp.operator.token, eqOp.precedence, 0);
            node.paramCount = 2;
            node.lhs = ao;
            node.rhs = bo;
            addWhereNode(parent, ai, node);
        } else {
            // (different tables)
            JoinContext jc = contextPool.next();
            jc.aIndexes.add(ai);
            jc.aNames.add(an);
            jc.aNodes.add(ao);
            jc.bIndexes.add(bi);
            jc.bNames.add(bn);
            jc.bNodes.add(bo);
            jc.slaveIndex = Math.max(ai, bi);
            jc.parents.add(Math.min(ai, bi));
            emittedJoinClauses.add(jc);
        }

        deletedContexts.add(idx);
    }

    private void addFunction(
            QueryColumn qc,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerVirtualModel,
            QueryModel distinctModel
    ) throws SqlException {
        // Adds what intended to be a function (rather than a literal) to the
        // inner virtual model. It is possible that the function will have
        // the same alias as the existing table columns. We will "temporarily"
        // rename the function alias, so it does not clash. And after that,
        // the "innerColumn" will restore the alias specified by the user. This
        // alias comes on the `qc.getAlias()`.

        QueryColumn virtualColumn = ensureAliasUniqueness(innerVirtualModel, qc);
        innerVirtualModel.addBottomUpColumn(virtualColumn);

        // we also create a column that references this inner layer from outer layer,
        // for example when we have:
        // select a, b+c ...
        // it should translate to:
        // select a, x from (select a, b+c x from (select a,b,c ...))
        final QueryColumn innerColumn = nextColumn(qc.getAlias(), virtualColumn.getAlias());

        // pull literals only into a translating model
        emitLiterals(qc.getAst(), translatingModel, innerVirtualModel, false, baseModel, false);
        groupByModel.addBottomUpColumn(innerColumn);
        windowModel.addBottomUpColumn(innerColumn);
        outerVirtualModel.addBottomUpColumn(innerColumn);
        distinctModel.addBottomUpColumn(innerColumn);
    }

    private void addJoinContext(QueryModel parent, JoinContext context) {
        QueryModel jm = parent.getJoinModels().getQuick(context.slaveIndex);
        JoinContext other = jm.getJoinContext();
        if (other == null || other.slaveIndex == -1) {
            jm.setContext(context);
        } else {
            jm.setContext(mergeContexts(parent, other, context));
        }
    }

    // add table prefix to all column references to make it easier to compare expressions
    private void addMissingTablePrefixesForGroupByQueries(ExpressionNode node, QueryModel baseModel, QueryModel innerVirtualModel) throws SqlException {
        sqlNodeStack.clear();

        ExpressionNode temp = addMissingTablePrefixesForGroupByQueries0(node, baseModel, innerVirtualModel);
        if (temp != node) {
            node.of(LITERAL, temp.token, node.precedence, node.position);
            return;
        }

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        temp = addMissingTablePrefixesForGroupByQueries0(node.rhs, baseModel, innerVirtualModel);
                        if (node.rhs == temp) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = temp;
                        }
                    }

                    if (node.lhs != null) {
                        temp = addMissingTablePrefixesForGroupByQueries0(node.lhs, baseModel, innerVirtualModel);
                        if (temp == node.lhs) {
                            node = node.lhs;
                        } else {
                            node.lhs = temp;
                            node = null;
                        }
                    } else {
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        temp = addMissingTablePrefixesForGroupByQueries0(e, baseModel, innerVirtualModel);
                        if (e == temp) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, temp);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    temp = addMissingTablePrefixesForGroupByQueries0(e, baseModel, innerVirtualModel);
                    if (e == temp) {
                        node = e;
                    } else {
                        node.args.setQuick(0, temp);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private ExpressionNode addMissingTablePrefixesForGroupByQueries0(
            ExpressionNode node,
            QueryModel baseModel,
            QueryModel innerVirtualModel
    ) throws SqlException {
        if (node != null && node.type == LITERAL) {
            CharSequence col = node.token;
            final int dot = Chars.indexOfLastUnquoted(col, '.');
            int modelIndex = validateColumnAndGetModelIndex(baseModel, innerVirtualModel, col, dot, node.position, true);
            boolean addAlias = dot == -1 && baseModel.getJoinModels().size() > 1;
            boolean removeAlias = dot > -1 && baseModel.getJoinModels().size() == 1;

            if (addAlias || removeAlias) {
                CharacterStoreEntry entry = characterStore.newEntry();
                if (addAlias) {
                    CharSequence alias = baseModel.getModelAliasIndexes().keys().get(modelIndex);
                    entry.put(alias).put('.').put(col);
                } else {
                    entry.put(col, dot + 1, col.length());
                }
                CharSequence prefixedCol = entry.toImmutable();
                return expressionNodePool.next().of(LITERAL, prefixedCol, node.precedence, node.position);
            }
        }
        return node;
    }

    private void addOuterJoinExpression(QueryModel parent, QueryModel model, int joinIndex, ExpressionNode node) {
        model.setOuterJoinExpressionClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, model.getOuterJoinExpressionClause(), node));
        // add dependency to prevent previous model reordering (left/right outer joins are not symmetric)
        if (joinIndex > 0) {
            linkDependencies(parent, joinIndex - 1, joinIndex);
        }
    }

    private void addPivotAggregatesToInnerModel(QueryModel model, QueryModel innerModel) throws SqlException {
        for (int i = 0, n = model.getPivotGroupByColumns().size(); i < n; i++) {
            QueryColumn qc = model.getPivotGroupByColumns().getQuick(i);
            innerModel.addBottomUpColumn(qc);
        }
    }

    private void addPivotColumnsToOuterModel(int totalCombinations, QueryModel model, QueryModel outerModel) throws SqlException {
        ObjList<PivotForColumn> pivotForColumns = model.getPivotForColumns();
        ObjList<QueryColumn> pivotAggregates = model.getPivotGroupByColumns();
        int forColumnCount = pivotForColumns.size();

        IntList indices = tempCrosses;
        indices.clear();
        for (int i = 0; i < forColumnCount; i++) {
            indices.add(0);
        }

        for (int combo = 0; combo < totalCombinations; combo++) {
            for (int k = 0, p = pivotAggregates.size(); k < p; k++) {
                QueryColumn aggColumn = pivotAggregates.getQuick(k);
                CharSequence aggAlias = aggColumn.getAlias();
                int position = aggColumn.getAst().position;
                ExpressionNode conditionNode = null;
                CharacterStoreEntry cse = characterStore.newEntry();

                for (int i = 0; i < forColumnCount; i++) {
                    PivotForColumn pivotForColumn = pivotForColumns.getQuick(i);
                    ObjList<ExpressionNode> valueList = pivotForColumn.getValueList();
                    int valueIndex = indices.getQuick(i);
                    ExpressionNode valueExpr = valueList.getQuick(valueIndex);
                    ExpressionNode colCondition = expressionNodePool.next().of(OPERATION, "=", 0, position);
                    colCondition.paramCount = 2;
                    colCondition.lhs = expressionNodePool.next().of(LITERAL, pivotForColumn.getInExprAlias(), 0, position);
                    colCondition.rhs = valueExpr;
                    CharSequence colAlias = pivotForColumn.getValueAliases().getQuick(valueIndex);

                    if (conditionNode == null) {
                        conditionNode = colCondition;
                    } else {
                        ExpressionNode andNode = expressionNodePool.next().of(OPERATION, "and", 0, position);
                        andNode.paramCount = 2;
                        andNode.lhs = conditionNode;
                        andNode.rhs = colCondition;
                        conditionNode = andNode;
                    }

                    if (i > 0) {
                        cse.put('_');
                    }
                    cse.put(colAlias);
                }

                if (!model.isPivotGroupByColumnHasNoAlias()) {
                    cse.put('_').put(aggAlias);
                }

                // Use switch when: single FOR column, single constant value (not ELSE mode)
                ExpressionNode thenNode = expressionNodePool.next().of(LITERAL, aggAlias, 0, position);
                boolean isCount = Chars.equalsIgnoreCase(aggColumn.getAst().token, "count") || Chars.equalsIgnoreCase(aggColumn.getAst().token, "count_distinct");
                ExpressionNode defaultNode;
                if (isCount) {
                    defaultNode = expressionNodePool.next().of(CONSTANT, "0", 0, position);
                } else {
                    defaultNode = expressionNodePool.next().of(CONSTANT, "null", 0, position);
                }

                ExpressionNode caseNode;
                PivotForColumn firstForColumn = pivotForColumns.getQuick(0);
                int firstValueIndex = indices.getQuick(0);

                if (forColumnCount == 1) { // use switch function
                    caseNode = expressionNodePool.next().of(FUNCTION, "switch", 0, position);
                    caseNode.paramCount = 4;
                    caseNode.args.add(defaultNode);
                    caseNode.args.add(thenNode);
                    caseNode.args.add(firstForColumn.getValueList().getQuick(firstValueIndex));
                    caseNode.args.add(expressionNodePool.next().of(LITERAL, firstForColumn.getInExprAlias(), 0, position));
                } else {
                    caseNode = expressionNodePool.next().of(FUNCTION, "case", 0, position);
                    caseNode.paramCount = 3;
                    caseNode.args.add(defaultNode);
                    caseNode.args.add(thenNode);
                    caseNode.args.add(conditionNode);
                }

                ExpressionNode aggNode = expressionNodePool.next().of(
                        FUNCTION,
                        isCount ? "sum" : "first_not_null",
                        0,
                        position
                );
                aggNode.paramCount = 1;
                aggNode.rhs = caseNode;

                outerModel.addBottomUpColumn(queryColumnPool.next().of(cse.toImmutable(), aggNode));
            }

            for (int i = forColumnCount - 1; i >= 0; i--) {
                PivotForColumn pivotForColumn = pivotForColumns.getQuick(i);
                int newIndex = indices.getQuick(i) + 1;
                if (newIndex < pivotForColumn.getValueList().size()) {
                    indices.setQuick(i, newIndex);
                    break;
                }
                indices.setQuick(i, 0);
            }
        }
    }

    private void addPostJoinWhereClause(QueryModel model, ExpressionNode node) {
        model.setPostJoinWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, model.getPostJoinWhereClause(), node));
    }

    private void addTimestampToProjection(
            CharSequence columnName,
            ExpressionNode columnAst,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel windowModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        LowerCaseCharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence innerAlias = createColumnAlias(columnName, innerVirtualModel);
            final QueryColumn translatedColumn = nextColumn(innerAlias, translatedColumnName);
            innerVirtualModel.addBottomUpColumn(translatedColumn);

            // window model is used together with the inner model
            final CharSequence windowAlias = createColumnAlias(innerAlias, windowModel);
            final QueryColumn windowColumn = nextColumn(windowAlias, innerAlias);
            windowModel.addBottomUpColumn(windowColumn);
        } else {
            final CharSequence alias = createColumnAlias(columnName, translatingModel);
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(alias, columnAst),
                    translatingModel,
                    innerVirtualModel,
                    baseModel
            );

            final QueryColumn translatedColumn = nextColumn(alias);

            // create column that references inner alias we just created
            innerVirtualModel.addBottomUpColumn(translatedColumn);
            windowModel.addBottomUpColumn(translatedColumn);
        }
    }

    private void addTopDownColumn(@Transient ExpressionNode node, QueryModel model) {
        if (node != null && node.type == LITERAL) {
            final CharSequence columnName = node.token;
            final int dotIndex = Chars.indexOfLastUnquoted(columnName, '.');
            if (dotIndex == -1) {
                // When there is no dot in column name, it is still possible that column comes from
                // one of the join models. What we need to do here is to assign column to that model
                // which already have this column in alias map
                addTopDownColumn(columnName, model);
            } else {
                int modelIndex = model.getModelAliasIndex(node.token, 0, dotIndex);
                if (modelIndex < 0) {
                    // alias cannot be resolved; we will trust that the calling side will handle this
                    // in this context we do not have a model that is able to resolve table alias
                    return;
                }

                addTopDownColumn0(
                        node,
                        model.getJoinModels().getQuick(modelIndex),
                        node.token.subSequence(dotIndex + 1, node.token.length())
                );
            }
        }
    }

    private void addTopDownColumn(CharSequence columnName, QueryModel model) {
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        final int joinCount = joinModels.size();
        for (int i = 0; i < joinCount; i++) {
            final QueryModel m = joinModels.getQuick(i);
            final QueryColumn column = m.getAliasToColumnMap().get(columnName);
            if (column != null) {
                switch (m.getSelectModelType()) {
                    case QueryModel.SELECT_MODEL_NONE:
                        m.addTopDownColumn(queryColumnPool.next().of(columnName, nextLiteral(columnName)), columnName);
                        break;
                    case QueryModel.SELECT_MODEL_VIRTUAL:
                        tempCharSequenceHashSet.clear();
                        collectSameModelProjectionColumns(column.getAst(), m);
                        m.addTopDownColumn(column, columnName);
                        break;
                    default:
                        m.addTopDownColumn(column, columnName);
                }
                break;
            }
        }
    }

    private void addTopDownColumn0(@Transient ExpressionNode node, QueryModel model, CharSequence name) {
        if (model.isTopDownNameMissing(name)) {
            model.addTopDownColumn(
                    queryColumnPool.next().of(
                            name,
                            expressionNodePool.next().of(node.type, name, node.precedence, node.position)
                    ),
                    name
            );
        }
    }

    /**
     * Adds filters derived from transitivity of equals operation, for example,
     * if there is filter:
     * <p>
     * a.x = b.x and b.x = 10
     * <p>
     * derived filter would be:
     * <p>
     * a.x = 10
     * <p>
     * this filter is not explicitly mentioned, but it might help pre-filtering record sources
     * before hashing.
     */
    private void addTransitiveFilters(QueryModel model) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getJoinContext();
            if (jc != null) {
                for (int k = 0, kn = jc.bNames.size(); k < kn; k++) {
                    CharSequence name = jc.bNames.getQuick(k);
                    if (constNameToIndex.get(name) == jc.bIndexes.getQuick(k)) {
                        OperatorExpression op = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition(constNameToToken.get(name));
                        ExpressionNode node = expressionNodePool.next().of(OPERATION, op.operator.token, op.precedence, 0);
                        node.lhs = jc.aNodes.getQuick(k);
                        node.rhs = constNameToNode.get(name);
                        node.paramCount = 2;
                        addWhereNode(model, jc.slaveIndex, node);
                    }
                }
            }
        }
    }

    private void addWhereNode(QueryModel model, int joinModelIndex, ExpressionNode node) {
        addWhereNode(model.getJoinModels().getQuick(joinModelIndex), node);
    }

    private void addWhereNode(QueryModel model, ExpressionNode node) {
        model.setWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, model.getWhereClause(), node));
    }

    /**
     * Move fields that belong to slave table to left and parent fields
     * to right of equals operator.
     */
    private void alignJoinClauses(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getJoinContext();
            if (jc != null) {
                int index = jc.slaveIndex;
                for (int k = 0, kc = jc.aIndexes.size(); k < kc; k++) {
                    if (jc.aIndexes.getQuick(k) != index) {
                        int idx = jc.aIndexes.getQuick(k);
                        CharSequence name = jc.aNames.getQuick(k);
                        ExpressionNode node = jc.aNodes.getQuick(k);

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

    /**
     * Checks that all the order by advice tokens appear as columns for this join model.
     *
     * @param model         model to check
     * @param orderByAdvice advice
     * @return all orders by advice appear in the model columns list
     */
    private boolean allAdviceIsForThisTable(QueryModel model, ObjList<ExpressionNode> orderByAdvice) {
        CharSequence alias;
        LowerCaseCharSequenceObjHashMap<QueryColumn> columnMap = model.getAliasToColumnMap();
        for (int i = 0, n = orderByAdvice.size(); i < n; i++) {
            alias = orderByAdvice.getQuick(i).token;
            if (!columnMap.contains(alias)) {
                return false;
            }
        }
        return true;
    }

    //checks join equality condition and pushes it to optimal join contexts (could be a different join context)
    //NOTE on LEFT/RIGHT JOIN :
    // - left/right join condition MUST remain as is otherwise it'll produce wrong results
    // - only predicates relating to LEFT table may be pushed down
    // - predicates on both or right table may be added to post join clause as long as they're marked properly (via ExpressionNode.isOuterJoinPredicate)
    private void analyseEquals(QueryModel parent, ExpressionNode node, boolean innerPredicate, QueryModel joinModel, int joinIndex) throws SqlException {
        traverseNamesAndIndices(parent, node);
        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        JoinContext jc;
        boolean canMovePredicate = joinBarriers.excludes(joinModel.getJoinType());
        //the switch code below assumes expression are simple column references
        if (literalCollector.functionCount > 0) {
            node.innerPredicate = innerPredicate;
            if (canMovePredicate) {
                parent.addParsedWhereNode(node, innerPredicate);
            } else {
                addOuterJoinExpression(parent, joinModel, joinIndex, node);
            }
            return;
        }

        switch (aSize) {
            case 0:
                if (!canMovePredicate) {
                    addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    break;
                }
                if (bSize == 1
                        && literalCollector.nullCount == 0
                        // the table must not be OUTER or ASOF joined
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorBIndexes.get(0)).getJoinType())
                ) {
                    // single table reference + constant
                    jc = contextPool.next();
                    jc.slaveIndex = literalCollectorBIndexes.get(0);

                    addWhereNode(parent, jc.slaveIndex, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorBNames.getQuick(0);
                    constNameToIndex.put(cs, jc.slaveIndex);
                    constNameToNode.put(cs, node.lhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node, innerPredicate);
                }
                break;
            case 1:
                jc = contextPool.next();
                int lhi = literalCollectorAIndexes.get(0);
                if (bSize == 1) {
                    int rhi = literalCollectorBIndexes.get(0);
                    if (lhi == rhi) {
                        // single table reference
                        jc.slaveIndex = lhi;
                        if (canMovePredicate) {
                            // we can't push anything into another left/right join
                            if (jc.slaveIndex != joinIndex &&
                                    joinBarriers.contains(parent.getJoinModels().get(jc.slaveIndex).getJoinType())) {
                                addPostJoinWhereClause(parent.getJoinModels().getQuick(jc.slaveIndex), node);
                            } else {
                                addWhereNode(parent, lhi, node);
                            }
                            return;
                        }
                    } else if (lhi < rhi) {
                        // we must align "a" nodes with slave index
                        // compiler will always be checking "a" columns
                        // against metadata of the slave the context is assigned to
                        jc.aNodes.add(node.lhs);
                        jc.bNodes.add(node.rhs);
                        jc.aNames.add(literalCollectorANames.getQuick(0));
                        jc.bNames.add(literalCollectorBNames.getQuick(0));
                        jc.aIndexes.add(lhi);
                        jc.bIndexes.add(rhi);
                        jc.slaveIndex = rhi;
                        jc.parents.add(lhi);
                    } else {
                        jc.aNodes.add(node.rhs);
                        jc.bNodes.add(node.lhs);
                        jc.aNames.add(literalCollectorBNames.getQuick(0));
                        jc.bNames.add(literalCollectorANames.getQuick(0));
                        jc.aIndexes.add(rhi);
                        jc.bIndexes.add(lhi);
                        jc.slaveIndex = lhi;
                        jc.parents.add(rhi);
                    }

                    if (canMovePredicate || (jc.slaveIndex == joinIndex && parent.getJoinModels().get(joinIndex).getJoinType() != JOIN_WINDOW)) {
                        //we can't push anything into another left/right join
                        if (jc.slaveIndex != joinIndex && joinBarriers.contains(parent.getJoinModels().get(jc.slaveIndex).getJoinType())) {
                            addPostJoinWhereClause(parent.getJoinModels().getQuick(jc.slaveIndex), node);
                        } else {
                            addJoinContext(parent, jc);
                            if (lhi != rhi) {
                                linkDependencies(parent, Math.min(lhi, rhi), Math.max(lhi, rhi));
                            }
                        }
                    } else {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    }
                } else if (bSize == 0
                        && literalCollector.nullCount == 0
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorAIndexes.get(0)).getJoinType())) {
                    // single table reference + constant
                    if (!canMovePredicate) {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                        break;
                    }
                    jc.slaveIndex = lhi;
                    addWhereNode(parent, lhi, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorANames.getQuick(0);
                    constNameToIndex.put(cs, lhi);
                    constNameToNode.put(cs, node.rhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    if (canMovePredicate) {
                        parent.addParsedWhereNode(node, innerPredicate);
                    } else {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    }
                }
                break;
            default:
                if (canMovePredicate) {
                    node.innerPredicate = innerPredicate;
                    parent.addParsedWhereNode(node, innerPredicate);
                } else {
                    addOuterJoinExpression(parent, joinModel, joinIndex, node);
                }

                break;
        }
    }

    private void analyseRegex(QueryModel parent, ExpressionNode node) throws SqlException {
        traverseNamesAndIndices(parent, node);

        if (literalCollector.nullCount == 0) {
            int aSize = literalCollectorAIndexes.size();
            int bSize = literalCollectorBIndexes.size();
            if (aSize == 1 && bSize == 0) {
                CharSequence name = literalCollectorANames.getQuick(0);
                constNameToIndex.put(name, literalCollectorAIndexes.get(0));
                constNameToNode.put(name, node.rhs);
                constNameToToken.put(name, node.token);
            }
        }
    }

    private void assignFilters(QueryModel parent) throws SqlException {
        tablesSoFar.clear();
        postFilterRemoved.clear();
        postFilterTableRefs.clear();

        literalCollector.withModel(parent);
        ObjList<ExpressionNode> filterNodes = parent.getParsedWhere();
        // collect table indexes from each part of global filter
        int pc = filterNodes.size();
        for (int i = 0; i < pc; i++) {
            IntHashSet indexes = intHashSetPool.next();
            literalCollector.resetCounts();
            traversalAlgo.traverse(filterNodes.getQuick(i), literalCollector.to(indexes));
            postFilterTableRefs.add(indexes);
        }

        IntList ordered = parent.getOrderedJoinModels();
        // match table references to a set of table in join order
        for (int i = 0, n = ordered.size(); i < n; i++) {
            int index = ordered.getQuick(i);
            tablesSoFar.add(index);

            for (int k = 0; k < pc; k++) {
                if (postFilterRemoved.contains(k)) {
                    continue;
                }

                final ExpressionNode node = filterNodes.getQuick(k);

                IntHashSet refs = postFilterTableRefs.getQuick(k);
                int rs = refs.size();
                if (rs == 0) {
                    // condition has no table references
                    postFilterRemoved.add(k);
                    parent.setConstWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, parent.getConstWhereClause(), node));
                } else if (rs == 1 && // single table reference and this table is not joined via OUTER or ASOF
                        joinBarriers.excludes(parent.getJoinModels().getQuick(refs.get(0)).getJoinType())) {
                    // get single table reference out of the way right away
                    // we don't have to wait until "our" table comes along
                    addWhereNode(parent, refs.get(0), node);
                    postFilterRemoved.add(k);
                } else {
                    boolean qualifies = true;
                    // check if filter references table processed so far
                    for (int y = 0; y < rs; y++) {
                        if (tablesSoFar.excludes(refs.get(y))) {
                            qualifies = false;
                            break;
                        }
                    }
                    if (qualifies) {
                        postFilterRemoved.add(k);
                        QueryModel m = parent.getJoinModels().getQuick(index);
                        m.setPostJoinWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, m.getPostJoinWhereClause(), node));
                    }
                }
            }
        }
        assert postFilterRemoved.size() == pc;
    }

    // The model for the following SQL:
    // select * from t1 union all select * from t2 order by x
    // will have "order by" clause on the last model of the union linked list.
    // Semantically, order by must be executed after union. To get there, we will
    // create outer model(s) for the union block and move "order by" there.
    private QueryModel bubbleUpOrderByAndLimitFromUnion(QueryModel model) throws SqlException {
        QueryModel m = model.getUnionModel();
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            QueryModel _n = bubbleUpOrderByAndLimitFromUnion(nested);
            if (_n != nested) {
                model.setNestedModel(_n);
            }
        }

        if (m != null) {
            // find order by clauses
            if (m.getNestedModel() != null) {
                final QueryModel m1 = bubbleUpOrderByAndLimitFromUnion(m.getNestedModel());
                if (m1 != m) {
                    m.setNestedModel(m1);
                }
            }

            do {
                if (m.getUnionModel() == null) {
                    // last model in the linked list
                    QueryModel un = m.getNestedModel();
                    if (un != null) {
                        int n = un.getOrderBy().size();
                        // order by clause is on the nested model
                        final ObjList<ExpressionNode> orderBy = un.getOrderBy();
                        final IntList orderByDirection = un.getOrderByDirection();
                        // limit is on the parent model
                        final ExpressionNode limitLo = m.getLimitLo();
                        final ExpressionNode limitHi = m.getLimitHi();

                        if (n > 0 || limitHi != null || limitLo != null) {
                            // we have some order by clauses to move
                            QueryModel _nested = queryModelPool.next();
                            for (int i = 0; i < n; i++) {
                                _nested.addOrderBy(orderBy.getQuick(i), orderByDirection.getQuick(i));
                            }
                            orderBy.clear();
                            orderByDirection.clear();
                            m.setLimit(null, null);
                            _nested.setNestedModel(model);
                            QueryModel _model = queryModelPool.next();
                            _model.setNestedModel(_nested);
                            SqlUtil.addSelectStar(_model, queryColumnPool, expressionNodePool);
                            _model.setLimit(limitLo, limitHi);
                            return _model;
                        }
                    }
                    break;
                }

                m = m.getUnionModel();
            } while (true);
        }
        return model;
    }

    // pushing predicates to sample by model is only allowed for sample by fill none align to calendar and expressions on non-timestamp columns
    // pushing for other fill options or sample by first observation could alter a result
    private boolean canPushToSampleBy(final QueryModel model, ObjList<CharSequence> expressionColumns) {
        ObjList<ExpressionNode> fill = model.getSampleByFill();
        int fillCount = fill.size();
        boolean isFillNone = fillCount == 0 || (fillCount == 1 && isNoneKeyword(fill.getQuick(0).token));

        if (!isFillNone || model.getSampleByOffset() == null) {
            return false;
        }

        CharSequence timestamp = findTimestamp(model);
        if (timestamp == null) {
            return true;
        }

        for (int i = 0, n = expressionColumns.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(expressionColumns.get(i), timestamp)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkForChildAggregates(ExpressionNode node) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.paramCount < 3) {
                if (node.rhs != null) {
                    // Skip window functions - they have windowContext set even if function name is also aggregate
                    if (node.rhs.type == FUNCTION && node.rhs.windowExpression == null
                            && functionParser.getFunctionFactoryCache().isGroupBy(node.rhs.token)) {
                        return true;
                    }
                    sqlNodeStack.push(node.rhs);
                }

                if (node.lhs != null) {
                    // Skip window functions - they have windowContext set even if function name is also aggregate
                    if (node.lhs.type == FUNCTION && node.lhs.windowExpression == null
                            && functionParser.getFunctionFactoryCache().isGroupBy(node.lhs.token)) {
                        return true;
                    }
                    node = node.lhs;
                } else if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            } else {
                // for nodes with paramCount >= 3, arguments are stored in args list (e.g., CASE expressions)
                for (int i = 0, k = node.paramCount; i < k; i++) {
                    ExpressionNode arg = node.args.getQuick(i);
                    if (arg != null) {
                        // Skip window functions - they have windowContext set even if function name is also aggregate
                        if (arg.type == FUNCTION && arg.windowExpression == null
                                && functionParser.getFunctionFactoryCache().isGroupBy(arg.token)) {
                            return true;
                        }
                        sqlNodeStack.push(arg);
                    }
                }
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }
        return false;
    }

    private boolean checkForChildWindowFunctions(ExpressionNode node) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.windowExpression != null) {
                return true;
            }
            if (node.paramCount < 3) {
                if (node.rhs != null) {
                    if (node.rhs.windowExpression != null) {
                        return true;
                    }
                    sqlNodeStack.push(node.rhs);
                }

                if (node.lhs != null) {
                    if (node.lhs.windowExpression != null) {
                        return true;
                    }
                    node = node.lhs;
                } else if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            } else {
                // for nodes with paramCount >= 3, arguments are stored in args list (e.g., CASE expressions)
                for (int i = 0, k = node.paramCount; i < k; i++) {
                    ExpressionNode arg = node.args.getQuick(i);
                    if (arg != null) {
                        if (arg.windowExpression != null) {
                            return true;
                        }
                        sqlNodeStack.push(arg);
                    }
                }
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }
        return false;
    }

    /**
     * Checks whether the given advice is for one table only i.e. consistent table prefix.
     *
     * @param orderByAdvice the given advice
     * @return whether the prefix is consistent or not
     */
    private boolean checkForConsistentPrefix(ObjList<ExpressionNode> orderByAdvice) {
        CharSequence prefix = "";
        for (int i = 0, n = orderByAdvice.size(); i < n; i++) {
            CharSequence token = orderByAdvice.getQuick(i).token;
            int loc = Chars.indexOfLastUnquoted(token, '.');
            if (loc > -1) {
                if (prefix.isEmpty()) {
                    prefix = token.subSequence(0, loc);
                } else if (!Chars.equalsIgnoreCase(prefix, token, 0, loc)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks for a dot in the token.
     *
     * @param orderByAdvice the given advice
     * @return whether dot is present or not
     */
    private boolean checkForDot(ObjList<ExpressionNode> orderByAdvice) {
        for (int i = 0, n = orderByAdvice.size(); i < n; i++) {
            if (Chars.indexOfLastUnquoted(orderByAdvice.getQuick(i).token, '.') > -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the expression tree contains window functions (with OVER clause)
     * or pure window function names (without OVER clause, e.g., row_number, rank).
     * This is used for validation where the OVER clause might have been lost during parsing.
     * Functions that can be both aggregates and window functions (like sum, count) are not
     * flagged here unless they have an OVER clause.
     */
    private boolean checkForWindowFunctionsOrNames(ExpressionNode node) {
        FunctionFactoryCache cache = functionParser.getFunctionFactoryCache();
        sqlNodeStack.clear();
        while (node != null) {
            if (node.windowExpression != null) {
                return true;
            }
            // Check for pure window function names (row_number, rank, etc.) - not aggregate functions
            if (node.type == FUNCTION && cache.isPureWindowFunction(node.token)) {
                return true;
            }
            if (node.paramCount < 3) {
                if (node.rhs != null) {
                    if (node.rhs.windowExpression != null ||
                            (node.rhs.type == FUNCTION && cache.isPureWindowFunction(node.rhs.token))) {
                        return true;
                    }
                    sqlNodeStack.push(node.rhs);
                }

                if (node.lhs != null) {
                    if (node.lhs.windowExpression != null ||
                            (node.lhs.type == FUNCTION && cache.isPureWindowFunction(node.lhs.token))) {
                        return true;
                    }
                    node = node.lhs;
                } else if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            } else {
                // for nodes with paramCount >= 3, arguments are stored in args list (e.g., CASE expressions)
                for (int i = 0, k = node.paramCount; i < k; i++) {
                    ExpressionNode arg = node.args.getQuick(i);
                    if (arg != null) {
                        if (arg.windowExpression != null ||
                                (arg.type == FUNCTION && cache.isPureWindowFunction(arg.token))) {
                            return true;
                        }
                        sqlNodeStack.push(arg);
                    }
                }
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }
        return false;
    }

    private boolean checkIfTranslatingModelIsRedundant(
            boolean useInnerModel,
            boolean useGroupByModel,
            boolean useWindowModel,
            boolean useWindowJoinModel,
            boolean forceTranslatingModel,
            boolean checkTranslatingModel,
            QueryModel translatingModel
    ) {
        // check if the translating model is redundant, e.g.
        // that it neither chooses between tables nor renames columns
        boolean translationIsRedundant = (useInnerModel || useGroupByModel || useWindowModel || useWindowJoinModel) && !forceTranslatingModel;
        if (translationIsRedundant && checkTranslatingModel) {
            for (int i = 0, n = translatingModel.getBottomUpColumns().size(); i < n; i++) {
                QueryColumn column = translatingModel.getBottomUpColumns().getQuick(i);
                if (!Chars.equalsIgnoreCase(column.getAst().token, column.getAlias())) {
                    translationIsRedundant = false;
                    break;
                }
            }
        }
        return translationIsRedundant;
    }

    private QueryColumn checkSimpleIntegerColumn(ExpressionNode column, QueryModel model) {
        if (column == null || column.type != LITERAL) {
            return null;
        }

        CharSequence tok = column.token;
        final int dot = Chars.indexOfLastUnquoted(tok, '.');
        QueryColumn qc = getQueryColumn(model, tok, dot);

        if (qc != null &&
                (qc.getColumnType() == ColumnType.BYTE ||
                        qc.getColumnType() == ColumnType.SHORT ||
                        qc.getColumnType() == ColumnType.INT ||
                        qc.getColumnType() == ColumnType.LONG)) {
            return qc;
        }
        return null;
    }

    /**
     * Clears the window function hash map and resets the column list pool.
     */
    private void clearWindowFunctionHashMap() {
        windowFunctionHashMap.clear();
        windowColumnListPool.clear();
        referencedAliasesSet.clear();
    }

    /**
     * Choose models are required in 3 specific cases:
     * - Column duplicating.
     * - Column reordering.
     * - Column hiding. Nested model might select more columns for the purpose of using them for
     * ordering. But the ordered column is not visible to the user and hidden by the select model.
     * - Multiplexing joined tables.
     * <p>
     * It is conceivable that the optimiser creates degenerate cases of "choose" models:
     * - Column renaming
     * - No-op, e.g. selecting the exact same columns as the nested models
     * - Choose model stacking, e.g. choose->choose->group by
     * <p>
     * This particular method implementation deals with stacked models.
     * <p>
     * Limitation: this does not collapse top-down-model, which makes it
     * necessary to call this method before top-down-models.
     *
     * @param model the starting model.
     */
    private void collapseStackedChooseModels(@Nullable QueryModel model) {
        if (model == null) {
            return;
        }

        QueryModel nested = model.getNestedModel();
        if (
                model.getSelectModelType() == SELECT_MODEL_CHOOSE
                        && nested != null
                        && nested.getSelectModelType() == SELECT_MODEL_CHOOSE
                        && nested.getBottomUpColumns().size() <= model.getBottomUpColumns().size()
        ) {
            QueryModel nn = nested.getNestedModel();
            model.mergePartially(nested, queryColumnPool);
            model.setNestedModel(nn);

            // same model, we changed nested
            collapseStackedChooseModels(model);
        } else {
            collapseStackedChooseModels(nested);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            collapseStackedChooseModels(model.getJoinModels().getQuick(i));
        }

        collapseStackedChooseModels(model.getUnionModel());
    }

    private void collectModelAlias(QueryModel parent, int modelIndex, QueryModel model) throws SqlException {
        final ExpressionNode alias = model.getAlias() != null ? model.getAlias() : model.getTableNameExpr();
        if (parent.addModelAliasIndex(alias, modelIndex)) {
            return;
        }
        // If both models are the same, and we already added the alias to it via a subquery, it's not a duplicate,
        // it's only a duplicate if its being applied to a different model.
        if (parent != model) {
            throw SqlException.position(alias.position).put("Duplicate table or alias: ").put(alias.token);
        }
    }

    /**
     * Collects all literal (column reference) aliases from an expression tree.
     * This is used to determine which columns from inner window models are
     * actually referenced by outer models, for pass-through optimization.
     */
    private void collectReferencedAliases(ExpressionNode node, LowerCaseCharSequenceHashSet aliases) {
        if (node == null) {
            return;
        }
        if (node.type == LITERAL) {
            aliases.add(node.token);
            return;
        }
        // Traverse children
        if (node.paramCount < 3) {
            collectReferencedAliases(node.lhs, aliases);
            collectReferencedAliases(node.rhs, aliases);
        } else {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                collectReferencedAliases(node.args.getQuick(i), aliases);
            }
        }
    }

    /**
     * Collects all literal (column reference) aliases from a query column.
     * For window columns, this also traverses the PARTITION BY, ORDER BY,
     * and frame bound expressions in addition to the function AST.
     */
    private void collectReferencedAliasesFromColumn(QueryColumn col, LowerCaseCharSequenceHashSet aliases) {
        collectReferencedAliases(col.getAst(), aliases);
        if (col.isWindowExpression()) {
            WindowExpression wc = (WindowExpression) col;
            // Traverse partitionBy expressions
            ObjList<ExpressionNode> partitionBy = wc.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                collectReferencedAliases(partitionBy.getQuick(i), aliases);
            }
            // Traverse orderBy expressions
            ObjList<ExpressionNode> orderBy = wc.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                collectReferencedAliases(orderBy.getQuick(i), aliases);
            }
            // Traverse frame bound expressions
            collectReferencedAliases(wc.getRowsLoExpr(), aliases);
            collectReferencedAliases(wc.getRowsHiExpr(), aliases);
        }
    }

    private void collectSameModelProjectionColumns(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return;
        }

        if (node.type == LITERAL) {
            final QueryColumn dep = model.getAliasToColumnMap().get(node.token);
            QueryModel nested = model.getNestedModel();
            if (dep != null && (nested == null || !nested.getAliasToColumnMap().contains(node.token)) && model.isTopDownNameMissing(node.token) && tempCharSequenceHashSet.add(node.token)) {
                collectSameModelProjectionColumns(dep.getAst(), model);
                model.addTopDownColumn(dep, node.token);
            }
            return;
        }

        if (node.paramCount < 3) {
            collectSameModelProjectionColumns(node.lhs, model);
            collectSameModelProjectionColumns(node.rhs, model);
        } else {
            for (int i = 0, k = node.paramCount; i < k; i++) {
                collectSameModelProjectionColumns(node.args.getQuick(i), model);
            }
        }
    }

    /**
     * Checks if the token is a time function that returns the current time.
     * These functions cannot be pushed through dateadd transformations because
     * their values are not constant relative to the timestamp column.
     * Uses SqlKeywords methods for efficient case-insensitive matching.
     */
    private static boolean isTimeFunctionKeyword(CharSequence token) {
        return isNowKeyword(token)
                || isSysdateKeyword(token)
                || isSystimestampKeyword(token)
                || isCurrentTimestampKeyword(token);
    }

    private void copyColumnTypesFromMetadata(QueryModel model, TableRecordMetadata m) {
        for (int i = 0, k = m.getColumnCount(); i < k; i++) {
            model.addUpdateTableColumnMetadata(m.getColumnType(i), m.getColumnName(i));
        }
    }

    private void copyColumnsFromMetadata(QueryModel model, RecordMetadata m) throws SqlException {
        // column names are not allowed to have a dot
        for (int i = 0, k = m.getColumnCount(); i < k; i++) {
            CharSequence columnName = createColumnAlias(m.getColumnName(i), model, false);
            QueryColumn column = queryColumnPool.next().of(
                    columnName,
                    expressionNodePool.next().of(
                            LITERAL,
                            columnName,
                            0,
                            0
                    ),
                    true,
                    m.getColumnType(i)
            );
            model.addField(column);
        }

        // validate explicitly defined timestamp, if it exists
        ExpressionNode timestamp = model.getTimestamp();
        if (timestamp == null) {
            if (m.getTimestampIndex() != -1) {
                model.setTimestamp(expressionNodePool.next().of(LITERAL, m.getColumnName(m.getTimestampIndex()), 0, 0));
            }
        } else {
            int index = m.getColumnIndexQuiet(timestamp.token);
            if (index == -1) {
                throw SqlException.invalidColumn(timestamp.position, timestamp.token);
            } else if (!ColumnType.isTimestamp(m.getColumnType(index))) {
                throw SqlException.$(timestamp.position, "not a TIMESTAMP");
            }
        }
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model, boolean nonLiteral) {
        return SqlUtil.createColumnAlias(
                characterStore,
                name,
                Chars.indexOfLastUnquoted(name, '.'),
                model.getAliasToColumnMap(),
                model.getAliasSequenceMap(),
                nonLiteral
        );
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model) {
        return SqlUtil.createColumnAlias(
                characterStore,
                name,
                Chars.indexOfLastUnquoted(name, '.'),
                model.getAliasToColumnMap(),
                model.getAliasSequenceMap(),
                false
        );
    }

    private CharSequence createColumnAlias(ExpressionNode node, QueryModel model) {
        return SqlUtil.createColumnAlias(
                characterStore,
                node.token,
                Chars.indexOfLastUnquoted(node.token, '.'),
                model.getAliasToColumnMap(),
                model.getAliasSequenceMap(),
                false
        );
    }

    // use only if input is a column literal!
    private QueryColumn createGroupByColumn(
            CharSequence columnAlias,
            ExpressionNode columnAst,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel groupByModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        LowerCaseCharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence innerAlias = createColumnAlias(columnAlias, groupByModel);
            final QueryColumn translatedColumn = nextColumn(innerAlias, translatedColumnName);
            innerVirtualModel.addBottomUpColumn(columnAst.position, translatedColumn, true);
            groupByModel.addBottomUpColumn(translatedColumn);
            return translatedColumn;
        } else {
            final CharSequence alias = createColumnAlias(columnAlias, translatingModel);
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(alias, columnAst),
                    translatingModel,
                    innerVirtualModel,
                    baseModel
            );

            final QueryColumn translatedColumn = nextColumn(alias);
            groupByModel.addBottomUpColumn(translatedColumn);
            return translatedColumn;
        }
    }

    /**
     * Creates dependencies via implied columns, typically timestamp.
     * Dependencies like that are not explicitly expressed in SQL query and
     * therefore are not created by analyzing "where" clause.
     * <p>
     * Explicit dependencies however are required for table ordering.
     *
     * @param parent the parent model
     */
    private void createImpliedDependencies(QueryModel parent) {
        ObjList<QueryModel> models = parent.getJoinModels();
        JoinContext jc;
        for (int i = 0, n = models.size(); i < n; i++) {
            QueryModel m = models.getQuick(i);
            if (joinsRequiringTimestamp[m.getJoinType()]) {
                linkDependencies(parent, 0, i);
                if (m.getJoinContext() == null) {
                    m.setContext(jc = contextPool.next());
                    jc.parents.add(0);
                    jc.slaveIndex = i;
                }
            }
        }
    }

    // order hash is used to determine redundant order when parsing window function definition
    private void createOrderHash(QueryModel model) {
        LowerCaseCharSequenceIntHashMap hash = model.getOrderHash();
        hash.clear();

        final ObjList<ExpressionNode> orderBy = model.getOrderBy();
        final int n = orderBy.size();

        if (n > 0) {
            final IntList orderByDirection = model.getOrderByDirection();
            for (int i = 0; i < n; i++) {
                // if a column appears multiple times in the ORDER BY clause, then we use only the first occurrence.
                // why? consider this clause: ORDER BY A DESC, A ASC. In this case we want to order to be "A DESC" only.
                // why? "A ASC" is a lower in priority, and it's applicable if and only if the items within first clause
                // are equal. but if the items are already equal then there is no point in ordering by the same column again.
                // unconditional put() would be a bug as a lower priority ordering would replace ordering with a higher priority.
                hash.putIfAbsent(orderBy.getQuick(i).token, orderByDirection.getQuick(i));
            }
        }

        final QueryModel nestedModel = model.getNestedModel();
        if (nestedModel != null) {
            createOrderHash(nestedModel);
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, z = joinModels.size(); i < z; i++) {
            createOrderHash(joinModels.getQuick(i));
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            createOrderHash(union);
        }
    }

    // add the existing group by column to outer and distinct models
    private boolean createSelectColumn(
            CharSequence alias,
            CharSequence columnName,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        QueryColumn groupByColumn = groupByModel.getAliasToColumnMap().get(columnName);
        QueryColumn outerColumn = nextColumn(alias, groupByColumn.getAlias());
        outerColumn = ensureAliasUniqueness(outerModel, outerColumn);
        outerModel.addBottomUpColumn(outerColumn);

        boolean sameAlias = Chars.equalsIgnoreCase(groupByColumn.getAlias(), outerColumn.getAlias());
        if (distinctModel != null) {
            if (sameAlias) {
                distinctModel.addBottomUpColumn(outerColumn);
            } else { // we've to use alias from the outer model
                QueryColumn distinctColumn = nextColumn(outerColumn.getAlias());
                distinctColumn = ensureAliasUniqueness(distinctModel, distinctColumn);
                distinctModel.addBottomUpColumn(distinctColumn);
            }
        }

        return sameAlias;
    }

    private void createSelectColumn(
            CharSequence columnName,
            ExpressionNode columnAst,
            boolean isGroupBy,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerVirtualModel,
            QueryModel distinctModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        final CharSequence translatedColumnName = translatingModel.getColumnNameToAliasMap().get(columnAst.token);
        if (translatedColumnName != null) {
            // the column is already being referenced by the translating model
            final CharSequence groupByColumnName = groupByModel.getColumnNameToAliasMap().get(translatedColumnName);
            final QueryColumn translatedColumn;
            if (isGroupBy && groupByColumnName != null) {
                // there is already a key referencing the column in the group-by model;
                // to minimize the number of group-by keys, we simply refer to the key in the outer models
                translatedColumn = nextColumn(columnName, groupByColumnName);
            } else {
                // no key in the group-by model;
                // create an alias and add it to the inner models
                final CharSequence innerAlias = createColumnAlias(columnName, groupByModel);
                translatedColumn = nextColumn(innerAlias, translatedColumnName);
                innerVirtualModel.addBottomUpColumn(columnAst.position, translatedColumn, true);
                groupByModel.addBottomUpColumn(translatedColumn);
                windowModel.addBottomUpColumn(translatedColumn);
            }

            // expose the column in the outer models
            outerVirtualModel.addBottomUpColumn(translatedColumn);
            if (distinctModel != null) {
                distinctModel.addBottomUpColumn(translatedColumn);
            }
        } else {
            // the column is not referenced by the translating model
            final CharSequence innerAlias;
            if (groupByModel.getAliasToColumnMap().contains(columnName)) {
                // the column is not yet translated, but another column is referenced via the same name
                innerAlias = createColumnAlias(columnName, groupByModel);
            } else {
                innerAlias = createColumnAlias(columnName, translatingModel);
            }
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(innerAlias, columnAst),
                    translatingModel,
                    innerVirtualModel,
                    baseModel
            );

            // create column that references inner alias we just created
            final QueryColumn translatedColumn = nextColumn(innerAlias, columnAst.position);
            innerVirtualModel.addBottomUpColumn(translatedColumn);
            groupByModel.addBottomUpColumn(translatedColumn);
            windowModel.addBottomUpColumn(translatedColumn);

            // expose the column in the outer models
            outerVirtualModel.addBottomUpColumn(translatedColumn);
            if (distinctModel != null) {
                distinctModel.addBottomUpColumn(translatedColumn);
            }
        }
    }

    private void createSelectColumnsForWildcard(
            QueryColumn qc,
            boolean hasJoins,
            boolean isGroupBy,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // this could be a wildcard, such as '*' or 'a.*'
        int dot = Chars.indexOfLastUnquoted(qc.getAst().token, '.');
        if (dot > -1) {
            int index = baseModel.getModelAliasIndex(qc.getAst().token, 0, dot);
            if (index == -1) {
                throw SqlException.$(qc.getAst().position, "invalid table alias");
            }

            // we are targeting a single table
            createSelectColumnsForWildcard0(
                    baseModel.getJoinModels().getQuick(index),
                    hasJoins,
                    isGroupBy,
                    qc.getAst().position,
                    translatingModel,
                    innerModel,
                    windowModel,
                    groupByModel,
                    outerModel,
                    distinctModel
            );
        } else {
            ObjList<QueryModel> models = baseModel.getJoinModels();
            for (int j = 0, z = models.size(); j < z; j++) {
                createSelectColumnsForWildcard0(
                        models.getQuick(j),
                        hasJoins,
                        isGroupBy,
                        qc.getAst().position,
                        translatingModel,
                        innerModel,
                        windowModel,
                        groupByModel,
                        outerModel,
                        distinctModel
                );
            }
        }
    }

    private void createSelectColumnsForWildcard0(
            QueryModel srcModel,
            boolean hasJoins,
            boolean isGroupBy,
            int wildcardPosition,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        final ObjList<CharSequence> columnNames = srcModel.getWildcardColumnNames();
        for (int j = 0, z = columnNames.size(); j < z; j++) {
            CharSequence name = columnNames.getQuick(j);
            // this is a check to see if a column has to be added to a wildcard list
            QueryColumn qc = srcModel.getAliasToColumnMap().get(name);
            if (qc.isIncludeIntoWildcard()) {
                CharSequence token;
                if (hasJoins) {
                    CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                    characterStoreEntry.put(srcModel.getName());
                    characterStoreEntry.put('.');
                    characterStoreEntry.put(name);
                    token = characterStoreEntry.toImmutable();
                } else {
                    token = name;
                }
                createSelectColumn(
                        name,
                        nextLiteral(token, wildcardPosition),
                        isGroupBy,
                        null, // do not validate
                        translatingModel,
                        innerModel,
                        windowModel,
                        groupByModel,
                        outerModel,
                        distinctModel
                );
            }
        }
    }

    @NotNull
    private QueryModel createWrapperModel(QueryModel model) {
        // these are early stages of model processing
        // to create the outer query, we will need a pair of models
        QueryModel _model = queryModelPool.next();
        QueryModel _nested = queryModelPool.next();

        // nest them
        _model.setNestedModel(_nested);
        _nested.setNestedModel(model);

        _model.setModelPosition(model.getModelPosition());
        _nested.setModelPosition(model.getModelPosition());

        // bubble up the union model, so that wrapper models are
        // subject to set operations
        QueryModel unionModel = model.getUnionModel();
        model.setUnionModel(null);
        _model.setUnionModel(unionModel);
        return _model;
    }

    /**
     * Detects if there are any duplicate aggregate expressions in the column list.
     * Uses hash-based detection for O(n) average complexity instead of O(n^2) pairwise comparison.
     * tempIntList stores interleaved (hash, columnIndex) pairs for all aggregate expressions.
     */
    private boolean detectDuplicateAggregates(ObjList<QueryColumn> columns) {
        final int n = columns.size();
        if (n < 2) {
            return false;
        }

        // First pass: collect hashes and column indices for non-window aggregate expressions
        // Store as interleaved pairs: [hash0, colIdx0, hash1, colIdx1, ...]
        tempIntList.clear();
        for (int i = 0; i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                continue;
            }
            ExpressionNode ast = qc.getAst();
            if (ast.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
                tempIntList.add(ExpressionNode.deepHashCode(ast));
                tempIntList.add(i);
            }
        }

        int count = tempIntList.size() / 2; // number of aggregates
        if (count < 2) {
            return false;
        }

        // Second pass: check for duplicate hashes
        tempIntHashSet.clear();
        for (int i = 0; i < count; i++) {
            int hash = tempIntList.getQuick(i * 2);
            if (tempIntHashSet.contains(hash)) {
                // Hash collision - verify with exact comparison against previous aggregates with same hash
                int colIdx = tempIntList.getQuick(i * 2 + 1);
                ExpressionNode ast = columns.getQuick(colIdx).getAst();
                for (int j = 0; j < i; j++) {
                    if (tempIntList.getQuick(j * 2) == hash) {
                        int prevColIdx = tempIntList.getQuick(j * 2 + 1);
                        if (compareNodesExact(ast, columns.getQuick(prevColIdx).getAst())) {
                            return true;
                        }
                    }
                }
            }
            tempIntHashSet.add(hash);
        }
        return false;
    }

    /**
     * Checks if the expression contains disallowed functions for timestamp offset pushdown.
     * Disallowed functions:
     * - now(), sysdate (time functions)
     * - dateadd that doesn't reference the timestamp column
     */
    private boolean containsDisallowedFunction(ExpressionNode node, CharSequence timestampColumn) {
        if (node == null) {
            return false;
        }

        if (node.type == FUNCTION) {
            // Check for time functions like now(), sysdate
            if (isTimeFunctionKeyword(node.token)) {
                return true;
            }
            // Check for dateadd that doesn't reference the timestamp
            if (Chars.equalsLowerCaseAscii(node.token, "dateadd")) {
                if (!isDateaddWithTimestamp(node, timestampColumn)) {
                    return true;
                }
            }
        }

        // Check children
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (containsDisallowedFunction(node.args.getQuick(i), timestampColumn)) {
                return true;
            }
        }
        if (containsDisallowedFunction(node.lhs, timestampColumn)) {
            return true;
        }
        return containsDisallowedFunction(node.rhs, timestampColumn);
    }

    /**
     * Detects if the model's timestamp column is derived from a dateadd expression
     * on the nested model's timestamp. If so, stores the offset information on the model
     * for use during predicate pushdown.
     *
     * @param parent the parent model to detect and annotate
     * @param nested the nested model
     * @throws SqlException if the offset value exceeds the allowed range for dateadd
     */
    private void detectTimestampOffset(QueryModel parent, QueryModel nested) throws SqlException {
        ExpressionNode modelTimestamp = parent.getTimestamp();
        if (modelTimestamp == null) {
            return;
        }

        // First try parent's column map (for cases where parent defines the column)
        QueryColumn tsColumn = parent.getAliasToColumnMap().get(modelTimestamp.token);
        ExpressionNode ast = tsColumn != null ? tsColumn.getAst() : null;

        // Track where we found the dateadd definition - that's where we need to set offset
        QueryModel modelToAnnotate = parent;

        // If parent just passes through the column, look at nested model's column map
        // This handles: SELECT * FROM (SELECT dateadd(...) as ts FROM ...) timestamp(ts)
        // where the outer select-choose just references 'ts' but doesn't define it
        if (ast == null || ast.type == LITERAL) {
            // The timestamp column is defined in the nested model
            tsColumn = nested.getAliasToColumnMap().get(modelTimestamp.token);
            if (tsColumn == null) {
                return;
            }
            ast = tsColumn.getAst();
            // Set offset on nested model since that's where dateadd is defined
            // The NonLiteralException will be thrown when pushing FROM nested
            modelToAnnotate = nested;
        }

        // Check nested model for its timestamp to validate dateadd argument
        // Traverse down through nested models to find one with timestamp info
        QueryModel timestampSourceModel = nested.getNestedModel();
        if (timestampSourceModel == null) {
            timestampSourceModel = nested;
        }
        // Keep traversing until we find a model with timestamp info
        while (timestampSourceModel != null && lacksTimestampInfo(timestampSourceModel)) {
            timestampSourceModel = timestampSourceModel.getNestedModel();
        }
        if (timestampSourceModel == null) {
            return;
        }

        if (!isDateaddTimestampExpression(ast, timestampSourceModel)) {
            return;
        }

        // Extract offset info (args are reversed: [timestamp, stride, unit])
        ObjList<ExpressionNode> args = ast.args;
        ExpressionNode unitArg = args.getQuick(2);
        ExpressionNode strideArg = args.getQuick(1);
        ExpressionNode timestampArg = args.getQuick(0);

        // Parse the unit character
        char unit = parseUnitCharacter(unitArg.token);
        if (unit == 0) {
            return;  // Invalid unit format
        }

        try {
            long stride = extractInvertedConstantLong(strideArg);

            // Validate that offset fits in int range.
            // The offset must match dateadd's signature: dateadd(char, int, timestamp).
            // See TimestampAddFunctionFactory for the function definition.
            long inverseOffset = -stride;
            if (inverseOffset < Integer.MIN_VALUE || inverseOffset > Integer.MAX_VALUE) {
                throw SqlException.position(strideArg.position)
                        .put("timestamp offset value ")
                        .put(inverseOffset)
                        .put(" exceeds maximum allowed range for dateadd function (must be between ")
                        .put(Integer.MIN_VALUE)
                        .put(" and ")
                        .put(Integer.MAX_VALUE)
                        .put(")");
            }

            // Store INVERSE offset (negate stride) for pushdown.
            // When timestamp = dateadd('h', -1, original), pushing predicate needs +1h offset.
            modelToAnnotate.setTimestampOffsetUnit(unit);
            modelToAnnotate.setTimestampOffsetValue((int) inverseOffset);
            modelToAnnotate.setTimestampSourceColumn(timestampArg.token);
            // Store the alias name so predicates can be matched
            modelToAnnotate.setTimestampOffsetAlias(modelTimestamp.token);

            // Find and store the column index for the timestamp column
            ObjList<QueryColumn> columns = modelToAnnotate.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                QueryColumn col = columns.getQuick(i);
                if (Chars.equalsIgnoreCase(col.getAlias(), modelTimestamp.token)) {
                    modelToAnnotate.setTimestampColumnIndex(i);
                    break;
                }
            }
        } catch (NumericException e) {
            // Cannot parse stride, don't set offset
        }
    }

    private int doReorderTables(QueryModel parent, IntList ordered) {
        tempCrossIndexes.clear();
        ordered.clear();
        this.orderingStack.clear();
        ObjList<QueryModel> joinModels = parent.getJoinModels();

        int cost = 0;

        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getJoinType() == JOIN_CROSS || q.getJoinContext() == null || q.getJoinContext().parents.size() == 0) {
                if (q.getDependencies().size() > 0) {
                    orderingStack.add(i);
                } else {
                    tempCrossIndexes.add(i);
                }
            } else {
                q.getJoinContext().inCount = q.getJoinContext().parents.size();
            }
        }

        while (orderingStack.notEmpty()) {
            // remove a node n from orderingStack
            int index = orderingStack.poll();

            ordered.add(index);

            QueryModel m = joinModels.getQuick(index);

            if (m.getJoinType() == JOIN_CROSS) {
                cost += 10;
            } else {
                cost += 5;
            }

            IntHashSet dependencies = m.getDependencies();

            //for each node m with an-edge e from n to m do
            for (int i = 0, k = dependencies.size(); i < k; i++) {
                int depIndex = dependencies.get(i);
                JoinContext jc = joinModels.getQuick(depIndex).getJoinContext();
                if (jc != null && --jc.inCount == 0) {
                    orderingStack.add(depIndex);
                }
            }
        }

        //Check to see if all edges are removed
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            if (m.getJoinContext() != null && m.getJoinContext().inCount > 0) {
                return Integer.MAX_VALUE;
            }
        }

        // add pure crosses at the end of ordered table list
        for (int i = 0, n = tempCrossIndexes.size(); i < n; i++) {
            ordered.add(tempCrossIndexes.getQuick(i));
        }

        return cost;
    }

    private ExpressionNode doReplaceLiteral(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            boolean addColumnToInnerVirtualModel,
            QueryModel baseModel,
            boolean windowCall
    ) throws SqlException {
        if (windowCall) {
            assert innerVirtualModel != null;
            ExpressionNode n = doReplaceLiteral0(node, translatingModel, innerVirtualModel, false, baseModel);
            LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = innerVirtualModel.getColumnNameToAliasMap();
            int index = columnNameToAliasMap.keyIndex(n.token);
            if (index > -1) {
                // Column not yet referenced by inner model - validate and add it
                validateWindowColumnReference(node, n.token, translatingModel, innerVirtualModel, baseModel);
                CharSequence alias = createColumnAlias(n.token, innerVirtualModel);
                innerVirtualModel.addBottomUpColumn(queryColumnPool.next().of(alias, n));
                if (alias != n.token) {
                    return nextLiteral(alias);
                } else {
                    return n;
                }
            } else {
                // Column already referenced - return existing alias
                return nextLiteral(columnNameToAliasMap.valueAt(index), node.position);
            }
        }
        return doReplaceLiteral0(node, translatingModel, innerVirtualModel, addColumnToInnerVirtualModel, baseModel);
    }

    private ExpressionNode doReplaceLiteral0(
            ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            boolean addColumnToInnerVirtualModel,
            QueryModel baseModel
    ) throws SqlException {
        final LowerCaseCharSequenceObjHashMap<CharSequence> map = translatingModel.getColumnNameToAliasMap();
        int index = map.keyIndex(node.token);
        final CharSequence alias;
        if (index > -1) {
            // there is a possibility that column references join the table, but in a different way
            // for example. main column could be tab1.y and the "missing" one just "y"
            // which is the same thing.
            // To disambiguate this situation we need to go over all join tables and see if the
            // column matches any of join tables unambiguously.

            final int joinCount = baseModel.getJoinModels().size();
            if (joinCount > 1) {
                boolean found = false;
                final StringSink sink = Misc.getThreadLocalSink();
                for (int i = 0; i < joinCount; i++) {
                    final QueryModel jm = baseModel.getJoinModels().getQuick(i);
                    if (jm.getAliasToColumnMap().keyIndex(node.token) < 0) {
                        if (found) {
                            throw SqlException.ambiguousColumn(node.position, node.token);
                        }
                        if (jm.getAlias() != null) {
                            sink.put(jm.getAlias().token);
                        } else {
                            sink.put(jm.getTableName());
                        }

                        sink.put('.');
                        sink.put(node.token);

                        if ((index = map.keyIndex(sink)) < 0) {
                            found = true;
                        }
                    }
                }

                if (found) {
                    return nextLiteral(map.valueAtQuick(index), node.position);
                }
            }

            // also search the virtual model and do not register the literal with the
            // translating model if this is a projection only reference.
            if (baseModel.getAliasToColumnMap().excludes(node.token) && innerVirtualModel != null && innerVirtualModel.getAliasToColumnMap().contains(node.token)) {
                return node;
            }

            // this is the first time we've seen this column and must create alias
            alias = createColumnAlias(node, translatingModel);
            QueryColumn column = queryColumnPool.next().of(alias, node);
            // add column to both models
            addColumnToTranslatingModel(column, translatingModel, innerVirtualModel, baseModel);
            if (addColumnToInnerVirtualModel) {
                assert innerVirtualModel != null;
                ExpressionNode innerToken = expressionNodePool.next().of(LITERAL, alias, node.precedence, node.position);
                QueryColumn innerColumn = queryColumnPool.next().of(alias, innerToken);
                innerVirtualModel.addBottomUpColumn(innerColumn);
            }
        } else {
            // It might be the case that we previously added the column to
            // the translating model, but not to the inner one.
            alias = map.valueAtQuick(index);
            if (addColumnToInnerVirtualModel && innerVirtualModel.getAliasToColumnMap().excludes(alias)) {
                innerVirtualModel.addBottomUpColumn(nextColumn(alias), true);
            }
        }
        return nextLiteral(alias, node.position);
    }

    private void doRewriteOrderByPositionForUnionModels(QueryModel model, QueryModel parent, QueryModel next) throws SqlException {
        final int columnCount = model.getBottomUpColumns().size();
        while (next != null) {
            if (next.getBottomUpColumns().size() != columnCount) {
                throw SqlException.$(next.getModelPosition(), "queries have different number of columns");
            }
            rewriteOrderByPosition(next);
            parent.setUnionModel(next);
            parent = next;
            next = next.getUnionModel();
        }
    }

    /**
     * Copies orderByAdvice and removes table prefixes.
     *
     * @return prefix-less advice
     */
    private ObjList<ExpressionNode> duplicateAdviceAndTakeSuffix() {
        int d;
        CharSequence token;
        ExpressionNode node;
        ObjList<ExpressionNode> advice = new ObjList<>();
        for (int j = 0, m = orderByAdvice.size(); j < m; j++) {
            node = orderByAdvice.getQuick(j);
            token = node.token;
            d = Chars.indexOfLastUnquoted(token, '.');
            advice.add(expressionNodePool.next().of(node.type, token.subSequence(d + 1, token.length()), node.precedence, node.position));
        }
        return advice;
    }

    private void emitAggregatesAndLiterals(
            @Transient ExpressionNode node,
            QueryModel groupByModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        ExpressionNode n = replaceIfAggregateOrLiteral(
                                node.rhs,
                                groupByModel,
                                translatingModel,
                                innerVirtualModel,
                                baseModel,
                                groupByNodes,
                                groupByAliases
                        );
                        if (node.rhs == n) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = n;
                        }
                    }

                    ExpressionNode n = replaceIfAggregateOrLiteral(
                            node.lhs,
                            groupByModel,
                            translatingModel,
                            innerVirtualModel,
                            baseModel,
                            groupByNodes,
                            groupByAliases
                    );
                    if (n == node.lhs) {
                        node = node.lhs;
                    } else {
                        node.lhs = n;
                        node = null;
                    }
                } else {
                    for (int i = 0, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        ExpressionNode n = replaceIfAggregateOrLiteral(
                                e,
                                groupByModel,
                                translatingModel,
                                innerVirtualModel,
                                baseModel,
                                groupByNodes,
                                groupByAliases
                        );
                        if (e == n) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, n);
                        }
                    }
                    node = sqlNodeStack.poll();
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitColumnLiteralsTopDown(ObjList<QueryColumn> columns, QueryModel target) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            emitLiteralsTopDown(qc.getAst(), target);
            if (qc.isWindowExpression()) {
                final WindowExpression ac = (WindowExpression) qc;
                emitLiteralsTopDown(ac.getPartitionBy(), target);
                emitLiteralsTopDown(ac.getOrderBy(), target);
            }
        }
    }

    // This method will create CROSS-join models in the "baseModel" for all unique cursor
    // function it finds on the node. The "translatingModel" is used to ensure uniqueness
    private void emitCursors(
            @Transient ExpressionNode node,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.rhs != null) {
                    final ExpressionNode n = replaceIfCursor(
                            node.rhs,
                            cursorModel,
                            innerVirtualModel,
                            translatingModel,
                            baseModel,
                            sqlExecutionContext,
                            sqlParserCallback
                    );
                    if (node.rhs == n) {
                        sqlNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                final ExpressionNode n = replaceIfCursor(
                        node.lhs,
                        cursorModel,
                        innerVirtualModel,
                        translatingModel,
                        baseModel,
                        sqlExecutionContext,
                        sqlParserCallback
                );
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    // warning: this method replaces literal with aliases (changes the node)
    private void emitLiterals(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            boolean addColumnToInnerVirtualModel,
            QueryModel baseModel,
            boolean windowCall
    ) throws SqlException {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        ExpressionNode n = replaceLiteral(
                                node.rhs,
                                translatingModel,
                                innerVirtualModel,
                                addColumnToInnerVirtualModel,
                                baseModel,
                                windowCall
                        );
                        if (node.rhs == n) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = n;
                        }
                    }

                    ExpressionNode n = replaceLiteral(
                            node.lhs,
                            translatingModel,
                            innerVirtualModel,
                            addColumnToInnerVirtualModel,
                            baseModel,
                            windowCall
                    );
                    if (n == node.lhs) {
                        node = node.lhs;
                    } else {
                        node.lhs = n;
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        ExpressionNode n = replaceLiteral(
                                e,
                                translatingModel,
                                innerVirtualModel,
                                addColumnToInnerVirtualModel,
                                baseModel,
                                windowCall
                        );
                        if (e == n) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, n);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    ExpressionNode n = replaceLiteral(
                            e,
                            translatingModel,
                            innerVirtualModel,
                            addColumnToInnerVirtualModel,
                            baseModel,
                            windowCall
                    );
                    if (e == n) {
                        node = e;
                    } else {
                        node.args.setQuick(0, n);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitLiteralsTopDown(@Transient ExpressionNode node, QueryModel model) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        addTopDownColumn(node, model);

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        addTopDownColumn(node.rhs, model);
                        sqlNodeStack.push(node.rhs);
                    }

                    if (node.lhs != null) {
                        addTopDownColumn(node.lhs, model);
                    }
                    node = node.lhs;
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        addTopDownColumn(e, model);
                        sqlNodeStack.push(e);
                    }

                    final ExpressionNode e = node.args.getQuick(0);
                    addTopDownColumn(e, model);
                    node = e;
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitLiteralsTopDown(ObjList<ExpressionNode> list, QueryModel nested) {
        for (int i = 0, m = list.size(); i < m; i++) {
            emitLiteralsTopDown(list.getQuick(i), nested);
        }
    }

    private void emitWindowFunctions(
            @Transient ExpressionNode node,
            QueryModel windowModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel
    ) throws SqlException {
        // Use windowNodeStack (not sqlNodeStack) because replaceIfWindowFunction
        // calls emitLiterals which clears and reuses sqlNodeStack for its own traversal.
        windowNodeStack.clear();
        while (!windowNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        ExpressionNode n = replaceIfWindowFunction(
                                node.rhs,
                                windowModel,
                                translatingModel,
                                innerVirtualModel,
                                baseModel
                        );
                        if (node.rhs == n) {
                            windowNodeStack.push(node.rhs);
                        } else {
                            node.rhs = n;
                        }
                    }

                    ExpressionNode n = replaceIfWindowFunction(
                            node.lhs,
                            windowModel,
                            translatingModel,
                            innerVirtualModel,
                            baseModel
                    );
                    if (n == node.lhs) {
                        node = node.lhs;
                    } else {
                        node.lhs = n;
                        node = null;
                    }
                } else {
                    for (int i = 0, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        ExpressionNode n = replaceIfWindowFunction(
                                e,
                                windowModel,
                                translatingModel,
                                innerVirtualModel,
                                baseModel
                        );
                        if (e == n) {
                            windowNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, n);
                        }
                    }
                    node = windowNodeStack.poll();
                }
            } else {
                node = windowNodeStack.poll();
            }
        }
    }

    private QueryColumn ensureAliasUniqueness(QueryModel model, QueryColumn qc) {
        CharSequence alias = createColumnAlias(qc.getAlias(), model);
        if (alias != qc.getAlias()) {
            qc = queryColumnPool.next().of(alias, qc.getAst());
        }
        return qc;
    }

    private void enumerateColumns(QueryModel model, TableRecordMetadata metadata) throws SqlException {
        model.setMetadataVersion(metadata.getMetadataVersion());
        model.setTableId(metadata.getTableId());
        copyColumnsFromMetadata(model, metadata);
        if (model.isUpdate()) {
            copyColumnTypesFromMetadata(model, metadata);
        }
    }

    private void enumerateTableColumns(QueryModel model, SqlExecutionContext executionContext, SqlParserCallback sqlParserCallback) throws SqlException {
        final ObjList<QueryModel> jm = model.getJoinModels();

        // we have plain tables and possibly joins
        // a deal with _this_ model first, it will always be the first element in the join model list
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        if (tableNameExpr != null || model.getSelectModelType() == SELECT_MODEL_SHOW) {
            if (model.getSelectModelType() == SELECT_MODEL_SHOW || (tableNameExpr != null && tableNameExpr.type == FUNCTION)) {
                parseFunctionAndEnumerateColumns(model, executionContext, sqlParserCallback);
            } else {
                openReaderAndEnumerateColumns(executionContext, model, sqlParserCallback);
            }
        } else {
            final QueryModel nested = model.getNestedModel();
            if (nested != null) {
                enumerateTableColumns(nested, executionContext, sqlParserCallback);
                if (model.isUpdate()) {
                    model.copyUpdateTableMetadata(nested);
                }
            }
        }
        for (int i = 1, n = jm.size(); i < n; i++) {
            enumerateTableColumns(jm.getQuick(i), executionContext, sqlParserCallback);
        }

        if (model.getUnionModel() != null) {
            enumerateTableColumns(model.getUnionModel(), executionContext, sqlParserCallback);
        }
    }

    private void eraseColumnPrefixInWhereClauses(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            ExpressionNode where = m.getWhereClause();

            // join models can have "where" clause
            // although in context of SQL where is executed after joins, this model
            // always localises "where" to a single table and therefore "where" is
            // applied before join. Please see post-join-where for filters that are
            // executed in line with standard SQL behaviour.

            if (where != null) {
                if (where.type == LITERAL) {
                    m.setWhereClause(columnPrefixEraser.rewrite(where));
                } else {
                    traversalAlgo.traverse(where, columnPrefixEraser);
                }
            }

            QueryModel nested = m.getNestedModel();
            if (nested != null) {
                eraseColumnPrefixInWhereClauses(nested);
            }

            nested = m.getUnionModel();
            if (nested != null) {
                eraseColumnPrefixInWhereClauses(nested);
            }
        }
    }

    /**
     * Checks if the given AST node contains nested window functions in its arguments,
     * and if so, extracts them to a new inner window model. The AST is modified in-place
     * to replace nested window functions with literal references.
     *
     * @param ast               The expression node to check and potentially modify
     * @param translatingModel  Model for literal propagation
     * @param innerVirtualModel Virtual model for intermediate calculations
     * @param baseModel         Original table model
     * @param depth             Current nesting depth (for limiting recursion)
     */
    private void extractAndRegisterNestedWindowFunctions(
            ExpressionNode ast,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel,
            int depth
    ) throws SqlException {
        if (depth > MAX_WINDOW_FUNCTION_NESTING_DEPTH) {
            throw SqlException.$(ast.position, "too many levels of nested window functions [max=")
                    .put(MAX_WINDOW_FUNCTION_NESTING_DEPTH).put(']');
        }

        // Check if arguments contain nested window functions
        boolean hasNestedWindows = false;
        if (ast.paramCount < 3) {
            hasNestedWindows = checkForChildWindowFunctions(ast.lhs) || checkForChildWindowFunctions(ast.rhs);
        } else {
            for (int i = 0, k = ast.paramCount; i < k && !hasNestedWindows; i++) {
                hasNestedWindows = checkForChildWindowFunctions(ast.args.getQuick(i));
            }
        }

        if (!hasNestedWindows) {
            return;
        }

        // Create an inner window model for nested window functions
        QueryModel innerWindowModel = queryModelPool.next();
        innerWindowModel.setSelectModelType(SELECT_MODEL_WINDOW);

        // Extract nested window functions from arguments (modifies AST in-place)
        if (ast.paramCount < 3) {
            if (ast.lhs != null) {
                ast.lhs = extractNestedWindowFunctions(ast.lhs, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth);
            }
            if (ast.rhs != null) {
                ast.rhs = extractNestedWindowFunctions(ast.rhs, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth);
            }
        } else {
            for (int i = 0, k = ast.paramCount; i < k; i++) {
                ExpressionNode arg = ast.args.getQuick(i);
                if (arg != null) {
                    ast.args.setQuick(i, extractNestedWindowFunctions(arg, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth));
                }
            }
        }

        // Store inner window model for later chaining (if it has columns)
        if (innerWindowModel.getBottomUpColumns().size() > 0) {
            innerWindowModels.add(innerWindowModel);
            // Register inner window column aliases with the translating model's alias map
            // so they can be resolved when emitLiterals is called on the outer function.
            // This prevents the literal from being converted back to a function.
            ObjList<QueryColumn> innerCols = innerWindowModel.getBottomUpColumns();
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap = translatingModel.getColumnNameToAliasMap();
            for (int i = 0, n = innerCols.size(); i < n; i++) {
                QueryColumn innerCol = innerCols.getQuick(i);
                CharSequence alias = innerCol.getAlias();
                if (alias != null && aliasMap.keyIndex(alias) > -1) {
                    aliasMap.put(alias, alias);
                }
            }
        }
    }

    /**
     * Recursively walks the model tree and detects dateadd patterns on timestamp columns.
     * For virtual models that have a dateadd expression on the nested model's timestamp,
     * this sets the timestampColumnIndex so SqlCodeGenerator can use it as the virtual model's timestamp.
     * This enables the timestamp(ts) clause to be optional when using dateadd on the designated timestamp.
     *
     * @throws SqlException if a dateadd offset exceeds the allowed int range
     */
    private void detectTimestampOffsetsRecursive(QueryModel model) throws SqlException {
        if (model == null) {
            return;
        }

        // Process joins first
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            detectTimestampOffsetsRecursive(joinModels.getQuick(i));
        }

        // Process union models
        detectTimestampOffsetsRecursive(model.getUnionModel());

        // Process nested model
        QueryModel nested = model.getNestedModel();
        detectTimestampOffsetsRecursive(nested);

        // Now process this model - only for virtual models
        if (model.getSelectModelType() != SELECT_MODEL_VIRTUAL) {
            return;
        }

        // Skip if timestamp column index is already set
        if (model.getTimestampColumnIndex() >= 0) {
            return;
        }

        // Skip if no nested model
        if (nested == null) {
            return;
        }

        // Skip if nested model doesn't preserve row ordering
        int nestedType = nested.getSelectModelType();
        if (nestedType == SELECT_MODEL_GROUP_BY
                || nestedType == SELECT_MODEL_DISTINCT
                || nestedType == SELECT_MODEL_WINDOW) {
            return;
        }

        // Find the source of the timestamp - traverse down through nested models
        // We look for a model that has either:
        // 1. A designated timestamp (getTimestamp() != null)
        // 2. A timestampColumnIndex set (from a previous dateadd detection)
        // 3. A timestampOffsetAlias set
        QueryModel timestampSourceModel = nested.getNestedModel();
        if (timestampSourceModel == null) {
            timestampSourceModel = nested;
        }
        // Keep traversing until we find a model with timestamp info
        while (timestampSourceModel != null && lacksTimestampInfo(timestampSourceModel)) {
            timestampSourceModel = timestampSourceModel.getNestedModel();
        }

        // If no timestamp info found, we can't detect offset
        if (timestampSourceModel == null) {
            return;
        }

        // Get the source timestamp column name
        CharSequence sourceTimestampName = null;
        if (timestampSourceModel.getTimestamp() != null) {
            sourceTimestampName = timestampSourceModel.getTimestamp().token;
        } else if (timestampSourceModel.getTimestampOffsetAlias() != null) {
            sourceTimestampName = timestampSourceModel.getTimestampOffsetAlias();
        }

        // Look for a column that is dateadd on the nested timestamp
        ObjList<QueryColumn> columns = model.getBottomUpColumns();

        // First, check if the original timestamp column is in the projection as a literal.
        // If so, skip ts_offset detection - the original timestamp should be the designated one.
        if (sourceTimestampName != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                QueryColumn col = columns.getQuick(i);
                ExpressionNode ast = col.getAst();
                if (ast != null && ast.type == LITERAL && isLiteralTimestampReference(ast, nested, sourceTimestampName)) {
                    // Original timestamp is in projection - it takes precedence over dateadd columns
                    return;
                }
            }
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn col = columns.getQuick(i);
            ExpressionNode ast = col.getAst();

            if (isDateaddTimestampExpression(ast, timestampSourceModel)) {
                // Found a dateadd on the timestamp - set this as the timestamp column
                model.setTimestampColumnIndex(i);

                // Also set the offset info for predicate pushdown
                ObjList<ExpressionNode> args = ast.args;
                ExpressionNode unitArg = args.getQuick(2);
                ExpressionNode strideArg = args.getQuick(1);
                ExpressionNode timestampArg = args.getQuick(0);

                // Parse the unit character
                char unit = parseUnitCharacter(unitArg.token);
                if (unit == 0) {
                    break;  // Invalid unit format, just use index without offset
                }

                try {
                    long stride = extractInvertedConstantLong(strideArg);

                    // Validate that offset fits in int range.
                    // The offset must match dateadd's signature: dateadd(char, int, timestamp).
                    // See TimestampAddFunctionFactory for the function definition.
                    long inverseOffset = -stride;
                    if (inverseOffset < Integer.MIN_VALUE || inverseOffset > Integer.MAX_VALUE) {
                        throw SqlException.position(strideArg.position)
                                .put("timestamp offset value ")
                                .put(inverseOffset)
                                .put(" exceeds maximum allowed range for dateadd function (must be between ")
                                .put(Integer.MIN_VALUE)
                                .put(" and ")
                                .put(Integer.MAX_VALUE)
                                .put(")");
                    }

                    // Store INVERSE offset (negate stride) for pushdown
                    model.setTimestampOffsetUnit(unit);
                    model.setTimestampOffsetValue((int) inverseOffset);
                    model.setTimestampSourceColumn(timestampArg.token);
                    model.setTimestampOffsetAlias(col.getAlias());
                } catch (NumericException e) {
                    // Cannot parse stride, just set the index without offset info
                }
                break; // Only handle first dateadd column
            }
        }
    }

    /**
     * Extracts nested window functions from an expression tree and replaces them with literal references.
     * Processes depth-first (innermost window functions first) by recursively processing children
     * before checking if the current node is a window function.
     *
     * @param node              The expression node to process (modified in-place)
     * @param innerWindowModel  Model to add extracted window functions to
     * @param translatingModel  Model for literal propagation
     * @param innerVirtualModel Virtual model for intermediate calculations
     * @param baseModel         Original table model
     * @param depth             Current nesting depth (for limiting recursion)
     * @return The possibly modified node (same node with modified children, or a literal if node was a window function)
     */
    private ExpressionNode extractNestedWindowFunctions(
            ExpressionNode node,
            QueryModel innerWindowModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel,
            int depth
    ) throws SqlException {
        if (node == null) {
            return null;
        }

        // If this node is a window function, first extract any nested windows from its arguments
        // to a deeper inner model, then add this window function to the current inner model
        if (node.windowExpression != null) {
            // Extract any nested window functions from THIS window function's arguments
            // This creates deeper inner models if needed (recursive structure)
            extractAndRegisterNestedWindowFunctions(node, translatingModel, innerVirtualModel, baseModel, depth + 1);

            // Validate that PARTITION BY and ORDER BY don't contain window functions
            validateNoWindowFunctionsInWindowSpec(node.windowExpression);

            // Compute hash once for both duplicate check and registration
            int hash = ExpressionNode.deepHashCode(node);

            // Check if an identical window function already exists (O(1) hash lookup + O(k) comparison where k is collision count)
            CharSequence existingAlias = findDuplicateWindowFunction(node, hash);
            if (existingAlias != null) {
                return nextLiteral(existingAlias);
            }

            // Now add this window function (with nested windows replaced by literals) to current inner model
            WindowExpression wc = node.windowExpression;
            CharSequence alias = wc.getAlias();
            if (alias == null) {
                // Use translatingModel for alias uniqueness to ensure inner window columns
                // get unique aliases across multiple inner window models
                alias = createColumnAlias(node, translatingModel);
                wc.of(alias, node);
                // Register the alias immediately so subsequent calls see it for uniqueness
                translatingModel.getAliasToColumnMap().putIfAbsent(alias, wc);
            }
            innerWindowModel.addBottomUpColumn(wc);
            // Register in hash map for future deduplication
            registerWindowFunction(wc, hash);
            // Emit literals referenced by the window column to inner models
            emitLiterals(node, translatingModel, innerVirtualModel, true, baseModel, true);
            return nextLiteral(alias);
        }

        // For non-window nodes, recursively process children
        if (node.paramCount < 3) {
            if (node.lhs != null) {
                node.lhs = extractNestedWindowFunctions(node.lhs, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth);
            }
            if (node.rhs != null) {
                node.rhs = extractNestedWindowFunctions(node.rhs, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth);
            }
        } else {
            for (int i = 0, k = node.paramCount; i < k; i++) {
                ExpressionNode arg = node.args.getQuick(i);
                if (arg != null) {
                    node.args.setQuick(i, extractNestedWindowFunctions(arg, innerWindowModel, translatingModel, innerVirtualModel, baseModel, depth));
                }
            }
        }

        return node;
    }

    private CharSequence findColumnByAst(ObjList<ExpressionNode> groupByNodes, ObjList<CharSequence> groupByAliases, ExpressionNode node) {
        for (int i = 0, max = groupByNodes.size(); i < max; i++) {
            ExpressionNode n = groupByNodes.getQuick(i);
            if (compareNodesExact(node, n)) {
                return groupByAliases.getQuick(i);
            }
        }
        return null;
    }

    private int findColumnIdxByAst(ObjList<ExpressionNode> groupByNodes, ExpressionNode node) {
        for (int i = 0, max = groupByNodes.size(); i < max; i++) {
            ExpressionNode n = groupByNodes.getQuick(i);
            if (compareNodesExact(node, n)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Looks up a duplicate window function in the hash map.
     * Returns the alias of an existing identical window function, or null if none found.
     *
     * @param node The window function expression node to look up
     * @param hash Pre-computed hash from ExpressionNode.deepHashCode(node)
     * @return The alias of an existing identical window function, or null if none found
     */
    private CharSequence findDuplicateWindowFunction(ExpressionNode node, int hash) {
        ObjList<QueryColumn> candidates = windowFunctionHashMap.get(hash);
        if (candidates != null) {
            for (int i = 0, n = candidates.size(); i < n; i++) {
                QueryColumn existing = candidates.getQuick(i);
                if (ExpressionNode.compareNodesExact(node, existing.getAst())) {
                    return existing.getAlias();
                }
            }
        }
        return null;
    }

    private QueryColumn findQueryColumnByAst(ObjList<QueryColumn> bottomUpColumns, ExpressionNode node) {
        for (int i = 0, max = bottomUpColumns.size(); i < max; i++) {
            QueryColumn qc = bottomUpColumns.getQuick(i);
            if (compareNodesExact(qc.getAst(), node)) {
                return qc;
            }
        }
        return null;
    }

    private CharSequence findTimestamp(QueryModel model) {
        if (model != null) {
            CharSequence timestamp;
            if (model.getTimestamp() != null) {
                timestamp = model.getTimestamp().token;
            } else {
                timestamp = findTimestamp(model.getNestedModel());
            }

            if (timestamp != null) {
                return model.getColumnNameToAliasMap().get(timestamp);
            }
        }
        return null;
    }

    /**
     * Finds the position of the first window function or pure window function name in an expression tree.
     * Returns 0 if no window function is found.
     */
    private int findWindowFunctionOrNamePosition(ExpressionNode node) {
        if (node == null) {
            return 0;
        }
        FunctionFactoryCache cache = functionParser.getFunctionFactoryCache();
        if (node.windowExpression != null ||
                (node.type == FUNCTION && cache.isPureWindowFunction(node.token))) {
            return node.position;
        }
        // Check children using paramCount to distinguish binary nodes from variadic nodes
        if (node.paramCount < 3) {
            if (node.lhs != null) {
                int pos = findWindowFunctionOrNamePosition(node.lhs);
                if (pos > 0) return pos;
            }
            if (node.rhs != null) {
                int pos = findWindowFunctionOrNamePosition(node.rhs);
                if (pos > 0) return pos;
            }
        } else {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                int pos = findWindowFunctionOrNamePosition(node.args.getQuick(i));
                if (pos > 0) return pos;
            }
        }
        return 0;
    }

    private void fixTimestampAndCollectMissingTokens(
            ExpressionNode node,
            CharSequence timestampColumn,
            CharSequence timestampAlias,
            CharSequenceHashSet missingTokens
    ) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type == LITERAL) {
                if (Chars.equalsIgnoreCase(node.token, timestampColumn)) {
                    node.token = timestampAlias;
                } else {
                    missingTokens.add(node.token);
                }
            }

            if (node.paramCount < 3) {
                if (node.lhs != null) {
                    sqlNodeStack.push(node.lhs);
                }

                if (node.rhs != null) {
                    node = node.rhs;
                    continue;
                }
            } else {
                for (int i = 1, k = node.paramCount; i < k; i++) {
                    sqlNodeStack.push(node.args.getQuick(i));
                }
                node = node.args.getQuick(0);
                continue;
            }

            if (!sqlNodeStack.isEmpty()) {
                node = sqlNodeStack.poll();
            } else {
                node = null;
            }
        }
    }

    private Function getLoFunction(ExpressionNode limit, SqlExecutionContext executionContext) throws SqlException {
        final Function func = functionParser.parseFunction(limit, EmptyRecordMetadata.INSTANCE, executionContext);
        final int type = func.getType();
        if (limitTypes.excludes(type)) {
            return null;
        }
        func.init(null, executionContext);
        return func;
    }

    /**
     * Gets or creates a list for storing QueryColumns with the given hash.
     */
    private ObjList<QueryColumn> getOrCreateColumnListForHash(int hash) {
        ObjList<QueryColumn> list = windowFunctionHashMap.get(hash);
        if (list == null) {
            list = windowColumnListPool.next();
            windowFunctionHashMap.put(hash, list);
        }
        return list;
    }

    private ObjList<ExpressionNode> getOrderByAdvice(QueryModel model, int orderByMnemonic) {
        orderByAdvice.clear();
        ObjList<ExpressionNode> orderBy = model.getOrderBy();

        int len = orderBy.size();
        if (len == 0) {
            // propagate advice in case nested model can implement it efficiently (e.g., with backward scan)
            if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT && model.getOrderByAdvice().size() > 0) {
                orderBy = model.getOrderByAdvice();
                len = orderBy.size();
            } else {
                return orderByAdvice;
            }
        }

        LowerCaseCharSequenceObjHashMap<QueryColumn> map = model.getAliasToColumnMap();
        for (int i = 0; i < len; i++) {
            ExpressionNode orderByNode = orderBy.getQuick(i);
            QueryColumn queryColumn = map.get(orderByNode.token);
            if (queryColumn == null) { // order by can't be pushed down
                orderByAdvice.clear();
                break;
            }

            if (queryColumn.getAst().type == LITERAL) {
                orderByAdvice.add(queryColumn.getAst());
            } else {
                orderByAdvice.clear();
                break;
            }
        }
        return orderByAdvice;
    }

    private IntList getOrderByAdviceDirection(QueryModel model, int orderByMnemonic) {
        IntList orderByDirection = model.getOrderByDirection();
        if (model.getOrderBy().size() == 0
                && orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT) {
            return model.getOrderByDirectionAdvice();
        }
        return orderByDirection;
    }

    private QueryColumn getQueryColumn(QueryModel model, CharSequence columnName, int dot) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        QueryColumn column = null;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                QueryColumn qc = joinModels.getQuick(i).getAliasToColumnMap().get(columnName);
                if (qc == null) {
                    continue;
                }
                if (column != null) {
                    return null;//more than one column match
                }
                column = qc;
            }
        } else {
            int index = model.getModelAliasIndex(columnName, 0, dot);
            if (index == -1) {
                return null;
            }
            return joinModels.getQuick(index).getAliasToColumnMap().get(columnName, dot + 1, columnName.length());
        }
        return column;
    }

    /**
     * Extracts a constant long value from an expression node.
     * Handles both positive constants and unary minus expressions.
     */
    private long extractInvertedConstantLong(ExpressionNode node) throws NumericException {
        if (node.type == CONSTANT) {
            return Numbers.parseLong(node.token);
        }
        // Handle unary minus: -(constant)
        if (node.type == OPERATION && Chars.equals(node.token, "-") && node.paramCount == 1) {
            return -Numbers.parseLong(node.rhs.token);
        }
        throw NumericException.INSTANCE;
    }

    private CharSequence getTranslatedColumnAlias(QueryModel model, QueryModel stopModel, CharSequence token) {
        if (model == stopModel) {
            return token;
        }

        CharSequence nestedAlias = getTranslatedColumnAlias(model.getNestedModel(), stopModel, token);

        if (nestedAlias != null) {
            CharSequence alias = model.getColumnNameToAliasMap().get(nestedAlias);
            if (alias == null) {
                tempQueryModel = model;
                tempColumnAlias = nestedAlias;
            }
            return alias;
        } else {
            return null;
        }
    }

    private boolean hasNoAggregateQueryColumns(QueryModel model) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.getAst().type != LITERAL) {
                if (qc.getAst().type == FUNCTION) {
                    if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                        return false;
                    } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
                        continue;
                    }
                }

                if (checkForChildAggregates(qc.getAst())) {
                    return false;
                }
            }
        }
        return true;
    }

    private void homogenizeCrossJoins(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            JoinContext c = m.getJoinContext();

            if (m.getJoinType() == JOIN_CROSS) {
                if (c != null && c.parents.size() > 0) {
                    m.setJoinType(JOIN_INNER);
                }
            } else if (m.getJoinType() == JOIN_LEFT_OUTER &&
                    c == null &&
                    m.getJoinCriteria() != null) {
                m.setJoinType(JOIN_CROSS_LEFT);
            } else if (m.getJoinType() == JOIN_RIGHT_OUTER &&
                    c == null &&
                    m.getJoinCriteria() != null) {
                m.setJoinType(JOIN_CROSS_RIGHT);
            } else if (m.getJoinType() == JOIN_FULL_OUTER &&
                    c == null &&
                    m.getJoinCriteria() != null) {
                m.setJoinType(JOIN_CROSS_FULL);
            } else if (m.getJoinType() != JOIN_ASOF &&
                    m.getJoinType() != JOIN_SPLICE &&
                    (c == null || c.parents.size() == 0)
            ) {
                m.setJoinType(JOIN_CROSS);
            }
        }
    }

    private void initialiseOperatorExpressions() {
        final OperatorRegistry registry = OperatorExpression.getRegistry();
        opGeq = registry.map.get(">=");
        opLt = registry.map.get("<");
        opAnd = registry.map.get("and");
    }

    private boolean isAmbiguousColumn(QueryModel model, CharSequence columnName) {
        final int dot = Chars.indexOfLastUnquoted(columnName, '.');
        if (dot == -1) {
            ObjList<QueryModel> joinModels = model.getJoinModels();
            int index = -1;
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                if (joinModels.getQuick(i).getColumnNameToAliasMap().excludes(columnName)) {
                    continue;
                }
                if (index != -1) {
                    return true;
                }
                index = i;
            }
        }
        return false;
    }

    /**
     * Checks if the given expression is a constant integer (positive or negative).
     */
    private boolean isConstantIntegerExpression(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == CONSTANT) {
            try {
                Numbers.parseLong(node.token);
                return true;
            } catch (NumericException e) {
                return false;
            }
        }
        // Handle unary minus: -(constant)
        if (node.type == OPERATION && Chars.equals(node.token, "-") && node.paramCount == 1) {
            if (node.rhs != null && node.rhs.type == CONSTANT) {
                try {
                    Numbers.parseLong(node.rhs.token);
                    return true;
                } catch (NumericException e) {
                    return false;
                }
            }
            return false;
        }
        return false;
    }

    /**
     * Detects if the given expression is a dateadd function call with constant unit and stride,
     * where the timestamp argument references the nested model's designated timestamp or
     * a column that is itself a dateadd-transformed timestamp (for chained dateadd detection).
     *
     * @param ast         the expression node to check
     * @param nestedModel the nested model whose timestamp we're checking against
     * @return true if the expression is dateadd(unit, constant, timestamp)
     */
    private boolean isDateaddTimestampExpression(ExpressionNode ast, QueryModel nestedModel) {
        if (ast == null || ast.type != FUNCTION) {
            return false;
        }
        if (!Chars.equalsLowerCaseAscii(ast.token, "dateadd")) {
            return false;
        }
        if (ast.paramCount != 3) {
            return false;
        }

        // Args are reversed: args[0]=timestamp, args[1]=stride, args[2]=unit
        ObjList<ExpressionNode> args = ast.args;
        if (args.size() != 3) {
            return false;
        }

        ExpressionNode timestampArg = args.getQuick(0);
        ExpressionNode strideArg = args.getQuick(1);
        ExpressionNode unitArg = args.getQuick(2);

        // Check unit is constant char (like 'h', 'd', etc.)
        if (unitArg == null || unitArg.type != CONSTANT) {
            return false;
        }

        // Check stride is constant integer
        if (!isConstantIntegerExpression(strideArg)) {
            return false;
        }

        // Check timestamp arg references a timestamp column
        if (timestampArg == null || timestampArg.type != LITERAL) {
            return false;
        }

        // Check 1: Does it match the designated timestamp?
        // Use matchesColumnName to handle qualified references like v.timestamp
        ExpressionNode nestedTimestamp = nestedModel.getTimestamp();
        if (nestedTimestamp != null && matchesColumnName(timestampArg.token, nestedTimestamp.token)) {
            return true;
        }

        // Check 2: Does it match a column that has timestampColumnIndex set?
        // This handles chained dateadd: dateadd('d', -1, ts1) where ts1 is from dateadd('h', -1, timestamp)
        int tsColIndex = nestedModel.getTimestampColumnIndex();
        if (tsColIndex >= 0) {
            // Use getColumns() to match how the index was set in detectTimestampOffsetInfo
            ObjList<QueryColumn> cols = nestedModel.getColumns();
            if (tsColIndex < cols.size()) {
                QueryColumn tsCol = cols.getQuick(tsColIndex);
                if (matchesColumnName(timestampArg.token, tsCol.getAlias())) {
                    return true;
                }
            }
        }

        // Check 3: Does it match the timestampOffsetAlias? (for virtual models)
        CharSequence offsetAlias = nestedModel.getTimestampOffsetAlias();
        return offsetAlias != null && matchesColumnName(timestampArg.token, offsetAlias);
    }

    /**
     * Gets the timestamp column name for pushdown validation.
     * Returns the timestamp alias or designated timestamp token.
     */
    private CharSequence getTimestampColumnForPushdown(QueryModel model) {
        CharSequence alias = model.getTimestampOffsetAlias();
        if (alias != null) {
            return alias;
        }
        ExpressionNode ts = model.getTimestamp();
        if (ts != null) {
            return ts.token;
        }
        return null;
    }

    private boolean isEffectivelyConstantExpression(ExpressionNode node) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type != OPERATION
                    && node.type != CONSTANT
                    && !(node.type == FUNCTION && functionParser.getFunctionFactoryCache().isRuntimeConstant(node.token))) {
                return false;
            }

            if (node.lhs != null) {
                sqlNodeStack.push(node.lhs);
            }

            if (node.rhs != null) {
                node = node.rhs;
            } else {
                if (!sqlNodeStack.isEmpty()) {
                    node = this.sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }

        return true;
    }

    private boolean isIntegerConstant(@Nullable ExpressionNode n) {
        if (n == null || n.type != CONSTANT) {
            return false;
        }

        try {
            Numbers.parseLong(n.token);
            return true;
        } catch (NumericException ne) {
            return false;
        }
    }

    /**
     * Checks if a literal expression references the timestamp column, either directly or through
     * nested model aliases. This handles the case where qualified column references like "t.timestamp"
     * get resolved to aliases in intermediate translating models.
     */
    private boolean isLiteralTimestampReference(ExpressionNode ast, QueryModel nested, CharSequence sourceTimestampName) {
        // First, check direct match
        if (matchesColumnName(ast.token, sourceTimestampName)) {
            return true;
        }

        // Check if ast.token references a column in the nested model that maps to the timestamp
        if (nested != null) {
            QueryColumn nestedCol = nested.getAliasToColumnMap().get(ast.token);
            if (nestedCol != null) {
                ExpressionNode nestedAst = nestedCol.getAst();
                return nestedAst != null && nestedAst.type == LITERAL && matchesColumnName(nestedAst.token, sourceTimestampName);
            }
        }
        return false;
    }

    /**
     * Checks if the expression is dateadd(constant, constant, timestamp).
     */
    private boolean isDateaddWithTimestamp(ExpressionNode node, CharSequence timestampColumn) {
        if (node == null || node.type != FUNCTION) {
            return false;
        }
        if (!Chars.equalsLowerCaseAscii(node.token, "dateadd")) {
            return false;
        }
        // dateadd has 3 arguments: unit, amount, timestamp
        if (node.args.size() != 3) {
            return false;
        }
        // args are in reverse order: [timestamp, amount, unit]
        ExpressionNode timestampArg = node.args.getQuick(0);
        ExpressionNode amountArg = node.args.getQuick(1);
        ExpressionNode unitArg = node.args.getQuick(2);

        // unit and amount must be constants
        if (!isSimpleConstant(unitArg) || !isSimpleConstant(amountArg)) {
            return false;
        }
        // timestamp argument must reference the timestamp column
        return isTimestampLiteral(timestampArg, timestampColumn);
    }

    private boolean isSimpleIntegerColumn(ExpressionNode column, QueryModel model) {
        return checkSimpleIntegerColumn(column, model) != null;
    }

    /**
     * Checks if the expression is a simple constant value (not a function).
     * CONSTANT and LITERAL types are allowed (string/numeric constants).
     * FUNCTION types are rejected (like now(), sysdate, etc.).
     */
    private boolean isSimpleConstant(ExpressionNode node) {
        if (node == null) {
            return true;
        }
        return switch (node.type) {
            case CONSTANT, LITERAL ->
                // CONSTANT is for numeric constants, LITERAL includes string constants
                // Column references are also LITERAL but are handled by literalCheckingVisitor
                    true;
            case OPERATION ->
                // Allow arithmetic on simple constants (e.g., 1 + 2)
                    isSimpleConstant(node.lhs) && isSimpleConstant(node.rhs);
            default ->
                // Reject FUNCTION (like now()), etc.
                    false;
        };
    }

    private boolean isTimestampLiteral(ExpressionNode node, CharSequence timestampColumn) {
        return node != null && node.type == LITERAL && matchesColumnName(node.token, timestampColumn);
    }

    /**
     * Checks if the given predicate references the model's timestamp column or offset alias.
     */
    private boolean isTimestampPredicate(ExpressionNode node, QueryModel model) {
        // Check against the model's designated timestamp
        ExpressionNode ts = model.getTimestamp();
        if (ts != null && referencesColumn(node, ts.token)) {
            return true;
        }
        // Also check against the timestamp offset alias (for virtual models without timestamp designation)
        CharSequence alias = model.getTimestampOffsetAlias();
        return alias != null && referencesColumn(node, alias);
    }

    /**
     * Checks if the model lacks timestamp information needed for offset detection.
     * Returns true if none of: designated timestamp, timestampColumnIndex, or timestampOffsetAlias are set.
     */
    private boolean lacksTimestampInfo(QueryModel model) {
        if (model == null) {
            return true;
        }
        return model.getTimestamp() == null
                && model.getTimestampColumnIndex() < 0
                && model.getTimestampOffsetAlias() == null;
    }

    // Walks only the nested-model chain (not join models) because named windows are defined
    // on the masterModel and propagated through nesting, never on join models.
    // Stops at subquery boundaries to prevent resolving names from inner scopes.
    private WindowExpression lookupNamedWindow(QueryModel model, CharSequence windowName) {
        QueryModel current = model;
        while (current != null) {
            WindowExpression namedWindow = current.getNamedWindows().get(windowName);
            if (namedWindow != null) {
                return namedWindow;
            }
            if (current.isNestedModelIsSubQuery()) {
                break;
            }
            current = current.getNestedModel();
        }
        return null;
    }

    private ExpressionNode makeJoinAlias() {
        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(SUB_QUERY_ALIAS_PREFIX).put(defaultAliasCount++);
        return nextLiteral(characterStoreEntry.toImmutable());
    }

    private ExpressionNode makeModelAlias(CharSequence modelAlias, ExpressionNode node) {
        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(modelAlias).put('.').put(node.token);
        return nextLiteral(characterStoreEntry.toImmutable(), node.position);
    }

    private ExpressionNode makeOperation(CharSequence token, ExpressionNode lhs, ExpressionNode rhs) {
        OperatorExpression op = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition(token);
        ExpressionNode node = expressionNodePool.next().of(OPERATION, op.operator.token, op.precedence, 0);
        node.paramCount = 2;
        node.lhs = lhs;
        node.rhs = rhs;
        return node;
    }

    /**
     * Checks if a token matches a column name, handling qualified names.
     * "ts" matches "ts", and "v.ts" also matches "ts" (suffix after last dot).
     */
    private boolean matchesColumnName(CharSequence token, CharSequence columnName) {
        if (Chars.equalsIgnoreCase(token, columnName)) {
            return true;
        }
        // Check if token is qualified (contains a dot) and the suffix matches
        int dotIndex = Chars.lastIndexOf(token, 0, token.length(), '.');
        if (dotIndex >= 0 && dotIndex < token.length() - 1) {
            CharSequence suffix = token.subSequence(dotIndex + 1, token.length());
            return Chars.equalsIgnoreCase(suffix, columnName);
        }
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean matchesWithOrWithoutTablePrefix(@NotNull CharSequence name, @NotNull CharSequence table, CharSequence target) {
        if (target == null) {
            return false;
        }
        final int dotIndex = Chars.indexOfLastUnquoted(name, '.');
        return dotIndex > 0
                ? Chars.equalsIgnoreCase(table, name, 0, dotIndex) && Chars.equalsIgnoreCase(target, name, dotIndex + 1, name.length())
                : Chars.equalsIgnoreCase(name, target);
    }

    private JoinContext mergeContexts(QueryModel parent, JoinContext a, JoinContext b) {
        assert a.slaveIndex == b.slaveIndex;

        deletedContexts.clear();
        JoinContext r = contextPool.next();
        // check if we are merging a.x = b.x to a.y = b.y
        // or a.x = b.x to a.x = b.y, e.g., one of the columns in the same table
        for (int i = 0, n = b.aNames.size(); i < n; i++) {
            CharSequence ban = b.aNames.getQuick(i);
            int bai = b.aIndexes.getQuick(i);
            ExpressionNode bao = b.aNodes.getQuick(i);

            CharSequence bbn = b.bNames.getQuick(i);
            int bbi = b.bIndexes.getQuick(i);
            ExpressionNode bbo = b.bNodes.getQuick(i);

            for (int k = 0, z = a.aNames.size(); k < z; k++) {
                final CharSequence aan = a.aNames.getQuick(k);
                final int aai = a.aIndexes.getQuick(k);
                final ExpressionNode aao = a.aNodes.getQuick(k);
                final CharSequence abn = a.bNames.getQuick(k);
                final int abi = a.bIndexes.getQuick(k);
                final ExpressionNode abo = a.bNodes.getQuick(k);

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
            int max = Math.max(bai, bbi);
            int min = Math.min(bai, bbi);
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
                if (r.parents.excludes(min)) {
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
                r.slaveIndex = max;
                linkDependencies(parent, min, max);
            }
        }
        return r;
    }

    private JoinContext moveClauses(QueryModel parent, JoinContext from, JoinContext to, IntList positions) {
        int p = 0;
        int m = positions.size();

        JoinContext result = contextPool.next();
        result.slaveIndex = from.slaveIndex;

        for (int i = 0, n = from.aIndexes.size(); i < n; i++) {
            // logically, those clauses we move away from "from" context
            // should no longer exist in "from", but instead of implementing
            // "delete" function, which would be manipulating an underlying array
            // on every invocation; we copy retained clauses to new context,
            // which is "result".
            // hence, whenever it exists in "positions" we copy clause to "to"
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

    private QueryModel moveOrderByFunctionsIntoOuterSelect(QueryModel model) throws SqlException {
        // at this point order by should be on the nested model of this model :)
        QueryModel unionModel = model.getUnionModel();
        if (unionModel != null) {
            model.setUnionModel(moveOrderByFunctionsIntoOuterSelect(unionModel));
        }

        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            for (int jm = 0, jmn = nested.getJoinModels().size(); jm < jmn; jm++) {
                QueryModel joinModel = nested.getJoinModels().getQuick(jm);
                if (joinModel != nested && joinModel.getNestedModel() != null) {
                    joinModel.setNestedModel(moveOrderByFunctionsIntoOuterSelect(joinModel.getNestedModel()));
                }
            }

            QueryModel nestedNested = nested.getNestedModel();
            if (nestedNested != null) {
                nested.setNestedModel(moveOrderByFunctionsIntoOuterSelect(nestedNested));
            }

            final ObjList<ExpressionNode> orderBy = nested.getOrderBy();
            final int n = orderBy.size();
            boolean moved = false;
            for (int i = 0; i < n; i++) {
                ExpressionNode node = orderBy.getQuick(i);
                if (node.type == FUNCTION || node.type == OPERATION) {
                    var qc = findQueryColumnByAst(model.getBottomUpColumns(), node);
                    if (qc == null) {
                        // add this function to bottom-up columns and replace this expression with index
                        CharSequence alias = SqlUtil.createColumnAlias(
                                characterStore,
                                node.token,
                                Chars.indexOfLastUnquoted(node.token, '.'),
                                model.getAliasToColumnMap(),
                                model.getAliasSequenceMap(),
                                true
                        );
                        qc = queryColumnPool.next().of(
                                alias,
                                node,
                                false
                        );
                        model.getAliasToColumnMap().put(alias, qc);
                        model.getBottomUpColumns().add(qc);
                        moved = true;
                    }
                    // on "else" branch, when order by expression matched projection
                    // we can just replace order by with projection alias without having to
                    // add an extra model
                    orderBy.setQuick(i, nextLiteral(qc.getAlias()));
                }
            }

            if (moved) {
                return wrapWithSelectWildcard(model);
            }
        }
        return model;
    }

    private void moveTimestampToChooseModel(QueryModel model) {
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
            ExpressionNode timestamp = nested.getTimestamp();
            if (
                    timestamp != null
                            && nested.getSelectModelType() == SELECT_MODEL_NONE
                            && nested.getTableName() == null
                            && nested.getTableNameFunction() == null
                            && nested.getLatestBy().size() == 0
            ) {
                model.setTimestamp(timestamp);
                model.setExplicitTimestamp(nested.isExplicitTimestamp());
                nested.setTimestamp(null);
                nested.setExplicitTimestamp(false);
            }
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        if (joinModels.size() > 1) {
            for (int i = 1, n = joinModels.size(); i < n; i++) {
                moveTimestampToChooseModel(joinModels.getQuick(i));
            }
        }

        nested = model.getUnionModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
        }
    }

    private void mergeWindowSpec(WindowExpression child, WindowExpression base) {
        // SQL standard merge rules:
        // PARTITION BY: child must not have its own (enforced by parser) — copy from base
        if (child.getPartitionBy().size() == 0 && base.getPartitionBy().size() > 0) {
            for (int i = 0, n = base.getPartitionBy().size(); i < n; i++) {
                child.getPartitionBy().add(ExpressionNode.deepClone(expressionNodePool, base.getPartitionBy().getQuick(i)));
            }
        }
        // ORDER BY: if child has ORDER BY, use child's; otherwise copy from base
        if (child.getOrderBy().size() == 0 && base.getOrderBy().size() > 0) {
            for (int i = 0, n = base.getOrderBy().size(); i < n; i++) {
                child.getOrderBy().add(ExpressionNode.deepClone(expressionNodePool, base.getOrderBy().getQuick(i)));
            }
            child.getOrderByDirection().addAll(base.getOrderByDirection());
        }
        // Frame: if child has a non-default frame, use child's; otherwise copy from base
        if (!child.isNonDefaultFrame() && base.isNonDefaultFrame()) {
            child.setFramingMode(base.getFramingMode());
            child.setRowsLo(base.getRowsLo());
            child.setRowsLoExpr(
                    ExpressionNode.deepClone(expressionNodePool, base.getRowsLoExpr()),
                    base.getRowsLoExprPos()
            );
            child.setRowsLoExprTimeUnit(base.getRowsLoExprTimeUnit());
            child.setRowsLoKind(base.getRowsLoKind(), base.getRowsLoKindPos());
            child.setRowsHi(base.getRowsHi());
            child.setRowsHiExpr(
                    ExpressionNode.deepClone(expressionNodePool, base.getRowsHiExpr()),
                    base.getRowsHiExprPos()
            );
            child.setRowsHiExprTimeUnit(base.getRowsHiExprTimeUnit());
            child.setRowsHiKind(base.getRowsHiKind(), base.getRowsHiKindPos());
            child.setExclusionKind(base.getExclusionKind(), base.getExclusionKindPos());
        }
    }

    private void moveWhereInsideSubQueries(QueryModel model) throws SqlException {
        if (
                model.getSelectModelType() != SELECT_MODEL_DISTINCT
                        // in theory, we could push down predicates as long as they align with ALL partition by clauses and remove whole partition(s)
                        && model.getSelectModelType() != SELECT_MODEL_WINDOW
        ) {
            model.getParsedWhere().clear();
            final ObjList<ExpressionNode> nodes = model.parseWhereClause();
            model.setWhereClause(null);

            final int n = nodes.size();
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    final ExpressionNode node = nodes.getQuick(i);
                    // collect table references this where clause element
                    literalCollectorAIndexes.clear();
                    literalCollectorANames.clear();
                    literalCollector.withModel(model);
                    literalCollector.resetCounts();
                    traversalAlgo.traverse(node, literalCollector.lhs());

                    tempIntList.clear();
                    for (int j = 0; j < literalCollectorAIndexes.size(); j++) {
                        int tableExpressionReference = literalCollectorAIndexes.get(j);
                        int position = tempIntList.binarySearchUniqueList(tableExpressionReference);
                        if (position < 0) {
                            tempIntList.insert(-(position + 1), tableExpressionReference);
                        }
                    }

                    int distinctIndexes = tempIntList.size();

                    // at this point, we must not have constant conditions in where clause
                    // this could be either referencing constant of a sub-query
                    if (literalCollectorAIndexes.size() == 0) {
                        // keep condition with this model
                        addWhereNode(model, node);
                        continue;
                    } else if (distinctIndexes > 1) {
                        int greatest = tempIntList.get(distinctIndexes - 1);
                        final QueryModel m = model.getJoinModels().get(greatest);
                        m.setPostJoinWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, m.getPostJoinWhereClause(), nodes.getQuick(i)));
                        continue;
                    }

                    // by now all where clause must reference single table only and all column references have to be valid,
                    // they would have been rewritten and validated as join analysis stage
                    final int tableIndex = literalCollectorAIndexes.get(0);
                    final QueryModel parent = model.getJoinModels().getQuick(tableIndex);

                    // Do not move where clauses inside outer join models because that'd change result
                    int joinType = parent.getJoinType();
                    if (tableIndex > 0
                            && (joinBarriers.contains(joinType))
                    ) {
                        QueryModel joinModel = model.getJoinModels().getQuick(tableIndex);
                        joinModel.setPostJoinWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, joinModel.getPostJoinWhereClause(), node));
                        continue;
                    }

                    final QueryModel nested = parent.getNestedModel();

                    // Detect if the parent's timestamp column is derived from a dateadd expression
                    // on the nested model's timestamp. This enables timestamp predicate pushdown with offset.
                    if (nested != null && !parent.hasTimestampOffset()) {
                        detectTimestampOffset(parent, nested);
                    }

                    if (nested == null
                            || nested.getLatestBy().size() > 0
                            || nested.getLimitLo() != null
                            || nested.getLimitHi() != null
                            || nested.getUnionModel() != null
                            || (nested.getSampleBy() != null && !canPushToSampleBy(nested, literalCollectorANames))
                    ) {
                        // there is no nested model for this table, keep where clause element with this model
                        addWhereNode(parent, node);
                    } else {
                        // now that we have identified sub-query we have to rewrite our where clause
                        // to potentially replace all column references with actual literals used inside
                        // sub-query, for example:
                        // (select a x, b from T) where x = 10
                        // we can't move "x" inside sub-query because it is not a field.
                        // Instead, we have to translate "x" to actual column expression, which is "a":
                        // select a x, b from T where a = 10

                        // because we are rewriting SqlNode in-place we need to make sure that
                        // none of expression literals reference non-literals in nested query, e.g.
                        // (select a+b x from T) where x > 10
                        // does not warrant inlining of "x > 10" because "x" is not a column
                        //
                        // at this step we would throw exception if one of our literals hits non-literal
                        // in sub-query

                        try {
                            traversalAlgo.traverse(node, literalCheckingVisitor.of(parent.getAliasToColumnMap()));

                            // go ahead and rewrite expression
                            traversalAlgo.traverse(node, literalRewritingVisitor.of(parent.getAliasToColumnNameMap()));

                            // whenever nested model has explicitly defined columns it must also
                            // have its own nested model, where we assign new "where" clauses
                            addWhereNode(nested, node);
                            // we do not have to deal with "union" models here
                            // because "where" clause is made to apply to the result of the union
                        } catch (NonLiteralException ignore) {
                            // Check if this is a timestamp predicate on a model with timestamp offset.
                            // If so, we can still push it down by wrapping it in and_offset.
                            // But only if the predicate references only the timestamp alias.
                            CharSequence timestampCol = getTimestampColumnForPushdown(parent);
                            if (parent.hasTimestampOffset()
                                    && isTimestampPredicate(node, parent)
                                    && referencesOnlyTimestampAlias(node, parent)
                                    && !containsDisallowedFunction(node, timestampCol)) {
                                // Rewrite column references from virtual timestamp to source column
                                rewriteTimestampColumnForOffset(node, parent);
                                // Wrap in and_offset and push to nested model
                                ExpressionNode wrapped = wrapInAndOffset(node, parent);
                                addWhereNode(nested, wrapped);
                            } else {
                                // keep node where it is
                                addWhereNode(parent, node);
                            }
                        }
                    }
                }
                model.getParsedWhere().clear();
            }
        }

        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            moveWhereInsideSubQueries(nested);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, m = joinModels.size(); i < m; i++) {
            nested = joinModels.getQuick(i);
            if (nested != model) {
                moveWhereInsideSubQueries(nested);
            }
        }

        nested = model.getUnionModel();
        if (nested != null) {
            moveWhereInsideSubQueries(nested);
        }
    }

    private QueryColumn nextColumn(CharSequence name) {
        return nextColumn(name, 0);
    }

    private QueryColumn nextColumn(CharSequence name, int position) {
        return SqlUtil.nextColumn(queryColumnPool, expressionNodePool, name, name, position);
    }

    private QueryColumn nextColumn(CharSequence alias, CharSequence column) {
        return SqlUtil.nextColumn(queryColumnPool, expressionNodePool, alias, column, 0);
    }

    private ExpressionNode nextLiteral(CharSequence token, int position) {
        return SqlUtil.nextLiteral(expressionNodePool, token, position);
    }

    private ExpressionNode nextLiteral(CharSequence token) {
        return nextLiteral(token, 0);
    }

    private void normalizeWindowFrame(WindowExpression ac, SqlExecutionContext sqlExecutionContext) throws SqlException {
        long rowsLo = evalNonNegativeLongConstantOrDie(functionParser, ac.getRowsLoExpr(), sqlExecutionContext);
        long rowsHi = evalNonNegativeLongConstantOrDie(functionParser, ac.getRowsHiExpr(), sqlExecutionContext);

        switch (ac.getRowsLoKind()) {
            case WindowExpression.PRECEDING:
                rowsLo = rowsLo != Long.MAX_VALUE ? -rowsLo : Long.MIN_VALUE;
                break;
            case WindowExpression.FOLLOWING:
                break;
            default:
                // CURRENT ROW
                rowsLo = 0;
                break;
        }

        switch (ac.getRowsHiKind()) {
            case WindowExpression.PRECEDING:
                rowsHi = rowsHi != Long.MAX_VALUE ? -rowsHi : Long.MIN_VALUE;
                break;
            case WindowExpression.FOLLOWING:
                break;
            default:
                // CURRENT ROW
                rowsHi = 0;
                break;
        }

        ac.setRowsLo(rowsLo);
        ac.setRowsHi(rowsHi);
    }

    private boolean nonAggregateFunctionDependsOn(ExpressionNode node, ExpressionNode timestampNode) {
        if (timestampNode == null) {
            return false;
        }

        final CharSequence timestamp = timestampNode.token;
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type == LITERAL && Chars.equalsIgnoreCase(node.token, timestamp)) {
                return true;
            }

            if (node.type != FUNCTION || !functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
                if (node.paramCount < 3) {
                    if (node.lhs != null) {
                        sqlNodeStack.push(node.lhs);
                    }

                    if (node.rhs != null) {
                        node = node.rhs;
                        continue;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        sqlNodeStack.push(node.args.getQuick(i));
                    }
                    node = node.args.getQuick(0);
                    continue;
                }
            }

            if (!sqlNodeStack.isEmpty()) {
                node = this.sqlNodeStack.poll();
            } else {
                node = null;
            }
        }

        return false;
    }

    private void openReaderAndEnumerateColumns(
            SqlExecutionContext executionContext,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final ExpressionNode tableNameExpr = model.getTableNameExpr();

        // table name must not contain quotes by now
        final CharSequence tableName = tableNameExpr.token;
        final int tableNamePosition = tableNameExpr.position;

        int lo = 0;
        int hi = tableName.length();
        if (Chars.startsWith(tableName, NO_ROWID_MARKER)) {
            lo += NO_ROWID_MARKER.length();
        }

        if (lo == hi) {
            throw SqlException.$(tableNamePosition, "come on, where is the table name?");
        }

        final TableToken tableToken = executionContext.getTableTokenIfExists(tableName, lo, hi);
        int status = executionContext.getTableStatus(path, tableToken);

        if (status == TableUtils.TABLE_DOES_NOT_EXIST) {
            try {
                model.getTableNameExpr().type = FUNCTION;
                parseFunctionAndEnumerateColumns(model, executionContext, sqlParserCallback);
                return;
            } catch (SqlException e) {
                throw SqlException.tableDoesNotExist(tableNamePosition, tableName);
            }
        }

        if (status == TableUtils.TABLE_RESERVED) {
            throw SqlException.$(tableNamePosition, "table directory is of unknown format [table=").put(tableName).put(']');
        }

        if (model.isUpdate()) {
            assert lo == 0;
            try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken, model.getMetadataVersion())) {
                enumerateColumns(model, metadata);
            } catch (CairoException e) {
                if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                    throw e;
                }
                throw SqlException.position(tableNamePosition).put(e);
            }
        } else {
            try (TableReader reader = executionContext.getReader(tableToken)) {
                enumerateColumns(model, reader.getMetadata());
            } catch (EntryLockedException e) {
                throw SqlException.position(tableNamePosition).put("table is locked: ").put(tableToken.getTableName());
            } catch (CairoException e) {
                if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                    throw e;
                }
                throw SqlException.position(tableNamePosition).put(e);
            }
        }
    }

    private ExpressionNode optimiseBooleanNot(final ExpressionNode node, boolean reverse) {
        if (node.token != null) {
            switch (notOps.get(node.token)) {
                case NOT_OP_NOT:
                    if (reverse) {
                        return optimiseBooleanNot(node.rhs, false);
                    } else {
                        switch (node.rhs.type) {
                            case LITERAL:
                            case CONSTANT:
                                break;
                            default:
                                return optimiseBooleanNot(node.rhs, true);
                        }
                    }
                    break;
                case NOT_OP_AND:
                    if (reverse) {
                        node.token = "or";
                    }
                    node.lhs = optimiseBooleanNot(node.lhs, reverse);
                    node.rhs = optimiseBooleanNot(node.rhs, reverse);
                    break;
                case NOT_OP_OR:
                    if (reverse) {
                        node.token = "and";
                    }
                    node.lhs = optimiseBooleanNot(node.lhs, reverse);
                    node.rhs = optimiseBooleanNot(node.rhs, reverse);
                    break;
                case NOT_OP_GREATER:
                    if (reverse) {
                        node.token = "<=";
                    }
                    break;
                case NOT_OP_GREATER_EQ:
                    if (reverse) {
                        node.token = "<";
                    }
                    break;
                case NOT_OP_LESS:
                    if (reverse) {
                        node.token = ">=";
                    }
                    break;
                case NOT_OP_LESS_EQ:
                    if (reverse) {
                        node.token = ">";
                    }
                    break;
                case NOT_OP_EQUAL:
                    if (reverse) {
                        node.token = "!=";
                    }
                    break;
                case NOT_OP_NOT_EQ:
                    if (reverse) {
                        node.token = "=";
                    } else {
                        node.token = "!=";
                    }
                    break;
                default:
                    if (reverse) {
                        ExpressionNode n = expressionNodePool.next();
                        n.token = "not";
                        n.paramCount = 1;
                        n.rhs = node;
                        n.type = OPERATION;
                        return n;
                    }
                    break;
            }
        }
        return node;
    }

    private void optimiseBooleanNot(QueryModel model) {
        ExpressionNode where = model.getWhereClause();
        if (where != null) {
            model.setWhereClause(optimiseBooleanNot(where, false));
        }

        if (model.getNestedModel() != null) {
            optimiseBooleanNot(model.getNestedModel());
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            optimiseBooleanNot(joinModels.getQuick(i));
        }

        if (model.getUnionModel() != null && model.getNestedModel() != null) {
            optimiseBooleanNot(model.getNestedModel());
        }
    }

    private void optimiseExpressionModels(
            QueryModel model,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        ObjList<ExpressionNode> expressionModels = model.getExpressionModels();
        final int n = expressionModels.size();
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                final ExpressionNode node = expressionModels.getQuick(i);
                // for expression models that have been converted to
                // the joins, the query model will be set to null.
                if (node.queryModel != null) {
                    QueryModel optimised = optimise(node.queryModel, executionContext, sqlParserCallback);
                    if (optimised != node.queryModel) {
                        node.queryModel = optimised;
                    }
                }
            }
        }

        if (model.getNestedModel() != null) {
            optimiseExpressionModels(model.getNestedModel(), executionContext, sqlParserCallback);
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        final int m = joinModels.size();
        // as usual, we already optimised self (index=0), now optimised others
        if (m > 1) {
            for (int i = 1; i < m; i++) {
                optimiseExpressionModels(joinModels.getQuick(i), executionContext, sqlParserCallback);
            }
        }

        // call out to union models
        if (model.getUnionModel() != null) {
            optimiseExpressionModels(model.getUnionModel(), executionContext, sqlParserCallback);
        }
    }

    private void optimiseJoins(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();

        int n = joinModels.size();
        if (n > 1) {
            emittedJoinClauses = joinClausesSwap1;
            emittedJoinClauses.clear();

            // for sake of clarity, "model" model is the first in the list of
            // joinModels, e.g. joinModels.get(0) == model
            // only "model" model is allowed to have "where" clause,
            // so we can assume that "where" clauses of joinModel elements are all null (except for element 0).
            // in case one of joinModels is suburb, its entire query model will be set as
            // nestedModel, e.g. "where" clause is still null there as well

            ExpressionNode where = model.getWhereClause();

            // clear where clause of model so that
            // optimiser can assign there correct nodes

            model.setWhereClause(null);
            processJoinConditions(model, where, false, model, -1);

            for (int i = 1; i < n; i++) {
                processJoinConditions(model, joinModels.getQuick(i).getJoinCriteria(), true, joinModels.getQuick(i), i);
            }

            processEmittedJoinClauses(model);
            createImpliedDependencies(model);
            homogenizeCrossJoins(model);
            reorderTables(model);
            assignFilters(model);
            alignJoinClauses(model);
            addTransitiveFilters(model);
        }

        for (int i = 0; i < n; i++) {
            QueryModel m = model.getJoinModels().getQuick(i).getNestedModel();
            if (m != null) {
                optimiseJoins(m);
            }

            m = model.getJoinModels().getQuick(i).getUnionModel();
            if (m != null) {
                clearForUnionModelInJoin();
                optimiseJoins(m);
            }
        }
    }

    // removes redundant order by clauses from sub-queries (only those that don't force materialization of other order by clauses )
    private void optimiseOrderBy(QueryModel model, int topLevelOrderByMnemonic) {
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        int orderByMnemonic;
        int n = columns.size();

        // limit x,y forces order materialization; we can't push order by past it and need to discover actual nested ordering
        if (model.getLimitLo() != null) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_UNKNOWN;
        }

        // if model has explicit timestamp then we should detect and preserve actual order because it might be used for asof/lt/splice join
        if (model.getTimestamp() != null) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
        }

        // keep order by on model with window functions to speed up query (especially when it matches window order by)
        if (model.getSelectModelType() == SELECT_MODEL_WINDOW && model.getOrderBy().size() > 0) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
        }

        // determine if ordering is required
        switch (topLevelOrderByMnemonic) {
            case OrderByMnemonic.ORDER_BY_UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                if (model.getOrderBy().size() > 0 && model.getSampleBy() == null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                if (model.getSampleBy() == null && orderByMnemonic != OrderByMnemonic.ORDER_BY_INVARIANT) {
                    for (int i = 0; i < n; i++) {
                        QueryColumn col = columns.getQuick(i);
                        if (hasGroupByFunc(sqlNodeStack, functionParser.getFunctionFactoryCache(), col.getAst())) {
                            orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                            break;
                        }
                    }
                }
                break;
            case OrderByMnemonic.ORDER_BY_REQUIRED:
                // parent requires order
                // if this model forces ordering - sub-query ordering is not needed
                if (model.getOrderBy().size() > 0) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                break;
            default:
                // sub-query ordering is not needed, but we'd like to propagate order by advice (if possible)
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                }
                break;
        }

        // some aggregate functions (e.g. first()) rely on the rows being properly ordered to return the correct result
        if (model.getSelectModelType() == SELECT_MODEL_GROUP_BY) {
            for (int i = 0; i < n && orderByMnemonic != OrderByMnemonic.ORDER_BY_REQUIRED; i++) {
                if (hasOrderedGroupByFunc(columns.getQuick(i).getAst())) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                    break;
                }
            }
        }

        final ObjList<ExpressionNode> orderByAdvice = getOrderByAdvice(model, orderByMnemonic);
        final IntList orderByDirectionAdvice = getOrderByAdviceDirection(model, orderByMnemonic);

        if (
                model.getSelectModelType() == SELECT_MODEL_WINDOW
                        && model.getOrderBy().size() > 0
                        && model.getOrderByAdvice().size() > 0
                        && model.getLimitLo() == null
        ) {
            boolean orderChanges = false;
            if (orderByAdvice.size() != model.getOrderByAdvice().size()) {
                orderChanges = true;
            } else {
                for (int i = 0, max = orderByAdvice.size(); i < max; i++) {
                    if (!orderByAdvice.getQuick(i).equals(model.getOrderBy().getQuick(i)) ||
                            orderByDirectionAdvice.getQuick(i) != model.getOrderByDirection().getQuick(i)) {
                        orderChanges = true;
                    }
                }
            }

            if (orderChanges) {
                // Set artificial limit to trigger LimitRCF use, so that parent models don't use the followedOrderByAdvice flag and skip necessary sort
                // Currently the only way to delineate order by advice is through use of factory that returns false for followedOrderByAdvice().
                // TODO: factories should provide order metadata to enable better sort-skipping
                model.setLimit(expressionNodePool.next().of(CONSTANT, LONG_MAX_VALUE_STR, Integer.MIN_VALUE, 0), null);
            }
        }

        final ObjList<QueryModel> jm = model.getJoinModels();
        pushDownOrderByAdviceToJoinModels(model, jm, orderByMnemonic, orderByDirectionAdvice);

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            union.copyOrderByAdvice(orderByAdvice);
            union.copyOrderByDirectionAdvice(orderByDirectionAdvice);
            union.setOrderByAdviceMnemonic(orderByMnemonic);
            optimiseOrderBy(union, orderByMnemonic);
        }
    }

    private void parseFunctionAndEnumerateColumns(
            @NotNull QueryModel model,
            @NotNull SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final RecordCursorFactory tableFactory;
        TableToken tableToken;
        if (model.getSelectModelType() == SELECT_MODEL_SHOW) {
            switch (model.getShowKind()) {
                case SHOW_TABLES:
                    tableFactory = new AllTablesFunctionFactory.AllTablesCursorFactory();
                    break;
                case SHOW_COLUMNS:
                    tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
                    if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
                        throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
                    }
                    tableFactory = new ShowColumnsRecordCursorFactory(tableToken, model.getTableNameExpr().position);
                    break;
                case SHOW_PARTITIONS:
                    tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
                    if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
                        throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
                    }

                    int timestampType;
                    try (TableMetadata metadata = executionContext.getCairoEngine().getTableMetadata(tableToken)) {
                        timestampType = metadata.getTimestampType();
                    }
                    tableFactory = new ShowPartitionsRecordCursorFactory(tableToken, timestampType);
                    break;
                case SHOW_TRANSACTION:
                case SHOW_TRANSACTION_ISOLATION_LEVEL:
                    tableFactory = new ShowTransactionIsolationLevelCursorFactory();
                    break;
                case SHOW_DEFAULT_TRANSACTION_READ_ONLY:
                    tableFactory = new ShowDefaultTransactionReadOnlyCursorFactory();
                    break;
                case SHOW_MAX_IDENTIFIER_LENGTH:
                    tableFactory = new ShowMaxIdentifierLengthCursorFactory();
                    break;
                case SHOW_STANDARD_CONFORMING_STRINGS:
                    tableFactory = new ShowStandardConformingStringsCursorFactory();
                    break;
                case SHOW_SEARCH_PATH:
                    tableFactory = new ShowSearchPathCursorFactory();
                    break;
                case SHOW_DATE_STYLE:
                    tableFactory = new ShowDateStyleCursorFactory();
                    break;
                case SHOW_TIME_ZONE:
                    tableFactory = new ShowTimeZoneFactory();
                    break;
                case SHOW_PARAMETERS:
                    tableFactory = new ShowParametersCursorFactory();
                    break;
                case SHOW_SERVER_VERSION:
                    tableFactory = new ShowServerVersionCursorFactory();
                    break;
                case SHOW_SERVER_VERSION_NUM:
                    tableFactory = new ShowServerVersionNumCursorFactory();
                    break;
                case SHOW_CREATE_TABLE:
                    tableFactory = sqlParserCallback.generateShowCreateTableFactory(model, executionContext, path);
                    break;
                case SHOW_CREATE_MAT_VIEW:
                    tableFactory = sqlParserCallback.generateShowCreateMatViewFactory(model, executionContext, path);
                    break;
                case QueryModel.SHOW_CREATE_VIEW:
                    tableFactory = sqlParserCallback.generateShowCreateViewFactory(model, executionContext, path);
                    break;
                default:
                    tableFactory = sqlParserCallback.generateShowSqlFactory(model);
                    break;
            }
            model.setTableNameFunction(tableFactory);
        } else {
            // if we haven't initialised the model, initialise it
            if (model.getTableNameFunction() == null) {
                tableFactory = TableUtils.createCursorFunction(functionParser, model, executionContext).getRecordCursorFactory();
                model.setTableNameFunction(tableFactory);
                tableFactoriesInFlight.add(tableFactory);
            }
        }
        copyColumnsFromMetadata(model, model.getTableNameFunction().getMetadata());
    }

    /**
     * Executes PIVOT IN subqueries and converts them to value lists.
     * <p>
     * Subqueries must be executed during the planning phase (not at runtime) because
     * PIVOT's IN values determine the output column metadata. Since RecordCursorFactory
     * requires fixed metadata at compile time, we cannot defer subquery execution to runtime.
     *
     * @return the number of FOR value combinations (Cartesian product of all FOR column values)
     */
    private int preparePivotForSelectSubquery(QueryModel model, QueryModel outerModel, SqlExecutionContext sqlExecutionContext) throws SqlException {
        ObjList<PivotForColumn> pivotForColumns = model.getPivotForColumns();
        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        int forValueCombinations = 1;
        int maxPivotProducedColumns = engine.getConfiguration().getSqlPivotMaxProducedColumns();
        SqlCompiler compiler = null;
        try {
            for (int i = 0, n = pivotForColumns.size(); i < n; i++) {
                if (!pivotForColumns.getQuick(i).isValueList()) {
                    compiler = engine.getSqlCompiler();
                    break;
                }
            }

            for (int i = 0, n = pivotForColumns.size(); i < n; i++) {
                PivotForColumn pivotForColumn = pivotForColumns.getQuick(i);
                if (!pivotForColumn.isValueList()) {
                    ExpressionNode subQueryNode = pivotForColumn.getSelectSubqueryExpr();
                    assert subQueryNode != null;
                    assert compiler != null;
                    try (RecordCursorFactory inListFactory = compiler.generateSelectWithRetries(subQueryNode.queryModel, null, sqlExecutionContext, true)) {
                        final RecordMetadata inListMetadata = inListFactory.getMetadata();
                        final int columnCount = inListMetadata.getColumnCount();
                        if (columnCount != 1) {
                            throw SqlException
                                    .$(subQueryNode.position, "PIVOT IN subquery must return exactly one column, got ")
                                    .put(columnCount);
                        }
                        final int columnType = inListMetadata.getColumnMetadata(0).getColumnType();
                        final boolean quote = switch (ColumnType.tagOf(columnType)) {
                            case ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.TIMESTAMP,
                                 ColumnType.DATE, ColumnType.CHAR, ColumnType.UUID, ColumnType.IPv4, ColumnType.ARRAY,
                                 ColumnType.LONG128, ColumnType.LONG256 -> true;
                            default -> false;
                        };
                        int valueCount = 0;
                        tempCharSequenceHashSet.clear();
                        pivotAliasMap.clear();
                        pivotAliasSequenceMap.clear();

                        try (RecordCursor cursor = inListFactory.getCursor(sqlExecutionContext)) {
                            while (cursor.hasNext()) {
                                final Record record = cursor.getRecord();
                                boolean isNull = printRecordColumnOrNull(record, inListMetadata, tmpStringSink, subQueryNode.position);
                                if (!tempCharSequenceHashSet.add(tmpStringSink)) {
                                    continue;
                                }
                                CharacterStoreEntry quotedCse = characterStore.newEntry();
                                if (quote && !isNull) {
                                    quotedCse.put('\'');
                                }
                                quotedCse.put(tmpStringSink);
                                if (quote && !isNull) {
                                    quotedCse.put('\'');
                                }

                                CharSequence token = quotedCse.toImmutable();
                                CharSequence alias = SqlUtil.createExprColumnAlias(
                                        characterStore,
                                        unquote(token),
                                        pivotAliasMap,
                                        pivotAliasSequenceMap,
                                        configuration.getColumnAliasGeneratedMaxSize(),
                                        true
                                );
                                pivotAliasMap.add(alias);
                                pivotForColumn.addValue(expressionNodePool.next().of(CONSTANT, token, 0, subQueryNode.position), alias);
                                valueCount++;
                                if (forValueCombinations * valueCount > maxPivotProducedColumns) {
                                    throw SqlException
                                            .$(subQueryNode.position, "PIVOT produces too many columns: ")
                                            .put(forValueCombinations * valueCount)
                                            .put(", limit is ")
                                            .put(maxPivotProducedColumns);
                                }
                            }
                        }
                        if (valueCount == 0) {
                            throw SqlException.$(subQueryNode.position, "PIVOT IN subquery returned empty result set");
                        }
                    }
                    pivotForColumn.setSelectSubqueryExpr(null);
                    pivotForColumn.setIsValueList(true);
                    outerModel.setCacheable(false);
                }
                forValueCombinations *= pivotForColumn.getValueList().size();
                if (forValueCombinations > maxPivotProducedColumns) {
                    throw SqlException
                            .$(pivotForColumn.getInExpr().position, "PIVOT produces too many columns: ")
                            .put(forValueCombinations)
                            .put(", limit is ")
                            .put(maxPivotProducedColumns);
                }
            }

            int totalColumnCount = forValueCombinations * model.getPivotGroupByColumns().size();
            if (totalColumnCount > maxPivotProducedColumns) {
                throw SqlException
                        .$(model.getModelPosition(), "PIVOT produces too many columns: ")
                        .put(totalColumnCount)
                        .put(", limit is ")
                        .put(maxPivotProducedColumns);
            }
            return forValueCombinations;
        } finally {
            Misc.free(compiler);
        }
    }

    private void processEmittedJoinClauses(QueryModel model) {
        // pick up join clauses emitted at initial analysis stage
        // as we merge contexts at this level no more clauses is to be emitted
        for (int i = 0, k = emittedJoinClauses.size(); i < k; i++) {
            addJoinContext(model, emittedJoinClauses.getQuick(i));
        }
    }

    /**
     * Splits "where" clauses into "and" concatenated list of boolean expressions.
     *
     * @param node expression n
     */
    private void processJoinConditions(
            QueryModel parent,
            ExpressionNode node,
            boolean innerPredicate,
            QueryModel joinModel,
            int joinIndex
    ) throws SqlException {
        ExpressionNode n = node;
        // pre-order traversal
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (joinOps.get(n.token)) {
                    case JOIN_OP_EQUAL:
                        analyseEquals(parent, n, innerPredicate, joinModel, joinIndex);
                        n = null;
                        break;
                    case JOIN_OP_AND:
                        if (n.rhs != null) {
                            sqlNodeStack.push(n.rhs);
                        }
                        n = n.lhs;
                        break;
                    case JOIN_OP_REGEX:
                        analyseRegex(parent, n);
                        if (joinBarriers.contains(joinModel.getJoinType())) {
                            addOuterJoinExpression(parent, joinModel, joinIndex, n);
                        } else {
                            parent.addParsedWhereNode(n, innerPredicate);
                        }
                        n = null;
                        break;
                    default:
                        if (joinBarriers.contains(joinModel.getJoinType())) {
                            addOuterJoinExpression(parent, joinModel, joinIndex, n);
                        } else {
                            parent.addParsedWhereNode(n, innerPredicate);
                        }
                        n = null;
                        break;
                }
            } else {
                n = sqlNodeStack.poll();
            }
        }
    }

    private void propagateHintsTo(QueryModel targetModel, LowerCaseCharSequenceObjHashMap<CharSequence> hints) {
        if (targetModel == null) {
            return;
        }

        targetModel.copyHints(hints);
        var h = targetModel.getHints();

        QueryModel nestedModel = targetModel.getNestedModel();
        if (nestedModel != null) {
            // A CTE is not within the lexical scope of its parent model. Don't propagate hints into it.
            // However, starting from the CTE model, do propagate its hints into its nested models.
            propagateHintsTo(nestedModel, nestedModel.isCteModel() ? nestedModel.getHints() : h);
        }

        // propagate hints to join models
        var joinModels = targetModel.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            propagateHintsTo(joinModels.getQuick(i), h);
        }
        // propagate to union model
        propagateHintsTo(targetModel.getUnionModel(), h);
    }

    private void propagateTopDownColumns(QueryModel model, boolean allowColumnChange) {
        propagateTopDownColumns0(model, true, null, allowColumnChange);
    }

    /**
     * Pushes columns from top to bottom models.
     * <p>
     * Adding or removing columns to/from union, except, intersect should not happen!
     * UNION/INTERSECT/EXCEPT-ed columns MUST be exactly as specified in the query, otherwise they might produce different result, e.g.
     * <pre>
     * SELECT a
     * FROM (
     *   SELECT 1 as a, 'b' as status
     *   UNION
     *   SELECT 1 as a, 'c' as status
     * );
     * </pre>
     * Now if we push a top-to-bottom and remove b from union column list then we'll get a single '1' but we should get two!
     * Same thing applies to INTERSECT & EXCEPT
     * The only thing that'd be safe to add SET models is a constant literal (but what's the point?).
     * Column/expression pushdown should (probably) ONLY happen for UNION with ALL!
     * <p>
     * allowColumnsChange - determines whether changing columns of given model is acceptable.
     * It is not for columns used in distinct, except, intersect, union (even transitively for the latter three!).
     */
    private void propagateTopDownColumns0(QueryModel model, boolean topLevel, @Nullable QueryModel papaModel, boolean allowColumnsChange) {
        // copy columns to 'protect' column list that shouldn't be modified
        if (!allowColumnsChange && model.getBottomUpColumns().size() > 0) {
            model.copyBottomToTopColumns();
        }

        // skip over NONE model that does not have a table name
        final QueryModel nested = skipNoneTypeModels(model.getNestedModel());
        model.setNestedModel(nested);
        final boolean nestedIsFlex = modelIsFlex(nested);
        final boolean nestedAllowsColumnChange = nested != null && nested.allowsColumnsChange()
                && model.allowsNestedColumnsChange();

        final QueryModel union = skipNoneTypeModels(model.getUnionModel());
        if (!topLevel && modelIsFlex(union)) {
            emitColumnLiteralsTopDown(model.getColumns(), union);
        }

        // process join models and their join conditions
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            final QueryModel jm = joinModels.getQuick(i);
            final JoinContext jc = jm.getJoinContext();
            if (jc != null && jc.aIndexes.size() > 0) {
                // join clause
                for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                    emitLiteralsTopDown(jc.aNodes.getQuick(k), model);
                    emitLiteralsTopDown(jc.bNodes.getQuick(k), model);

                    emitLiteralsTopDown(jc.aNodes.getQuick(k), jm);
                    emitLiteralsTopDown(jc.bNodes.getQuick(k), jm);

                    if (papaModel != null) {
                        emitLiteralsTopDown(jc.aNodes.getQuick(k), papaModel);
                        emitLiteralsTopDown(jc.bNodes.getQuick(k), papaModel);
                    }
                }
            }

            // process post-join-where
            final ExpressionNode postJoinWhere = jm.getPostJoinWhereClause();
            if (postJoinWhere != null) {
                emitLiteralsTopDown(postJoinWhere, jm);
                emitLiteralsTopDown(postJoinWhere, model);
            }

            final ExpressionNode leftJoinWhere = jm.getOuterJoinExpressionClause();
            if (leftJoinWhere != null) {
                emitLiteralsTopDown(leftJoinWhere, jm);
                emitLiteralsTopDown(leftJoinWhere, model);
            }
        }

        final ExpressionNode postJoinWhere = model.getPostJoinWhereClause();
        if (postJoinWhere != null) {
            emitLiteralsTopDown(postJoinWhere, model);
        }

        // propagate join models columns in separate loop to catch columns added to models prior to the current one
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            final QueryModel jm = joinModels.getQuick(i);
            propagateTopDownColumns0(jm, false, model, true);
        }

        // If this is group by model we need to add all non-selected keys, only if this is sub-query
        // For top level models top-down column list will be empty
        if (model.getSelectModelType() == SELECT_MODEL_GROUP_BY && model.getTopDownColumns().size() > 0) {
            final ObjList<QueryColumn> bottomUpColumns = model.getBottomUpColumns();
            for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
                QueryColumn qc = bottomUpColumns.getQuick(i);
                if (qc.getAst().type != FUNCTION || !functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                    model.addTopDownColumn(qc, qc.getAlias());
                }
            }
        }

        // latest on
        if (model.getLatestBy().size() > 0) {
            emitLiteralsTopDown(model.getLatestBy(), model);
        }

        // propagate explicit timestamp declaration
        if (model.getTimestamp() != null && nestedIsFlex && nestedAllowsColumnChange) {
            emitLiteralsTopDown(model.getTimestamp(), nested);

            QueryModel unionModel = nested.getUnionModel();
            while (unionModel != null) {
                emitLiteralsTopDown(model.getTimestamp(), unionModel);
                unionModel = unionModel.getUnionModel();
            }
        }

        if (model.getWhereClause() != null) {
            if (allowColumnsChange) {
                emitLiteralsTopDown(model.getWhereClause(), model);
            }
            if (nestedAllowsColumnChange) {
                emitLiteralsTopDown(model.getWhereClause(), nested);

                QueryModel unionModel = nested.getUnionModel();
                while (unionModel != null) {
                    emitLiteralsTopDown(model.getWhereClause(), unionModel);
                    unionModel = unionModel.getUnionModel();
                }
            }
        }

        // propagate 'order by'
        if (!topLevel) {
            emitLiteralsTopDown(model.getOrderBy(), model);
        }

        if (nestedIsFlex && nestedAllowsColumnChange) {
            emitColumnLiteralsTopDown(model.getColumns(), nested);

            final IntList unionColumnIndexes = tempIntList;
            unionColumnIndexes.clear();
            ObjList<QueryColumn> nestedTopDownColumns = nested.getTopDownColumns();
            for (int i = 0, n = nestedTopDownColumns.size(); i < n; i++) {
                unionColumnIndexes.add(nested.getColumnAliasIndex(nestedTopDownColumns.getQuick(i).getAlias()));
            }

            QueryModel unionModel = nested.getUnionModel();
            while (unionModel != null) {
                // promote bottom-up columns to top-down columns, which
                // indexes correspond to the chosen columns in the "nested" model
                ObjList<QueryColumn> cols = unionModel.getBottomUpColumns();
                for (int i = 0, n = unionColumnIndexes.size(); i < n; i++) {
                    QueryColumn qc = cols.getQuick(unionColumnIndexes.getQuick(i));
                    unionModel.addTopDownColumn(qc, qc.getAlias());
                }
                unionModel = unionModel.getUnionModel();
            }
        }

        // go down the nested path
        if (nested != null) {
            propagateTopDownColumns0(nested, false, null, nestedAllowsColumnChange);
        }

        final QueryModel unionModel = model.getUnionModel();
        if (unionModel != null) {
            // we've to use this value because union-ed models don't have a back-reference and might not know they participate in set operation
            propagateTopDownColumns(unionModel, allowColumnsChange);
        }
    }

    /**
     * Propagates orderByAdvice to nested join models if certain conditions are met.
     * Advice should only be propagated if relevant.
     * Cases:
     * ASOF JOIN
     * Propagate if the ordering is for the primary table only, and timestamp-first
     * OTHER JOINs
     * Propagate down primary table only.
     *
     * @param model                  The current query model
     * @param jm                     The join model list for the current query model
     * @param orderByMnemonic        The advice 'strength'
     * @param orderByDirectionAdvice The advice direction
     */
    private void pushDownOrderByAdviceToJoinModels(QueryModel model, ObjList<QueryModel> jm, int orderByMnemonic, IntList orderByDirectionAdvice) {
        if (model == null) {
            return;
        }
        // loop over the join models
        // get primary model
        QueryModel jm1 = jm.getQuiet(0);
        jm1 = jm1 != null ? jm1.getNestedModel() : null;
        if (jm1 == null) {
            return;
        }

        // get secondary model
        QueryModel jm2 = jm1.getJoinModels().getQuiet(1);
        // don't propagate though group by, sample by, distinct or some window functions
        if (model.getGroupBy().size() != 0
                || model.getSampleBy() != null
                || model.getSelectModelType() == SELECT_MODEL_DISTINCT
                || model.getSelectModelType() == SELECT_MODEL_WINDOW_JOIN
                || model.windowStopPropagate()) {
            jm1.setAllowPropagationOfOrderByAdvice(false);
            if (jm2 != null) {
                jm2.setAllowPropagationOfOrderByAdvice(false);
            }
        }
        // placeholder for prefix-stripped advice
        ObjList<ExpressionNode> advice;
        // Check if the orderByAdvice has names qualified by table names i.e 't1.ts' versus 'ts'
        final boolean orderByAdviceHasDot = checkForDot(orderByAdvice);
        // if order by advice has no table prefixes, we preserve original behaviour and pass it on.
        if (!orderByAdviceHasDot) {
            if (allAdviceIsForThisTable(jm1, orderByAdvice)) {
                orderByMnemonic = setAndCopyAdvice(jm1, orderByAdvice, orderByMnemonic, orderByDirectionAdvice);
            }
            optimiseOrderBy(jm1, orderByMnemonic);
            return;
        }
        // if the order by advice is for more than one table, don't propagate it, as a sort will be needed anyway
        if (!checkForConsistentPrefix(orderByAdvice)) {
            return;
        }
        // if the orderByAdvice prefixes do not match the primary table name, don't propagate it
        final CharSequence adviceToken = orderByAdvice.getQuick(0).token;
        final int dotLoc = Chars.indexOfLastUnquoted(adviceToken, '.');
        if (!(Chars.equalsNc(jm1.getTableName(), adviceToken, 0, dotLoc)
                || (jm1.getAlias() != null && Chars.equals(jm1.getAlias().token, adviceToken, 0, dotLoc)))) {
            optimiseOrderBy(jm1, orderByMnemonic);
            return;
        }
        // order by advice is pushable, so now we copy it and strip the table prefix
        advice = duplicateAdviceAndTakeSuffix();
        // if there's a join, we need to handle it differently.
        if (jm2 != null) {
            final int joinType = jm2.getJoinType();
            switch (joinType) {
                case JOIN_ASOF:
                case JOIN_LT:
                    // asof joins required ascending timestamp order, external advice must not
                    // influence them, especially passing INVARIANT mnemonic is dangerous,
                    // because existing ordering might be omitted
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_UNKNOWN;
                    break;
                default:
                    orderByMnemonic = setAndCopyAdvice(jm1, advice, orderByMnemonic, orderByDirectionAdvice);
            }
        } else {
            // fallback to copy the advice to primary
            orderByMnemonic = setAndCopyAdvice(jm1, advice, orderByMnemonic, orderByDirectionAdvice);
        }
        // recursive call
        optimiseOrderBy(jm1, orderByMnemonic);
    }

    /**
     * For a query like this: SELECT ts, b, c from x ORDER BY ts DESC, b DESC LIMIT 100
     * See the model:
     * `select-choose ts, b, c from (x timestamp (ts) order by ts desc, b desc) limit 100`
     * <p>
     * The limit is on the outer select-choose, and not the select-none.
     * This means that we fail to specialise the query whereas we would automatically
     * perform this push down in the case of negative limits.
     * <p>
     * After transformation, we get this model:
     * `select-choose ts, b, c from (x timestamp (ts) order by ts desc, b desc limit 100)`
     */
    private void pushLimitFromChooseToNone(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model == null) {
            return;
        }

        QueryModel nested = model.getNestedModel();
        Function loFunction;
        long limitValue;

        if (
                model.getSelectModelType() == SELECT_MODEL_CHOOSE
                        && model.getLimitLo() != null
                        && model.getLimitHi() == null
                        && model.getUnionModel() == null
                        && model.getJoinModels().size() == 1
                        && model.getGroupBy().size() == 0
                        && model.getSampleBy() == null
                        && !model.isDistinct()
                        && hasNoAggregateQueryColumns(model)
                        && nested != null
                        && nested.getSelectModelType() == SELECT_MODEL_NONE
                        && nested.getOrderBy().size() > 1 // only for multi-sort case, to get limited size cursor
                        && nested.getWhereClause() == null
                        && nested.getTimestamp() != null
                        && Chars.equalsIgnoreCase(nested.getTimestamp().token, nested.getOrderBy().get(0).token)
                        && nested.getOrderByDirection().get(0) == ORDER_DIRECTION_DESCENDING
                        && (loFunction = getLoFunction(model.getLimitLo(), executionContext)) != null
                        && (loFunction.isConstant() || loFunction.isRuntimeConstant())
                        && (limitValue = loFunction.getLong(null)) > 0
                        && (limitValue >= -executionContext.getCairoEngine().getConfiguration().getSqlMaxNegativeLimit())) {

            nested.setLimit(model.getLimitLo(), null);
            model.setLimit(null, null);

            if (nested.getOrderByAdvice().size() == 0) {
                for (int i = 0, n = nested.getOrderBy().size(); i < n; i++) {
                    nested.getOrderByAdvice().add(nested.getOrderBy().get(i));
                    nested.getOrderByDirectionAdvice().add(nested.getOrderByDirection().get(i));
                }
                nested.setAllowPropagationOfOrderByAdvice(false);
            }
        } else {
            pushLimitFromChooseToNone(model.getNestedModel(), executionContext);
        }
    }

    private ExpressionNode pushOperationOutsideAgg(ExpressionNode agg, ExpressionNode op, ExpressionNode column, ExpressionNode constant, QueryModel model) {
        final QueryColumn qc = checkSimpleIntegerColumn(column, model);
        if (qc == null) {
            return agg;
        }

        agg.rhs = column;

        ExpressionNode count = expressionNodePool.next();
        count.token = "COUNT";
        count.type = FUNCTION;
        // INT and LONG are nullable, so we need to use COUNT(column) for them.
        if (qc.getColumnType() == ColumnType.INT || qc.getColumnType() == ColumnType.LONG) {
            count.paramCount = 1;
            count.rhs = column;
        } else {
            count.paramCount = 0;
        }
        count.position = agg.position;

        OperatorExpression mulOp = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition("*");
        ExpressionNode mul = expressionNodePool.next().of(OPERATION, mulOp.operator.token, mulOp.precedence, agg.position);
        mul.paramCount = 2;
        mul.lhs = count;
        mul.rhs = constant;

        if (op.lhs == column) { // maintain order for subtraction
            op.lhs = agg;
            op.rhs = mul;
        } else {
            op.lhs = mul;
            op.rhs = agg;
        }

        return op;
    }

    private void pushPivotFiltersToInnerModel(QueryModel model, QueryModel innerModel) throws SqlException {
        final ObjList<PivotForColumn> pivotForColumns = model.getPivotForColumns();
        ExpressionNode pushedFilter = null;

        for (int i = 0, n = pivotForColumns.size(); i < n; i++) {
            PivotForColumn pivotForColumn = pivotForColumns.getQuick(i);
            ExpressionNode inExpr = pivotForColumn.getInExpr();
            CharacterStoreEntry entry = characterStore.newEntry();
            inExpr.toSink(entry);
            CharSequence alias = SqlUtil.createExprColumnAlias(
                    characterStore,
                    entry.toImmutable(),
                    pivotAliasMap,
                    pivotAliasSequenceMap,
                    configuration.getColumnAliasGeneratedMaxSize(),
                    true
            );
            pivotAliasMap.add(alias);
            pivotForColumn.setInExprAlias(alias);
            innerModel.addBottomUpColumn(queryColumnPool.next().of(alias, inExpr));
            ObjList<ExpressionNode> valueLists = pivotForColumn.getValueList();
            assert valueLists.size() != 0;

            ExpressionNode filter = expressionNodePool.next().of(FUNCTION, "IN", 0, inExpr.position);
            filter.paramCount = 1 + valueLists.size();
            if (filter.paramCount == 2) {
                filter.lhs = inExpr;
                filter.rhs = valueLists.getQuick(0);
            } else {
                filter.args.addReverseAll(valueLists);
                filter.args.add(inExpr);
            }
            if (pushedFilter != null) {
                ExpressionNode node = expressionNodePool.next().of(FUNCTION, "AND", 0, 0);
                node.paramCount = 2;
                node.lhs = pushedFilter;
                node.rhs = filter;
                pushedFilter = node;
            } else {
                pushedFilter = filter;
            }
        }

        if (pushedFilter != null) {
            ExpressionNode childWhereClause = model.getNestedModel().getWhereClause();
            if (childWhereClause == null) {
                model.getNestedModel().setWhereClause(pushedFilter);
            } else {
                ExpressionNode whereClause = expressionNodePool.next().of(FUNCTION, "AND", 0, 0);
                whereClause.paramCount = 2;
                whereClause.lhs = pushedFilter;
                whereClause.rhs = childWhereClause;
                model.getNestedModel().setWhereClause(whereClause);
            }
        }
    }

    /**
     * Checks if the given expression references a specific column name.
     * Handles both simple names ("ts") and qualified names ("v.ts").
     */
    private boolean referencesColumn(ExpressionNode node, CharSequence columnName) {
        if (node == null) {
            return false;
        }
        if (node.type == LITERAL && matchesColumnName(node.token, columnName)) {
            return true;
        }
        // Check children
        if (node.paramCount < 3) {
            return referencesColumn(node.lhs, columnName) || referencesColumn(node.rhs, columnName);
        } else {
            ObjList<ExpressionNode> args = node.args;
            for (int i = 0, n = args.size(); i < n; i++) {
                if (referencesColumn(args.getQuick(i), columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if the given predicate references ONLY the timestamp column/alias and constants.
     * This is used to determine if a predicate can safely be pushed down with timestamp offset.
     * Returns false if the predicate references any other columns (non-literal aliases).
     */
    private boolean referencesOnlyTimestampAlias(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return true;
        }

        // Get allowed timestamp tokens
        CharSequence timestampToken = model.getTimestamp() != null ? model.getTimestamp().token : null;
        CharSequence offsetAlias = model.getTimestampOffsetAlias();

        return referencesOnlyTimestampAliasRecursive(node, timestampToken, offsetAlias);
    }

    private boolean referencesOnlyTimestampAliasRecursive(ExpressionNode node, CharSequence timestampToken, CharSequence offsetAlias) {
        if (node == null) {
            return true;
        }

        // If this is a literal (column reference), check if it's the timestamp
        if (node.type == LITERAL) {
            // Check if it matches the timestamp token or offset alias
            return timestampToken != null && matchesColumnName(node.token, timestampToken) || offsetAlias != null && matchesColumnName(node.token, offsetAlias);
            // It's a column reference but not the timestamp - not allowed
        }

        // Constants are allowed
        if (node.type == CONSTANT) {
            return true;
        }

        // For functions/operations, check all children
        if (node.paramCount < 3) {
            return referencesOnlyTimestampAliasRecursive(node.lhs, timestampToken, offsetAlias)
                    && referencesOnlyTimestampAliasRecursive(node.rhs, timestampToken, offsetAlias);
        } else {
            ObjList<ExpressionNode> args = node.args;
            for (int i = 0, n = args.size(); i < n; i++) {
                if (!referencesOnlyTimestampAliasRecursive(args.getQuick(i), timestampToken, offsetAlias)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Registers a window function in the hash map for future deduplication lookups.
     *
     * @param column The window column to register
     * @param hash   Pre-computed hash from ExpressionNode.deepHashCode(column.getAst())
     */
    private void registerWindowFunction(QueryColumn column, int hash) {
        ObjList<QueryColumn> list = getOrCreateColumnListForHash(hash);
        list.add(column);
    }

    /**
     * Identify joined tables without join clause and try to find other reversible join clauses
     * that may be applied to it. For example when these tables joined
     * <p>
     * from a
     * join b on c.x = b.x
     * join c on c.y = a.y
     * <p>
     * the system that prefers child table with the lowest index will attribute c.x = b.x clause to
     * table "c" leaving "b" without clauses.
     */
    @SuppressWarnings({"StatementWithEmptyBody"})
    private void reorderTables(QueryModel model) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        int n = joinModels.size();

        tempCrosses.clear();
        // collect crosses
        for (int i = 0; i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getJoinContext() == null || q.getJoinContext().parents.size() == 0) {
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
                    final JoinContext jc = joinModels.getQuick(to).getJoinContext();
                    // look above i up to OUTER join
                    for (int k = i - 1; k > -1 && swapJoinOrder(model, to, k, jc); k--) ;
                    // look below i for up to OUTER join
                    for (int k = i + 1; k < n && swapJoinOrder(model, to, k, jc); k++) ;
                }
            }

            IntList ordered = model.nextOrderedJoinModels();
            int thisCost = doReorderTables(model, ordered);

            // we have to have root, even if it is expensive
            // so the first iteration sets the root regardless
            // the following iterations might improve it
            if (thisCost < cost || root == -1) {
                root = z;
                cost = thisCost;
                model.setOrderedJoinModels(ordered);
            }
        }

        assert root != -1;
    }

    private ExpressionNode replaceColumnWithAlias(ExpressionNode node, QueryModel model) throws SqlException {
        if (node != null && node.type == LITERAL) {
            final CharSequence col = node.token;
            final int dot = Chars.indexOfLastUnquoted(col, '.');
            final CharSequence alias = validateColumnAndGetAlias(model, col, dot, node.position);
            if (alias != null) {
                return expressionNodePool.next().of(LITERAL, alias, node.precedence, node.position);
            }
        }
        return node;
    }

    private void replaceColumnsWithAliases(ExpressionNode node, QueryModel model) throws SqlException {
        sqlNodeStack.clear();

        ExpressionNode temp = replaceColumnWithAlias(node, model);
        if (temp != node) {
            node.of(LITERAL, temp.token, node.precedence, node.position);
            return;
        }

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        temp = replaceColumnWithAlias(node.rhs, model);
                        if (node.rhs == temp) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = temp;
                        }
                    }

                    if (node.lhs != null) {
                        temp = replaceColumnWithAlias(node.lhs, model);
                        if (temp == node.lhs) {
                            node = node.lhs;
                        } else {
                            node.lhs = temp;
                            node = null;
                        }
                    } else {
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        temp = replaceColumnWithAlias(e, model);
                        if (e == temp) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, temp);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    temp = replaceColumnWithAlias(e, model);
                    if (e == temp) {
                        node = e;
                    } else {
                        node.args.setQuick(0, temp);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private ExpressionNode replaceIfAggregateOrLiteral(
            @Transient ExpressionNode node,
            QueryModel groupByModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        if (node != null &&
                ((node.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.token)) || node.type == LITERAL)) {
            CharSequence alias = findColumnByAst(groupByNodes, groupByAliases, node);
            if (alias == null) {
                // check if it's an already selected column, so that we don't need to add it as a key
                if (node.type == LITERAL) {
                    final CharSequence translatingAlias = translatingModel.getColumnNameToAliasMap().get(node.token);
                    if (translatingAlias != null) {
                        // For window joins, the caller passes windowJoinModel as both translatingModel and
                        // groupByModel (see emitAggregatesAndLiterals call in rewriteSelect0HandleOperation).
                        // In this case, the column was already added to windowJoinModel, so we can use
                        // the translating alias directly. The groupByNodes/groupByAliases lists are not
                        // used for window joins since GROUP BY is disallowed with WINDOW JOIN.
                        if (translatingModel == groupByModel) {
                            return nextLiteral(translatingAlias);
                        }
                        final CharSequence existingAlias = groupByModel.getColumnNameToAliasMap().get(translatingAlias);
                        if (existingAlias != null) {
                            // great! there is a matching key, so let's refer its alias and call it a day
                            final ExpressionNode replaceNode = !Chars.equalsIgnoreCase(existingAlias, node.token)
                                    ? nextLiteral(existingAlias)
                                    : node;
                            // don't forget to add the column to group by lists, if it's not there already
                            if (findColumnByAst(groupByNodes, groupByAliases, replaceNode) == null) {
                                groupByNodes.add(replaceNode);
                                groupByAliases.add(existingAlias);
                            }
                            return replaceNode;
                        }
                    }
                }

                // it's an aggregate function, or a non-selected column, so let's add it to the group by model and lists

                alias = createColumnAlias(node, groupByModel);
                groupByAliases.add(alias);

                if (node.type == LITERAL) {
                    // it's a non-selected column, first of all, add it to the inner models
                    doReplaceLiteral(node, translatingModel, innerVirtualModel, true, baseModel, false);
                    // the column is now present in the inner models under the alias, thus we have to refer to it via the alias
                    node = nextLiteral(alias);
                    groupByNodes.add(node);
                    groupByModel.addBottomUpColumn(queryColumnPool.next().of(alias, node));
                    return node;
                }

                groupByNodes.add(deepClone(expressionNodePool, node));
                groupByModel.addBottomUpColumn(queryColumnPool.next().of(alias, node));
            }

            return nextLiteral(alias);
        }
        return node;
    }

    private ExpressionNode replaceIfCursor(
            @Transient ExpressionNode node,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        if (node != null && functionParser.getFunctionFactoryCache().isCursor(node.token)) {
            return nextLiteral(
                    addCursorFunctionAsCrossJoin(
                            node,
                            null,
                            cursorModel,
                            innerVirtualModel,
                            translatingModel,
                            baseModel,
                            sqlExecutionContext,
                            sqlParserCallback
                    ).getAlias()
            );
        }
        return node;
    }

    private ExpressionNode replaceIfGroupByExpressionOrAggregate(
            ExpressionNode node,
            QueryModel groupByModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        CharSequence alias = findColumnByAst(groupByNodes, groupByAliases, node);
        if (alias != null) {
            return nextLiteral(alias);
        } else if (node.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
            QueryColumn qc = queryColumnPool.next().of(createColumnAlias(node, groupByModel), node);
            groupByModel.addBottomUpColumn(qc);
            return nextLiteral(qc.getAlias());
        }

        if (node.type == LITERAL) {
            throw SqlException.$(node.position, "column must appear in GROUP BY clause or aggregate function");
        }

        return node;
    }

    private ExpressionNode replaceIfWindowFunction(
            @Transient ExpressionNode node,
            QueryModel windowModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel
    ) throws SqlException {
        if (node != null && node.windowExpression != null) {
            // Extract any nested window functions from arguments to inner window model.
            // This modifies the AST in-place, replacing nested windows with literal references.
            extractAndRegisterNestedWindowFunctions(node, translatingModel, innerVirtualModel, baseModel, 0);

            // Validate that PARTITION BY and ORDER BY don't contain window functions
            validateNoWindowFunctionsInWindowSpec(node.windowExpression);

            // Check if an identical window function already exists in the model
            // (must be done AFTER extraction so we compare the modified AST)
            ObjList<QueryColumn> existingColumns = windowModel.getBottomUpColumns();
            for (int i = 0, n = existingColumns.size(); i < n; i++) {
                QueryColumn existing = existingColumns.getQuick(i);
                if (ExpressionNode.compareNodesExact(node, existing.getAst())) {
                    // Found duplicate - reuse the existing alias
                    return nextLiteral(existing.getAlias());
                }
            }

            WindowExpression wc = node.windowExpression;
            // Create alias for the window column if not already set
            CharSequence alias = wc.getAlias();
            if (alias == null) {
                alias = createColumnAlias(node, windowModel);
                wc.of(alias, node);
            }
            windowModel.addBottomUpColumn(wc);
            // Register in hash map for future deduplication lookups
            // (ensures findDuplicateWindowFunction can find this window when processing later columns)
            int hash = ExpressionNode.deepHashCode(node);
            registerWindowFunction(wc, hash);
            // Emit literals referenced by the window column to inner models
            emitLiterals(node, translatingModel, innerVirtualModel, true, baseModel, true);
            return nextLiteral(alias);
        }
        return node;
    }

    private ExpressionNode replaceLiteral(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            boolean addColumnToInnerVirtualModel,
            QueryModel baseModel,
            boolean windowCall
    ) throws SqlException {
        if (node != null && node.type == LITERAL) {
            try {
                return doReplaceLiteral(
                        node,
                        translatingModel,
                        innerVirtualModel,
                        addColumnToInnerVirtualModel,
                        baseModel,
                        windowCall
                );
            } catch (SqlException e) {
                if (functionParser.findNoArgFunction(node)) {
                    node.type = FUNCTION;
                } else {
                    throw e;
                }
            }
        }
        return node;
    }

    private void replaceLiteralList(
            QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            ObjList<ExpressionNode> list
    ) throws SqlException {
        for (int j = 0, n = list.size(); j < n; j++) {
            final ExpressionNode node = list.getQuick(j);
            emitLiterals(node, translatingModel, innerVirtualModel, true, baseModel, true);
            list.setQuick(j, replaceLiteral(node, translatingModel, innerVirtualModel, true, baseModel, true));
        }
    }

    private void resolveJoinColumns(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        final int size = joinModels.size();
        final CharSequence modelAlias = setAndGetModelAlias(model);
        // collect own alias
        collectModelAlias(model, 0, model);
        if (size > 1) {
            for (int i = 1; i < size; i++) {
                final QueryModel jm = joinModels.getQuick(i);
                final ObjList<ExpressionNode> jc = jm.getJoinColumns();
                final int joinColumnsSize = jc.size();

                if (joinColumnsSize > 0) {
                    final CharSequence jmAlias = setAndGetModelAlias(jm);
                    ExpressionNode joinCriteria = jm.getJoinCriteria();
                    for (int j = 0; j < joinColumnsSize; j++) {
                        ExpressionNode node = jc.getQuick(j);
                        ExpressionNode eq = makeOperation("=", makeModelAlias(modelAlias, node), makeModelAlias(jmAlias, node));
                        if (joinCriteria == null) {
                            joinCriteria = eq;
                        } else {
                            joinCriteria = makeOperation("and", joinCriteria, eq);
                        }
                    }
                    jm.setJoinCriteria(joinCriteria);
                }
                resolveJoinColumns(jm);
                collectModelAlias(model, i, jm);
            }
        }

        if (model.getNestedModel() != null) {
            resolveJoinColumns(model.getNestedModel());
        }

        // and union models too
        if (model.getUnionModel() != null) {
            resolveJoinColumns(model.getUnionModel());
        }
    }

    private void resolveNamedWindowReference(WindowExpression ac, QueryModel model) throws SqlException {
        CharSequence windowName = ac.getWindowName();
        WindowExpression namedWindow = lookupNamedWindow(model, windowName);
        if (namedWindow == null) {
            throw SqlException.$(ac.getWindowNamePosition(), "window '").put(windowName).put("' is not defined");
        }
        ac.copySpecFrom(namedWindow, expressionNodePool);
    }

    /**
     * Resolves all named window references in the model tree by copying the spec
     * from the named window definition into each referencing WindowExpression.
     * Called between validateNoWindowFunctionsInWhereClauses and rewriteSelectClause
     * so the optimizer sees fully populated partition-by / order-by lists before
     * dedup, ORDER BY propagation, or literal emission.
     */
    private void resolveNamedWindows(QueryModel model) throws SqlException {
        // Window expressions only exist in bottomUpColumns (topDownColumns only contain LITERALs)
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                WindowExpression ac = (WindowExpression) qc;
                if (ac.isNamedWindowReference()) {
                    resolveNamedWindowReference(ac, model);
                }
            }
            // Resolve named window references inside nested expressions (e.g., sum(row_number() OVER w) OVER ())
            resolveNamedWindowsInExpr(qc.getAst(), model);
        }

        // recurse into nested, join, and union models
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            resolveNamedWindows(nested);
        }
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            resolveNamedWindows(joinModels.getQuick(i));
        }
        QueryModel union = model.getUnionModel();
        if (union != null) {
            resolveNamedWindows(union);
        }
    }

    private void resolveNamedWindowsInExpr(ExpressionNode node, QueryModel model) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null && node.windowExpression.isNamedWindowReference()) {
            resolveNamedWindowReference(node.windowExpression, model);
        }
        if (node.paramCount < 3) {
            resolveNamedWindowsInExpr(node.lhs, model);
            resolveNamedWindowsInExpr(node.rhs, model);
        } else {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                resolveNamedWindowsInExpr(node.args.getQuick(i), model);
            }
        }
    }

    private void resolveWindowInheritance(QueryModel model) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = model.getNamedWindows();
        if (namedWindows.size() > 0) {
            ObjList<CharSequence> keys = namedWindows.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                CharSequence windowName = keys.getQuick(i);
                WindowExpression window = namedWindows.get(windowName);
                if (window != null && window.hasBaseWindow()) {
                    IntHashSet visited = intHashSetPool.next();
                    resolveWindowInheritanceChain(model, window, visited);
                }
            }
        }

        // recurse into nested, join, and union models
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            resolveWindowInheritance(nested);
        }
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            resolveWindowInheritance(joinModels.getQuick(i));
        }
        QueryModel union = model.getUnionModel();
        if (union != null) {
            resolveWindowInheritance(union);
        }
    }

    private void resolveWindowInheritanceChain(
            QueryModel model,
            WindowExpression window,
            IntHashSet visited
    ) throws SqlException {
        if (!window.hasBaseWindow()) {
            return;
        }

        // Cycle detection using the base window name position as unique key
        int pos = window.getBaseWindowNamePosition();
        if (visited.contains(pos)) {
            throw SqlException.$(pos, "circular window reference");
        }
        visited.add(pos);

        CharSequence baseName = window.getBaseWindowName();
        WindowExpression baseWindow = model.getNamedWindows().get(baseName);
        if (baseWindow == null) {
            throw SqlException.$(pos, "window '").put(baseName).put("' is not defined");
        }

        // Recursively resolve the base window first if it also inherits
        if (baseWindow.hasBaseWindow()) {
            resolveWindowInheritanceChain(model, baseWindow, visited);
        }

        // Merge the base spec into the child
        mergeWindowSpec(window, baseWindow);

        // Clear the base reference — inheritance is now resolved
        window.setBaseWindowName(null, 0);
    }

    // Rewrite:
    // sum(x*10) into sum(x) * 10, etc.
    // sum(x+10) into sum(x) + count(x)*10
    // sum(x-10) into sum(x) - count(x)*10
    private ExpressionNode rewriteAggregate(ExpressionNode agg, QueryModel model) {
        if (agg == null) {
            return null;
        }

        ExpressionNode op = agg.rhs;
        if (
                op != null
                        && agg.type == FUNCTION
                        && functionParser.getFunctionFactoryCache().isGroupBy(agg.token)
                        && Chars.equalsIgnoreCase("sum", agg.token)
                        && op.type == OPERATION
                        && op.paramCount == 2
        ) {
            if (Chars.equals(op.token, '*')) { // sum(x*10) == sum(x)*10
                if (isIntegerConstant(op.rhs) && isSimpleIntegerColumn(op.lhs, model)) {
                    agg.rhs = op.lhs;
                    op.lhs = agg;
                    return op;
                } else if (isIntegerConstant(op.lhs) && isSimpleIntegerColumn(op.rhs, model)) {
                    agg.rhs = op.rhs;
                    op.rhs = agg;
                    return op;
                }
            } else if (Chars.equals(op.token, '+') || Chars.equals(op.token, '-')) { // sum(x+10) == sum(x)+count(x)*10 , sum(x-10) == sum(x)-count(x)*10
                if (isIntegerConstant(op.rhs)) {
                    return pushOperationOutsideAgg(agg, op, op.lhs, op.rhs, model);
                } else if (isIntegerConstant(op.lhs)) {
                    return pushOperationOutsideAgg(agg, op, op.rhs, op.lhs, model);
                }
            }
        }

        return agg;
    }

    /**
     * Recursively rewrites all column references from one name to another.
     * Handles both simple names ("ts") and qualified names ("v.ts").
     */
    private void rewriteColumnReferences(ExpressionNode node, CharSequence from, CharSequence to) {
        if (node == null) {
            return;
        }
        if (node.type == LITERAL && matchesColumnName(node.token, from)) {
            node.token = rewriteColumnToken(node.token, to);
        }
        if (node.paramCount < 3) {
            rewriteColumnReferences(node.lhs, from, to);
            rewriteColumnReferences(node.rhs, from, to);
        } else {
            ObjList<ExpressionNode> args = node.args;
            for (int i = 0, n = args.size(); i < n; i++) {
                rewriteColumnReferences(args.getQuick(i), from, to);
            }
        }
    }

    /**
     * Rewrites a column token to a new name, preserving any table prefix.
     * "ts" -> "timestamp", "v.ts" -> "v.timestamp"
     * If 'to' is already qualified (contains a dot), return it as-is to avoid double-qualification.
     */
    private CharSequence rewriteColumnToken(CharSequence token, CharSequence to) {
        // If 'to' is already qualified, return it as-is to avoid double-qualification
        // e.g., rewriting "v.ts" to "t.timestamp" should return "t.timestamp", not "v.t.timestamp"
        if (Chars.lastIndexOf(to, 0, to.length(), '.') >= 0) {
            return to;
        }

        int dotIndex = Chars.lastIndexOf(token, 0, token.length(), '.');
        if (dotIndex >= 0 && dotIndex < token.length() - 1) {
            // Qualified name: preserve prefix and replace suffix
            CharSequence prefix = token.subSequence(0, dotIndex + 1);
            CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(prefix);
            entry.put(to);
            return entry.toImmutable();
        } else {
            // Simple name: just replace
            return to;
        }
    }

    // This rewrite should be invoked before the select rewrite!
    // Rewrites the following:
    // select count(constant) ... -> select count() ...
    private void rewriteCount(QueryModel model) {
        if (model == null) {
            return;
        }

        if (model.getModelType() == SELECT_MODEL_CHOOSE) {
            final ObjList<QueryColumn> columns = model.getBottomUpColumns();
            int columnCount = columns.size();

            for (int i = 0; i < columnCount; i++) {
                ExpressionNode expr = columns.getQuick(i).getAst();
                if (isCountKeyword(expr.token) && expr.paramCount == 1) {
                    if (expr.rhs.type == CONSTANT && !isNullKeyword(expr.rhs.token)) {
                        // erase constant
                        expr.rhs = null;
                        expr.paramCount = 0;
                    }
                }
            }
        }
        rewriteCount(model.getNestedModel());
        rewriteCount(model.getUnionModel());

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteCount(joinModels.getQuick(i));
        }
    }

    /**
     * Rewrites expressions such as:
     * <pre>
     * SELECT count_distinct(s) FROM tab WHERE s like '%a';
     * </pre>
     * into more parallel-friendly:
     * <pre>
     * SELECT count(*) FROM (SELECT s FROM tab WHERE s like '%a' AND s IS NOT NULL GROUP BY s);
     * </pre>
     */
    private void rewriteCountDistinct(QueryModel model) throws SqlException {
        final QueryModel nested = model.getNestedModel();
        ExpressionNode countDistinctExpr;

        if (
                nested != null
                        && nested.getNestedModel() == null
                        && nested.getTableName() != null
                        && model.getColumns().size() == 1
                        && (countDistinctExpr = model.getColumns().getQuick(0).getAst()).type == FUNCTION
                        && Chars.equalsIgnoreCase("count_distinct", countDistinctExpr.token)
                        && countDistinctExpr.paramCount == 1
                        && countDistinctExpr.rhs != null
                        && countDistinctExpr.rhs.type != CONSTANT
                        && !isSymbolColumn(countDistinctExpr, nested) // don't rewrite for symbol column because there's a separate optimization in count_distinct
                        && model.getJoinModels().size() == 1
                        && model.getWhereClause() == null
                        && model.getSampleBy() == null
                        && model.getGroupBy().size() == 0
        ) {
            ExpressionNode distinctExpr = countDistinctExpr.rhs;

            QueryModel middle = queryModelPool.next();
            middle.setNestedModel(nested);
            middle.setSelectModelType(SELECT_MODEL_GROUP_BY);
            model.setNestedModel(middle);

            CharSequence innerAlias = createColumnAlias(distinctExpr.token, middle, true);
            QueryColumn qc = queryColumnPool.next().of(innerAlias, distinctExpr);
            middle.addBottomUpColumn(qc);

            ExpressionNode nullExpr = expressionNodePool.next();
            nullExpr.type = CONSTANT;
            nullExpr.token = "null";
            nullExpr.precedence = 0;

            OperatorExpression neqOp = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition("!=");
            ExpressionNode node = expressionNodePool.next().of(OPERATION, neqOp.operator.token, neqOp.precedence, 0);
            node.paramCount = 2;
            node.lhs = nullExpr;
            node.rhs = distinctExpr;

            nested.setWhereClause(concatFilters(configuration.getCairoSqlLegacyOperatorPrecedence(), expressionNodePool, nested.getWhereClause(), node));
            middle.addGroupBy(distinctExpr);

            countDistinctExpr.token = "count";
            countDistinctExpr.paramCount = 0;
            countDistinctExpr.rhs = null;
        }

        if (nested != null) {
            rewriteCountDistinct(nested);
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            rewriteCountDistinct(union);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteCountDistinct(joinModels.getQuick(i));
        }
    }

    /**
     * Rewrites "select distinct a,b,c from tab" into "select a,b,c from (select a,b,c,d,count() from tab)".
     * We will not rewrite distinct of group-by or windows functions.
     *
     * @param model input model, it will be validated for the rewrite possibility. If rewrite is impossible,
     *              the input model will be returned verbatim.
     * @return either the input model or newly rewritten model.
     */
    private QueryModel rewriteDistinct(QueryModel model) throws SqlException {
        if (model == null) {
            return null;
        }

        if (model.isDistinct()) {
            // bingo
            // create wrapper models
            final QueryModel wrapperNested = queryModelPool.next();
            wrapperNested.setNestedModel(model);
            final QueryModel wrapperModel = queryModelPool.next();
            wrapperModel.setNestedModel(wrapperNested);

            final ObjList<QueryColumn> bottomUpColumns = model.getBottomUpColumns();
            // Columns might be aliased, if they are, we have to create a new
            // column instance. We also check if we should abandon the rewrite
            // in case we find it counterproductive
            boolean abandonRewrite = false;
            for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
                final QueryColumn qc = bottomUpColumns.getQuick(i);
                final ExpressionNode ast = qc.getAst();
                final CharSequence alias = qc.getAlias();
                if (qc.isWindowExpression() || (ast.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(ast.token))) {
                    abandonRewrite = true;
                    break;
                }
                if (alias == ast.token && ast.type != FUNCTION && ast.type != ARRAY_ACCESS && ast.type != ARRAY_CONSTRUCTOR) {
                    wrapperModel.addBottomUpColumn(qc);
                } else {
                    wrapperModel.addBottomUpColumn(queryColumnPool.next().of(alias, nextLiteral(alias)));
                }
            }

            if (!abandonRewrite) {
                // remove the distinct flag, model is no longer that
                model.setDistinct(false);

                // if we add "count" aggregate we will transform our model pair into
                // simple group by with an extra column, that wasn't requested. But,
                // it is a good start.

                final ExpressionNode countAst = expressionNodePool.next();
                countAst.token = "count";
                countAst.paramCount = 0;
                countAst.type = FUNCTION;

                final QueryColumn countColumn = queryColumnPool.next();
                countColumn.of(
                        createColumnAlias("count", model),
                        countAst,
                        false,
                        -1
                );
                model.getBottomUpColumns().add(countColumn);

                // move model attributes to the wrapper
                wrapperModel.setAlias(model.getAlias());
                wrapperModel.setTimestamp(model.getTimestamp());
                wrapperNested.setAlias(model.getAlias());
                wrapperNested.setTimestamp(model.getTimestamp());

                // before dispatching our rewrite, make sure our sub-query has also been re-written
                // the immediate "nested" model is part of the "distinct" pair or model, we skip rewriting that
                // and move onto its nested model.
                model.getNestedModel().setNestedModel(rewriteDistinct(model.getNestedModel().getNestedModel()));

                // if the model has a union, we need to move this union to the wrapper and rewrite it
                wrapperModel.setUnionModel(rewriteDistinct(model.getUnionModel()));
                wrapperModel.setSetOperationType(model.getSetOperationType());
                // also clear the union on the model we just wrapped
                model.setUnionModel(null);
                model.setSetOperationType(SET_OPERATION_UNION_ALL);

                return wrapperModel;
            }
        }

        // recurse into the model hierarchy
        model.setNestedModel(rewriteDistinct(model.getNestedModel()));

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            joinModels.setQuick(i, rewriteDistinct(joinModels.getQuick(i)));
        }

        model.setUnionModel(rewriteDistinct(model.getUnionModel()));

        return model;
    }

    // push aggregate function calls to group by model, replace key column expressions with group by aliases
    // raise error if raw column usage doesn't match one of expressions on group by list
    private ExpressionNode rewriteGroupBySelectExpression(
            final @Transient ExpressionNode topLevelNode,
            QueryModel groupByModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        ExpressionNode temp = replaceIfGroupByExpressionOrAggregate(topLevelNode, groupByModel, groupByNodes, groupByAliases);
        if (temp != topLevelNode) {
            return temp;
        }

        ExpressionNode node = topLevelNode;

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        temp = replaceIfGroupByExpressionOrAggregate(node.rhs, groupByModel, groupByNodes, groupByAliases);
                        if (node.rhs == temp) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = temp;
                        }
                    }

                    if (node.lhs != null) {
                        temp = replaceIfGroupByExpressionOrAggregate(node.lhs, groupByModel, groupByNodes, groupByAliases);
                        if (temp == node.lhs) {
                            node = node.lhs;
                        } else {
                            node.lhs = temp;
                            node = null;
                        }
                    } else {
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        temp = replaceIfGroupByExpressionOrAggregate(e, groupByModel, groupByNodes, groupByAliases);
                        if (e == temp) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, temp);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    temp = replaceIfGroupByExpressionOrAggregate(e, groupByModel, groupByNodes, groupByAliases);
                    if (e == temp) {
                        node = e;
                    } else {
                        node.args.setQuick(0, temp);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }

        return topLevelNode;
    }

    /**
     * For queries on tables with designated timestamp, no where clause and no order by or order by ts only :
     * <p>
     * For example:
     * select a,b from X limit -10 -> select a,b from (select * from X order by ts desc limit 10) order by ts asc
     * select a,b from X order by ts limit -10 -> select a,b  from (select * from X order by ts desc limit 10) order by ts asc
     * select a,b from X order by ts desc limit -10 -> select a,b from (select * from X order by ts asc limit 10) order by ts desc
     *
     * @param model input model
     */
    private void rewriteMultipleTermLimitedOrderByPart1(QueryModel model) {
        if (model != null) {
            if (
                    model.getSelectModelType() == SELECT_MODEL_CHOOSE
                            && model.getNestedModel() != null
                            && model.getNestedModel().getSelectModelType() == SELECT_MODEL_NONE
                            && model.getNestedModel().getOrderBy() != null
                            && model.getNestedModel().getOrderBy().size() > 1
                            && model.getNestedModel().getTimestamp() != null
                            && model.getLimitLo() != null
                            && model.getLimitHi() == null
                            && Chars.equals(model.getLimitLo().token, '-')
            ) {
                QueryModel nested = model.getNestedModel();

                int firstOrderByDir = nested.getOrderByDirection().get(0);
                if (firstOrderByDir != ORDER_DIRECTION_ASCENDING) {
                    return;
                }

                ExpressionNode firstOrderByArg = nested.getOrderBy().get(0);

                if (!Chars.equalsIgnoreCase(firstOrderByArg.token, nested.getTimestamp().token)) {
                    return;
                }

                // we want to push down a limited reverse scan, and then sort into the intended ordering afterward

                // first, copy the order by up
                for (int i = 0, n = nested.getOrderBy().size(); i < n; i++) {
                    model.addOrderBy(nested.getOrderBy().get(i), nested.getOrderByDirection().get(i));
                }

                if (nested.getOrderByAdvice().size() == 0) {
                    for (int i = 0, n = nested.getOrderBy().size(); i < n; i++) {
                        nested.getOrderByAdvice().add(nested.getOrderBy().get(i));
                        // we have to reverse the sorting order of the additional (to timestamp)
                        // terms because we are using reverse timestamp scan and also topN sort logic
                        // this is negative limit!
                        int orderDirection = nested.getOrderByDirection().get(i) == ORDER_DIRECTION_DESCENDING ? ORDER_DIRECTION_ASCENDING : ORDER_DIRECTION_DESCENDING;
                        nested.getOrderByDirectionAdvice().add(orderDirection);
                        nested.getOrderByDirection().setQuick(i, orderDirection);
                    }
                    // reverse the scan
                    nested.getOrderByDirection().set(0, ORDER_DIRECTION_DESCENDING);
                } else {
                    // assume already filled
                    nested.getOrderByDirectionAdvice().set(0, ORDER_DIRECTION_DESCENDING);
                }

                nested.setAllowPropagationOfOrderByAdvice(false); // stop propagation

                // copy the integral part, i.e. if it's -3, then 3
                nested.setLimit(model.getLimitLo().rhs, null);

                // remove limit from outer
                model.setLimit(null, null);
                rewriteMultipleTermLimitedOrderByPart1(nested.getNestedModel());
            } else {
                rewriteMultipleTermLimitedOrderByPart1(model.getNestedModel());
            }
            final ObjList<QueryModel> joinModels = model.getJoinModels();
            for (int i = 1, n = joinModels.size(); i < n; i++) {
                rewriteMultipleTermLimitedOrderByPart1(joinModels.getQuick(i));
            }
            rewriteMultipleTermLimitedOrderByPart1(model.getUnionModel());
        }
    }

    /**
     * Scans for NONE models, basically those that query table and if they have
     * order by descending timestamp only. These models are tagged for the generator
     * to handle the entire pipeline correctly.
     * <p>
     * Additionally, we scan the same models and separately tag them if they
     * ordered by descending timestamp, together with other fields.
     * <p>
     * We go deep and wide, as in include joins and unions.
     *
     * @param model the starting point
     */
    private void rewriteMultipleTermLimitedOrderByPart2(QueryModel model) {
        if (model == null) {
            return;
        }

        if (model.getModelType() == SELECT_MODEL_NONE || model.getModelType() == SELECT_MODEL_CHOOSE) {
            boolean orderDescendingByDesignatedTimestampOnly;
            IntList direction = model.getOrderByDirectionAdvice();
            if (direction.size() < 1) {
                orderDescendingByDesignatedTimestampOnly = false;
            } else {
                orderDescendingByDesignatedTimestampOnly = model.getOrderByAdvice().size() == 1
                        && model.getTimestamp() != null
                        && Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token, model.getTimestamp().token)
                        && direction.get(0) == ORDER_DIRECTION_DESCENDING;
            }

            model.setOrderDescendingByDesignatedTimestampOnly(orderDescendingByDesignatedTimestampOnly);
            model.setForceBackwardScan(
                    orderDescendingByDesignatedTimestampOnly || (
                            model.getOrderByAdvice().size() > 1
                                    && model.getTimestamp() != null
                                    && Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token, model.getTimestamp().token)
                                    && model.getLimitLo() != null && !Chars.equals(model.getLimitLo().token, '-'))
            );
        }

        rewriteMultipleTermLimitedOrderByPart2(model.getNestedModel());
        rewriteMultipleTermLimitedOrderByPart2(model.getUnionModel());

        ObjList<QueryModel> joinModels = model.getJoinModels();
        // ignore self
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteMultipleTermLimitedOrderByPart2(joinModels.getQuick(i));
        }
    }

    /**
     * Rewrites order by clause to achieve simple column resolution for model parser.
     * Order by must never reference column that doesn't exist in its own select list.
     * <p>
     * Because order by clause logically executes after "select" it must be able to
     * reference results of arithmetic expression, aggregation function results, arithmetic with
     * aggregation results and window functions. Somewhat contradictory to this order by must
     * also be able to reference columns of table or sub-query that are not even in select clause.
     *
     * @param model inbound model
     * @return outbound model
     * @throws SqlException when column names are ambiguous or not found at all.
     */
    private QueryModel rewriteOrderBy(final QueryModel model) throws SqlException {
        // find base model and check if there is "group-by" model in between
        // when we are dealing with "group by" model some implicit "order by" columns have to be dropped,
        // However, in the following example
        // select a, b from T order by c
        // ordering does affect query result
        QueryModel result = model;
        QueryModel base = model;
        QueryModel baseParent = model;
        QueryModel wrapper = null;
        QueryModel limitModel = model;//bottom-most model which contains limit, order by can't be moved past it
        final int modelColumnCount = model.getBottomUpColumns().size();
        boolean groupByOrDistinct = false;

        while (base.getBottomUpColumns().size() > 0 && !base.isNestedModelIsSubQuery()) {
            baseParent = base;

            final QueryModel union = base.getUnionModel();
            if (union != null) {
                final QueryModel rewritten = rewriteOrderBy(union);
                if (rewritten != union) {
                    base.setUnionModel(rewritten);
                }
            }

            base = base.getNestedModel();
            if (base.getLimitLo() != null) {
                limitModel = base;
            }
            final int selectModelType = baseParent.getSelectModelType();
            groupByOrDistinct = groupByOrDistinct
                    || selectModelType == SELECT_MODEL_GROUP_BY
                    || selectModelType == SELECT_MODEL_DISTINCT;
        }

        // find out how "order by" columns are referenced
        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            // for each order by column check how deep we need to go between "model" and "base"
            for (int i = 0; i < sz; i++) {
                final ExpressionNode orderBy = orderByNodes.getQuick(i);
                CharSequence column = orderBy.token;
                int dot = Chars.indexOfLastUnquoted(column, '.');
                // is this a table reference?
                if (dot > -1 || model.getAliasToColumnMap().excludes(column)) {
                    // validate column
                    validateColumnAndGetModelIndex(base, null, column, dot, orderBy.position, false);
                    // good news, our column matched base model
                    // this condition is to ignore order by columns that are not in select and behind group by
                    if (base != model) {
                        // check if column is aliased as either
                        // "x y" or "tab.x y" or "t.x y", where "t" is alias of table "tab"
                        final LowerCaseCharSequenceObjHashMap<CharSequence> map = baseParent.getColumnNameToAliasMap();
                        CharSequence alias = null;
                        int index = map.keyIndex(column);
                        if (index > -1 && dot > -1) {
                            // we have the following that are true:
                            // 1. column does have table alias, e.g. tab.x
                            // 2. column definitely exists
                            // 3. column is _not_ referenced as select tab.x from tab
                            //
                            // lets check if column is referenced as select x from tab
                            // this will determine is column is referenced by select at all
                            index = map.keyIndex(column, dot + 1, column.length());
                        }

                        if (index > -1) {
                            QueryColumn qc = getQueryColumn(baseParent, column, dot);
                            if (qc != null) {
                                index = map.keyIndex(qc.getAst().token);
                            }
                        }

                        if (index < 0) {
                            // we have found alias, rewrite order by column
                            orderBy.token = map.valueAtQuick(index);

                            tempQueryModel = null;
                            tempColumnAlias = null;

                            // if necessary, propagate column to limit model that'll receive order by
                            if (limitModel != baseParent) {
                                CharSequence translatedColumnAlias = getTranslatedColumnAlias(limitModel, baseParent, orderBy.token);
                                if (translatedColumnAlias == null) {
                                    // add column ref to the most-nested model that doesn't have it
                                    alias = SqlUtil.createColumnAlias(characterStore, tempColumnAlias, Chars.indexOfLastUnquoted(tempColumnAlias, '.'), tempQueryModel.getAliasToColumnMap(), tempQueryModel.getAliasSequenceMap(), false);
                                    tempQueryModel.addBottomUpColumn(nextColumn(alias, tempColumnAlias));

                                    // and then push to upper models
                                    QueryModel m = limitModel;
                                    while (m != tempQueryModel) {
                                        m.addBottomUpColumn(nextColumn(alias));
                                        m = m.getNestedModel();
                                    }

                                    tempQueryModel = null;
                                    tempColumnAlias = null;
                                    orderBy.token = alias;

                                    // if necessary, add external model to maintain output
                                    if (limitModel == model && wrapper == null) {
                                        wrapper = queryModelPool.next();
                                        wrapper.setSelectModelType(SELECT_MODEL_CHOOSE);
                                        for (int j = 0; j < modelColumnCount; j++) {
                                            QueryColumn qc = model.getBottomUpColumns().getQuick(j);
                                            wrapper.addBottomUpColumn(nextColumn(qc.getAlias()));
                                        }
                                        result = wrapper;
                                        wrapper.setNestedModel(model);
                                    }
                                } else {
                                    orderBy.token = translatedColumnAlias;
                                }
                            }
                        } else {
                            if (dot > -1 && !base.getModelAliasIndexes().contains(column, 0, dot)) {
                                throw SqlException.invalidColumn(orderBy.position, column);
                            }

                            // we must attempt to ascend order by column
                            // when we have group-by or distinct model, ascent is not possible
                            if (groupByOrDistinct) {
                                throw SqlException.position(orderBy.position)
                                        .put("ORDER BY expressions must appear in select list. ")
                                        .put("Invalid column: ")
                                        .put(column);
                            } else {
                                if (dot > -1
                                        && base.getModelAliasIndexes().contains(column, 0, dot)
                                        && base.getModelAliasIndexes().size() == 1) {
                                    column = column.subSequence(dot + 1, column.length()); // remove alias
                                    dot = -1;
                                }

                                if (baseParent.getSelectModelType() != SELECT_MODEL_CHOOSE && baseParent.getSelectModelType() != SELECT_MODEL_WINDOW_JOIN) {
                                    QueryModel synthetic = queryModelPool.next();
                                    synthetic.setSelectModelType(SELECT_MODEL_CHOOSE);
                                    for (int j = 0, z = baseParent.getBottomUpColumns().size(); j < z; j++) {
                                        QueryColumn qc = baseParent.getBottomUpColumns().getQuick(j);
                                        if (qc.getAst().type == FUNCTION || qc.getAst().type == OPERATION) {
                                            emitLiterals(qc.getAst(), synthetic, null, false, baseParent.getNestedModel(), false);
                                        } else {
                                            synthetic.addBottomUpColumnIfNotExists(qc);
                                        }
                                    }
                                    synthetic.setNestedModel(base);
                                    baseParent.setNestedModel(synthetic);
                                    baseParent = synthetic;

                                    // the column may appear in the list after literals from expressions have been emitted
                                    index = synthetic.getColumnNameToAliasMap().keyIndex(column);

                                    if (index < 0) {
                                        alias = synthetic.getColumnNameToAliasMap().valueAtQuick(index);
                                    }
                                }

                                if (alias == null) {
                                    alias = SqlUtil.createColumnAlias(characterStore, column, dot, baseParent.getAliasToColumnMap(), baseParent.getAliasSequenceMap(), false);
                                    baseParent.addBottomUpColumn(nextColumn(alias, column));
                                }

                                // do we have more than one parent model?
                                if (model != baseParent) {
                                    QueryModel m = model;
                                    do {
                                        m.addBottomUpColumn(nextColumn(alias));
                                        m = m.getNestedModel();
                                    } while (m != baseParent);
                                }

                                orderBy.token = alias;

                                if (wrapper == null) {
                                    wrapper = queryModelPool.next();
                                    wrapper.setSelectModelType(SELECT_MODEL_CHOOSE);
                                    for (int j = 0; j < modelColumnCount; j++) {
                                        QueryColumn qc = model.getBottomUpColumns().getQuick(j);
                                        wrapper.addBottomUpColumn(nextColumn(qc.getAlias()));
                                    }
                                    result = wrapper;
                                    wrapper.setNestedModel(model);
                                }
                            }
                        }
                    }
                }
                // order by can't be pushed through limit clause because it'll produce bad results
                if (base != baseParent && base != limitModel) {
                    limitModel.addOrderBy(orderBy, base.getOrderByDirection().getQuick(i));
                }
            }

            if (base != model && base != limitModel) {
                base.clearOrderBy();
            }
        }

        final QueryModel nested = base.getNestedModel();
        if (nested != null) {
            final QueryModel rewritten = rewriteOrderBy(nested);
            if (rewritten != nested) {
                base.setNestedModel(rewritten);
            }
        }

        final QueryModel union = base.getUnionModel();
        if (union != null) {
            final QueryModel rewritten = rewriteOrderBy(union);
            if (rewritten != union) {
                base.setUnionModel(rewritten);
            }
        }

        ObjList<QueryModel> joinModels = base.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            // we can ignore result of order by rewrite for because
            // 1. when join model is not a sub-query it will always have all the fields, so order by wouldn't
            //    introduce synthetic model (no column needs to be hidden)
            // 2. when join model is a sub-query it will have nested model, which can be rewritten. Parent model
            //    would remain the same again.
            rewriteOrderBy(joinModels.getQuick(i));
        }

        return result;
    }

    /**
     * Replaces references to column index in "order by" clause, such as "... order by 1"
     * with column names. The method traverses "deep" model hierarchy excluding "unions"
     *
     * @param model root model
     * @throws SqlException in case of any SQL syntax issues.
     */
    private void rewriteOrderByPosition(final QueryModel model) throws SqlException {
        QueryModel base = model;
        QueryModel baseParent = model;
        QueryModel baseGroupBy = null;
        QueryModel baseOuter = null;
        QueryModel baseDistinct = null;
        QueryModel baseWindowJoin = null;
        // while order by is initially kept in the base model (most inner one)
        // columns used in order by could be stored in one of many models : inner model, group by model, window or outer model
        // here we've to descend and keep track of all of those
        while (base.getBottomUpColumns().size() > 0) {
            // Check if the model contains the full list of selected columns and, thus, can be used as the parent.
            if (!base.isSelectTranslation()) {
                baseParent = base;
            }
            switch (base.getSelectModelType()) {
                case SELECT_MODEL_DISTINCT:
                    baseDistinct = base;
                    break;
                case SELECT_MODEL_GROUP_BY:
                    baseGroupBy = base;
                    break;
                case SELECT_MODEL_WINDOW_JOIN:
                    baseWindowJoin = base;
                    break;
                case SELECT_MODEL_VIRTUAL:
                case SELECT_MODEL_CHOOSE:
                    QueryModel nested = base.getNestedModel();
                    if (nested != null && (nested.getSelectModelType() == SELECT_MODEL_GROUP_BY
                            || nested.getSelectModelType() == SELECT_MODEL_WINDOW)) {
                        baseOuter = base;
                    }
                    break;
            }
            base = base.getNestedModel();
        }

        if (baseDistinct != null) {
            baseParent = baseDistinct;
        } else if (baseOuter != null) {
            baseParent = baseOuter;
        } else if (baseGroupBy != null) {
            baseParent = baseGroupBy;
        } else if (baseWindowJoin != null) {
            baseParent = baseWindowJoin;
        }

        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            final ObjList<QueryColumn> columns = baseParent.getBottomUpColumns();
            final int columnCount = columns.size();
            for (int i = 0; i < sz; i++) {
                final ExpressionNode orderBy = orderByNodes.getQuick(i);
                final CharSequence column = orderBy.token;

                // fast check to rule out an obvious non-numeric
                char first = column.charAt(0);
                if (first < '0' || first > '9') {
                    continue;
                }

                try {
                    final int position = Numbers.parseInt(column);
                    if (position < 1 || position > columnCount) {
                        // it could be out of range, or it could be intended as a column name. let's check for a
                        // matching column.
                        if (baseParent.getAliasToColumnMap().get(column) != null) {
                            continue;
                        } else {
                            throw SqlException.$(
                                            orderBy.position,
                                            "order column position is out of range [max="
                                    )
                                    .put(columnCount)
                                    .put(']');
                        }
                    }
                    orderByNodes.setQuick(
                            i,
                            expressionNodePool.next().of(
                                    LITERAL,
                                    columns.get(position - 1).getName(),
                                    -1,
                                    orderBy.position
                            )
                    );
                } catch (NumericException e) {
                    // fallback when not a valid numeric
                }
            }
        }

        QueryModel nested = base.getNestedModel();
        if (nested != null) {
            rewriteOrderByPosition(nested);
        }

        ObjList<QueryModel> joinModels = base.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            // we can ignore result of order by rewrite for because
            // 1. when join model is not a sub-query it will always have all the fields, so order by wouldn't
            //    introduce synthetic model (no column needs to be hidden)
            // 2. when join model is a sub-query it will have nested model, which can be rewritten. Parent model
            //    would remain the same again.
            rewriteOrderByPosition(joinModels.getQuick(i));
        }
    }

    private void rewriteOrderByPositionForUnionModels(final QueryModel model) throws SqlException {
        QueryModel next = model.getUnionModel();
        if (next != null) {
            doRewriteOrderByPositionForUnionModels(model, model, next);
        }

        next = model.getNestedModel();
        if (next != null) {
            rewriteOrderByPositionForUnionModels(next);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteOrderByPositionForUnionModels(joinModels.getQuick(i));
        }
    }

    private QueryModel rewritePivot(QueryModel model, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (model == null) {
            return null;
        }
        if (model.isPivot()) {
            final QueryModel innerModel = queryModelPool.next();
            final QueryModel outerModel = queryModelPool.next();
            final QueryModel emptyModel = queryModelPool.next();
            innerModel.setNestedModel(rewritePivot(model.getNestedModel(), sqlExecutionContext));

            final QueryModel emptyModel2 = queryModelPool.next();
            emptyModel.setNestedModel(innerModel);
            outerModel.setNestedModel(emptyModel);
            emptyModel2.setNestedModel(outerModel);
            emptyModel2.setAlias(model.getAlias());

            // Step 1: Execute subqueries in PIVOT IN clause (if any) and convert to value lists.
            // Returns total number of pivot columns (Cartesian product of all FOR column values).
            final int totalCombinations = preparePivotForSelectSubquery(model, outerModel, sqlExecutionContext);

            // Step 2: Add GROUP BY columns to both inner model (for grouping) and outer model (for output).
            // These columns will be passed through unchanged from the aggregated inner query.
            rewritePivotGroupByColumns(model, innerModel, outerModel);

            // Step 3: Add FOR column expressions to inner model's SELECT list with generated aliases.
            // Also generates IN filter (e.g., col IN ('a','b','c')) and pushes it to the nested WHERE clause.
            //
            // NOTE: This filter pushdown is debatable - when FOR values don't exist in the data,
            // we return empty rows instead of rows with NULL pivot columns (unlike DuckDB).
            // This behavior is kept for performance reasons. See PivotTest.testPivotWithNoMatchingForValues.
            pushPivotFiltersToInnerModel(model, innerModel);

            // Step 4: Add aggregate expressions (e.g., sum(amount)) to inner model's SELECT list.
            // These are the measures that will be pivoted across the FOR column values.
            addPivotAggregatesToInnerModel(model, innerModel);

            // Step 5: Generate pivoted output columns in outer model using CASE/SWITCH expressions.
            // Creates one column per combination of (FOR values × aggregates).
            // Uses first_not_null() to pick the matching aggregate value, or sum() for count aggregates.
            addPivotColumnsToOuterModel(totalCombinations, model, outerModel);

            emptyModel2.moveLimitFrom(model);
            emptyModel2.moveOrderByFrom(model);
            model = emptyModel2;
        } else {
            model.setNestedModel(rewritePivot(model.getNestedModel(), sqlExecutionContext));
            for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
                model.getJoinModels().set(i, rewritePivot(model.getJoinModels().getQuick(i), sqlExecutionContext));
            }

            model.setUnionModel(rewritePivot(model.getUnionModel(), sqlExecutionContext));
        }
        return model;
    }

    private void rewritePivotGroupByColumns(QueryModel model, QueryModel innerModel, QueryModel outerModel) throws SqlException {
        ObjList<ExpressionNode> nestedGroupBy = model.getGroupBy();
        pivotAliasMap.clear();
        pivotAliasSequenceMap.clear();
        for (int i = 0, n = nestedGroupBy.size(); i < n; i++) {
            ExpressionNode groupByExpr = nestedGroupBy.getQuick(i);
            if (groupByExpr.type == CONSTANT) {
                throw SqlException.$(groupByExpr.position, "cannot use positional group by inside `PIVOT`");
            } else {
                CharacterStoreEntry entry = characterStore.newEntry();
                groupByExpr.toSink(entry);
                CharSequence alias = SqlUtil.createExprColumnAlias(
                        characterStore,
                        entry.toImmutable(),
                        pivotAliasMap,
                        pivotAliasSequenceMap,
                        configuration.getColumnAliasGeneratedMaxSize(),
                        true
                );
                pivotAliasMap.add(alias);
                innerModel.addBottomUpColumn(queryColumnPool.next().of(alias, groupByExpr));
                outerModel.addBottomUpColumn(queryColumnPool.next().of(alias, expressionNodePool.next().of(LITERAL, alias, 0, groupByExpr.position)));
            }
        }
    }

    /**
     * Recursive. Replaces SAMPLE BY models with GROUP BY + ORDER BY. For now, the rewrite
     * avoids the following:
     * - linear and prev fills
     * <p>
     * When "timestamp" column is not explicitly selected, this method has to do
     * a trick to add artificial timestamp to the original model and then wrap the original
     * model into a sub-query, to select all the intended columns but the artificial timestamp.
     * <p>
     * When time zone or offset is specified, an intermediate model is also added.
     *
     * @param model the input model, it is expected to be very early in optimisation process
     *              the typical sample by model consists of two objects, the outer one with the
     *              list of columns and the inner one with sample by clauses
     */
    private QueryModel rewriteSampleBy(@Nullable QueryModel model, @Transient SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (model == null) {
            return null;
        }

        final QueryModel nested = model.getNestedModel();
        if (nested != null) {
            // "sample by" details will be on the nested model of the query
            final ExpressionNode sampleBy = nested.getSampleBy();
            final ExpressionNode sampleByOffset = nested.getSampleByOffset();
            final ObjList<ExpressionNode> sampleByFill = nested.getSampleByFill();
            final ExpressionNode sampleByTimezoneName = nested.getSampleByTimezoneName() != null && !isUTC(nested.getSampleByTimezoneName().token)
                    ? nested.getSampleByTimezoneName()
                    : null;
            final ExpressionNode sampleByUnit = nested.getSampleByUnit();
            final ExpressionNode timestamp = nested.getTimestamp();
            final int sampleByFillSize = sampleByFill.size();
            final ExpressionNode sampleByFrom = nested.getSampleByFrom();
            final ExpressionNode sampleByTo = nested.getSampleByTo();

            final ObjList<ExpressionNode> groupBy = nested.getGroupBy();
            if (sampleBy != null && groupBy != null && groupBy.size() > 0) {
                throw SqlException.$(groupBy.getQuick(0).position, "SELECT query must not contain both GROUP BY and SAMPLE BY");
            }

            if (sampleBy != null && isWindowJoin(nested)) {
                throw SqlException.$(sampleBy.position, "SAMPLE BY cannot be used with WINDOW JOIN");
            }

            if (
                    sampleBy != null
                            && timestamp != null
                            // null offset means ALIGN TO FIRST OBSERVATION, and we only support ALIGN TO CALENDAR
                            && sampleByOffset != null
                            // for now, time zone and offset are supported only when there is no FILL()
                            && (sampleByFillSize == 0 || (sampleByTimezoneName == null && isZeroOffset(sampleByOffset.token)))
                            && (sampleByFillSize == 0 || (sampleByFillSize == 1 && !isPrevKeyword(sampleByFill.getQuick(0).token) && !isLinearKeyword(sampleByFill.getQuick(0).token)))
                            && sampleByUnit == null
                            && (sampleByFrom == null || ((sampleByFrom.type != BIND_VARIABLE) && (sampleByFrom.type != FUNCTION) && (sampleByFrom.type != OPERATION)))
            ) {
                // Validate that the model does not have wildcard column names.
                // Using wildcard in group-by expression makes SQL ambiguous and
                // error-prone. For example, additive table metadata changes (adding a column)
                // will change the outcome of existing queries, if those supported wildcards for
                // as group-by keys.
                final ObjList<ExpressionNode> maybeKeyed = new ObjList<>();
                for (int i = 0, n = model.getColumns().size(); i < n; i++) {
                    final QueryColumn column = model.getColumns().getQuick(i);
                    final ExpressionNode ast = column.getAst();
                    if (ast.isWildcard()) {
                        throw SqlException.$(column.getAst().position, "wildcard column select is not allowed in sample-by queries");
                    }

                    if (ast.type == LITERAL || ast.type == FUNCTION || ast.type == OPERATION) {
                        maybeKeyed.add(ast);
                    }
                }

                if (hasNoAggregateQueryColumns(model)) {
                    throw SqlException.$(nested.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
                }

                validateConstOrRuntimeConstFunction(sampleByTimezoneName, sqlExecutionContext);

                // When timestamp is not explicitly selected, we will
                // need to add it artificially to enable group-by to
                // have access to bucket key. If this happens, the artificial
                // timestamp is added as the last column and this flag is set.
                // SAMPLE_BY_REWRITE_WRAP_REMOVE_TIMESTAMP

                // Another reason for creating the wrapper is to re-introduce the copies of the
                // timestamp column. These are actively removed from the group-by model, to make sure they
                // don't pollute the group-by keys.

                int wrapAction = SAMPLE_BY_REWRITE_NO_WRAP;

                if (sampleByTimezoneName != null && !isUTC(sampleByTimezoneName.token)) {
                    wrapAction |= SAMPLE_BY_REWRITE_WRAP_CONVERT_TIME_ZONE;
                }

                // this may or may not be our guy, but may not be, depending on fill settings

                // plan of action:
                // 1. analyze sample by expression and replace expression in the same position where timestamp
                //    is in the select clause (timestamp may not be selected, we have to have a wrapper)
                // 2. clear the sample by clause
                // 3. wrap the result into an order by to maintain the timestamp order, but this can be optional

                // the timestamp could be selected but also aliased
                CharSequence timestampColumn = timestamp.token;
                CharSequence timestampAlias = null;
                for (int i = 0, n = model.getColumns().size(); i < n; i++) {
                    final QueryColumn qc = model.getBottomUpColumns().getQuick(i);
                    if (qc.getAst().type == LITERAL && Chars.equalsIgnoreCase(qc.getAst().token, timestampColumn)) {
                        timestampAlias = qc.getAlias();
                    }
                }

                if (timestampAlias == null) {
                    // Let's not give up yet, the timestamp column might be prefixed
                    // with either table name or table alias
                    if (nested.getAlias() != null) {
                        // table is indeed aliased
                        CharacterStoreEntry e = characterStore.newEntry();
                        e.put(nested.getAlias().token).putAscii('.').put(timestamp.token);
                        CharSequence tableAliasPrefixedTimestampColumn = e.toImmutable();
                        timestampAlias = model.getColumnNameToAliasMap().get(tableAliasPrefixedTimestampColumn);
                        if (timestampAlias != null) {
                            timestampColumn = tableAliasPrefixedTimestampColumn;
                        }
                    }

                    // still nothing? Let's try table prefix very last time.
                    if (timestampAlias == null && nested.getTableName() != null) {
                        CharacterStoreEntry e = characterStore.newEntry();
                        e.put(nested.getTableName()).putAscii('.').put(timestamp.token);
                        CharSequence tableNamePrefixedTimestampColumn = e.toImmutable();
                        timestampAlias = model.getColumnNameToAliasMap().get(tableNamePrefixedTimestampColumn);
                        if (timestampAlias != null) {
                            timestampColumn = tableNamePrefixedTimestampColumn;
                        }
                    }
                }

                if (timestampAlias == null && nested.getJoinModels().size() > 1 && isAmbiguousColumn(nested, timestampColumn)) {
                    // We're dealing with a join, let's check if the timestamp needs a prefix.
                    final CharSequence tableAlias = nested.getAlias() != null ? nested.getAlias().token : nested.getTableName();
                    final CharacterStoreEntry e = characterStore.newEntry();
                    e.put(tableAlias).putAscii('.').put(timestamp.token);
                    timestampColumn = e.toImmutable();
                }

                if (maybeKeyed.size() > 0 &&
                        ((sampleByFrom != null || sampleByTo != null) || (sampleByFillSize > 0 && !isNoneKeyword(sampleByFill.getQuick(0).token)))) {
                    boolean isKeyed = false;

                    final CharSequence tableName = nested.getTableName();
                    // down-sampling of sub-queries will yield a null table name
                    if (tableName == null) {
                        return model;
                    }
                    for (int i = 0, n = maybeKeyed.size(); i < n; i++) {
                        final ExpressionNode expr = maybeKeyed.getQuick(i);
                        switch (expr.type) {
                            case LITERAL:
                                if (!matchesWithOrWithoutTablePrefix(expr.token, tableName, timestamp.token)
                                        && !matchesWithOrWithoutTablePrefix(expr.token, tableName, timestampAlias)) {
                                    isKeyed = true;
                                }
                                break;
                            case OPERATION:
                                isKeyed = true;
                                break;
                            case FUNCTION:
                                if (!functionParser.getFunctionFactoryCache().isGroupBy(expr.token)) {
                                    isKeyed = true;
                                }
                                break;
                        }
                    }

                    if (isKeyed) {
                        // drop out early, since we don't handle keyed
                        nested.setNestedModel(rewriteSampleBy(nested.getNestedModel(), sqlExecutionContext));

                        // join models
                        for (int j = 1, m = nested.getJoinModels().size(); j < m; j++) {
                            QueryModel joinModel = nested.getJoinModels().getQuick(j);
                            joinModel.setNestedModel(rewriteSampleBy(joinModel.getNestedModel(), sqlExecutionContext));
                        }

                        // unions
                        model.setUnionModel(rewriteSampleBy(model.getUnionModel(), sqlExecutionContext));
                        return model;
                    }
                }

                // These lists collect timestamp copies that we remove from the group-by model.
                // The goal is to re-populate the wrapper model with the copies in the correct positions.
                tempColumns.clear();
                tempIntList.clear();
                int needRemoveColumns = 0;
                boolean timestampOnly = true;

                // Check if there are aliased timestamp-only references, e.g.
                // `select timestamp a, timestamp b, timestamp c`
                // or it's an expression with timestamp references and aggregate functions, e.g.
                // `select timestamp, count() / timestamp::long`
                // We will remove copies from this model and add them back in the wrapper.
                // The aggregate functions need to be added to the model and referenced in the wrapper.

                // columnToAlias map is lossy, it only stores "last" alias (non-deterministic)
                // to find other aliases we have to loop thru all the columns. We are removing
                // columns in this loop, that is why there is no auto-increment.
                for (int i = 0, k = 0, n = model.getBottomUpColumns().size(); i < n; k++) {
                    final QueryColumn qc = model.getBottomUpColumns().getQuick(i);
                    final boolean isFunctionWithTsColumn = (qc.getAst().type == FUNCTION || qc.getAst().type == OPERATION)
                            && nonAggregateFunctionDependsOn(qc.getAst(), nested.getTimestamp());
                    if (
                            isFunctionWithTsColumn ||
                                    // also check all literals that refer timestamp column, except the one
                                    // with our chosen timestamp alias
                                    (timestampAlias != null && qc.getAst().type == LITERAL
                                            && Chars.equalsIgnoreCase(qc.getAst().token, timestampColumn)
                                            && !Chars.equalsIgnoreCase(qc.getAlias(), timestampAlias))
                    ) {
                        model.removeColumn(i);
                        n--;

                        if (checkForChildAggregates(qc.getAst())) {
                            // Replace aggregate functions with aliases and add the functions to the group by model.
                            // E.g. `select ts, count() / ts::long` -> `select ts, count / ts::long from (select count(), ...`
                            // If other key columns are also present in the expression, they'll be dealt with later,
                            // when we collect tempCharSequenceHashSet.
                            needRemoveColumns += rewriteTimestampMixedWithAggregates(qc.getAst(), model);
                        }

                        // Collect indexes of the removed columns, as they appear in the original list.
                        // This is a "deleting" loop, the "i" is not representative of the original column
                        // positions, which is why we need another index "k".
                        tempIntList.add(k);
                        tempColumns.add(qc);
                        wrapAction |= SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES;
                        if (isFunctionWithTsColumn) {
                            timestampOnly = false;
                        }
                    } else {
                        if (!Chars.equalsIgnoreCase(qc.getAst().token, timestampColumn)) {
                            timestampOnly = false;
                        }
                        i++;
                    }
                }

                // the "select" clause does not include timestamp
                if (timestampAlias == null) {
                    wrapAction |= SAMPLE_BY_REWRITE_WRAP_REMOVE_TIMESTAMP;

                    // Add artificial timestamp column at the end of the
                    // selected column list. While doing that we also
                    // need to avoid alias conflicts.

                    timestampAlias = createColumnAlias(timestampColumn, model);
                    model.addBottomUpColumnIfNotExists(nextColumn(timestampAlias, timestamp.position));

                    timestampOnly = false;
                    needRemoveColumns++;
                }

                if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES) != 0) {
                    // column alias indexes have shifted because of the removal of duplicate timestamp columns
                    model.updateColumnAliasIndexes();
                }

                final int timestampPos = model.getColumnAliasIndex(timestampAlias);
                if (timestampPos == -1) {
                    throw SqlException.$(timestamp.position, "unexpected timestamp expression");
                }

                final ExpressionNode tsFloorFunc = expressionNodePool.next();
                tsFloorFunc.token = TimestampFloorFunctionFactory.NAME;
                tsFloorFunc.type = FUNCTION;
                tsFloorFunc.paramCount = 5;

                CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                characterStoreEntry.put('\'').put(sampleBy.token).put('\'');

                final ExpressionNode tsFloorIntervalParam = expressionNodePool.next();
                tsFloorIntervalParam.token = characterStoreEntry.toImmutable();
                tsFloorIntervalParam.paramCount = 0;
                tsFloorIntervalParam.type = CONSTANT;
                tsFloorIntervalParam.position = sampleBy.position;

                final ExpressionNode tsFloorTsParam = expressionNodePool.next();
                tsFloorTsParam.token = timestampColumn;
                tsFloorTsParam.position = model.getBottomUpColumns().getQuick(timestampPos).getAst().position;
                tsFloorTsParam.paramCount = 0;
                tsFloorTsParam.type = LITERAL;

                if (sampleByTimezoneName != null) {
                    tsFloorFunc.args.add(sampleByTimezoneName);
                } else {
                    final ExpressionNode nullTimezone = expressionNodePool.next();
                    nullTimezone.type = CONSTANT;
                    nullTimezone.token = "null";
                    nullTimezone.precedence = 0;
                    tsFloorFunc.args.add(nullTimezone);
                }
                tsFloorFunc.args.add(sampleByOffset);
                // If SAMPLE BY FROM ... is present, we need to include it in the timestamp_floor() call.
                // This value is populated from the FROM clause and anchors the calendar-aligned buckets
                // to an offset other than the unix epoch.
                if (sampleByFrom != null) {
                    tsFloorFunc.args.add(sampleByFrom);
                } else {
                    final ExpressionNode nullExpr = expressionNodePool.next();
                    nullExpr.type = CONSTANT;
                    nullExpr.token = "null";
                    nullExpr.precedence = 0;
                    tsFloorFunc.args.add(nullExpr);
                }
                tsFloorFunc.args.add(tsFloorTsParam);
                tsFloorFunc.args.add(tsFloorIntervalParam);

                model.getBottomUpColumns().setQuick(
                        timestampPos,
                        queryColumnPool.next().of(timestampAlias, tsFloorFunc)
                );

                if (timestampOnly || nested.getGroupBy().size() > 0) {
                    nested.addGroupBy(tsFloorFunc);
                }

                nested.setFillFrom(sampleByFrom);
                nested.setFillTo(sampleByTo);
                nested.setFillStride(sampleBy);
                nested.setFillValues(sampleByFill);

                // clear sample by (but keep FILL and FROM-TO)
                nested.setSampleBy(null);
                nested.setSampleByOffset(null);
                nested.setSampleByFromTo(null, null);

                if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES) != 0) {
                    tempCharSequenceHashSet.clear();
                    for (int i = 0, n = tempColumns.size(); i < n; i++) {
                        fixTimestampAndCollectMissingTokens(tempColumns.get(i).getAst(), timestampColumn, timestampAlias, tempCharSequenceHashSet);
                    }
                    for (int i = 0, size = tempCharSequenceHashSet.size(); i < size; i++) {
                        final CharSequence dependentToken = tempCharSequenceHashSet.get(i);
                        if (model.getAliasToColumnMap().excludes(dependentToken)) {
                            model.addBottomUpColumn(nextColumn(dependentToken));
                            needRemoveColumns++;
                        }
                    }
                }

                // Normalize ORDER BY by replacing column names with their aliases.
                // That's because we may have to move explicit ORDER BY to an upper level, e.g. after to_utc() conversion.
                if (nested.getOrderBy().size() > 0) {
                    final ObjList<ExpressionNode> orderBy = nested.getOrderBy();
                    for (int i = 0, n = orderBy.size(); i < n; i++) {
                        replaceColumnsWithAliases(orderBy.getQuick(i), model);
                    }
                }

                QueryModel orderByModel = nested;
                CharSequence orderByTimestamp = timestamp.token;
                // Inject an intermediate model with to_utc() function in place of the timestamp.
                if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_CONVERT_TIME_ZONE) != 0) {
                    model = wrapWithSelectModel(model, model.getBottomUpColumns().size());
                    model.setSelectModelType(SELECT_MODEL_CHOOSE);
                    orderByModel = model.getNestedModel();
                    orderByTimestamp = timestampAlias;

                    final QueryColumn qc = model.getBottomUpColumns().getQuick(timestampPos);
                    if (timestampAlias == null || qc.getAst().type != LITERAL && !Chars.equalsIgnoreCase(qc.getAlias(), timestampAlias)) {
                        throw SqlException.$(qc.getAst().position, "unexpected non-timestamp column at position ").put(timestampPos);
                    }

                    final ExpressionNode toUtcFunc = expressionNodePool.next();
                    toUtcFunc.token = ToUTCTimestampFunctionFactory.NAME;
                    toUtcFunc.type = FUNCTION;
                    toUtcFunc.paramCount = 2;
                    final ExpressionNode toUtcParam = expressionNodePool.next();
                    toUtcParam.token = timestampAlias;
                    toUtcParam.position = timestamp.position;
                    toUtcParam.paramCount = 0;
                    toUtcParam.type = LITERAL;
                    toUtcFunc.lhs = toUtcParam;
                    toUtcFunc.rhs = sampleByTimezoneName;
                    qc.of(timestampAlias, toUtcFunc);
                }

                if (nested.getOrderBy().size() == 0) {
                    // There is no explicit ORDER BY, so we need to add one.
                    final ExpressionNode orderBy = expressionNodePool.next();
                    orderBy.token = timestampAlias;
                    orderBy.type = LITERAL;
                    orderByModel.getOrderBy().add(orderBy);
                    orderByModel.getOrderByDirection().add(0);
                    orderByModel.setTimestamp(nextLiteral(orderByTimestamp));
                }

                if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES) != 0 && needRemoveColumns > 0) {
                    model = wrapWithSelectModel(model, model.getBottomUpColumns().size() - needRemoveColumns);
                    addColumnToSelectModel(model, tempIntList, tempColumns, timestampAlias);
                    orderByModel = model.getNestedModel();
                } else if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES) != 0) {
                    model = wrapWithSelectModel(model, tempIntList, tempColumns, timestampAlias);
                    orderByModel = model.getNestedModel();
                } else if ((wrapAction & SAMPLE_BY_REWRITE_WRAP_REMOVE_TIMESTAMP) != 0) {
                    // We added artificial timestamp, which has to be removed
                    // in the outer query. Single query consists of two
                    // nested QueryModel instances. The outer of the two is
                    // SELECT model and the inner of the two is the model providing
                    // all available columns.

                    // copy columns from the "sample by" SELECT model
                    model = wrapWithSelectModel(model, model.getBottomUpColumns().size() - 1);
                    orderByModel = model.getNestedModel();
                }

                // We need to move explicit ORDER BY upper level in two cases:
                // 1. If there is to_utc() conversion due to time zone, we need to place the ORDER BY
                //    at the to_utc() level as to_utc() may change the order of rows.
                // 2. If we removed functions with timestamp column as an argument, they could be
                //    used in the ORDER BY clause, so we need to move it at the level where
                //    the functions are restored.
                final boolean orderByMoveRequired = (wrapAction & SAMPLE_BY_REWRITE_WRAP_CONVERT_TIME_ZONE) != 0
                        || (wrapAction & SAMPLE_BY_REWRITE_WRAP_ADD_TIMESTAMP_COPIES) != 0;
                if (orderByMoveRequired && nested.getOrderBy().size() > 0) {
                    final ObjList<ExpressionNode> orderBy = nested.getOrderBy();
                    final IntList orderByDirection = nested.getOrderByDirection();
                    for (int i = 0, n = orderBy.size(); i < n; i++) {
                        orderByModel.addOrderBy(orderBy.getQuick(i), orderByDirection.getQuick(i));
                    }
                    nested.clearOrderBy();
                }
            }

            // recurse nested models
            nested.setNestedModel(rewriteSampleBy(nested.getNestedModel(), sqlExecutionContext));

            // join models
            for (int i = 1, n = nested.getJoinModels().size(); i < n; i++) {
                QueryModel joinModel = nested.getJoinModels().getQuick(i);
                joinModel.setNestedModel(rewriteSampleBy(joinModel.getNestedModel(), sqlExecutionContext));
            }
        }

        // unions
        model.setUnionModel(rewriteSampleBy(model.getUnionModel(), sqlExecutionContext));
        return model;
    }

    /**
     * Copies the SAMPLE BY FROM-TO interval into a WHERE clause if no WHERE clause over designated
     * timestamps has been provided.
     * <p>
     * This is to allow for the generation of an interval scan and minimise reading of un-needed data.
     */
    private void rewriteSampleByFromTo(QueryModel model) throws SqlException {
        QueryModel curr;
        QueryModel fromToModel;
        QueryModel whereModel = null;
        ExpressionNode sampleFrom;
        ExpressionNode sampleTo;

        // extract sample from-to expressions
        // these should be in the nested model
        if (model == null) {
            return;
        }

        fromToModel = model.getNestedModel();
        if (fromToModel == null) {
            return;
        }

        sampleFrom = fromToModel.getSampleByFrom();
        sampleTo = fromToModel.getSampleByTo();

        // if from-to is present
        if (sampleFrom != null || sampleTo != null) {
            curr = model;
            ExpressionNode whereClause = null;
            while (curr != null && whereClause == null) {
                whereClause = curr.getWhereClause();
                if (whereClause != null) {
                    whereModel = curr;
                }
                curr = curr.getNestedModel();
            }

            // Add TO-FROM interval to WHERE clause.
            // If WHERE present and already contains a timestamp clause,
            // add it anyway, as it will be ANDed with the existing clause narrowing down existing filtering.

            ExpressionNode intervalClause;
            ExpressionNode timestamp = fromToModel.getTimestamp();
            QueryModel toAddWhereClause = fromToModel;

            if (timestamp == null) {
                // we probably need to check for a where clause
                if (whereClause != null) {
                    toAddWhereClause = whereModel;
                    timestamp = whereModel.getTimestamp();
                }
            }
            if (timestamp == null) {
                throw SqlException.$(fromToModel.getSampleBy().position, "Sample by requires a designated TIMESTAMP");
            }

            if (Chars.indexOf(timestamp.token, '.') < 0) {
                // prefix the timestamp column name only if the table is not dotted
                // this is to handle cases where we use system tables, which are prefixed
                // downstream code cannot handle `"sys.telemetry.wal".created`
                // it will break in `where` optimization and later metadata lookups
                CharacterStoreEntry e = characterStore.newEntry();
                if (Chars.indexOf(toAddWhereClause.getTableName(), '.') != -1) {
                    // Table name has . in the name, quote it
                    e.putAscii('\"').put(toAddWhereClause.getTableName()).putAscii("\".").put(timestamp.token);
                } else {
                    e.put(toAddWhereClause.getTableName()).put('.').put(timestamp.token);
                }
                CharSequence prefixedTimestamp = e.toImmutable();
                timestamp = expressionNodePool.next().of(LITERAL, prefixedTimestamp, timestamp.precedence, timestamp.position);
            }

            // construct an appropriate where clause
            if (sampleFrom != null && sampleTo != null) {
                ExpressionNode geqNode = expressionNodePool.next().of(OPERATION, opGeq.operator.token, opGeq.precedence, 0);
                geqNode.lhs = timestamp;
                geqNode.rhs = sampleFrom;
                geqNode.paramCount = 2;

                ExpressionNode ltNode = expressionNodePool.next().of(OPERATION, opLt.operator.token, opLt.precedence, 0);
                ltNode.lhs = timestamp;
                ltNode.rhs = sampleTo;
                ltNode.paramCount = 2;

                ExpressionNode andNode = expressionNodePool.next().of(OPERATION, opAnd.operator.token, opAnd.precedence, 0);
                andNode.lhs = geqNode;
                andNode.rhs = ltNode;
                andNode.paramCount = 2;
                intervalClause = andNode;
            } else if (sampleFrom != null) {
                ExpressionNode geqNode = expressionNodePool.next().of(OPERATION, opGeq.operator.token, opGeq.precedence, 0);
                geqNode.lhs = timestamp;
                geqNode.rhs = sampleFrom;
                geqNode.paramCount = 2;
                intervalClause = geqNode;
            } else { // sampleTo != null
                ExpressionNode ltNode = expressionNodePool.next().of(OPERATION, opLt.operator.token, opLt.precedence, 0);
                ltNode.lhs = timestamp;
                ltNode.rhs = sampleTo;
                ltNode.paramCount = 2;
                intervalClause = ltNode;
            }

            if (whereClause != null) {
                ExpressionNode andNode = expressionNodePool.next().of(OPERATION, "and", 15, 0);
                andNode.lhs = intervalClause;
                andNode.rhs = whereClause;
                andNode.paramCount = 2;
                toAddWhereClause.setWhereClause(andNode);
            } else {
                toAddWhereClause.setWhereClause(intervalClause);
            }
        }

        // recurse
        rewriteSampleByFromTo(fromToModel.getNestedModel());

        // join
        for (int i = 1, n = fromToModel.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = fromToModel.getJoinModels().getQuick(i);
            rewriteSampleByFromTo(joinModel.getNestedModel());
        }

        // union
        rewriteSampleByFromTo(model.getUnionModel());
    }

    private @Nullable QueryColumn rewriteSelect0HandleFunction(
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback,
            QueryColumn qc,
            QueryModel windowModel,
            QueryModel outerVirtualModel,
            QueryModel distinctModel,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel,
            boolean useOuterModel,
            QueryModel groupByModel,
            ExpressionNode sampleBy,
            QueryModel cursorModel,
            QueryModel windowJoinModel,
            boolean isWindowJoin
    ) throws SqlException {
        // when column is direct call to aggregation function, such as
        // select sum(x) ...
        // we can add it to group-by model right away
        if (qc.isWindowExpression()) {
            ExpressionNode ast = qc.getAst();

            // Validate that PARTITION BY and ORDER BY don't contain window functions
            validateNoWindowFunctionsInWindowSpec((WindowExpression) qc);

            // Extract any nested window functions from arguments to inner window model
            extractAndRegisterNestedWindowFunctions(ast, translatingModel, innerVirtualModel, baseModel, 0);

            // Compute hash once for both duplicate check and registration
            int hash = ExpressionNode.deepHashCode(ast);

            // Check for duplicate window column using O(1) hash lookup
            CharSequence existingAlias = findDuplicateWindowFunction(ast, hash);

            QueryColumn ref;
            if (existingAlias != null) {
                // Duplicate found - create reference to existing window column
                ref = nextColumn(qc.getAlias(), existingAlias);
            } else {
                // Not a duplicate - add to window model and register in hash map
                windowModel.addBottomUpColumn(qc);
                registerWindowFunction(qc, hash);
                ref = nextColumn(qc.getAlias());
            }
            outerVirtualModel.addBottomUpColumn(ref);
            distinctModel.addBottomUpColumn(ref);
            // ensure literals referenced by window column are present in nested models
            emitLiterals(ast, translatingModel, innerVirtualModel, true, baseModel, true);
            return null;
        } else if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
            ExpressionNode ast = qc.getAst();

            // Add missing table prefixes BEFORE extracting nested windows
            // because addMissingTablePrefixesForGroupByQueries validates columns against baseModel
            addMissingTablePrefixesForGroupByQueries(ast, baseModel, innerVirtualModel);

            // Extract any nested window functions from arguments to inner window model
            extractAndRegisterNestedWindowFunctions(ast, translatingModel, innerVirtualModel, baseModel, 0);

            // Only search for existing column if we'll actually use the result
            if (useOuterModel) {
                CharSequence matchingCol = findColumnByAst(groupByNodes, groupByAliases, ast);
                if (matchingCol != null) {
                    QueryColumn ref = nextColumn(qc.getAlias(), matchingCol);
                    ref = ensureAliasUniqueness(outerVirtualModel, ref);
                    outerVirtualModel.addBottomUpColumn(ref);
                    distinctModel.addBottomUpColumn(ref);
                    return null;
                }
            }
            QueryModel aggModel = isWindowJoin ? windowJoinModel : groupByModel;

            qc = ensureAliasUniqueness(aggModel, qc);
            aggModel.addBottomUpColumn(qc);

            groupByNodes.add(deepClone(expressionNodePool, ast));
            groupByAliases.add(qc.getAlias());

            // group-by column references might be needed when we have
            // outer model supporting arithmetic such as:
            // select sum(a)+sum(b) ...
            QueryColumn ref = nextColumn(qc.getAlias());
            // it is possible to order by a group-by column, which isn't referenced by
            // the SQL projection. In this case we need to preserve the wildcard visibility
            ref.setIncludeIntoWildcard(qc.isIncludeIntoWildcard());
            outerVirtualModel.addBottomUpColumn(ref);
            distinctModel.addBottomUpColumn(ref);
            if (!isWindowJoin) {
                // sample-by implementation requires innerVirtualModel
                emitLiterals(ast, translatingModel, innerVirtualModel, sampleBy != null, baseModel, false);
            }
            return null;
        } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
            // this is a select on projection, e.g. something like
            // select a, b, c, (select 1 from tab) from tab2
            addCursorFunctionAsCrossJoin(
                    qc.getAst(),
                    qc.getAlias(),
                    cursorModel,
                    innerVirtualModel,
                    isWindowJoin ? windowJoinModel : translatingModel,
                    baseModel,
                    sqlExecutionContext,
                    sqlParserCallback
            );
            return null;
        }
        return qc;
    }

    private int rewriteSelect0HandleOperation(
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback,
            QueryColumn qc,
            boolean explicitGroupBy,
            AtomicInteger nonAggSelectCount,
            int rewriteStatus,
            QueryModel outerVirtualModel,
            QueryModel distinctModel,
            QueryModel groupByModel,
            QueryModel baseModel,
            QueryModel innerVirtualModel,
            int columnIndex,
            QueryModel cursorModel,
            QueryModel translatingModel,
            QueryModel windowJoinModel,
            ExpressionNode sampleBy,
            QueryModel windowModel,
            boolean isWindowJoin
    ) throws SqlException {
        // dealing with OPERATIONs here (and FUNCTIONS that have not bailed out yet)
        if (explicitGroupBy) {
            nonAggSelectCount.incrementAndGet();
            if (isEffectivelyConstantExpression(qc.getAst())) {
                rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                rewriteStatus &= ~REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE;
                outerVirtualModel.addBottomUpColumn(qc);
                distinctModel.addBottomUpColumn(qc);
                return rewriteStatus;
            }

            final int beforeSplit = groupByModel.getBottomUpColumns().size();

            final ExpressionNode originalNode = qc.getAst();
            // if the alias is in groupByAliases, it means that we've already seen
            // the column in the GROUP BY clause and emitted literals for it to
            // the inner models; in this case, if we add a missing table prefix to
            // column's nodes, it may break the references; to avoid that, clone the node
            ExpressionNode node = groupByAliases.indexOf(qc.getAlias()) != -1
                    ? deepClone(expressionNodePool, originalNode)
                    : originalNode;

            // add table or alias prefix to the literal arguments of this function or operator
            addMissingTablePrefixesForGroupByQueries(node, baseModel, innerVirtualModel);

            // if there is explicit GROUP BY clause then we've to replace matching expressions with aliases in outer virtual model
            node = rewriteGroupBySelectExpression(node, groupByModel, groupByNodes, groupByAliases);
            if (originalNode == node) {
                rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
            } else {
                if (Chars.equalsIgnoreCase(originalNode.token, qc.getAlias())) {
                    int idx = groupByAliases.indexOf(originalNode.token);
                    if (columnIndex != idx) {
                        rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                    }
                    tempBoolList.set(idx, true);
                } else {
                    rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                }
                qc.of(qc.getAlias(), node, qc.isIncludeIntoWildcard(), qc.getColumnType());
            }

            emitCursors(
                    qc.getAst(),
                    cursorModel,
                    innerVirtualModel,
                    translatingModel,
                    baseModel,
                    sqlExecutionContext,
                    sqlParserCallback
            );
            qc = ensureAliasUniqueness(outerVirtualModel, qc);
            outerVirtualModel.addBottomUpColumn(qc);
            distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));

            // group-by column could have spit out a function call, e.g.
            // select sum(f(x)) from t -> select sum(col) from (select f(x) col) from t)
            for (int j = beforeSplit, n = groupByModel.getBottomUpColumns().size(); j < n; j++) {
                emitLiterals(
                        groupByModel.getBottomUpColumns().getQuick(j).getAst(),
                        translatingModel,
                        innerVirtualModel,
                        true,
                        baseModel,
                        false
                );
            }
            return rewriteStatus;
        }

        // this is not a direct call to aggregation function, in which case
        // we emit aggregation function into group-by model and leave the rest in outer model
        // more specifically we are dealing with something like
        // select sum(f(x)) - sum(f(y)) from tab
        // we don't know if "f(x)" is a function call or a literal, e.g. sum(x)

        // Check for window functions nested inside operations (e.g., row_number() OVER (...)::string)
        // Window functions need to be extracted to windowModel before processing aggregates.
        // However, if the expression ALSO contains aggregate functions (like sum() OVER (...)),
        // let the aggregate handling take precedence - it may optimize to GROUP BY.
        if (checkForChildWindowFunctions(qc.getAst()) && !checkForChildAggregates(qc.getAst())) {
            emitWindowFunctions(
                    qc.getAst(),
                    windowModel,
                    translatingModel,
                    innerVirtualModel,
                    baseModel
            );
            qc = ensureAliasUniqueness(outerVirtualModel, qc);
            outerVirtualModel.addBottomUpColumn(qc);
            distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));
            rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
            rewriteStatus |= REWRITE_STATUS_USE_WINDOW_MODEL;
            return rewriteStatus;
        }

        QueryModel aggModel = isWindowJoin ? windowJoinModel : groupByModel;
        final int beforeSplit = aggModel.getBottomUpColumns().size();
        if (checkForChildAggregates(qc.getAst()) || (sampleBy != null && nonAggregateFunctionDependsOn(qc.getAst(), baseModel.getTimestamp()))) {
            // push aggregates and literals outside aggregate functions
            emitAggregatesAndLiterals(
                    qc.getAst(),
                    aggModel,
                    isWindowJoin ? windowJoinModel : translatingModel,
                    innerVirtualModel,
                    baseModel,
                    groupByNodes,
                    groupByAliases
            );
            emitCursors(
                    qc.getAst(),
                    cursorModel,
                    innerVirtualModel,
                    translatingModel,
                    baseModel,
                    sqlExecutionContext,
                    sqlParserCallback
            );

            qc = ensureAliasUniqueness(outerVirtualModel, qc);
            outerVirtualModel.addBottomUpColumn(qc);
            distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));
            if (!isWindowJoin) {
                for (int j = beforeSplit, n = groupByModel.getBottomUpColumns().size(); j < n; j++) {
                    emitLiterals(
                            groupByModel.getBottomUpColumns().getQuick(j).getAst(),
                            translatingModel,
                            innerVirtualModel,
                            true,
                            baseModel,
                            false
                    );
                }
            }
            rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
        } else {
            emitCursors(
                    qc.getAst(),
                    cursorModel,
                    null,
                    translatingModel,
                    baseModel,
                    sqlExecutionContext,
                    sqlParserCallback
            );
            if ((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0) {
                // exclude constant columns from group-by, for example:
                // select 1, id, sum(x) from ...
                // keying map on constant '1' is unnecessary; this column can be selected
                // after the group-by in the "outerVirtualModel"
                if (isEffectivelyConstantExpression(qc.getAst())) {
                    rewriteStatus &= ~REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE;
                    outerVirtualModel.addBottomUpColumn(qc);
                    distinctModel.addBottomUpColumn(qc);
                    return rewriteStatus;
                }
            }

            // sample-by queries will still require innerVirtualModel
            if ((((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0) && sampleBy == null)) {
                qc = ensureAliasUniqueness(groupByModel, qc);
                groupByModel.addBottomUpColumn(qc);
                // group-by column references might be needed when we have
                // outer model supporting arithmetic such as:
                // select sum(a)+sum(b) ...
                QueryColumn ref = nextColumn(qc.getAlias());
                outerVirtualModel.addBottomUpColumn(ref);
                distinctModel.addBottomUpColumn(ref);
                emitLiterals(
                        qc.getAst(),
                        translatingModel,
                        innerVirtualModel,
                        false,
                        baseModel,
                        false
                );
                // this model won't be used in group-by case; this is because
                // group-by can process virtual functions by itself. However,
                // we need innerVirtualModel complete to do the projection validation
                // We add column after literal emission/validation to avoid allowing expression
                // self-referencing and passing validation
                innerVirtualModel.addBottomUpColumn(qc);
                return rewriteStatus;
            } else if ((rewriteStatus & REWRITE_STATUS_USE_WINDOWS_JOIN_MODE) != 0) {
                qc = ensureAliasUniqueness(outerVirtualModel, qc);
                outerVirtualModel.addBottomUpColumn(qc);
                QueryColumn ref = nextColumn(qc.getAlias());
                distinctModel.addBottomUpColumn(ref);
                emitLiterals(
                        qc.getAst(),
                        windowJoinModel,
                        innerVirtualModel,
                        false,
                        baseModel,
                        false
                );
                return rewriteStatus;
            }

            addFunction(
                    qc,
                    baseModel,
                    isWindowJoin ? windowJoinModel : translatingModel,
                    innerVirtualModel,
                    windowModel,
                    groupByModel,
                    outerVirtualModel,
                    distinctModel
            );
        }
        return rewriteStatus;
    }

    // flatParent = true means that parent model does not have selected columns
    private QueryModel rewriteSelectClause(
            QueryModel model,
            boolean flatParent,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        if (model.getUnionModel() != null) {
            QueryModel rewrittenUnionModel = rewriteSelectClause(
                    model.getUnionModel(),
                    true,
                    sqlExecutionContext,
                    sqlParserCallback
            );
            if (rewrittenUnionModel != model.getUnionModel()) {
                model.setUnionModel(rewrittenUnionModel);
            }
        }

        ObjList<QueryModel> models = model.getJoinModels();
        for (int i = 0, n = models.size(); i < n; i++) {
            final QueryModel m = models.getQuick(i);
            final boolean flatModel = m.getBottomUpColumns().size() == 0;
            final QueryModel nestedModel = m.getNestedModel();
            if (nestedModel != null) {
                QueryModel rewritten = rewriteSelectClause(nestedModel, flatModel, sqlExecutionContext, sqlParserCallback);
                if (rewritten != nestedModel) {
                    m.setNestedModel(rewritten);
                    // since we have rewritten nested model, we also have to update column hash
                    m.copyColumnsFrom(rewritten, queryColumnPool, expressionNodePool);
                }
            }

            if (flatModel) {
                if (flatParent && m.getSampleBy() != null) {
                    throw SqlException.$(m.getSampleBy().position, "'sample by' must be used with 'select' clause, which contains aggregate expression(s)");
                }
            } else {
                model.replaceJoinModel(i, rewriteSelectClause0(m, sqlExecutionContext, sqlParserCallback));
            }
        }

        // "model" is always the first in its own list of join models
        return models.getQuick(0);
    }

    @NotNull
    private QueryModel rewriteSelectClause0(
            final QueryModel model,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        assert model.getNestedModel() != null;

        groupByAliases.clear();
        groupByNodes.clear();
        innerWindowModels.clear();
        clearWindowFunctionHashMap();

        final QueryModel groupByModel = queryModelPool.next();
        groupByModel.setSelectModelType(SELECT_MODEL_GROUP_BY);
        final QueryModel distinctModel = queryModelPool.next();
        distinctModel.setSelectModelType(SELECT_MODEL_DISTINCT);
        final QueryModel outerVirtualModel = queryModelPool.next();
        outerVirtualModel.setSelectModelType(SELECT_MODEL_VIRTUAL);
        final QueryModel innerVirtualModel = queryModelPool.next();
        innerVirtualModel.setSelectModelType(SELECT_MODEL_VIRTUAL);
        final QueryModel windowModel = queryModelPool.next();
        windowModel.setSelectModelType(SELECT_MODEL_WINDOW);
        final QueryModel translatingModel = queryModelPool.next();
        translatingModel.setSelectModelType(SELECT_MODEL_CHOOSE);
        final QueryModel windowJoinModel = queryModelPool.next();
        windowJoinModel.setSelectModelType(SELECT_MODEL_WINDOW_JOIN);
        // this is a dangling model, which isn't chained with any other
        // we use it to ensure expression and alias uniqueness
        final QueryModel cursorModel = queryModelPool.next();
        int rewriteStatus = 0;
        if (model.isDistinct()) {
            rewriteStatus |= REWRITE_STATUS_USE_DISTINCT_MODEL;
        }
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        final QueryModel baseModel = model.getNestedModel();
        final boolean hasJoins = baseModel.getJoinModels().size() > 1;

        final boolean isWindowJoin = isWindowJoin(baseModel);
        if (isWindowJoin) {
            rewriteStatus |= REWRITE_STATUS_USE_WINDOWS_JOIN_MODE;
        }
        // sample by clause should be promoted to all the models as well as validated
        final ExpressionNode sampleBy = baseModel.getSampleBy();
        if (sampleBy != null) {
            // move sample by to group by model
            groupByModel.moveSampleByFrom(baseModel);
        }

        if (baseModel.getGroupBy().size() > 0) {
            if (isWindowJoin) {
                throw SqlException.$(baseModel.getGroupBy().getQuick(0).position, "GROUP BY cannot be used with WINDOW JOIN");
            }
            groupByModel.moveGroupByFrom(baseModel);
            // group by should be implemented even if there are no aggregate functions
            rewriteStatus |= REWRITE_STATUS_USE_GROUP_BY_MODEL;
        }

        // cursor model should have all columns that base model has to properly resolve duplicate names
        cursorModel.getAliasToColumnMap().putAll(baseModel.getAliasToColumnMap());

        // pre-detect duplicate aggregates using hash-based detection;
        // this is only needed when there's no sample by fill
        boolean hasDuplicateAggregates = false;
        if (groupByModel.getSampleByFill().size() == 0) {
            hasDuplicateAggregates = detectDuplicateAggregates(columns);
        }

        // take a look at the select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            final boolean isWindowExpr = qc.isWindowExpression();

            // fail-fast if this is an arithmetic expression where we expect window function
            if (isWindowExpr && qc.getAst().type != FUNCTION) {
                throw SqlException.$(qc.getAst().position, "Window function expected");
            }
            if (isWindowExpr && isWindowJoin) {
                throw SqlException.$(qc.getAst().position, "WINDOW functions are not allowed in WINDOW JOIN queries");
            }

            if (qc.getAst().type == BIND_VARIABLE) {
                rewriteStatus |= REWRITE_STATUS_USE_INNER_MODEL;
            } else if (qc.getAst().type != LITERAL) {
                if (qc.getAst().type == FUNCTION) {
                    if (isWindowExpr) {
                        rewriteStatus |= REWRITE_STATUS_USE_WINDOW_MODEL;
                        continue;
                    } else if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                        if (!isWindowJoin) {
                            rewriteStatus |= REWRITE_STATUS_USE_GROUP_BY_MODEL;
                        }

                        if (groupByModel.getSampleByFill().size() > 0) { // fill breaks if column is de-duplicated
                            continue;
                        }

                        // aggregates cannot yet reference the projection, they have to reference the columns from
                        // the underlying table(s) or sub-queries
                        ExpressionNode repl = rewriteAggregate(qc.getAst(), baseModel);
                        if (repl == qc.getAst()) { // no rewrite
                            // use pre-computed duplicate detection result
                            if (hasDuplicateAggregates) {
                                rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                            }
                            continue;
                        }

                        rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                        qc.of(qc.getAlias(), repl);
                    } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
                        continue;
                    }
                }
                if (isWindowJoin) {
                    rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                }

                if (checkForChildAggregates(qc.getAst())) {
                    if (!isWindowJoin) {
                        rewriteStatus |= REWRITE_STATUS_USE_GROUP_BY_MODEL;
                    }
                    rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                } else {
                    rewriteStatus |= REWRITE_STATUS_USE_INNER_MODEL;
                }
            }
        }

        // group-by generator can cope with virtual columns, it does not require virtual model to be its base
        // however, sample-by single-threaded implementation still relies on the innerVirtualModel, hence the fork
        boolean forceNotUseInnerModel = false;
        if ((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0 && sampleBy == null || ((rewriteStatus & REWRITE_STATUS_USE_WINDOWS_JOIN_MODE) != 0)) {
            rewriteStatus &= ~REWRITE_STATUS_USE_INNER_MODEL;
            forceNotUseInnerModel = true;
        }

        rewriteStatus |= REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE;
        // if there are explicit group by columns then nothing else should go to group by model
        // select columns should either match group by columns exactly or go to outer virtual model
        final ObjList<ExpressionNode> groupBy = groupByModel.getGroupBy();
        final boolean explicitGroupBy = groupBy.size() > 0;

        if (explicitGroupBy) {
            // Outer model is not needed only if select clause is the same as group by plus aggregate function calls
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                ExpressionNode node = groupBy.getQuick(i);
                CharSequence alias = null;
                int originalNodePosition = -1;

                // group by select clause alias
                if (node.type == LITERAL) {
                    // If literal is select clause alias then use its AST //sym1 -> ccy x -> a
                    // NOTE: this is merely a shortcut and doesn't mean that alias exists at group by stage !
                    // while
                    //   select a as d from t group by d;
                    // works, the following does not
                    //   select a as d from t group by d + d;
                    QueryColumn qc = model.getAliasToColumnMap().get(node.token);
                    if (qc != null && (qc.getAst().type != LITERAL || !Chars.equals(node.token, qc.getAst().token))) {
                        originalNodePosition = node.position;
                        node = qc.getAst();
                        alias = qc.getAlias();
                    }
                } else if (node.type == CONSTANT) { // group by column index
                    try {
                        int columnIdx = Numbers.parseInt(node.token);
                        // group by column index is 1-based
                        if (columnIdx < 1 || columnIdx > columns.size()) {
                            throw SqlException.$(node.position, "GROUP BY position ").put(columnIdx).put(" is not in select list");
                        }
                        columnIdx--;

                        QueryColumn qc = columns.getQuick(columnIdx);

                        originalNodePosition = node.position;
                        node = qc.getAst();
                        alias = qc.getAlias();
                    } catch (NumericException e) {
                        // ignore
                    }
                }

                if (node.type == LITERAL && Chars.endsWith(node.token, '*')) {
                    throw SqlException.$(node.position, "'*' is not allowed in GROUP BY");
                }

                // this loop is processing group-by columns that can only reference
                // the columns from the underlying table(s). They cannot reference the projection itself
                addMissingTablePrefixesForGroupByQueries(node, baseModel, innerVirtualModel);
                // ignore duplicates in group by
                if (findColumnByAst(groupByNodes, groupByAliases, node) != null) {
                    continue;
                }

                validateGroupByExpression(node, originalNodePosition);

                if (node.type == LITERAL) {
                    if (alias == null) {
                        alias = createColumnAlias(node, groupByModel);
                    } else {
                        alias = createColumnAlias(alias, innerVirtualModel, true);
                    }
                    QueryColumn groupByColumn = createGroupByColumn(
                            alias,
                            node,
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            groupByModel
                    );

                    groupByNodes.add(node);
                    groupByAliases.add(groupByColumn.getAlias());
                }
                // If there's at least one other group by column then we can ignore constant expressions,
                // otherwise we've to include not to affect the outcome, e.g.
                // if table t is empty then
                // select count(*) from t  returns 0  but
                // select count(*) from t group by 12+3 returns empty result
                // if we removed 12+3 then we'd affect result
                else if (!(isEffectivelyConstantExpression(node) && n > 1)) {
                    alias = createColumnAlias(alias != null ? alias : node.token, groupByModel, true);
                    final QueryColumn qc = queryColumnPool.next().of(alias, node);
                    groupByModel.addBottomUpColumn(qc);
                    groupByNodes.add(deepClone(expressionNodePool, node));
                    groupByAliases.add(qc.getAlias());
                    emitLiterals(qc.getAst(), translatingModel, innerVirtualModel, false, baseModel, false);
                }
            }
        }

        tempBoolList.setAll(groupBy.size(), false);
        nonAggSelectCount.set(0);

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            switch (qc.getAst().type) {
                case LITERAL:
                    if (Chars.endsWith(qc.getAst().token, '*')) {
                        // in general sense we need to create new column in case
                        // there is change of alias, for example we may have something as simple as
                        // select a.f, b.f from ....
                        createSelectColumnsForWildcard(
                                qc,
                                hasJoins,
                                (rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0,
                                baseModel,
                                isWindowJoin ? windowJoinModel : translatingModel,
                                innerVirtualModel,
                                windowModel,
                                groupByModel,
                                outerVirtualModel,
                                distinctModel
                        );
                    } else {
                        if (explicitGroupBy) {
                            nonAggSelectCount.incrementAndGet();
                            addMissingTablePrefixesForGroupByQueries(qc.getAst(), baseModel, innerVirtualModel);
                            int matchingColIdx = findColumnIdxByAst(groupByNodes, qc.getAst());
                            if (matchingColIdx == -1) {
                                throw SqlException.$(qc.getAst().position, "column must appear in GROUP BY clause or aggregate function");
                            }

                            boolean sameAlias = createSelectColumn(
                                    qc.getAlias(),
                                    groupByAliases.get(matchingColIdx),
                                    groupByModel,
                                    outerVirtualModel,
                                    (rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) != 0 ? distinctModel : null
                            );
                            if (sameAlias && i == matchingColIdx) {
                                tempBoolList.set(matchingColIdx, true);
                            } else {
                                rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                            }
                        } else {
                            // check what this column would reference to establish the priority
                            // TODO: currently we don't support referencing columns from the same projection
                            //  when there are aggregate functions. Even if we wanted to support this, the
                            //  current implementation has issues: if this column appears after other expression
                            //  columns, those expressions have already been added to groupByModel and
                            //  innerVirtualModel. The groupByModel would either fail to find the column at
                            //  runtime, or the expression would be executed multiple times. To properly support
                            //  this, we would need to analyze all columns upfront to determine if any reference
                            //  the same model's columns, then introduce an innerValueModel to handle this case.
                            //  This would change the processing logic for all columns.
                            if (
                                    (!forceNotUseInnerModel && baseModel.getAliasToColumnMap().excludes(qc.getAst().token) &&
                                            innerVirtualModel.getAliasToColumnMap().contains(qc.getAst().token))
                            ) {
                                // column is referencing another column or function on the same projection
                                // we must not add it to the translating model. This model is inserted between
                                // the base table and the virtual model, and will not be able to resolve the reference (we
                                // are not referencing the base table)
                                addFunction(
                                        qc,
                                        baseModel,
                                        isWindowJoin ? windowJoinModel : translatingModel,
                                        innerVirtualModel,
                                        windowModel,
                                        groupByModel,
                                        outerVirtualModel,
                                        distinctModel
                                );
                                rewriteStatus |= REWRITE_STATUS_USE_INNER_MODEL;
                                rewriteStatus |= REWRITE_STATUS_FORCE_INNER_MODEL;
                            } else {
                                createSelectColumn(
                                        qc.getAlias(),
                                        qc.getAst(),
                                        (rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0,
                                        baseModel,
                                        isWindowJoin ? windowJoinModel : translatingModel,
                                        innerVirtualModel,
                                        windowModel,
                                        groupByModel,
                                        outerVirtualModel,
                                        (rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) != 0 ? distinctModel : null
                                );
                            }
                        }
                    }
                    break;
                case BIND_VARIABLE:
                    if (explicitGroupBy) {
                        rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
                        rewriteStatus &= ~REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE;
                        outerVirtualModel.addBottomUpColumn(qc);
                        distinctModel.addBottomUpColumn(qc);
                    } else {
                        // todo: bind variable might be referenced by another function on the same projection
                        addFunction(
                                qc,
                                baseModel,
                                isWindowJoin ? windowJoinModel : translatingModel,
                                innerVirtualModel,
                                windowModel,
                                groupByModel,
                                outerVirtualModel,
                                distinctModel
                        );
                        rewriteStatus |= REWRITE_STATUS_USE_INNER_MODEL;
                    }
                    break;
                case FUNCTION:
                    qc = rewriteSelect0HandleFunction(
                            sqlExecutionContext,
                            sqlParserCallback,
                            qc,
                            windowModel,
                            outerVirtualModel,
                            distinctModel,
                            isWindowJoin ? windowJoinModel : translatingModel,
                            innerVirtualModel,
                            baseModel,
                            (rewriteStatus & REWRITE_STATUS_USE_OUTER_MODEL) != 0,
                            groupByModel,
                            sampleBy,
                            cursorModel,
                            windowJoinModel,
                            isWindowJoin
                    );
                    if (qc == null) continue;
                    // fall through and do the same thing as for OPERATIONS (default)
                default:
                    rewriteStatus = rewriteSelect0HandleOperation(
                            sqlExecutionContext,
                            sqlParserCallback,
                            qc,
                            explicitGroupBy,
                            nonAggSelectCount,
                            rewriteStatus,
                            outerVirtualModel,
                            distinctModel,
                            groupByModel,
                            baseModel,
                            innerVirtualModel,
                            i,
                            cursorModel,
                            isWindowJoin ? windowJoinModel : translatingModel,
                            windowJoinModel,
                            sampleBy,
                            windowModel,
                            isWindowJoin
                    );
                    break;
            }
        }

        if (explicitGroupBy && (rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) == 0
                && (nonAggSelectCount.get() != groupBy.size() || tempBoolList.getTrueCount() != groupBy.size())) {
            rewriteStatus |= REWRITE_STATUS_USE_OUTER_MODEL;
        }

        // fail if we have both window and group-by models
        if ((rewriteStatus & REWRITE_STATUS_USE_WINDOW_MODEL) != 0 && (rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0) {
            throw SqlException.$(0, "Window function is not allowed in context of aggregation. Use sub-query.");
        }

        boolean forceTranslatingModel = false;
        if ((rewriteStatus & REWRITE_STATUS_USE_WINDOW_MODEL) != 0) {
            // We need one more pass for window model to emit potentially missing columns.
            // For example, 'SELECT row_number() over (partition by col_c order by col_c), col_a, col_b FROM tab'
            // needs col_c to be emitted.
            for (int i = 0, k = columns.size(); i < k; i++) {
                QueryColumn qc = columns.getQuick(i);
                final boolean window = qc.isWindowExpression();

                if (window & qc.getAst().type == FUNCTION) {
                    final WindowExpression ac = (WindowExpression) qc;
                    int innerColumnsPre = innerVirtualModel.getBottomUpColumns().size();

                    replaceLiteralList(innerVirtualModel, translatingModel, baseModel, ac.getPartitionBy());
                    replaceLiteralList(innerVirtualModel, translatingModel, baseModel, ac.getOrderBy());
                    int innerColumnsPost = innerVirtualModel.getBottomUpColumns().size();
                    forceTranslatingModel |= innerColumnsPre != innerColumnsPost;
                }
            }
        }

        // check if innerVirtualModel is trivial, e.g, it does not contain any arithmetic
        // we must not "cancel" the inner model if inner virtual model columns reference
        // their own projection
        if ((rewriteStatus & REWRITE_STATUS_USE_INNER_MODEL) != 0 && (rewriteStatus & REWRITE_STATUS_FORCE_INNER_MODEL) == 0) {
            rewriteStatus &= ~REWRITE_STATUS_USE_INNER_MODEL;
            final ObjList<QueryColumn> innerColumns = innerVirtualModel.getBottomUpColumns();
            for (int i = 0, k = innerColumns.size(); i < k; i++) {
                QueryColumn qc = innerColumns.getQuick(i);
                if (qc.getAst().type != LITERAL) {
                    rewriteStatus |= REWRITE_STATUS_USE_INNER_MODEL;
                    break;
                }
            }
        }

        // When innerVirtualModel is not used but we have innerWindowModels,
        // copy renamed aliases from innerVirtualModel to translatingModel.
        // This ensures window function arguments (which may use renamed aliases like "b1")
        // can be resolved directly from translatingModel.
        if (innerWindowModels.size() > 0 && (rewriteStatus & REWRITE_STATUS_USE_INNER_MODEL) == 0) {
            ObjList<QueryColumn> innerCols = innerVirtualModel.getBottomUpColumns();
            LowerCaseCharSequenceObjHashMap<QueryColumn> translatingAliasMap = translatingModel.getAliasToColumnMap();
            for (int i = 0, n = innerCols.size(); i < n; i++) {
                QueryColumn col = innerCols.getQuick(i);
                CharSequence alias = col.getAlias();
                CharSequence token = col.getAst().token;
                // Only copy renamed table columns (where alias differs from token).
                // Skip if: alias already exists in translatingModel, OR token is an alias
                // for a DIFFERENT column (which means this is a projection alias like "a b"
                // where "a" is an alias for column "x", not a table column reference).
                // Allow if token is an alias for itself (e.g., table column "b" with alias "b").
                boolean tokenIsAliasForDifferentColumn = false;
                if (!translatingAliasMap.excludes(token)) {
                    QueryColumn existing = translatingAliasMap.get(token);
                    // If the existing column's AST token differs from 'token', then 'token'
                    // is an alias for a different column, not a table column reference.
                    tokenIsAliasForDifferentColumn = !Chars.equalsIgnoreCase(existing.getAst().token, token);
                }
                if (!Chars.equalsIgnoreCase(alias, token)
                        && translatingAliasMap.excludes(alias)
                        && !tokenIsAliasForDifferentColumn) {
                    translatingModel.addBottomUpColumn(queryColumnPool.next().of(alias, col.getAst()));
                }
            }
        }

        boolean translationIsRedundant = checkIfTranslatingModelIsRedundant(
                (rewriteStatus & REWRITE_STATUS_USE_INNER_MODEL) != 0,
                (rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0,
                (rewriteStatus & REWRITE_STATUS_USE_WINDOW_MODEL) != 0,
                (rewriteStatus & REWRITE_STATUS_USE_WINDOWS_JOIN_MODE) != 0,
                forceTranslatingModel,
                true,
                translatingModel
        );

        // If it wasn't redundant, we might be able to make it redundant.
        // Taking the query:
        // select a, b, c as z, count(*) as views from x where a = 1 group by a,b,z
        // A translation model is generated like  (select-choose a, b, c z from x)
        // A group by model is generated like     (select-group-by a, b, z, count views)
        // Since the select-choose node's purpose is just to alias the column c, this can be lifted into the parent node in this case, the group by node.
        // This makes the final query like        (select-group-by a, b, c z, count views from (select-choose a, b, c from x))
        // The translation model is now vestigial and can be elided.
        if ((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0 && sampleBy == null && !translationIsRedundant && !model.containsJoin() && SqlUtil.isPlainSelect(model.getNestedModel())) {
            ObjList<QueryColumn> translationColumns = translatingModel.getColumns();
            boolean appearsInFuncArgs = false;
            for (int i = 0, n = translationColumns.size(); i < n; i++) {
                QueryColumn col = translationColumns.getQuick(i);
                if (!Chars.equalsIgnoreCase(col.getAst().token, col.getAlias())) {
                    appearsInFuncArgs |= aliasAppearsInFuncArgs(groupByModel, col.getAlias(), sqlNodeStack);
                }
            }
            if (!appearsInFuncArgs) {
                groupByModel.mergePartially(translatingModel, queryColumnPool);
                translationIsRedundant = checkIfTranslatingModelIsRedundant(
                        (rewriteStatus & REWRITE_STATUS_USE_INNER_MODEL) != 0,
                        true,
                        false,
                        false,
                        false,
                        false,
                        translatingModel
                );
            }
        }

        if (sampleBy != null && baseModel.getTimestamp() != null) {
            CharSequence timestamp = baseModel.getTimestamp().token;
            // does model already select timestamp column?
            if (innerVirtualModel.getColumnNameToAliasMap().excludes(timestamp)) {
                // no, do we rename columns? does model select timestamp under a new name?
                if (translationIsRedundant) {
                    // columns were not renamed
                    addTimestampToProjection(
                            baseModel.getTimestamp().token,
                            baseModel.getTimestamp(),
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel
                    );
                } else {
                    // columns were renamed,
                    if (translatingModel.getColumnNameToAliasMap().excludes(timestamp)) {
                        // make alias name
                        final CharacterStoreEntry e = characterStore.newEntry();
                        e.put(baseModel.getName()).put('.').put(timestamp);
                        final CharSequence prefixedTimestampName = e.toImmutable();
                        if (translatingModel.getColumnNameToAliasMap().excludes(prefixedTimestampName)) {
                            if (baseModel.getJoinModels().size() > 0 && isAmbiguousColumn(baseModel, baseModel.getTimestamp().token)) {
                                // add prefixed column since the name is ambiguous
                                addTimestampToProjection(
                                        prefixedTimestampName,
                                        nextLiteral(prefixedTimestampName),
                                        baseModel,
                                        translatingModel,
                                        innerVirtualModel,
                                        windowModel
                                );
                            } else {
                                addTimestampToProjection(
                                        baseModel.getTimestamp().token,
                                        baseModel.getTimestamp(),
                                        baseModel,
                                        translatingModel,
                                        innerVirtualModel,
                                        windowModel
                                );
                            }
                        }
                    }
                }
            }
        }

        QueryModel root;
        QueryModel limitSource;

        if (translationIsRedundant) {
            root = baseModel;
            limitSource = model;
        } else {
            root = translatingModel;
            limitSource = translatingModel;
            translatingModel.setNestedModel(baseModel);

            // Translating model has limits to ensure clean factory separation
            // during code generation. However, in some cases limit could also
            // be implemented by nested model. Nested model must not implement limit
            // when parent model is order by or join.
            // The only exception is when order by is by designated timestamp because
            // it'll be implemented as forward or backward scan (no sorting required).
            pushDownLimitAdvice(model, baseModel, (rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) != 0);

            translatingModel.moveLimitFrom(model);
            translatingModel.moveJoinAliasFrom(model);
            translatingModel.setSelectTranslation(true);
            translatingModel.copyHints(model.getHints());
        }

        if ((rewriteStatus & REWRITE_STATUS_USE_INNER_MODEL) != 0) {
            innerVirtualModel.setNestedModel(root);
            innerVirtualModel.moveLimitFrom(limitSource);
            innerVirtualModel.moveJoinAliasFrom(limitSource);
            innerVirtualModel.copyHints(model.getHints());

            // Set limit hint if applicable.
            pushDownLimitAdvice(innerVirtualModel, root, (rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) != 0);

            root = innerVirtualModel;
            limitSource = innerVirtualModel;
        }

        // Add pass-through columns to innerWindowModels for non-window columns from windowModel.
        // This ensures that when propagateTopDownColumns runs, non-window columns (like "x as a")
        // can be resolved through the inner window model chain down to the translating model.
        if (innerWindowModels.size() > 0) {
            ObjList<QueryColumn> windowCols = windowModel.getBottomUpColumns();
            for (int i = 0, n = windowCols.size(); i < n; i++) {
                QueryColumn col = windowCols.getQuick(i);
                if (!col.isWindowExpression()) {
                    // Add this non-window column to each inner window model as pass-through
                    for (int j = 0, m = innerWindowModels.size(); j < m; j++) {
                        QueryModel innerWm = innerWindowModels.getQuick(j);
                        if (innerWm.getAliasToColumnMap().excludes(col.getAlias())) {
                            innerWm.addBottomUpColumn(col);
                        }
                    }
                }
            }

            // Propagate columns from earlier inner window models to later ones,
            // but ONLY columns that are actually referenced by the outer window model.
            // This avoids adding unnecessary pass-through columns.
            if (innerWindowModels.size() > 1) {
                // Collect all column aliases referenced by windowModel's window functions
                // (including PARTITION BY, ORDER BY, and frame bound expressions)
                referencedAliasesSet.clear();
                for (int i = 0, n = windowCols.size(); i < n; i++) {
                    QueryColumn col = windowCols.getQuick(i);
                    if (col.isWindowExpression()) {
                        collectReferencedAliasesFromColumn(col, referencedAliasesSet);
                    }
                }

                // Only propagate columns that are actually needed by the outer model
                for (int i = 1, n = innerWindowModels.size(); i < n; i++) {
                    QueryModel laterModel = innerWindowModels.getQuick(i);
                    for (int j = 0; j < i; j++) {
                        QueryModel earlierModel = innerWindowModels.getQuick(j);
                        ObjList<QueryColumn> earlierCols = earlierModel.getBottomUpColumns();
                        for (int k = 0, m = earlierCols.size(); k < m; k++) {
                            QueryColumn col = earlierCols.getQuick(k);
                            CharSequence alias = col.getAlias();
                            // Only add pass-through if the alias is referenced by outer model
                            // and not already present in the later model
                            if (referencedAliasesSet.contains(alias) &&
                                    laterModel.getAliasToColumnMap().excludes(alias)) {
                                laterModel.addBottomUpColumn(nextColumn(alias));
                            }
                        }
                    }
                }
            }
        }

        // Chain any inner window models (for nested window functions in either window or group-by expressions)
        // Process in forward order: models are added deepest-first during recursion,
        // so forward iteration chains deepest to base first, then progressively outer models
        if (innerWindowModels.size() > 0) {
            for (int i = 0, n = innerWindowModels.size(); i < n; i++) {
                QueryModel innerWm = innerWindowModels.getQuick(i);
                innerWm.setNestedModel(root);
                innerWm.copyHints(model.getHints());
                root = innerWm;
            }
        }

        if ((rewriteStatus & REWRITE_STATUS_USE_WINDOW_MODEL) != 0) {
            windowModel.setNestedModel(root);
            windowModel.moveLimitFrom(limitSource);
            windowModel.moveJoinAliasFrom(limitSource);
            windowModel.copyHints(model.getHints());
            root = windowModel;
            limitSource = windowModel;
        } else if ((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) != 0) {
            groupByModel.setNestedModel(root);
            groupByModel.moveLimitFrom(limitSource);
            groupByModel.moveJoinAliasFrom(limitSource);
            groupByModel.copyHints(model.getHints());
            root = groupByModel;
            limitSource = groupByModel;
        } else if ((rewriteStatus & REWRITE_STATUS_USE_WINDOWS_JOIN_MODE) != 0) {
            final ObjList<QueryModel> jms = root.getJoinModels();
            for (int i = 1, n = jms.size(); i < n; i++) {
                QueryModel jm = jms.getQuick(i);
                if (jm.getJoinType() == JOIN_WINDOW) {
                    jm.getWindowJoinContext().setParentModel(windowJoinModel);
                }
            }
            // Window join model takes over the role of translation model
            assert translationIsRedundant : "Window join must not have translation model";
            windowJoinModel.setNestedModel(root);
            windowJoinModel.moveLimitFrom(limitSource);
            windowJoinModel.moveJoinAliasFrom(limitSource);
            windowJoinModel.copyHints(model.getHints());
            root = windowJoinModel;
            limitSource = windowJoinModel;
        }

        if ((rewriteStatus & REWRITE_STATUS_USE_OUTER_MODEL) != 0) {
            outerVirtualModel.setNestedModel(root);
            outerVirtualModel.moveLimitFrom(limitSource);
            outerVirtualModel.moveJoinAliasFrom(limitSource);
            outerVirtualModel.copyHints(model.getHints());
            root = outerVirtualModel;
        } else if (root != outerVirtualModel && root.getBottomUpColumns().size() < outerVirtualModel.getBottomUpColumns().size()) {
            outerVirtualModel.setNestedModel(root);
            outerVirtualModel.moveLimitFrom(limitSource);
            outerVirtualModel.moveJoinAliasFrom(limitSource);
            outerVirtualModel.setSelectModelType((rewriteStatus & REWRITE_STATUS_OUTER_VIRTUAL_IS_SELECT_CHOOSE) != 0 ? SELECT_MODEL_CHOOSE : SELECT_MODEL_VIRTUAL);
            outerVirtualModel.copyHints(model.getHints());
            root = outerVirtualModel;
        }

        if ((rewriteStatus & REWRITE_STATUS_USE_DISTINCT_MODEL) != 0) {
            distinctModel.setNestedModel(root);
            distinctModel.moveLimitFrom(root);
            distinctModel.copyHints(model.getHints());
            root = distinctModel;
        }

        if ((rewriteStatus & REWRITE_STATUS_USE_GROUP_BY_MODEL) == 0 && groupByModel.getSampleBy() != null) {
            throw SqlException.$(groupByModel.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
        }

        if (model != root) {
            root.setUnionModel(model.getUnionModel());
            root.setSetOperationType(model.getSetOperationType());
            root.setModelPosition(model.getModelPosition());
            root.setOriginatingViewNameExpr(model.getOriginatingViewNameExpr());
            root.setViewNameExpr(model.getViewNameExpr());
            if (model.isUpdate()) {
                root.setIsUpdate(true);
                root.copyUpdateTableMetadata(model);
            }
        }
        root.setCacheable(model.isCacheable());
        return root;
    }

    /**
     * Rewrites queries like
     * <p>
     * SELECT last(timestamp) FROM table_name;
     * </p>
     * into query which can is optimised search
     * <p>
     * SELECT timestamp from table_name LIMIT -1;
     * </p>
     * Apart from the last() function, also supports single first/min/max functions.
     */
    private void rewriteSingleFirstLastGroupBy(QueryModel model) {
        final QueryModel nested = model.getNestedModel();

        if (
                nested != null
                        && nested.getJoinModels().size() == 1
                        && model.getSelectModelType() == SELECT_MODEL_GROUP_BY
                        && nested.getNestedModel() == null
                        && nested.getTableName() != null
                        && model.getSampleBy() == null
                        && model.getGroupBy().size() == 0
        ) {
            final ObjList<QueryColumn> queryColumns = model.getBottomUpColumns();

            if (nested.getTimestamp() == null || queryColumns.size() > 1) {
                rewriteSingleFirstLastGroupBy(nested);
                return;
            }

            // designated timestamp column name
            final CharSequence timestampColumn = nested.getTimestamp().token;
            final QueryColumn column = queryColumns.get(0);
            final ExpressionNode ast = column.getAst();
            CharSequence token = null;
            CharSequence rhs = null;
            if (ast != null) {
                token = ast.token;
                rhs = ast.rhs == null ? null : ast.rhs.token;
            }

            // type 1: add order by clause and changing model type (last/max)
            // type 2: only by change model type to erase group by (first/min)
            int optimisationType = 0;
            if (rhs != null && ast.type == FUNCTION && Chars.equals(timestampColumn, rhs)) {
                if (Chars.equalsIgnoreCase("last", token) || Chars.equalsIgnoreCase("max", token)) {
                    optimisationType = 1;
                } else if (Chars.equalsIgnoreCase("first", token) || Chars.equalsIgnoreCase("min", token)) {
                    optimisationType = 2;
                }
            }

            if (optimisationType == 1 || optimisationType == 2) {
                final QueryModel newNested = queryModelPool.next();
                final ExpressionNode lowerLimitNode = expressionNodePool.next();
                lowerLimitNode.token = "1";
                lowerLimitNode.type = CONSTANT;
                model.setLimit(lowerLimitNode, null);
                model.setSelectModelType(SELECT_MODEL_CHOOSE);

                final CharSequence oldToken = ast.token;
                ast.token = rhs;
                ast.paramCount = 0;
                ast.type = LITERAL;
                model.replaceColumnNameMap(column.getAlias(), oldToken, ast.token);

                final ExpressionNode newTimestampNode = expressionNodePool.next();
                newTimestampNode.token = timestampColumn;
                if (optimisationType == 1) {
                    newNested.addOrderBy(newTimestampNode, ORDER_DIRECTION_DESCENDING);
                }
                newNested.setTableNameExpr(nested.getTableNameExpr());
                newNested.setModelType(nested.getModelType());
                newNested.setTimestamp(nested.getTimestamp());
                newNested.setWhereClause(nested.getWhereClause());
                newNested.setLimitAdvice(lowerLimitNode, null);
                newNested.copyColumnsFrom(nested, queryColumnPool, expressionNodePool);
                model.setNestedModel(newNested);
            }
        }

        if (nested != null) {
            rewriteSingleFirstLastGroupBy(nested);
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            rewriteSingleFirstLastGroupBy(union);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteSingleFirstLastGroupBy(joinModels.getQuick(i));
        }
    }

    /**
     * Rewrites column references in the expression from the virtual timestamp alias to the source column.
     */
    private void rewriteTimestampColumnForOffset(ExpressionNode node, QueryModel model) {
        // Get the alias to rewrite from - use stored alias or timestamp token
        CharSequence virtualTs = model.getTimestampOffsetAlias();
        if (virtualTs == null && model.getTimestamp() != null) {
            virtualTs = model.getTimestamp().token;
        }
        CharSequence sourceTs = model.getTimestampSourceColumn();
        if (virtualTs != null && sourceTs != null) {
            rewriteColumnReferences(node, virtualTs, sourceTs);
        }
    }

    private int rewriteTimestampMixedWithAggregates(@NotNull ExpressionNode node, @NotNull QueryModel groupByModel) throws SqlException {
        int aggregates = 0;

        sqlNodeStack.clear();
        while (node != null) {
            if (node.rhs != null) {
                if (node.rhs.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.rhs.token)) {
                    // replace the aggregate with an alias
                    final QueryColumn existingColumn = findQueryColumnByAst(groupByModel.getBottomUpColumns(), node.rhs);
                    final CharSequence aggregateAlias;
                    if (existingColumn == null) {
                        // the aggregate is not already present in group by model, so add a column for it
                        aggregateAlias = createColumnAlias(node.rhs, groupByModel);
                        groupByModel.addBottomUpColumn(queryColumnPool.next().of(aggregateAlias, node.rhs));
                        aggregates++;
                    } else {
                        // yay! there is an existing column for the alias, let's use it
                        aggregateAlias = existingColumn.getAlias();
                    }
                    node.rhs = nextLiteral(aggregateAlias);
                } else {
                    sqlNodeStack.push(node.rhs);
                }
            }

            if (node.lhs != null) {
                if (node.lhs.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.lhs.token)) {
                    // replace the aggregate with an alias
                    final QueryColumn existingColumn = findQueryColumnByAst(groupByModel.getBottomUpColumns(), node.lhs);
                    final CharSequence aggregateAlias;
                    if (existingColumn == null) {
                        // the aggregate is not already present in group by model, so add a column for it
                        aggregateAlias = createColumnAlias(node.lhs, groupByModel);
                        groupByModel.addBottomUpColumn(queryColumnPool.next().of(aggregateAlias, node.lhs));
                        aggregates++;
                    } else {
                        // yay! there is an existing column for the alias, let's use it
                        aggregateAlias = existingColumn.getAlias();
                    }
                    node.lhs = nextLiteral(aggregateAlias);
                } else {
                    node = node.lhs;
                }
            } else {
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }
        return aggregates;
    }

    // the intent is to either validate top-level columns in select columns or replace them with function calls
    // if columns do not exist
    private void rewriteTopLevelLiteralsToFunctions(QueryModel model) {
        final QueryModel nested = model.getNestedModel();
        if (nested != null) {
            rewriteTopLevelLiteralsToFunctions(nested);
            final ObjList<QueryColumn> columns = model.getColumns();
            final int n = columns.size();
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    final QueryColumn qc = columns.getQuick(i);
                    final ExpressionNode node = qc.getAst();
                    if (node.type == LITERAL) {
                        if (nested.getAliasToColumnMap().contains(node.token)) {
                            continue;
                        }

                        if (functionParser.getFunctionFactoryCache().isValidNoArgFunction(node)) {
                            node.type = FUNCTION;
                        }
                    } else {
                        model.addField(qc);
                    }
                }
            } else {
                model.copyColumnsFrom(nested, queryColumnPool, expressionNodePool);
            }
        }
    }

    /**
     * Looks for models with trivial expressions over the same column, and lifts them from the group by.
     * <p>
     * For now, this rewrite is very specific and only kicks in for queries similar to ClickBench's Q35:
     * <pre>
     * SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c
     * FROM hits
     * GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3
     * ORDER BY c DESC
     * LIMIT 10;
     * </pre>
     * The above query gets effectively rewritten into:
     * <p>
     * SELECT ClientIP, ClientIP - 1, COUNT(*) AS c<br>
     * FROM (<br>
     * SELECT ClientIP, COUNT() c<br>
     * FROM hits <br>
     * ORDER BY c DESC<br>
     * LIMIT 10<br>
     * )
     * <pre>
     * SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, c
     * FROM (
     *   SELECT ClientIP, COUNT(*) AS c
     *   FROM hits
     *   GROUP BY ClientIP
     *   ORDER BY c DESC
     *   LIMIT 10
     * );
     * </pre>
     */
    private void rewriteTrivialGroupByExpressions(QueryModel model) throws SqlException {
        if (model == null) {
            return;
        }

        // First we want to see if this is an appropriate model to make this transformation.
        final QueryModel nestedModel = model.getNestedModel();
        if (nestedModel != null
                && model.getSelectModelType() == SELECT_MODEL_VIRTUAL
                && nestedModel.getSelectModelType() == SELECT_MODEL_GROUP_BY
                && nestedModel.getJoinModels().size() == 1
                && nestedModel.getUnionModel() == null
                && nestedModel.getSampleBy() == null) {
            trivialExpressions.clear();
            trivialExpressionCandidates.clear();

            final ObjList<QueryColumn> nestedColumns = nestedModel.getColumns();
            trivialExpressionCandidates.setAll(nestedColumns.size(), null);

            for (int i = 0, n = nestedColumns.size(); i < n; i++) {
                final QueryColumn nestedColumn = nestedColumns.getQuick(i);
                final ExpressionNode nestedAst = nestedColumn.getAst();
                if (nestedAst.type == OPERATION && nestedAst.paramCount == 2) {
                    // Is it an operation we care about?
                    // Check if it's a simple pattern, e.g. A + 1 or 1 + A.
                    trivialExpressionVisitor.clear();
                    traversalAlgo.traverse(nestedAst, trivialExpressionVisitor);
                    if (trivialExpressionVisitor.isTrivial()) {
                        final CharSequence token = trivialExpressionVisitor.getColumnNode() != null
                                ? trivialExpressionVisitor.getColumnNode().token
                                : null;
                        if (token != null) {
                            // Add it to candidates list.
                            trivialExpressions.inc(token);
                            // Put the literal to the candidate list, so that we don't have
                            // to look it up later.
                            trivialExpressionCandidates.setQuick(i, token);
                        }
                    }
                }

                // Or if it's a literal, add it, in case we have A, A + 1.
                if (nestedAst.type == LITERAL) {
                    trivialExpressions.inc(nestedAst.token);
                }
            }

            boolean anyCandidates = false;
            for (int i = 0, n = trivialExpressions.size(); i < n; i++) {
                anyCandidates |= trivialExpressions.get(trivialExpressions.keys().getQuick(i)) > 1;
            }

            if (anyCandidates) {
                for (int i = nestedColumns.size() - 1; i > 0; i--) {
                    final QueryColumn nestedColumn = nestedColumns.getQuick(i);
                    final ExpressionNode nestedAst = nestedColumn.getAst();

                    // If there is a matching column in this model, we can lift the candidate up.
                    final CharSequence currentAlias = model.getColumnNameToAliasMap().get(nestedColumn.getAlias());
                    if (currentAlias != null) {
                        if (nestedAst.type == FUNCTION) {
                            // Don't pull a function up.
                            continue;
                        }

                        final CharSequence candidate = trivialExpressionCandidates.getQuick(i);
                        // Check if the candidate is valid to be pulled up.
                        if (candidate != null && trivialExpressions.get(candidate) > 1) {
                            assert nestedAst.type == OPERATION;
                            // Remove from current, keep in new.
                            final QueryColumn currentColumn = model.getAliasToColumnMap().get(currentAlias);
                            currentColumn.of(currentColumn.getAlias(), nestedColumn.getAst());

                            final int nestedColumnIndex = nestedModel.getColumnAliasIndex(nestedColumn.getAlias());
                            nestedModel.removeColumn(nestedColumnIndex);
                        }
                    }
                }

                // If limit is on the virtual model, push it down to the group by.
                final ExpressionNode lo = model.getLimitLo();
                final ExpressionNode hi = model.getLimitHi();
                if (lo != null && nestedModel.getLimitLo() == null && nestedModel.getLimitHi() == null) {
                    nestedModel.setLimit(lo, hi);
                    model.setLimit(null, null);
                }
            }
        }

        // recurse
        rewriteTrivialGroupByExpressions(nestedModel);
        final QueryModel union = model.getUnionModel();
        if (union != null) {
            rewriteTrivialGroupByExpressions(union);
        }
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteTrivialGroupByExpressions(joinModels.getQuick(i));
        }
    }

    /**
     * Copies the provided order by advice into the given model.
     *
     * @param model                  The target model
     * @param advice                 The order by advice to copy
     * @param orderByMnemonic        The advice 'strength'
     * @param orderByDirectionAdvice The advice direction
     * @return boolean Don't pass through orderByMnemonic if `allowPropagationOfOrderByAdvice = false`
     */
    private int setAndCopyAdvice(QueryModel model, ObjList<ExpressionNode> advice, int orderByMnemonic, IntList orderByDirectionAdvice) {
        if (model.getAllowPropagationOfOrderByAdvice()) {
            model.setOrderByAdviceMnemonic(orderByMnemonic);
            model.copyOrderByAdvice(advice);
            model.copyOrderByDirectionAdvice(orderByDirectionAdvice);
            return orderByMnemonic;
        }
        return OrderByMnemonic.ORDER_BY_UNKNOWN;
    }

    private CharSequence setAndGetModelAlias(QueryModel model) {
        CharSequence name = model.getName();
        if (name != null) {
            return name;
        }
        ExpressionNode alias = makeJoinAlias();
        model.setAlias(alias);
        return alias.token;
    }

    private QueryModel skipNoneTypeModels(QueryModel model) {
        while (
                model != null
                        && model.getSelectModelType() == SELECT_MODEL_NONE
                        && model.getTableName() == null
                        && model.getTableNameFunction() == null
                        && model.getJoinModels().size() == 1
                        && model.getWhereClause() == null
                        && model.getLatestBy().size() == 0
        ) {
            model = model.getNestedModel();
        }
        return model;
    }

    /**
     * Moves reversible join clauses, such as a.x = b.x from table "from" to table "to".
     *
     * @param to      target table index
     * @param from    source table index
     * @param context context of target table index
     * @return false if "from" is outer joined table, otherwise - true
     */
    private boolean swapJoinOrder(QueryModel parent, int to, int from, final JoinContext context) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        QueryModel jm = joinModels.getQuick(from);
        if (joinBarriers.contains(jm.getJoinType())) {
            return false;
        }

        final JoinContext that = jm.getJoinContext();
        if (that != null && that.parents.contains(to)) {
            swapJoinOrder0(parent, jm, to, context);
        }
        return true;
    }

    private void swapJoinOrder0(QueryModel parent, QueryModel jm, int to, JoinContext jc) {
        final JoinContext that = jm.getJoinContext();
        clausesToSteal.clear();
        int zc = that.aIndexes.size();
        for (int z = 0; z < zc; z++) {
            if (that.aIndexes.getQuick(z) == to || that.bIndexes.getQuick(z) == to) {
                clausesToSteal.add(z);
            }
        }

        // we check that parent contains "to", so we must have something to do
        assert clausesToSteal.size() > 0;

        if (clausesToSteal.size() < zc) {
            QueryModel target = parent.getJoinModels().getQuick(to);
            if (jc == null) {
                target.setContext(jc = contextPool.next());
            }
            jc.slaveIndex = to;
            jm.setContext(moveClauses(parent, that, jc, clausesToSteal));
            if (target.getJoinType() == JOIN_CROSS) {
                target.setJoinType(JOIN_INNER);
            }
        }
    }

    private void traverseNamesAndIndices(QueryModel parent, ExpressionNode node) throws SqlException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withModel(parent);
        literalCollector.resetCounts();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());
    }

    private CharSequence validateColumnAndGetAlias(QueryModel model, CharSequence columnName, int dot, int position) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        CharSequence alias = null;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final QueryModel jm = joinModels.getQuick(i);
                if (jm.getColumnNameToAliasMap().excludes(columnName)) {
                    continue;
                }
                if (alias != null) {
                    throw SqlException.ambiguousColumn(position, columnName);
                }
                alias = jm.getColumnNameToAliasMap().get(columnName);
            }
        } else {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final QueryModel jm = joinModels.getQuick(i);
                final ExpressionNode tableAlias = jm.getAlias() != null ? jm.getAlias() : jm.getTableNameExpr();
                if (Chars.equalsIgnoreCase(tableAlias.token, columnName, 0, dot)) {
                    alias = jm.getColumnNameToAliasMap().get(columnName);
                    if (alias == null) {
                        alias = jm.getColumnNameToAliasMap().get(columnName, dot + 1, columnName.length());
                    }
                }
            }
        }
        return alias;
    }

    /**
     * Validates column reference alias (e.g. that the literal is a reference to an existing column or alias). When
     * the reference is valid, the method returns index of the table where reference is pointing. That is index of
     * table in the join clause. If the literal references the current projection, the table index is -1.
     * <p>
     * The validation order: the innerVirtualModel represent the current projection. If the referenced column is found
     * there first, this is where the search stops. The innerVirtualModel is allowed to be null, in which case the
     * validation is performed against the baseModel and its join model. In the joins referenced column must be
     * unambiguous.
     *
     * @param baseModel         we are rewriting models recursively, for every iteration there is "model" and "baseModel", the former
     *                          is the user-provided projection, the latter is the effective source of columns (excluding the projection
     *                          itself)
     * @param innerVirtualModel the model that contains the projection, this is not the "model" but a copy that maintains columns
     *                          added to the projection.
     * @param literal           the literal that references something, this could be either the projection itself or the base model
     * @param dot               index of '.' character in the literal, assuming this character would separate table alias and column name.
     *                          -1 when dot is missing. When dot is present, the literal will not be matched to the projection. We assume this is a
     *                          reference to the base model.
     * @param position          literal position in the SQL text to aid SQL error reporting
     * @param groupByCall       flag indicating that the return value is required, which is in turn asking to ignore projection lookup.
     *                          This lookup will be unhandled when the return value is needed to disambiguate the column name.
     * @return -1 if literal was matched to the projection, otherwise 0-based index of the join model where it was matched.
     * @throws SqlException exception is throw when literal could not be matched, or it matches several models at the same time (ambiguous).
     */
    private int validateColumnAndGetModelIndex(
            QueryModel baseModel,
            QueryModel innerVirtualModel,
            CharSequence literal,
            int dot,
            int position,
            boolean groupByCall
    ) throws SqlException {
        final ObjList<QueryModel> joinModels = baseModel.getJoinModels();
        int index = -1;
        if (dot == -1) {
            if (innerVirtualModel != null && innerVirtualModel.getAliasToColumnMap().contains(literal) && !groupByCall) {
                // For now, most places ignore the return values, except one - adding missing table prefixes in group-by
                // cases. We do not yet support projection reference in group-by. When we do, we will need to deal with
                // -1 there.
                return -1;
            }
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                if (joinModels.getQuick(i).getAliasToColumnMap().excludes(literal)) {
                    continue;
                }
                if (index != -1) {
                    throw SqlException.ambiguousColumn(position, literal);
                }
                index = i;
            }

            if (index == -1) {
                throw SqlException.invalidColumn(position, literal);
            }
        } else {
            index = baseModel.getModelAliasIndex(literal, 0, dot);
            if (index == -1) {
                throw SqlException.$(position, "Invalid table name or alias");
            }
            if (joinModels.getQuick(index).getAliasToColumnMap().excludes(literal, dot + 1, literal.length())) {
                throw SqlException.invalidColumn(position, literal);
            }
        }
        return index;
    }

    private void validateConstOrRuntimeConstFunction(ExpressionNode expr, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expr != null) {
            final Function func = functionParser.parseFunction(expr, EmptyRecordMetadata.INSTANCE, sqlExecutionContext);
            try {
                if (!func.isConstant() && !func.isRuntimeConstant()) {
                    throw SqlException.$(expr.position, "timezone must be a constant expression of STRING or CHAR type");
                }
            } finally {
                Misc.free(func);
            }
        }
    }

    /* Throws exception if given node tree contains reference to aggregate or window function that are not allowed in GROUP BY clause. */
    private void validateGroupByExpression(@Transient ExpressionNode node, int originalNodePosition) throws SqlException {
        try {
            validateNotAggregateOrWindowFunction(node);

            sqlNodeStack.clear();
            while (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        validateNotAggregateOrWindowFunction(node.rhs);
                        sqlNodeStack.push(node.rhs);
                    }

                    if (node.lhs != null) {
                        validateNotAggregateOrWindowFunction(node.lhs);
                        node = node.lhs;
                    } else {
                        if (!sqlNodeStack.isEmpty()) {
                            node = sqlNodeStack.poll();
                        } else {
                            node = null;
                        }
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        validateNotAggregateOrWindowFunction(e);
                        sqlNodeStack.push(e);
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    validateNotAggregateOrWindowFunction(e);
                    node = e;
                }
            }
        } catch (SqlException sqle) {
            if (originalNodePosition > -1) {
                sqle.setPosition(originalNodePosition);
            }
            throw sqle;
        }
    }

    /**
     * Validates that WHERE, ORDER BY, and JOIN ON clauses do not contain window functions.
     * Window functions are not allowed in these clauses per SQL standard.
     * This recursively checks all nested and joined models.
     *
     * @param model The query model to validate
     * @throws SqlException if a window function is found in WHERE, ORDER BY, or JOIN ON clause
     */
    private void validateNoWindowFunctionsInWhereClauses(QueryModel model) throws SqlException {
        if (model == null) {
            return;
        }

        // Check WHERE clause of this model
        ExpressionNode where = model.getWhereClause();
        if (where != null && checkForWindowFunctionsOrNames(where)) {
            // Find the position of the window function for the error message
            int position = findWindowFunctionOrNamePosition(where);
            throw SqlException.$(position, "window function is not allowed in WHERE clause");
        }

        // Check ORDER BY clause of this model
        ObjList<ExpressionNode> orderBy = model.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode node = orderBy.getQuick(i);
            if (checkForWindowFunctionsOrNames(node)) {
                int position = findWindowFunctionOrNamePosition(node);
                throw SqlException.$(position, "window function is not allowed in ORDER BY clause");
            }
        }

        // Check JOIN criteria of this model
        ExpressionNode joinCriteria = model.getJoinCriteria();
        if (joinCriteria != null && checkForWindowFunctionsOrNames(joinCriteria)) {
            int position = findWindowFunctionOrNamePosition(joinCriteria);
            throw SqlException.$(position, "window function is not allowed in JOIN ON clause");
        }

        // Recursively check nested model
        validateNoWindowFunctionsInWhereClauses(model.getNestedModel());

        // Recursively check all join models (including their join criteria)
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            validateNoWindowFunctionsInWhereClauses(joinModels.getQuick(i));
        }

        // Recursively check union model
        validateNoWindowFunctionsInWhereClauses(model.getUnionModel());
    }

    /**
     * Validates that a window expression does not contain window functions in its
     * PARTITION BY or ORDER BY clauses. Window functions are not allowed in these
     * clauses per SQL standard.
     *
     * @param windowExpr The window expression to validate
     * @throws SqlException if a window function is found in PARTITION BY or ORDER BY
     */
    private void validateNoWindowFunctionsInWindowSpec(WindowExpression windowExpr) throws SqlException {
        // Check PARTITION BY
        ObjList<ExpressionNode> partitionBy = windowExpr.getPartitionBy();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            ExpressionNode node = partitionBy.getQuick(i);
            if (checkForChildWindowFunctions(node)) {
                throw SqlException.$(node.position, "window function is not allowed in PARTITION BY clause");
            }
        }

        // Check ORDER BY
        ObjList<ExpressionNode> orderBy = windowExpr.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode node = orderBy.getQuick(i);
            if (checkForChildWindowFunctions(node)) {
                throw SqlException.$(node.position, "window function is not allowed in ORDER BY clause of window specification");
            }
        }
    }

    private void validateNotAggregateOrWindowFunction(ExpressionNode node) throws SqlException {
        if (node.type == FUNCTION) {
            if (functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
                throw SqlException.$(node.position, "aggregate functions are not allowed in GROUP BY");
            }
            if (functionParser.getFunctionFactoryCache().isWindow(node.token)) {
                throw SqlException.$(node.position, "window functions are not allowed in GROUP BY");
            }
        }
    }

    /**
     * Validates that a column reference in a window function argument can be resolved.
     * <p>
     * This catches invalid SQL like: {@code SELECT x as a, x as b, sum(sum(b) OVER ()) OVER () FROM t}
     * <p>
     * The problem: when the same column has multiple aliases ({@code x as a, x as b}), the second
     * alias internally stores a reference to the first alias's token, not the original column.
     * So {@code b}'s AST contains literal "a", not "x". When a window function references {@code b},
     * we must verify this alias chain resolves to an actual table column.
     * <p>
     * Valid cases that should NOT throw:
     * <ul>
     *   <li>{@code f(x) as c, lag(c)} - alias references a function result</li>
     *   <li>{@code x as a, sum(a)} - alias references a table column directly</li>
     *   <li>Table has column matching the alias name - normal column renaming handles it</li>
     * </ul>
     *
     * @param node              Original expression node (for error position)
     * @param token             The resolved token to validate
     * @param translatingModel  Model containing column-to-alias mappings
     * @param innerVirtualModel Model containing projection aliases
     * @param baseModel         Base model with actual table columns
     */
    private void validateWindowColumnReference(
            ExpressionNode node,
            CharSequence token,
            QueryModel translatingModel,
            QueryModel innerVirtualModel,
            QueryModel baseModel
    ) throws SqlException {
        // If the table has a column with this name, normal flow handles renaming (e.g., b -> b1)
        if (!baseModel.getAliasToColumnMap().excludes(token)) {
            return;
        }

        // Check if token exists as a projection alias
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = innerVirtualModel.getAliasToColumnMap();
        int aliasIndex = aliasMap.keyIndex(token);
        if (aliasIndex >= 0) {
            // Not an alias - will be handled by normal flow
            return;
        }

        // Token is a projection alias. Check what it references.
        QueryColumn aliasedColumn = aliasMap.valueAtQuick(aliasIndex);
        ExpressionNode aliasAst = aliasedColumn.getAst();

        // Only validate if alias references a column (LITERAL). Functions, constants, etc. are fine.
        if (aliasAst.type != ExpressionNode.LITERAL) {
            return;
        }

        // The alias references another literal. Verify that literal resolves to a table column.
        CharSequence referencedToken = aliasAst.token;
        boolean inTranslatingModel = translatingModel.getAliasToColumnMap().get(token) != null;
        boolean isTableColumn = !baseModel.getAliasToColumnMap().excludes(referencedToken);

        if (!inTranslatingModel && !isTableColumn) {
            // The alias references another alias that doesn't resolve to a table column
            throw SqlException.invalidColumn(node.position, node.token);
        }
    }

    private void validateWindowFunctions(
            QueryModel model, SqlExecutionContext sqlExecutionContext, int recursionLevel
    ) throws SqlException {
        if (model == null) {
            return;
        }

        if (recursionLevel > maxRecursion) {
            throw SqlException.$(0, "SQL model is too complex to evaluate");
        }

        if (model.getSelectModelType() == SELECT_MODEL_WINDOW) {
            final ObjList<QueryColumn> queryColumns = model.getColumns();
            for (int i = 0, n = queryColumns.size(); i < n; i++) {
                QueryColumn qc = queryColumns.getQuick(i);
                if (qc.isWindowExpression()) {
                    normalizeWindowFrame((WindowExpression) qc, sqlExecutionContext);
                }
            }
        }

        // Validate named window definitions (WINDOW clause). Referenced definitions were already
        // validated above via resolved copies, but unreferenced definitions need validation too.
        LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = model.getNamedWindows();
        if (namedWindows.size() > 0) {
            ObjList<CharSequence> keys = namedWindows.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                WindowExpression ac = namedWindows.get(keys.getQuick(i));
                if (ac != null) {
                    normalizeWindowFrame(ac, sqlExecutionContext);
                }
            }
        }

        validateWindowFunctions(model.getNestedModel(), sqlExecutionContext, recursionLevel + 1);

        // join models
        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            validateWindowFunctions(model.getJoinModels().getQuick(i), sqlExecutionContext, recursionLevel + 1);
        }

        validateWindowFunctions(model.getUnionModel(), sqlExecutionContext, recursionLevel + 1);
    }

    private void validateWindowJoins(
            QueryModel model, SqlExecutionContext sqlExecutionContext, int recursionLevel
    ) throws SqlException {
        if (model == null) {
            return;
        }
        if (recursionLevel > maxRecursion) {
            throw SqlException.$(0, "SQL model is too complex to evaluate");
        }

        if (isWindowJoin(model)) {
            for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
                QueryModel windowJoinModel = model.getJoinModels().get(i);
                if (windowJoinModel.getJoinType() != JOIN_WINDOW) {
                    continue;
                }
                WindowJoinContext context = windowJoinModel.getWindowJoinContext();
                long lo = 0, hi = 0;
                switch (context.getLoKind()) {
                    case WindowJoinContext.PRECEDING:
                        lo = evalNonNegativeLongConstantOrDie(functionParser, context.getLoExpr(), sqlExecutionContext);
                        break;
                    case WindowJoinContext.FOLLOWING:
                        lo = evalNonNegativeLongConstantOrDie(functionParser, context.getLoExpr(), sqlExecutionContext);
                        if (lo == Long.MAX_VALUE) {
                            lo = Long.MIN_VALUE;
                        } else {
                            lo *= -1;
                        }
                }
                if (lo == Long.MIN_VALUE || lo == Long.MAX_VALUE) {
                    throw SqlException.position(context.getLoKindPos()).put("unbounded preceding/following is not supported in WINDOW joins");
                }

                switch (context.getHiKind()) {
                    case WindowJoinContext.PRECEDING:
                        hi = evalNonNegativeLongConstantOrDie(functionParser, context.getHiExpr(), sqlExecutionContext);
                        if (hi == Long.MAX_VALUE) {
                            hi = Long.MIN_VALUE;
                        } else {
                            hi *= -1;
                        }
                        break;
                    case WindowJoinContext.FOLLOWING:
                        hi = evalNonNegativeLongConstantOrDie(functionParser, context.getHiExpr(), sqlExecutionContext);
                }
                if (hi == Long.MIN_VALUE || hi == Long.MAX_VALUE) {
                    throw SqlException.position(context.getHiKindPos()).put("unbounded preceding/following is not supported in WINDOW joins");
                }
                context.setHi(hi);
                context.setLo(lo);
            }
        }

        validateWindowJoins(model.getNestedModel(), sqlExecutionContext, recursionLevel + 1);
        validateWindowJoins(model.getUnionModel(), sqlExecutionContext, recursionLevel + 1);
    }

    /**
     * Wraps a predicate in an and_offset expression for timestamp predicate pushdown.
     * The and_offset wrapper indicates that interval extraction should apply an offset.
     *
     * @param predicate the predicate to wrap
     * @param model     the model containing timestamp offset info
     * @return the wrapped expression: and_offset(predicate, unit, offset)
     */
    private ExpressionNode wrapInAndOffset(ExpressionNode predicate, QueryModel model) {
        // Create: and_offset(predicate, unit, offset)
        // This is an internal pseudo-function used to pass offset information to WhereClauseParser.
        // It is NOT a user-facing function - WhereClauseParser consumes it during interval extraction.
        //
        // The optimizer intrinsically understands dateadd(char, int, timestamp) and pushes
        // predicates through it by wrapping them with offset adjustment info.
        // See TimestampAddFunctionFactory for the dateadd function definition.
        ExpressionNode wrapper = expressionNodePool.next().of(
                FUNCTION,
                "and_offset",
                0,
                predicate.position
        );
        wrapper.paramCount = 3;

        // Unit as constant char (quoted to match dateadd format)
        CharacterStoreEntry unitEntry = characterStore.newEntry();
        unitEntry.put('\'').put(model.getTimestampOffsetUnit()).put('\'');
        ExpressionNode unitNode = expressionNodePool.next().of(
                CONSTANT,
                unitEntry.toImmutable(),
                0,
                0
        );

        // Offset value as constant (int, matching dateadd's signature)
        CharacterStoreEntry offsetEntry = characterStore.newEntry();
        offsetEntry.put(model.getTimestampOffsetValue());
        ExpressionNode offsetNode = expressionNodePool.next().of(
                CONSTANT,
                offsetEntry.toImmutable(),
                0,
                0
        );

        // Add arguments to wrapper (args list is pre-initialized in ExpressionNode)
        // Note: args are stored in reverse order for rendering, so add [offset, unit, predicate]
        wrapper.args.add(offsetNode);
        wrapper.args.add(unitNode);
        wrapper.args.add(predicate);

        return wrapper;
    }

    @NotNull
    private QueryModel wrapWithSelectModel(QueryModel model, int columnCount) {
        final QueryModel outerModel = createWrapperModel(model);
        // then create columns on the outermost model
        for (int i = 0; i < columnCount; i++) {
            QueryColumn qcFrom = model.getBottomUpColumns().getQuick(i);
            outerModel.addBottomUpColumnIfNotExists(nextColumn(qcFrom.getAlias()));
        }
        return outerModel;
    }

    @NotNull
    private QueryModel wrapWithSelectModel(QueryModel model, IntList insetColumnIndexes, ObjList<QueryColumn> insertColumnAliases, CharSequence timestampAlias) {
        final QueryModel _model = createWrapperModel(model);

        // These are merged columns, the assumption is that the insetColumnIndexes are ordered.
        // This loop will fail miserably in indexes are unordered.
        final int src1ColumnCount = model.getBottomUpColumns().size();
        final int src2ColumnCount = insetColumnIndexes.size();
        for (int i = 0, k = 0, m = 0; i < src1ColumnCount || k < src2ColumnCount; m++) {
            if (k < src2ColumnCount && insetColumnIndexes.getQuick(k) == m) {
                final QueryColumn column = insertColumnAliases.get(k);
                // insert column at this position, this column must reference our timestamp, that
                // comes out of the group-by result set, but with user-provided aliases.
                if (column.getAst().type == LITERAL) {
                    _model.addBottomUpColumnIfNotExists(nextColumn(column.getAlias(), timestampAlias));
                } else {
                    _model.addBottomUpColumnIfNotExists(column);
                }
                k++;
            } else {
                final QueryColumn qcFrom = model.getBottomUpColumns().getQuick(i);
                _model.addBottomUpColumnIfNotExists(nextColumn(qcFrom.getAlias()));
                i++;
            }
        }

        return _model;
    }

    private QueryModel wrapWithSelectWildcard(QueryModel model) throws SqlException {
        final QueryModel outerModel = createWrapperModel(model);
        outerModel.addBottomUpColumn(nextColumn("*"));
        return outerModel;
    }

    static long evalNonNegativeLongConstantOrDie(FunctionParser functionParser, ExpressionNode expr, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expr != null) {
            final Function loFunc = functionParser.parseFunction(expr, EmptyRecordMetadata.INSTANCE, sqlExecutionContext);
            if (!loFunc.isConstant()) {
                Misc.free(loFunc);
                throw SqlException.$(expr.position, "constant expression expected");
            }

            try {
                long value;
                if (!(loFunc instanceof CharConstant)) {
                    value = loFunc.getLong(null);
                } else {
                    long tmp = (byte) (loFunc.getChar(null) - '0');
                    value = tmp > -1 && tmp < 10 ? tmp : Numbers.LONG_NULL;
                }

                if (value < 0) {
                    throw SqlException.$(expr.position, "non-negative integer expression expected");
                }
                return value;
            } catch (UnsupportedOperationException | ImplicitCastException e) {
                throw SqlException.$(expr.position, "integer expression expected");
            } finally {
                Misc.free(loFunc);
            }
        }
        return Long.MAX_VALUE;
    }

    @SuppressWarnings({"unused", "RedundantThrows"})
    // this method is used by Enterprise version
    protected void authorizeColumnAccess(SqlExecutionContext executionContext, QueryModel model) throws SqlException {
    }

    @SuppressWarnings("unused")
    protected void authorizeUpdate(QueryModel updateQueryModel, TableToken token) {
    }

    void collectColumnRefCount(QueryModel parent, QueryModel queryModel) {
        if (queryModel == null) {
            return;
        }
        if (queryModel.getSelectModelType() != SELECT_MODEL_CHOOSE && queryModel.getSelectModelType() != SELECT_MODEL_VIRTUAL) {
            collectColumnRefCountRecursive(parent, queryModel);
            return;
        }
        ObjList<QueryColumn> columns = queryModel.getColumns();
        if (parent == null) {
            // init ref count to 1 for top queryModel
            for (int i = 0, n = columns.size(); i < n; i++) {
                queryModel.incrementColumnRefCount(columns.getQuick(i).getAlias(), 1);
            }
        }

        tempExprs.clear();
        for (int i = 0, n = columns.size(); i < n; i++) {
            tempExprs.add(columns.getQuick(i).getAst());
        }
        if (queryModel.getOrderBy() != null) {
            tempExprs.addAll(queryModel.getOrderBy());
        }
        if (queryModel.getWhereClause() != null) {
            tempExprs.add(queryModel.getWhereClause());
        }
        if (queryModel.getLatestBy() != null) {
            tempExprs.addAll(queryModel.getLatestBy());
        }
        QueryModel child = queryModel.getNestedModel();

        for (int i = 0, n = tempExprs.size(); i < n; i++) {
            final ExpressionNode ast = tempExprs.getQuick(i);
            if (ast != null) {
                sqlNodeStack.clear();
                sqlNodeStack.push(ast);
                while (!sqlNodeStack.isEmpty()) {
                    final ExpressionNode node = sqlNodeStack.pop();
                    if (node.type == LITERAL) {
                        final CharSequence columnRef = node.token;
                        final int dot = Chars.indexOfLastUnquoted(columnRef, '.');
                        if (dot == -1) {
                            if (child == null || !child.getAliasToColumnMap().contains(columnRef)) {
                                queryModel.incrementColumnRefCount(columnRef, 1);
                            }
                        } else {
                            final CharSequence tableName = columnRef.subSequence(0, dot);
                            final CharSequence columnName = columnRef.subSequence(dot + 1, columnRef.length());
                            if (queryModel.getAlias() != null && Chars.equalsIgnoreCase(tableName, queryModel.getAlias().token)) {
                                if (child == null || !child.getAliasToColumnMap().contains(columnName)) {
                                    queryModel.incrementColumnRefCount(columnRef, 1);
                                }
                            }
                        }
                    } else if (node.type == FUNCTION) {
                        if (node.paramCount < 3) {
                            if (node.rhs != null) {
                                sqlNodeStack.push(node.rhs);
                            }
                            if (node.lhs != null) {
                                sqlNodeStack.push(node.lhs);
                            }
                        } else {
                            for (int k = 0, m = node.paramCount; k < m; k++) {
                                sqlNodeStack.push(node.args.getQuick(k));
                            }
                        }
                    } else {
                        if (node.rhs != null) {
                            sqlNodeStack.push(node.rhs);
                        }
                        if (node.lhs != null) {
                            sqlNodeStack.push(node.lhs);
                        }
                    }
                }
            }
        }

        tempExprs.clear();
        if (parent != null) {
            columns = parent.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                tempExprs.add(columns.getQuick(i).getAst());
            }
            int columnsSize = columns.size();
            // isRecordNotMaterialized flag indicates whether the generated RecordFactory will materialize records.
            // For materialized records, operations like ORDER BY/WHERE won't call the child queryModel's calculations.
            // For GroupBy, window functions, and joins, materialization depends on specific scenarios.
            boolean isRecordNotMaterialized = parent.getSelectModelType() == SELECT_MODEL_VIRTUAL || parent.getSelectModelType() == SELECT_MODEL_CHOOSE;
            if (isRecordNotMaterialized) {
                if (parent.getOrderBy() != null) {
                    tempExprs.addAll(parent.getOrderBy());
                }
                if (parent.getWhereClause() != null) {
                    tempExprs.add(parent.getWhereClause());
                }
                if (parent.getLatestBy() != null) {
                    tempExprs.addAll(parent.getLatestBy());
                }
            }

            for (int i = 0, n = tempExprs.size(); i < n; i++) {
                final ExpressionNode ast = tempExprs.getQuick(i);
                int refCount = 1;
                if (i < columnsSize && (parent.getSelectModelType() == SELECT_MODEL_CHOOSE || parent.getSelectModelType() == SELECT_MODEL_VIRTUAL)) {
                    refCount = parent.getRefCount(columns.getQuick(i).getAlias());
                }

                if (ast != null) {
                    sqlNodeStack.clear();
                    sqlNodeStack.push(ast);
                    while (!sqlNodeStack.isEmpty()) {
                        final ExpressionNode node = sqlNodeStack.pop();
                        if (node.type == LITERAL) {
                            final CharSequence columnRef = node.token;
                            final int dot = Chars.indexOfLastUnquoted(columnRef, '.');
                            if (dot == -1) {
                                queryModel.incrementColumnRefCount(columnRef, refCount);
                            } else {
                                final CharSequence tableName = columnRef.subSequence(0, dot);
                                final CharSequence columnName = columnRef.subSequence(dot + 1, columnRef.length());
                                if (queryModel.getAlias() != null && Chars.equalsIgnoreCase(tableName, queryModel.getAlias().token)) {
                                    queryModel.incrementColumnRefCount(columnName, refCount);
                                }
                            }
                        } else if (node.type == FUNCTION) {
                            if (node.paramCount < 3) {
                                if (node.rhs != null) {
                                    sqlNodeStack.push(node.rhs);
                                }
                                if (node.lhs != null) {
                                    sqlNodeStack.push(node.lhs);
                                }
                            } else {
                                for (int k = 0, m = node.paramCount; k < m; k++) {
                                    sqlNodeStack.push(node.args.getQuick(k));
                                }
                            }
                        } else {
                            if (node.rhs != null) {
                                sqlNodeStack.push(node.rhs);
                            }
                            if (node.lhs != null) {
                                sqlNodeStack.push(node.lhs);
                            }
                        }
                    }
                }
            }
        }

        collectColumnRefCountRecursive(parent, queryModel);
    }

    void collectColumnRefCountRecursive(QueryModel parent, QueryModel queryModel) {
        ObjList<QueryModel> joinModels = queryModel.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            collectColumnRefCount(parent, joinModels.getQuick(i));
        }
        collectColumnRefCount(parent, queryModel.getUnionModel());

        QueryModel parentModel = queryModel;
        if (queryModel.getSelectModelType() == SELECT_MODEL_NONE) {
            if (queryModel.getJoinModels().size() < 2) {
                if (queryModel.getTableNameExpr() != null) {
                    parentModel = null;
                } else {
                    parentModel = parent;
                }
            }
        }
        collectColumnRefCount(parentModel, queryModel.getNestedModel());
    }

    QueryModel optimise(
            @Transient final QueryModel model,
            @Transient SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryModel rewrittenModel = model;
        try {
            rewrittenModel = bubbleUpOrderByAndLimitFromUnion(rewrittenModel);
            optimiseExpressionModels(rewrittenModel, sqlExecutionContext, sqlParserCallback);
            enumerateTableColumns(rewrittenModel, sqlExecutionContext, sqlParserCallback);
            rewrittenModel = rewritePivot(rewrittenModel, sqlExecutionContext);
            rewriteTopLevelLiteralsToFunctions(rewrittenModel);
            rewriteSampleByFromTo(rewrittenModel);
            propagateHintsTo(rewrittenModel, rewrittenModel.getHints());
            rewrittenModel = rewriteDistinct(rewrittenModel);
            rewrittenModel = rewriteSampleBy(rewrittenModel, sqlExecutionContext);
            rewrittenModel = moveOrderByFunctionsIntoOuterSelect(rewrittenModel);
            rewriteCount(rewrittenModel);
            resolveJoinColumns(rewrittenModel);
            optimiseBooleanNot(rewrittenModel);
            validateNoWindowFunctionsInWhereClauses(rewrittenModel);
            resolveWindowInheritance(rewrittenModel);
            resolveNamedWindows(rewrittenModel);
            rewrittenModel = rewriteSelectClause(rewrittenModel, true, sqlExecutionContext, sqlParserCallback);
            detectTimestampOffsetsRecursive(rewrittenModel);
            rewriteSingleFirstLastGroupBy(rewrittenModel);
            rewriteTrivialGroupByExpressions(rewrittenModel);
            optimiseJoins(rewrittenModel);
            collapseStackedChooseModels(rewrittenModel);
            rewriteCountDistinct(rewrittenModel);
            rewriteMultipleTermLimitedOrderByPart1(rewrittenModel);
            pushLimitFromChooseToNone(rewrittenModel, sqlExecutionContext);
            validateWindowFunctions(rewrittenModel, sqlExecutionContext, 0);
            validateWindowJoins(rewrittenModel, sqlExecutionContext, 0);
            rewriteOrderByPosition(rewrittenModel);
            rewriteOrderByPositionForUnionModels(rewrittenModel);
            rewrittenModel = rewriteOrderBy(rewrittenModel);
            optimiseOrderBy(rewrittenModel, OrderByMnemonic.ORDER_BY_UNKNOWN);
            createOrderHash(rewrittenModel);
            moveWhereInsideSubQueries(rewrittenModel);
            eraseColumnPrefixInWhereClauses(rewrittenModel);
            moveTimestampToChooseModel(rewrittenModel);
            propagateTopDownColumns(rewrittenModel, rewrittenModel.allowsColumnsChange());
            rewriteMultipleTermLimitedOrderByPart2(rewrittenModel);
            rewrittenModel.recordViews(model.getReferencedViews());
            if (!sqlExecutionContext.isValidationOnly()) {
                authorizeColumnAccess(sqlExecutionContext, rewrittenModel);
            }
            if (ALLOW_FUNCTION_MEMOIZATION) {
                collectColumnRefCount(null, rewrittenModel);
            }
            return rewrittenModel;
        } catch (Throwable th) {
            // at this point, models may have functions that need to be freed
            Misc.freeObjListAndClear(tableFactoriesInFlight);
            throw th;
        }
    }

    void optimiseUpdate(
            QueryModel updateQueryModel,
            SqlExecutionContext sqlExecutionContext,
            TableRecordMetadata metadata,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final QueryModel selectQueryModel = updateQueryModel.getNestedModel();
        selectQueryModel.setIsUpdate(true);
        QueryModel optimisedNested = optimise(selectQueryModel, sqlExecutionContext, sqlParserCallback);
        assert optimisedNested.isUpdate();
        updateQueryModel.setNestedModel(optimisedNested);

        // And then generate plan for UPDATE top level QueryModel
        validateUpdateColumns(updateQueryModel, metadata, sqlExecutionContext);
    }

    void validateUpdateColumns(
            QueryModel updateQueryModel, TableRecordMetadata metadata, SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        try {
            literalCollectorANames.clear();
            tempIntList.clear(metadata.getColumnCount());
            tempIntList.setPos(metadata.getColumnCount());
            int timestampIndex = metadata.getTimestampIndex();
            int updateSetColumnCount = updateQueryModel.getUpdateExpressions().size();
            for (int i = 0; i < updateSetColumnCount; i++) {
                // SET left hand side expressions are stored in top level UPDATE QueryModel
                ExpressionNode columnExpression = updateQueryModel.getUpdateExpressions().get(i);
                int position = columnExpression.position;
                int columnIndex = metadata.getColumnIndexQuiet(columnExpression.token);

                // SET right hand side expressions are stored in the Nested SELECT QueryModel as columns
                QueryColumn queryColumn = updateQueryModel.getNestedModel().getColumns().get(i);
                if (columnIndex < 0) {
                    throw SqlException.invalidColumn(position, queryColumn.getName());
                }
                if (columnIndex == timestampIndex) {
                    throw SqlException.$(position, "Designated timestamp column cannot be updated");
                }
                if (tempIntList.getQuick(columnIndex) == 1) {
                    throw SqlException.$(position, "Duplicate column ").put(queryColumn.getName()).put(" in SET clause");
                }

                // When column name case does not match table column name in left side of SET
                // for example if table "tbl" column name is "Col" but update uses
                // UPDATE tbl SET coL = 1
                // we need to replace to match metadata name exactly
                CharSequence exactColName = metadata.getColumnName(columnIndex);
                queryColumn.of(exactColName, queryColumn.getAst());
                tempIntList.set(columnIndex, 1);
                literalCollectorANames.add(exactColName);

                ExpressionNode rhs = queryColumn.getAst();
                if (rhs.type == FUNCTION) {
                    if (functionParser.getFunctionFactoryCache().isGroupBy(rhs.token)) {
                        throw SqlException.$(rhs.position, "Unsupported function in SET clause");
                    }
                }
            }

            TableToken tableToken = metadata.getTableToken();
            sqlExecutionContext.getSecurityContext().authorizeTableUpdate(tableToken, literalCollectorANames);

            if (!sqlExecutionContext.isWalApplication() && !Chars.equalsIgnoreCase(tableToken.getTableName(), updateQueryModel.getTableName())) {
                // Table renamed
                throw TableReferenceOutOfDateException.of(updateQueryModel.getTableName());
            }
            updateQueryModel.setUpdateTableToken(tableToken);
            authorizeUpdate(updateQueryModel, tableToken);
        } catch (EntryLockedException e) {
            throw SqlException.position(updateQueryModel.getModelPosition()).put("table is locked: ").put(tableLookupSequence);
        } catch (CairoException e) {
            if (e.isAuthorizationError()) {
                throw e;
            }
            throw SqlException.position(updateQueryModel.getModelPosition()).put(e);
        }
    }

    private static class LiteralCheckingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private LowerCaseCharSequenceObjHashMap<QueryColumn> nameTypeMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == LITERAL) {
                final int len = node.token.length();
                final int dot = Chars.indexOf(node.token, 0, len, '.');
                int index = nameTypeMap.keyIndex(node.token, dot + 1, len);
                // these columns are pre-validated
                assert index < 0;
                if (nameTypeMap.valueAt(index).getAst().type != LITERAL) {
                    throw NonLiteralException.INSTANCE;
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(LowerCaseCharSequenceObjHashMap<QueryColumn> nameTypeMap) {
            this.nameTypeMap = nameTypeMap;
            return this;
        }
    }

    private static class LiteralRewritingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == LITERAL) {
                int dot = Chars.indexOfLastUnquoted(node.token, '.');
                int index = dot == -1 ? aliasToColumnMap.keyIndex(node.token) : aliasToColumnMap.keyIndex(node.token, dot + 1, node.token.length());
                // we have table column hit when alias is not found
                // in this case expression rewrite is unnecessary
                if (index < 0) {
                    CharSequence column = aliasToColumnMap.valueAtQuick(index);
                    assert column != null;
                    // it is also unnecessary to rewrite literal if target value is the same
                    if (!Chars.equals(node.token, column)) {
                        node.token = column;
                    }
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnMap) {
            this.aliasToColumnMap = aliasToColumnMap;
            return this;
        }
    }

    private static class NonLiteralException extends RuntimeException {
        private static final NonLiteralException INSTANCE = new NonLiteralException();
    }

    /**
     * Analyzes the given node for being a trivial expression. A trivial expression is a single column
     * with any number of arithmetical operations and constants.
     * <p>
     * Examples:
     * <pre>
     *   id + 1
     *   id / 2 + 1
     *   42 * id - 10001
     * </pre>
     */
    private static class TrivialExpressionVisitor implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
        private ExpressionNode columnNode;
        private boolean trivial = true;

        @Override
        public void clear() {
            trivial = true;
            columnNode = null;
        }

        public ExpressionNode getColumnNode() {
            return columnNode;
        }

        public boolean isTrivial() {
            return trivial;
        }

        @Override
        public void visit(ExpressionNode node) {
            switch (node.type) {
                case CONSTANT:
                    return; // trivial expression may have any number of constants
                case LITERAL:
                    if (columnNode == null) {
                        columnNode = node;
                        return; // so far, we've only seen a single column literal
                    }
                    break;
                case OPERATION:
                    if (node.token.length() == 1) {
                        char op = node.token.charAt(0);
                        switch (op) {
                            case '/':
                            case '*':
                            case '%':
                                if (node.paramCount == 2) {
                                    return; // arithmetical operations are fine
                                }
                                break;
                            case '-':
                            case '+':
                                if (node.paramCount == 1 || node.paramCount == 2) {
                                    return; // +number/-number expressions and arithmetical operations are fine
                                }
                                break;
                        }
                    }
                    break;
            }
            // we've seen something that makes the expression non-trivial
            trivial = false;
        }
    }

    private class ColumnPrefixEraser implements PostOrderTreeTraversalAlgo.Visitor {

        @Override
        public void visit(ExpressionNode node) {
            switch (node.type) {
                case FUNCTION:
                case OPERATION:
                case SET_OPERATION:
                    if (node.paramCount < 3) {
                        node.lhs = rewrite(node.lhs);
                        node.rhs = rewrite(node.rhs);
                    } else {
                        for (int i = 0, n = node.paramCount; i < n; i++) {
                            node.args.setQuick(i, rewrite(node.args.getQuick(i)));
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        private ExpressionNode rewrite(ExpressionNode node) {
            if (node != null && node.type == LITERAL) {
                final int dot = Chars.indexOfLastUnquoted(node.token, '.');
                if (dot != -1) {
                    return nextLiteral(node.token.subSequence(dot + 1, node.token.length()));
                }
            }
            return node;
        }
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private int functionCount;
        private IntHashSet indexes;
        private QueryModel model;
        private ObjList<CharSequence> names;
        private int nullCount;

        @Override
        public void visit(ExpressionNode node) throws SqlException {
            switch (node.type) {
                case LITERAL:
                    int dot = Chars.indexOfLastUnquoted(node.token, '.');
                    CharSequence name = dot == -1 ? node.token : node.token.subSequence(dot + 1, node.token.length());
                    indexes.add(validateColumnAndGetModelIndex(model, null, node.token, dot, node.position, false));
                    if (names != null) {
                        names.add(name);
                    }
                    break;
                case CONSTANT:
                    if (nullConstants.contains(node.token)) {
                        nullCount++;
                    }
                    break;
                case FUNCTION:
                case OPERATION:
                    functionCount++;
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

        private void resetCounts() {
            nullCount = 0;
            functionCount = 0;
        }

        private PostOrderTreeTraversalAlgo.Visitor rhs() {
            indexes = literalCollectorBIndexes;
            names = literalCollectorBNames;
            return this;
        }

        private PostOrderTreeTraversalAlgo.Visitor to(IntHashSet indexes) {
            this.indexes = indexes;
            this.names = null;
            return this;
        }

        private void withModel(QueryModel model) {
            this.model = model;
        }
    }

    static {
        notOps.put("not", NOT_OP_NOT);
        notOps.put("and", NOT_OP_AND);
        notOps.put("or", NOT_OP_OR);
        notOps.put(">", NOT_OP_GREATER);
        notOps.put(">=", NOT_OP_GREATER_EQ);
        notOps.put("<", NOT_OP_LESS);
        notOps.put("<=", NOT_OP_LESS_EQ);
        notOps.put("=", NOT_OP_EQUAL);
        notOps.put("!=", NOT_OP_NOT_EQ);
        notOps.put("<>", NOT_OP_NOT_EQ);

        joinBarriers = new IntHashSet();
        joinBarriers.add(JOIN_LEFT_OUTER);
        joinBarriers.add(JOIN_RIGHT_OUTER);
        joinBarriers.add(JOIN_FULL_OUTER);
        joinBarriers.add(JOIN_CROSS_LEFT);
        joinBarriers.add(JOIN_CROSS_RIGHT);
        joinBarriers.add(JOIN_CROSS_FULL);
        joinBarriers.add(JOIN_ASOF);
        joinBarriers.add(JOIN_SPLICE);
        joinBarriers.add(JOIN_LT);
        joinBarriers.add(JOIN_WINDOW);

        nullConstants.add("null");
        nullConstants.add("NaN");

        joinOps.put("=", JOIN_OP_EQUAL);
        joinOps.put("and", JOIN_OP_AND);
        joinOps.put("or", JOIN_OP_OR);
        joinOps.put("~", JOIN_OP_REGEX);

        flexColumnModelTypes.add(SELECT_MODEL_CHOOSE);
        flexColumnModelTypes.add(SELECT_MODEL_NONE);
        flexColumnModelTypes.add(SELECT_MODEL_DISTINCT);
        flexColumnModelTypes.add(SELECT_MODEL_VIRTUAL);
        flexColumnModelTypes.add(SELECT_MODEL_WINDOW);
        flexColumnModelTypes.add(SELECT_MODEL_GROUP_BY);
        flexColumnModelTypes.add(SELECT_MODEL_WINDOW_JOIN);
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
    }

    static {
        orderedGroupByFunctions = new LowerCaseAsciiCharSequenceHashSet();
        orderedGroupByFunctions.add("first");
        orderedGroupByFunctions.add("first_not_null");
        orderedGroupByFunctions.add("last");
        orderedGroupByFunctions.add("last_not_null");
    }
}
