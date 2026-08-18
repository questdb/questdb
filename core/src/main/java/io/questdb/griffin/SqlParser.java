/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.ops.CreateLiveViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateLiveViewOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilderImpl;
import io.questdb.griffin.engine.table.ShowCreateDatabaseRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.model.CompileViewModel;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.HorizonJoinContext;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RenameTableModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.griffin.model.WithClauseModel;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.LowerCaseAsciiCharSequenceHashSet;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;

import static io.questdb.cairo.SqlWalMode.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.SqlOptimiser.hasGroupByFunc;
import static io.questdb.griffin.engine.ops.CreateMatViewOperation.*;
import static io.questdb.std.GenericLexer.assertNoDotsAndSlashes;
import static io.questdb.std.GenericLexer.unquote;

public class SqlParser {
    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    public static final ExpressionNode ZERO_OFFSET = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "'00:00'", 0, 0);
    private static final ExpressionNode ONE = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "1", 0, 0);
    private static final LowerCaseAsciiCharSequenceHashSet columnAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet groupByStopSet = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap joinStartSet = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final LowerCaseAsciiCharSequenceHashSet pivotForStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet setOperations = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet tableAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private final IntList accumulatedColumnPositions = new IntList();
    private final ObjList<QueryColumn> accumulatedColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet aliasMap = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceIntHashMap aliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final CairoEngine cairoEngine;
    private final CharacterStore characterStore;
    private final CharSequence column;
    private final ObjectPool<CompileViewModel> compileViewModelPool;
    private final CairoConfiguration configuration;
    private final ObjectPool<ExportModel> copyModelPool;
    private final CreateLiveViewOperationBuilderImpl createLiveViewOperationBuilder = new CreateLiveViewOperationBuilderImpl();
    private final CreateMatViewOperationBuilderImpl createMatViewOperationBuilder = new CreateMatViewOperationBuilderImpl();
    private final ObjectPool<CreateTableColumnModel> createTableColumnModelPool;
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = createMatViewOperationBuilder.getCreateTableOperationBuilder();
    private final CreateViewOperationBuilderImpl createViewOperationBuilder = new CreateViewOperationBuilderImpl();
    private final ObjectPool<ExplainModel> explainModelPool;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ExpressionParser expressionParser;
    private final ExpressionTreeBuilder expressionTreeBuilder;
    private final ObjectPool<InsertModel> insertModelPool;
    private final LowerCaseCharSequenceHashSet pivotAliasMap = new LowerCaseCharSequenceHashSet();
    private final ObjectPool<PivotForColumn> pivotQueryColumnPool;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    // Map of view definitions encountered during query compilation.
    // Using a map ensures consistent view definitions even if views are modified concurrently.
    private final LowerCaseCharSequenceObjHashMap<ViewDefinition> recordedViews = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo.Visitor rejectJoinSubQueryRef = this::rejectJoinSubQuery;
    private final ObjectPool<RenameTableModel> renameTableModelPool;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteConcatRef = this::rewriteConcat;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCountAndWindowExpressionsRef = this::rewriteCountAndWindowExpressions;
    private final RewriteDeclaredVariablesInExpressionVisitor rewriteDeclaredVariablesInExpressionVisitor = new RewriteDeclaredVariablesInExpressionVisitor();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteJsonExtractCastRef = this::rewriteJsonExtractCast;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgCastRef = this::rewritePgCast;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgNumericRef = this::rewritePgNumeric;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final IntList tableNamePositions = new IntList();
    private final LowerCaseCharSequenceHashSet tableNames = new LowerCaseCharSequenceHashSet();
    private final CharSequenceHashSet tempCharSequenceSet = new CharSequenceHashSet();
    private final ObjList<ExpressionNode> tempExprNodes = new ObjList<>();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCaseRef = this::rewriteCase;
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> topLevelWithModel = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjectPool<GenericLexer> viewLexers;
    private final SqlParserCallback viewSqlParserCallback = new SqlParserCallback() {
    };
    // Track views currently being compiled to detect cycles during query parsing
    private final LowerCaseCharSequenceHashSet viewsBeingCompiled = new LowerCaseCharSequenceHashSet();
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private final ObjectPool<WithClauseModel> withClauseModelPool;
    private boolean copyMode = false;
    private boolean createTableMode = false;
    private boolean createViewMode = false;
    private int digit;
    // Track tables currently being wrapped by the row-expiry filter, so the synthetic inner
    // "SELECT * FROM t WHERE ..." resolves "t" as a plain table instead of recursing forever.
    // The key is the caller's own spelling of the name, re-emitted verbatim into the synthetic inner query,
    // so the recursion guard matches on that exact spelling. A case-folding set is unnecessary: the table
    // registry is case-insensitive, so case-distinct sibling tables/views cannot exist in the first place.
    private final CharSequenceHashSet expiringTablesBeingExpanded = new CharSequenceHashSet();
    // Tables the read filter swapped for a sub-query during this parse. CREATE LIVE VIEW checks
    // this when its FROM clause no longer holds the plain table the user wrote, so that it can
    // point at the EXPIRE ROWS policy that got in the way.
    private final CharSequenceHashSet expiryExpandedTables = new CharSequenceHashSet();
    // The execution context of the current parse, consulted for the PER-TABLE read-filter decision
    // (the mat-view refresh context keeps the filter on every table except the base). Null when parse()
    // was invoked without a context; rowExpiryReadFilterEnabled is the decision then.
    private SqlExecutionContext expiryFilterExecutionContext;
    // CairoTable whose EXPIRE ROWS predicate was last looked up from the metadata cache (null when the
    // lookup fell back to authoritative metadata). Carries the per-instance memo of derived read-filter
    // artifacts (flip eligibility, quoted column CSV); the instance's policy-relevant state (predicate,
    // timestamp, columns) is immutable per hydration, so it is safe to hold beyond the cache read lock.
    private CairoTable expiryPolicyTable;
    // Designated timestamp column of the table whose EXPIRE ROWS predicate was last looked up (set by
    // lookupExpiryPredicate), so the keep-filter rewrite can null-safely flip only timestamp comparisons.
    private CharSequence expiryTimestampColumnName;
    // For each table (by id) whose EXPIRE ROWS policy this parse read straight from the table metadata while a
    // SET/DROP EXPIRE was still in flight: the metadata version that read saw. The optimiser compares it
    // against the version the reader opens and rejects the compile if they differ, so a filter chosen from the
    // old policy is never paired with a reader on the new one. Empty unless a policy change is running at the
    // same time as this compile.
    private final IntLongHashMap pendingExpiryReadVersions = new IntLongHashMap();
    // Whether to apply the read-time row-expiry filter for the current parse. Set from the execution
    // context at parse() entry; the cleanup job disables it on its context so its survivor query is not
    // wrapped by the read filter (it uses its own authoritative keep-filter instead).
    private boolean rowExpiryReadFilterEnabled = true;
    private boolean pivotMode = false;
    private boolean subQueryMode = false;

    SqlParser(
            CairoEngine cairoEngine,
            CairoConfiguration configuration,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo
    ) {
        this.cairoEngine = cairoEngine;
        this.configuration = cairoEngine.getConfiguration();
        this.expressionNodePool = expressionNodePool;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.windowExpressionPool = windowExpressionPool;
        this.expressionTreeBuilder = new ExpressionTreeBuilder();
        this.createTableColumnModelPool = new ObjectPool<>(CreateTableColumnModel.FACTORY, configuration.getCreateTableColumnModelPoolCapacity());
        this.renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, configuration.getRenameTableModelPoolCapacity());
        this.withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, configuration.getWithClauseModelPoolCapacity());
        this.insertModelPool = new ObjectPool<>(InsertModel.FACTORY, configuration.getInsertModelPoolCapacity());
        this.compileViewModelPool = new ObjectPool<>(CompileViewModel.FACTORY, configuration.getCompileViewModelPoolCapacity());
        this.copyModelPool = new ObjectPool<>(ExportModel.FACTORY, configuration.getCopyPoolCapacity());
        this.explainModelPool = new ObjectPool<>(ExplainModel.FACTORY, configuration.getExplainPoolCapacity());
        this.pivotQueryColumnPool = new ObjectPool<>(PivotForColumn.FACTORY, configuration.getPivotColumnPoolCapacity());
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        this.viewLexers = new ObjectPool<>(this::createLexer, configuration.getViewLexerPoolCapacity());
        boolean tempCairoSqlLegacyOperatorPrecedence = configuration.getCairoSqlLegacyOperatorPrecedence();
        if (tempCairoSqlLegacyOperatorPrecedence) {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getLegacyRegistry(),
                    OperatorExpression.getRegistry(),
                    expressionNodePool,
                    this,
                    characterStore,
                    windowExpressionPool
            );
        } else {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getRegistry(),
                    null,
                    expressionNodePool,
                    this,
                    characterStore,
                    windowExpressionPool
            );
        }
        this.digit = 1;
        this.column = "column";
    }

    public static boolean isFullSampleByPeriod(ExpressionNode n) {
        return n != null && (n.type == ExpressionNode.CONSTANT || (n.type == ExpressionNode.LITERAL && isValidSampleByPeriodLetter(n.token)));
    }

    /**
     * Parses a DECIMAL[(precision[, scale])] type from the lexer.
     * The user may specify the precision and scale of the underlying DECIMAL type, if not provided, we use a default
     * precision of 18 and a scale of 3 (or 0 if precision &lt; 8) so that the underlying type will be a DECIMAL64.
     *
     * @return the concrete DECIMAL type with proper precision/scale set.
     */
    public static int parseDecimalColumnType(GenericLexer lexer) throws SqlException {
        int previousTokenPosition = lexer.lastTokenPosition();

        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) != '(') {
            lexer.unparseLast();
            return ColumnType.DECIMAL_DEFAULT_TYPE;
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) == ')') {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The precision is missing");
        }
        int precision = DecimalUtil.parsePrecision(lexer.lastTokenPosition(), tok, 0, tok.length());
        int scale = precision < 8 ? 0 : 3;

        tok = SqlUtil.fetchNext(lexer);

        // The user may provide a scale value
        if (tok != null && tok.charAt(0) == ',') {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) == ')') {
                throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The scale is missing");
            }
            scale = DecimalUtil.parseScale(lexer.lastTokenPosition(), tok, 0, tok.length());
            tok = SqlUtil.fetchNext(lexer);
        }

        if (tok == null || tok.charAt(0) != ')') {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. Missing ')'");
        }

        if (precision <= 0) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than zero");
        }
        if (precision > Decimals.MAX_PRECISION) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be less than ")
                    .put(Decimals.MAX_PRECISION);
        }
        if (scale < 0) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The scale (")
                    .put(scale)
                    .put(") must be greater than or equal to zero");
        }
        if (scale > precision) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than or equal to the scale (")
                    .put(scale)
                    .put(")");
        }

        return ColumnType.getDecimalType(precision, scale);
    }

    /**
     * Parses a GEOHASH(precision) type from the lexer.
     * The precision is specified as a number followed by 'b' (bits) or 'c' (chars),
     * e.g. GEOHASH(5c) or GEOHASH(30b).
     *
     * @return the concrete GEOHASH type with the specified precision.
     */
    public static int parseGeoHashColumnType(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) != '(') {
            throw SqlException.position(lexer.getPosition()).put("missing GEOHASH precision");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && tok.charAt(0) != ')') {
            int geoHashBits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, tok);
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) != ')') {
                if (tok != null) {
                    throw SqlException.position(lexer.lastTokenPosition())
                            .put("invalid GEOHASH type literal, expected ')'")
                            .put(" found='").put(tok.charAt(0)).put("'");
                }
                throw SqlException.position(lexer.getPosition())
                        .put("invalid GEOHASH type literal, expected ')'");
            }
            return ColumnType.getGeoHashTypeWithBits(geoHashBits);
        } else {
            throw SqlException.position(lexer.lastTokenPosition())
                    .put("missing GEOHASH precision");
        }
    }

    /**
     * Parses a value and time unit into a TTL value. If the returned value is positive, the time unit
     * is hours. If it's negative, the time unit is months (and the actual value is positive).
     */
    public static int parseTtlHoursOrMonths(GenericLexer lexer) throws SqlException {
        CharSequence tok;
        int valuePos = lexer.getPosition();
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.getPosition(), "missing argument, should be <number> <unit> or <number_with_unit>");
        }
        int tokLength = tok.length();
        int unit = -1;
        int unitPos = -1;
        char unitChar = tok.charAt(tokLength - 1);
        if (tokLength > 1 && Character.isLetter(unitChar)) {
            unit = PartitionBy.ttlUnitFromString(tok, tokLength - 1, tokLength);
            if (unit != -1) {
                unitPos = valuePos;
            } else {
                try {
                    Numbers.parseLong(tok, 0, tokLength - 1);
                } catch (NumericException e) {
                    throw SqlException.$(valuePos, "invalid argument, should be <number> <unit> or <number_with_unit>");
                }
                throw SqlException.$(valuePos + tokLength - 1, "invalid time unit, expecting 'H', 'D', 'W', 'M' or 'Y', but was '")
                        .put(unitChar).put('\'');
            }
        }
        // at this point, unit == -1 means the syntax wasn't of the "1H" form, it can still be of the "1 HOUR" form
        int ttlValue;
        try {
            long ttlLong = unit == -1 ? Numbers.parseLong(tok) : Numbers.parseLong(tok, 0, tokLength - 1);
            if (ttlLong > Integer.MAX_VALUE || ttlLong < 0) {
                throw SqlException.$(valuePos, "value out of range: ").put(ttlLong)
                        .put(". Max value: ").put(Integer.MAX_VALUE);
            }
            ttlValue = (int) ttlLong;
        } catch (NumericException e) {
            throw SqlException.$(valuePos, "invalid syntax, should be <number> <unit> but was ").put(tok);
        }
        if (unit == -1) {
            unitPos = lexer.getPosition();
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null) {
                throw SqlException.$(unitPos, "missing unit, 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)' expected");
            }
            unit = PartitionBy.ttlUnitFromString(tok, 0, tok.length());
        }
        if (unit == -1) {
            throw SqlException.$(unitPos, "invalid unit, expected 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)', but was '")
                    .put(tok).put('\'');
        }
        return CommonUtils.toHoursOrMonths(ttlValue, unit, valuePos);
    }

    /**
     * Rejects a line comment or an unterminated block comment inside captured EXPIRE ROWS text
     * ({@code [lo, hi)} of {@code content}). The captured text is stored verbatim and later embedded
     * into single-line generated SQL (the read filter, the cleanup survivor queries, SHOW CREATE
     * output), where a line comment swallows the closing tokens and fails every read of the view. A
     * terminated block comment embeds safely and stays legal, as do comment markers inside quoted
     * literals and identifiers.
     */
    private static void rejectCommentInExpiryClause(CharSequence content, int lo, int hi) throws SqlException {
        for (int i = lo; i < hi; i++) {
            final char c = content.charAt(i);
            if (c == '\'' || c == '"') {
                i++;
                while (i < hi && content.charAt(i) != c) {
                    i++;
                }
            } else if (c == '-' && i + 1 < hi && content.charAt(i + 1) == '-') {
                throw SqlException.$(i, "line comments are not supported in EXPIRE ROWS clauses");
            } else if (c == '/' && i + 1 < hi && content.charAt(i + 1) == '*') {
                // block comments nest (see SqlUtil.fetchNext), so track depth to find the real close
                int depth = 1;
                int j = i + 2;
                while (j < hi && depth > 0) {
                    final char cj = content.charAt(j);
                    if (cj == '/' && j + 1 < hi && content.charAt(j + 1) == '*') {
                        depth++;
                        j += 2;
                    } else if (cj == '*' && j + 1 < hi && content.charAt(j + 1) == '/') {
                        depth--;
                        j += 2;
                    } else {
                        j++;
                    }
                }
                if (depth > 0) {
                    throw SqlException.$(i, "unterminated block comment in EXPIRE ROWS clause");
                }
                i = j - 1;
            }
        }
    }

    private static long strideToMicros(int multiple, char unit, int position) throws SqlException {
        final long unitMicros = switch (unit) {
            case 's' -> 1_000_000L;
            case 'm' -> 60_000_000L;
            case 'h' -> 3_600_000_000L;
            case 'd' -> 86_400_000_000L;
            case 'w' -> 604_800_000_000L;
            default -> throw SqlException.$(position, "unsupported cleanup interval unit, expected s/m/h/d/w");
        };
        try {
            // int * long can overflow for absurd values (e.g. 999999999w); fail cleanly instead of
            // persisting a garbage (possibly negative) cleanup cadence.
            final long micros = Math.multiplyExact((long) multiple, unitMicros);
            if (micros <= 0) {
                throw SqlException.$(position, "cleanup interval must be positive");
            }
            return micros;
        } catch (ArithmeticException e) {
            throw SqlException.$(position, "cleanup interval is too large");
        }
    }

    /**
     * Parses the optional row-expiry clause of CREATE TABLE:
     * {@code EXPIRE ROWS WHEN <predicate> [CLEANUP EVERY <duration>]}.
     * <p>
     * The {@code EXPIRE} keyword has already been consumed by the caller. The predicate is captured
     * as raw SQL text (everything between WHEN and CLEANUP/end-of-statement, tracking parenthesis
     * depth) and stored on the builder unvalidated. Returns the next unconsumed token.
     */
    private CharSequence parseCreateTableExpireRows(
            GenericLexer lexer,
            CreateTableOperationBuilderImpl builder
    ) throws SqlException {
        final ExpireRowsClause clause = parseExpireRowsClause(lexer, true);
        builder.setExpiryPredicate(clause.predicate);
        builder.setExpiryCleanupIntervalMicros(clause.cleanupIntervalMicros);
        return clause.nextTok;
    }

    /**
     * Shared parser for the {@code ROWS WHEN <predicate> [CLEANUP EVERY <duration>]} body of an
     * EXPIRE clause (the {@code EXPIRE} keyword itself has already been consumed by the caller).
     * Used by both CREATE TABLE and ALTER TABLE SET EXPIRE.
     * <p>
     * The predicate is captured as raw SQL text: everything between WHEN and the next boundary,
     * tracking parenthesis depth so a boundary keyword inside parentheses doesn't terminate it.
     * Boundaries are CLEANUP (consumed here) and ';'/EOF. When {@code inCreateTable} is true, the
     * clauses that may follow EXPIRE in CREATE TABLE (WITH / IN VOLUME / DEDUP[LICATE]) are also
     * boundaries and the boundary token is returned to the caller in {@link ExpireRowsClause#nextTok}.
     * Cleanup interval defaults to 1 hour when omitted.
     */
    public ExpireRowsClause parseExpireRowsClause(GenericLexer lexer, boolean inCreateTable) throws SqlException {
        expectTok(lexer, "rows");
        CharSequence tok = tok(lexer, "'when' or 'keep'");
        final int predicateStart;
        final String predicateSql;
        boolean foundCleanup = false;

        if (isKeepKeyword(tok)) {
            // Relative retention modes (passthrough-mat-view-only, validated at create), stored encoded in
            // the predicate slot and rewritten by the read filter:
            //   KEEP LATEST [ON <ts>] PARTITION BY <cols>      -> latest row per key (LATEST ON)
            //   KEEP [<N>] HIGHEST|LOWEST <col> [PARTITION BY <cols>] -> group max/min (or top-N) by a column
            predicateStart = lexer.lastTokenPosition();
            tok = tok(lexer, "'latest', 'highest', 'lowest' or a row count");
            if (isLatestKeyword(tok)) {
                tok = tok(lexer, "'on' or 'partition'");
                String latestTs = "";
                if (isOnKeyword(tok)) {
                    // Optional "ON <ts>": captured and validated == the designated timestamp at create/alter
                    // (a table-input LATEST ON requires it). Stored so SHOW CREATE can round-trip it.
                    latestTs = Chars.toString(unquote(tok(lexer, "timestamp column name")));
                    tok = tok(lexer, "'partition'");
                }
                if (!isPartitionKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
                expectTok(lexer, "by");
                final ColumnListCapture cap = captureKeepColumnList(lexer, inCreateTable);
                if (cap.csv.isEmpty()) {
                    throw SqlException.$(cap.startPos, "EXPIRE ROWS KEEP LATEST requires a PARTITION BY column list");
                }
                predicateSql = RowExpiryUtil.encodeKeepLatest(latestTs, cap.csv);
                foundCleanup = cap.foundCleanup;
                tok = cap.nextTok;
            } else {
                // KEEP [<N>] HIGHEST|LOWEST <col> [PARTITION BY <cols>]. Stored structurally and desugared to
                // a window predicate at use (the designated timestamp is needed only for the top-N tiebreak).
                int n = 0;
                if (!isHighestKeyword(tok) && !isLowestKeyword(tok)) {
                    try {
                        n = Numbers.parseInt(tok);
                    } catch (NumericException e) {
                        throw SqlException.$(lexer.lastTokenPosition(), "'latest', 'highest', 'lowest' or a row count expected");
                    }
                    if (n < 1) {
                        throw SqlException.$(lexer.lastTokenPosition(), "EXPIRE ROWS KEEP <N> requires a positive row count");
                    }
                    tok = tok(lexer, "'highest' or 'lowest'");
                }
                final boolean highest = isHighestKeyword(tok);
                if (!highest && !isLowestKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'highest' or 'lowest' expected");
                }
                final String col = Chars.toString(unquote(tok(lexer, "column name")));
                tok = optTok(lexer);
                String keysCsv = "";
                if (tok != null && isPartitionKeyword(tok)) {
                    expectTok(lexer, "by");
                    final ColumnListCapture cap = captureKeepColumnList(lexer, inCreateTable);
                    if (cap.csv.isEmpty()) {
                        throw SqlException.$(cap.startPos, "EXPIRE ROWS KEEP ... PARTITION BY requires a column list");
                    }
                    keysCsv = cap.csv;
                    foundCleanup = cap.foundCleanup;
                    tok = cap.nextTok;
                } else if (tok != null && isCleanupKeyword(tok)) {
                    foundCleanup = true;
                }
                predicateSql = RowExpiryUtil.encodeKeepBy(n, highest, col, keysCsv);
            }
        } else {
            lexer.unparseLast();
            expectTok(lexer, "when");

            predicateStart = lexer.getPosition();
            int predicateEnd;
            int depth = 0;
            while (true) {
                tok = optTok(lexer);
                if (tok == null || Chars.equals(tok, ';')) {
                    predicateEnd = tok == null ? lexer.getPosition() : lexer.lastTokenPosition();
                    break;
                }
                if (depth == 0) {
                    if (isCleanupKeyword(tok)) {
                        // 'CLEANUP' is ambiguous: 'CLEANUP EVERY <dur>' ends the predicate, but a bare
                        // column reference named "cleanup" (e.g. WHEN cleanup > 5) is predicate content.
                        // Only treat CLEANUP as the boundary when it is followed by EVERY; otherwise keep
                        // scanning (mirrors the IN -> VOLUME lookahead below).
                        final int cleanupPos = lexer.lastTokenPosition();
                        final CharSequence afterCleanup = optTok(lexer);
                        if (afterCleanup != null && isEveryKeyword(afterCleanup)) {
                            lexer.unparseLast(); // hand EVERY back so we parse CLEANUP EVERY below
                            predicateEnd = cleanupPos;
                            foundCleanup = true;
                            break;
                        }
                        if (afterCleanup != null) {
                            lexer.unparseLast();
                        }
                    } else if (inCreateTable && (isWithKeyword(tok) || isDedupKeyword(tok) || isDeduplicateKeyword(tok))) {
                        predicateEnd = lexer.lastTokenPosition();
                        break;
                    } else if (inCreateTable && isInKeyword(tok)) {
                        // 'IN' is ambiguous: 'IN VOLUME' ends the predicate, but '<col> IN (...)' is part of
                        // it. Only treat IN as the boundary when it is followed by VOLUME; otherwise it is
                        // predicate content and we keep scanning (the following '(' bumps the paren depth).
                        final int inPos = lexer.lastTokenPosition();
                        final CharSequence afterIn = optTok(lexer);
                        if (afterIn != null && isVolumeKeyword(afterIn)) {
                            lexer.unparseLast(); // hand VOLUME back so the caller parses IN VOLUME
                            predicateEnd = inPos;
                            break;
                        }
                        if (afterIn != null) {
                            lexer.unparseLast();
                        }
                    }
                }
                if (Chars.equals(tok, '(')) {
                    depth++;
                } else if (Chars.equals(tok, ')')) {
                    depth--;
                    if (depth < 0) {
                        // An unexpected ')' at depth 0 closes a paren that was never opened: the predicate is
                        // malformed. Report at the offending ')' rather than swallowing the rest of the clause.
                        throw SqlException.$(lexer.lastTokenPosition(), "unbalanced parentheses in EXPIRE ROWS predicate");
                    }
                }
            }
            if (depth != 0) {
                // Reached the clause boundary / EOF with open parens still pending (e.g. CLEANUP/EOF got
                // swallowed into the predicate text). Report at the predicate start so the user sees where
                // the unbalanced expression began.
                throw SqlException.$(predicateStart, "unbalanced parentheses in EXPIRE ROWS predicate");
            }
            rejectCommentInExpiryClause(lexer.getContent(), predicateStart, predicateEnd);

            final String rawPredicate = Chars.toString(lexer.getContent(), predicateStart, predicateEnd).trim();
            if (rawPredicate.isEmpty()) {
                throw SqlException.$(predicateStart, "EXPIRE ROWS WHEN predicate is empty");
            }
            // A predicate referencing a window function (e.g. v < max(v) OVER (...)) is illegal in a plain
            // WHERE, so flag it for the projection-CASE read filter / cleanup instead.
            predicateSql = predicateHasWindowFunction(rawPredicate) ? RowExpiryUtil.encodeWindow(rawPredicate) : rawPredicate;
        }

        final long cleanupIntervalMicros;
        if (foundCleanup) {
            expectTok(lexer, "every");
            tok = tok(lexer, "cleanup interval value (e.g., 1h, 30m, 24h)");
            // Strict <digits><unit> parse, shared with SAMPLE BY intervals: a lenient parse of e.g.
            // "30ms" would read the trailing 's' as the unit and silently store a cadence the user did
            // not write. strideToMicros then restricts the unit to s/m/h/d/w and checks for overflow.
            final int unitIndex = CommonUtils.findPositiveIntervalEndIndex(tok, lexer.lastTokenPosition(), "cleanup");
            final int multiple = (int) CommonUtils.parsePositiveInterval(tok, unitIndex, lexer.lastTokenPosition(), "cleanup", Numbers.INT_NULL, ' ');
            cleanupIntervalMicros = strideToMicros(multiple, tok.charAt(unitIndex), lexer.lastTokenPosition());
            // Fetch the next clause keyword (WITH / IN / DEDUP / ';' / EOF) for the caller.
            tok = optTok(lexer);
        } else {
            cleanupIntervalMicros = RowExpiryUtil.DEFAULT_CLEANUP_INTERVAL_MICROS;
            // tok already holds the boundary token (WITH / IN / DEDUP / ';' / null) — hand it back as-is.
        }
        return new ExpireRowsClause(predicateSql, predicateStart, cleanupIntervalMicros, tok);
    }

    /**
     * Captures the raw column-list text after {@code PARTITION BY} in a KEEP clause, up to the next clause
     * boundary (CLEANUP / ';' / EOF, plus WITH / IN VOLUME / DEDUP in CREATE TABLE). Shared by KEEP LATEST
     * and KEEP HIGHEST/LOWEST.
     */
    private ColumnListCapture captureKeepColumnList(GenericLexer lexer, boolean inCreateTable) throws SqlException {
        final int startPos = lexer.getPosition();
        int end;
        boolean foundCleanup = false;
        CharSequence tok;
        while (true) {
            tok = optTok(lexer);
            if (tok == null || Chars.equals(tok, ';')) {
                end = tok == null ? lexer.getPosition() : lexer.lastTokenPosition();
                break;
            }
            if (isCleanupKeyword(tok)) {
                end = lexer.lastTokenPosition();
                foundCleanup = true;
                break;
            }
            // A column list cannot contain WITH/IN/DEDUP, so each unambiguously ends it (e.g. IN VOLUME).
            if (inCreateTable && (isWithKeyword(tok) || isInKeyword(tok) || isDedupKeyword(tok) || isDeduplicateKeyword(tok))) {
                end = lexer.lastTokenPosition();
                break;
            }
        }
        rejectCommentInExpiryClause(lexer.getContent(), startPos, end);
        final ColumnListCapture capture = new ColumnListCapture();
        capture.csv = Chars.toString(lexer.getContent(), startPos, end).trim();
        capture.foundCleanup = foundCleanup;
        capture.nextTok = tok;
        capture.startPos = startPos;
        return capture;
    }

    /**
     * Whether {@code predicate} references a window function, detected by an {@code OVER (} token sequence.
     * Window functions are illegal in a plain WHERE, so such a predicate is routed to the projection-CASE
     * read filter. Re-lexes the predicate text only (no binding/metadata needed).
     */
    private boolean predicateHasWindowFunction(String predicate) throws SqlException {
        final GenericLexer probe = viewLexers.next();
        probe.of(predicate);
        boolean prevOver = false;
        CharSequence tok;
        while ((tok = SqlUtil.fetchNext(probe)) != null) {
            if (prevOver && Chars.equals(tok, '(')) {
                return true;
            }
            prevOver = isOverKeyword(tok);
        }
        return false;
    }

    public static ExpressionNode recursiveReplace(ExpressionNode node, ReplacingVisitor visitor) throws SqlException {
        if (node == null) {
            return null;
        }

        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = recursiveReplace(node.rhs, visitor);
                break;
            case 2:
                node.lhs = recursiveReplace(node.lhs, visitor);
                node.rhs = recursiveReplace(node.rhs, visitor);
                break;
            default:
                for (int i = 0; i < node.paramCount; i++) {
                    ExpressionNode arg = node.args.get(i);
                    node.args.set(i, recursiveReplace(arg, visitor));
                }
                break;
        }

        // Traverse window context expressions (partition by, order by, frame bounds)
        if (node.windowExpression != null) {
            WindowExpression wc = node.windowExpression;
            ObjList<ExpressionNode> partitionBy = wc.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                partitionBy.set(i, recursiveReplace(partitionBy.get(i), visitor));
            }
            ObjList<ExpressionNode> orderBy = wc.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                orderBy.set(i, recursiveReplace(orderBy.get(i), visitor));
            }
            ExpressionNode loExpr = wc.getRowsLoExpr();
            if (loExpr != null) {
                wc.setRowsLoExpr(recursiveReplace(loExpr, visitor), wc.getRowsLoExprPos());
            }
            ExpressionNode hiExpr = wc.getRowsHiExpr();
            if (hiExpr != null) {
                wc.setRowsHiExpr(recursiveReplace(hiExpr, visitor), wc.getRowsHiExprPos());
            }
        }

        return visitor.visit(node);
    }

    public static void validateMatViewEveryUnit(char unit, int pos) throws SqlException {
        if (unit != 'M' && unit != 'y' && unit != 'w' && unit != 'd' && unit != 'h' && unit != 'm') {
            throw SqlException.position(pos).put("unsupported interval unit: ").put(unit)
                    .put(", supported units are 'm', 'h', 'd', 'w', 'y', 'M'");
        }
    }

    public static void validateMatViewPeriodDelay(int length, char lengthUnit, int delay, char delayUnit, int pos) throws SqlException {
        if (delay < 0) {
            throw SqlException.position(pos).put("delay cannot be negative");
        }

        final int lengthSeconds = matViewPeriodLengthSeconds(length, lengthUnit, pos);
        final int delaySeconds = matViewPeriodDelaySeconds(delay, delayUnit, pos);
        if (delaySeconds >= lengthSeconds) {
            throw SqlException.position(pos).put("delay cannot be equal to or greater than length");
        }
    }

    private static SqlException err(GenericLexer lexer, @Nullable CharSequence tok, @NotNull String msg) {
        return SqlException.parserErr(lexer.lastTokenPosition(), tok, msg);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token, @NotNull CharSequence extraMessage) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token, extraMessage);
    }

    private static boolean isJsonUnnestSupportedType(int type) {
        int tag = ColumnType.tagOf(type);
        return tag == ColumnType.BOOLEAN
                || tag == ColumnType.SHORT
                || tag == ColumnType.INT
                || tag == ColumnType.LONG
                || tag == ColumnType.DATE
                || tag == ColumnType.DOUBLE
                || tag == ColumnType.STRING
                || tag == ColumnType.VARCHAR
                || tag == ColumnType.TIMESTAMP;
    }

    private static boolean isLexerWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    private static boolean isValidSampleByPeriodLetter(CharSequence token) {
        if (token.length() != 1) return false;
        return switch (token.charAt(0)) {
            // nanos
            // micros
            // millis
            // seconds
            // minutes
            // hours
            // days
            // weeks
            // months
            // years
            case 'n', 'U', 'T', 's', 'm', 'h', 'd', 'w', 'M', 'y' -> true;
            default -> false;
        };
    }

    private static boolean isZeroOffsetToken(CharSequence token) {
        return Chars.equals(token, ZERO_OFFSET.token)
                || Chars.equals(token, "'+00:00'")
                || Chars.equals(token, "'-00:00'");
    }

    private static CreateLiveViewOperationBuilder parseCreateLiveViewExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateLiveViewOperationBuilderImpl builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateLiveViewExt(lexer, executionContext, builder, nextToken);
    }

    private static CreateMatViewOperationBuilder parseCreateMatViewExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateMatViewOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateMatViewExt(lexer, executionContext, builder, nextToken);
    }

    private static CreateTableOperationBuilder parseCreateTableExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateTableOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateTableExt(lexer, executionContext, builder, nextToken);
    }

    private static CreateViewOperationBuilder parseCreateViewExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateViewOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateViewExt(lexer, executionContext, builder, nextToken);
    }

    private static void validateShowTransactions(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && isIsolationKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && isLevelKeyword(tok)) {
                return;
            }
            throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'level'");
        }
        throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'isolation'");
    }

    private void addConcatArgs(ObjList<ExpressionNode> args, ExpressionNode leaf) {
        if (leaf.type != ExpressionNode.FUNCTION || !isConcatKeyword(leaf.token)) {
            args.add(leaf);
            return;
        }

        // Nested CONCAT. Expand it from CONCAT(x, CONCAT(y, z)) into CONCAT(x, y, z).
        if (leaf.args.size() > 0) {
            args.addAll(leaf.args);
        } else {
            args.add(leaf.rhs);
            args.add(leaf.lhs);
        }
    }

    private void assertNotDot(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.indexOfLastUnquoted(tok, '.') != -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "'.' is not allowed here");
        }
    }

    private void clearRecordedViews() {
        recordedViews.clear();
        viewsBeingCompiled.clear();
        expiringTablesBeingExpanded.clear();
    }

    private void compileViewQuery(IQueryModel model, TableToken viewToken, int viewPosition) throws SqlException {
        final CharSequence viewName = viewToken.getTableName();

        // Detect cycle: if we're already compiling this view, it's a circular reference
        if (viewsBeingCompiled.contains(viewName)) {
            throw SqlException.$(viewPosition, "circular view reference detected: ").put(viewName);
        }

        // Check if we already have this view definition (ensures consistent snapshot during compilation)
        ViewDefinition viewDefinition = recordedViews.get(viewName);
        if (viewDefinition == null) {
            viewDefinition = cairoEngine.getViewGraph().getViewDefinition(viewToken);
            if (viewDefinition == null) {
                throw SqlException.viewDoesNotExist(viewPosition, viewName);
            }
            recordedViews.put(viewName, viewDefinition);
        }

        // Track that we're compiling this view
        viewsBeingCompiled.add(viewName);
        try {
            final IQueryModel viewModel = compileViewQuery(viewDefinition, viewPosition, model.getDecls());
            viewModel.copyDeclsFrom(model, false);
            model.setNestedModel(viewModel);
            model.setNestedModelIsSubQuery(true);
            if (model.getAlias() == null) {
                model.setAlias(literal(viewName, viewPosition));
            }
        } finally {
            viewsBeingCompiled.remove(viewName);
        }
    }

    private IQueryModel compileViewQuery(
            ViewDefinition viewDefinition,
            int viewPosition,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final GenericLexer viewLexer = viewLexers.next();
        viewLexer.of(viewDefinition.getViewSql());

        final IQueryModel viewModel = parseAsSubQuery(viewLexer, null, false, viewSqlParserCallback, decls, true);
        final ExpressionNode viewExpr = literal(viewDefinition.getViewToken().getTableName(), viewPosition);
        viewModel.setOriginatingViewNameExpr(viewExpr);
        viewModel.setViewNameExpr(viewExpr);
        return viewModel;
    }

    /**
     * Rewrites a table reference that carries an EXPIRE ROWS policy into a nested
     * {@code SELECT * FROM "t" WHERE <keep-filter>} sub-query, so expired rows become invisible to
     * all reads. Mirrors {@link #compileViewQuery(IQueryModel, TableToken, int)} and the
     * expiring-view draft's expandExpiringView. The caller MUST have already added {@code tableName}
     * to {@link #expiringTablesBeingExpanded} so the synthetic inner {@code FROM "t"} resolves as a
     * plain table rather than re-expanding (infinite recursion).
     */
    private void expandExpiringTable(
            IQueryModel model,
            CharSequence tableName,
            String predicate,
            CharSequence designatedTimestampColumn,
            int position,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        // Captured before any nested parse: sub-query parsing recurses back into table resolution, which
        // overwrites the lookup fields when it meets another policied table.
        final CairoTable policyTable = expiryPolicyTable;
        if (RowExpiryUtil.isKeepLatest(predicate)) {
            // Relative "KEEP LATEST" retention (passthrough mat views): hide all but the latest row per key
            // by rewriting the reference into "SELECT * FROM "t" LATEST ON "<ts>" PARTITION BY <cols>". The
            // PARTITION BY list is stored as raw text (quoting preserved); the timestamp is always the
            // table's designated timestamp. LATEST ON cannot share a query level with WHERE, so isolating it
            // in this inner sub-query is exactly right: any outer predicate filters the already-latest rows.
            final CharSequence keys = RowExpiryUtil.keepLatestKeys(predicate);
            final String latestSql = "SELECT * FROM " + RowExpiryUtil.quoteIdentifier(tableName) + " LATEST ON "
                    + RowExpiryUtil.quoteIdentifier(designatedTimestampColumn) + " PARTITION BY " + keys;
            final GenericLexer latestLexer = viewLexers.next();
            latestLexer.of(latestSql);
            final IQueryModel latestSubQuery = parseAsSubQuery(latestLexer, null, false, sqlParserCallback, model.getDecls(), true);
            model.setNestedModel(latestSubQuery);
            model.setNestedModelIsSubQuery(true);
            if (model.getAlias() == null) {
                model.setAlias(literal(tableName, position));
            }
            return;
        }

        if (RowExpiryUtil.isKeepBy(predicate) || RowExpiryUtil.isWindow(predicate)) {
            // Window-based retention (keep-max/min, top-N, or an explicit window WHEN). The keep-filter
            // references a window function, illegal in a plain WHERE, so compute it as a boolean column in an
            // inner projection over the WHOLE view and filter on it in the outer query. Base columns are
            // enumerated so the synthetic keep column never leaks through the caller's SELECT *.
            //
            // The inner "SELECT *, CASE ..." projection drops the designated-timestamp property (the extra
            // synthetic column makes it a general projection, not a passthrough), so re-assert it with a
            // timestamp("<ts>") clause on the sub-query -- otherwise a downstream timestamp-requiring operator
            // over a window-policied view (ASOF/LT/SPLICE JOIN) fails to compile with "TIMESTAMP column is
            // required but not provided". The scalar and KEEP LATEST rewrites keep the designation naturally
            // (SELECT * / LATEST ON), so only this branch needs it.
            final String windowPredicate = RowExpiryUtil.windowPredicate(predicate, designatedTimestampColumn);
            // The quoted column CSV is a pure function of the CairoTable's column list; memoize it on the
            // instance so repeated compiles skip the rebuild. The cache-miss path (policyTable == null, the
            // brief pre-hydration window) still derives it from authoritative metadata per compile.
            String columnsCsv = policyTable != null ? policyTable.getExpiryQuotedColumnsCsv() : null;
            if (columnsCsv == null) {
                columnsCsv = policyTable != null ? buildQuotedColumnList(policyTable) : buildQuotedColumnList(tableName);
                if (policyTable != null) {
                    policyTable.setExpiryQuotedColumnsCsv(columnsCsv);
                }
            }
            final String windowSql = "SELECT " + columnsCsv + " FROM (SELECT *, CASE WHEN ("
                    + windowPredicate + ") THEN false ELSE true END " + RowExpiryUtil.KEEP_COLUMN + " FROM "
                    + RowExpiryUtil.quoteIdentifier(tableName) + ") timestamp("
                    + RowExpiryUtil.quoteIdentifier(designatedTimestampColumn) + ") WHERE " + RowExpiryUtil.KEEP_COLUMN;
            final GenericLexer windowLexer = viewLexers.next();
            windowLexer.of(windowSql);
            final IQueryModel windowSubQuery = parseAsSubQuery(windowLexer, null, false, sqlParserCallback, model.getDecls(), true);
            markExpiryWindowBarrier(windowSubQuery);
            model.setExpiryWindowBarrier(true);
            model.getExpiryWindowPartitionBy().addAll(windowSubQuery.getExpiryWindowPartitionBy());
            model.setNestedModel(windowSubQuery);
            model.setNestedModelIsSubQuery(true);
            if (model.getAlias() == null) {
                model.setAlias(literal(tableName, position));
            }
            return;
        }

        // Quote the table name so names that need quoting (or look like keywords) parse correctly. The
        // reference becomes "SELECT * FROM "t" WHERE <keep-filter>" so only rows that have NOT expired are
        // visible. The keep-filter is parsed inline (so the sub-query model processes it like any WHERE);
        // see keepFilterWhereText for the NULL/three-valued and partition-pruning details.
        // The flip verdict is a pure function of (predicate, designated timestamp) for a DECLARE-free
        // compile, so the probe parse runs once per CairoTable instance and every later compile reads the
        // memo. A DECLARE-carrying compile neither trusts nor populates it: column names may legally start
        // with '@', so a declared name can capture an unquoted such column reference in the predicate
        // during the probe parse and yield a query-specific verdict.
        final boolean flip;
        final LowerCaseCharSequenceObjHashMap<ExpressionNode> decls = model.getDecls();
        final boolean isMemoUsable = policyTable != null && (decls == null || decls.size() == 0);
        final int memoizedFlip = isMemoUsable ? policyTable.getExpiryFlipEligibility() : CairoTable.EXPIRY_FLIP_UNKNOWN;
        if (memoizedFlip != CairoTable.EXPIRY_FLIP_UNKNOWN) {
            flip = memoizedFlip == CairoTable.EXPIRY_FLIP_YES;
        } else {
            flip = isTimestampFlippablePredicate(predicate, designatedTimestampColumn, sqlParserCallback, decls);
            if (isMemoUsable) {
                policyTable.setExpiryFlipEligibility(flip ? CairoTable.EXPIRY_FLIP_YES : CairoTable.EXPIRY_FLIP_NO);
            }
        }
        final String syntheticSql = "SELECT * FROM " + RowExpiryUtil.quoteIdentifier(tableName) + " WHERE "
                + keepFilterWhereText(predicate, flip);

        final GenericLexer subLexer = viewLexers.next();
        subLexer.of(syntheticSql);

        final IQueryModel subQuery = parseAsSubQuery(subLexer, null, false, sqlParserCallback, model.getDecls(), true);
        if (flip) {
            subQuery.setWhereClause(simplifyKeepFilter(subQuery.getWhereClause(), designatedTimestampColumn));
        }
        model.setNestedModel(subQuery);
        model.setNestedModelIsSubQuery(true);
        if (model.getAlias() == null) {
            model.setAlias(literal(tableName, position));
        }
    }

    private void markExpiryWindowBarrier(IQueryModel model) {
        tempExprNodes.clear();
        collectExpiryWindowExpressions(model);
        model.setExpiryWindowBarrier(true);
        if (tempExprNodes.size() == 0) {
            return;
        }

        final ObjList<ExpressionNode> semanticKeys = model.getExpiryWindowPartitionBy();
        final ObjList<ExpressionNode> firstKeys = tempExprNodes.getQuick(0).windowExpression.getPartitionBy();
        semanticKeys.addAll(firstKeys);
        for (int i = 1, n = tempExprNodes.size(); i < n && semanticKeys.size() > 0; i++) {
            final ObjList<ExpressionNode> keys = tempExprNodes.getQuick(i).windowExpression.getPartitionBy();
            if (!sameSemanticKeys(semanticKeys, keys)) {
                // A raw policy may contain windows over different partitions. No predicate can cross that
                // barrier unless it preserves every window, so leave the semantic key set empty.
                semanticKeys.clear();
            }
        }
    }

    private void collectExpiryWindowExpressions(IQueryModel model) {
        if (model == null) {
            return;
        }
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            collectExpiryWindowExpressions(columns.getQuick(i).getAst());
        }
        collectExpiryWindowExpressions(model.getNestedModel());
        final ObjList<IQueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            collectExpiryWindowExpressions(joinModels.getQuick(i));
        }
        collectExpiryWindowExpressions(model.getUnionModel());
    }

    private void collectExpiryWindowExpressions(ExpressionNode node) {
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.windowExpression != null) {
                    tempExprNodes.add(node);
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
    }

    private static boolean sameSemanticKeys(ObjList<ExpressionNode> a, ObjList<ExpressionNode> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0, n = a.size(); i < n; i++) {
            if (!ExpressionNode.compareNodesExact(a.getQuick(i), b.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds the quoted, comma-separated column list of the given cached table, for the window
     * read-filter's outer projection (so the synthetic {@link RowExpiryUtil#KEEP_COLUMN} is not exposed
     * through SELECT *).
     */
    private static String buildQuotedColumnList(CairoTable table) {
        final StringSink sink = new StringSink();
        final ObjList<CharSequence> names = table.getColumnNames();
        for (int i = 0, n = names.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(RowExpiryUtil.quoteIdentifier(names.getQuick(i)));
        }
        return sink.toString();
    }

    /**
     * Name-based variant of {@link #buildQuotedColumnList(CairoTable)}: reads from the in-memory metadata
     * cache, falling back to the authoritative table metadata on a cache miss -- exactly as
     * {@link #lookupExpiryPredicate} does. The two MUST agree: the read path reaches this only after
     * {@code lookupExpiryPredicate} returned a (window/keep-by) predicate, which itself uses the fallback,
     * so during the brief startup window before the cache hydrates the predicate is non-null while the
     * cache has no entry; without the same fallback here the column list would be empty and the rewrite
     * would emit {@code SELECT  FROM (...)} and fail every read of the view until hydration caught up.
     */
    private String buildQuotedColumnList(CharSequence tableName) {
        final StringSink sink = new StringSink();
        final TableToken tt = cairoEngine.getTableTokenIfExists(tableName);
        if (tt == null) {
            return sink.toString();
        }
        try (MetadataCacheReader metadataRO = cairoEngine.getMetadataCache().readLock()) {
            final CairoTable table = metadataRO.getTable(tt);
            if (table != null) {
                return buildQuotedColumnList(table);
            }
        }
        // Cache miss (brief startup window before MetadataCache hydration): fall back to the authoritative
        // table metadata so the column list matches the predicate that lookupExpiryPredicate already resolved.
        try (TableMetadata metadata = cairoEngine.getTableMetadata(tt)) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    sink.putAscii(',');
                }
                sink.put(RowExpiryUtil.quoteIdentifier(metadata.getColumnName(i)));
            }
        } catch (CairoException ignore) {
            // Table concurrently dropped/renamed: getTableMetadata throws on open (before the loop), so the
            // sink is empty; return that empty list and the caller's read fails closed rather than exposing
            // rows (same posture as lookupExpiryPredicate treating this as "no policy").
        }
        return sink.toString();
    }

    /**
     * Returns the inverse of an ordering-comparison operator ({@code <}->{@code >=}, {@code <=}->{@code >},
     * {@code >}->{@code <=}, {@code >=}->{@code <}), so {@code NOT(a <op> b)} can be rewritten as the bare
     * {@code a <inverse> b} that {@link WhereClauseParser} can prune partitions on. Returns null for any
     * other operator (the caller then keeps the {@code NOT(...)} wrap, which is always correct).
     */
    private static CharSequence invertOrderingOperator(CharSequence op) {
        if (Chars.equals(op, '<')) {
            return ">=";
        }
        if (Chars.equals(op, "<=")) {
            return ">";
        }
        if (Chars.equals(op, '>')) {
            return "<=";
        }
        if (Chars.equals(op, ">=")) {
            return "<";
        }
        return null;
    }

    /**
     * Returns the keep-filter WHERE-clause text for an EXPIRE ROWS predicate — the rows that have NOT
     * expired. A row expires only when the predicate is TRUE, so the keep-filter must keep rows for which
     * the predicate is FALSE or NULL.
     * <ul>
     *     <li>{@code flip} (a designated-timestamp ordering comparison, see
     *     {@link #isTimestampFlippablePredicate}): {@code NOT (predicate)}, which the caller then rewrites
     *     in-place to the flipped bare comparison (e.g. {@code ts >= now()}) via {@link #simplifyKeepFilter}
     *     so {@link WhereClauseParser} can extract a timestamp interval and prune partitions. Safe because
     *     the timestamp is never NULL.</li>
     *     <li>otherwise: {@code CASE WHEN (predicate) THEN false ELSE true END}, which keeps FALSE and NULL
     *     rows for EVERY predicate shape. A plain {@code NOT(predicate)} is unsafe here:
     *     {@code SqlOptimiser.optimiseBooleanNot} rewrites {@code NOT(a < b)} into the inverted bare
     *     comparison {@code a >= b}, and a comparison and its inversion BOTH evaluate to false on a
     *     NULL/NaN operand, so the rewritten filter drops rows the policy must keep (e.g.
     *     {@code NOT(v < 2.0)} compiles to {@code v >= 2.0}, which hides NULL rows).
     *     {@code (predicate) IS NOT TRUE} is likewise unreliable for composite booleans such as
     *     {@code IN}. The CASE form is not JIT-serializable, so reads of a value-policied view run the
     *     interpreted filter; that standing cost is the price of NULL-correctness.</li>
     * </ul>
     */
    private static String keepFilterWhereText(String predicate, boolean flip) {
        return flip
                ? "NOT (" + predicate + ")"
                : "CASE WHEN (" + predicate + ") THEN false ELSE true END";
    }

    /**
     * Whether the predicate is a designated-timestamp ordering comparison whose other operand references
     * no column — i.e. the {@code ts < now()} shape, for which {@code NOT(predicate)} can be safely flipped
     * to a bare comparison for partition pruning. Parses the predicate purely to inspect it (the node is
     * discarded). Returns false (keep the always-correct CASE form) for everything else.
     */
    private boolean isTimestampFlippablePredicate(
            String predicate,
            CharSequence designatedTimestampColumn,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        if (designatedTimestampColumn == null) {
            return false;
        }
        final GenericLexer probeLexer = viewLexers.next();
        probeLexer.of(predicate);
        final ExpressionNode pred = expr(probeLexer, (IQueryModel) null, sqlParserCallback, decls);
        return pred != null && pred.type == ExpressionNode.OPERATION && pred.paramCount == 2
                && invertOrderingOperator(pred.token) != null
                && isNullSafeOrderingFlip(pred.lhs, pred.rhs, designatedTimestampColumn);
    }

    /**
     * Rewrites a {@code NOT(<ordering comparison>)} keep-filter into the equivalent flipped comparison
     * (e.g. {@code NOT(ts < now())} -> {@code ts >= now()}) so {@link WhereClauseParser} can extract a
     * timestamp interval and prune partitions. Only applied when the caller has already established (via
     * {@link #isTimestampFlippablePredicate}) that the comparison is on the never-NULL designated timestamp.
     */
    private static ExpressionNode simplifyKeepFilter(ExpressionNode keepFilter, CharSequence designatedTimestampColumn) {
        if (keepFilter != null && keepFilter.paramCount == 1 && keepFilter.rhs != null
                && Chars.equalsIgnoreCase(keepFilter.token, "not")) {
            final ExpressionNode inner = keepFilter.rhs;
            final CharSequence inverted = invertOrderingOperator(inner.token);
            if (inverted != null && inner.type == ExpressionNode.OPERATION && inner.paramCount == 2
                    && isNullSafeOrderingFlip(inner.lhs, inner.rhs, designatedTimestampColumn)) {
                inner.token = inverted;
                return inner;
            }
        }
        return keepFilter;
    }

    /**
     * Whether {@code NOT(a <op> b)} can be safely rewritten to the flipped bare comparison {@code a <inv> b}.
     * QuestDB comparisons are two-valued: a NULL operand makes BOTH {@code a < b} and {@code a >= b} false, so
     * {@code NOT(a < b)} (which is true when an operand is NULL) is NOT equivalent to {@code a >= b} (false)
     * unless both operands are guaranteed non-null. The flip is therefore allowed only when one operand is the
     * designated timestamp column (never NULL) and the other is <i>provably</i> non-NULL per
     * {@link #isOperandProvablyNonNull} (a non-null literal, the now()/systimestamp()/sysdate() clock, or
     * null-preserving timestamp arithmetic over non-null operands) — exactly the {@code ts < now()} shape the
     * partition-pruning optimisation targets. Every other shape (including a column-free but possibly-NULL
     * constant like {@code cast(null as timestamp)}) keeps the {@code NOT(...)}/CASE wrap, which is always
     * correct (it just does not prune). Without this guard, a policy like {@code EXPIRE ROWS WHEN v < 2.0} on
     * a nullable column would hide (and the cleanup job would delete) rows whose {@code v} is NULL even though
     * they never expired.
     */
    private static boolean isNullSafeOrderingFlip(ExpressionNode a, ExpressionNode b, CharSequence designatedTimestampColumn) {
        if (a == null || b == null || designatedTimestampColumn == null) {
            return false;
        }
        return (isDesignatedTimestamp(a, designatedTimestampColumn) && isOperandProvablyNonNull(b))
                || (isDesignatedTimestamp(b, designatedTimestampColumn) && isOperandProvablyNonNull(a));
    }

    private static boolean isDesignatedTimestamp(ExpressionNode node, CharSequence designatedTimestampColumn) {
        // Case-sensitive exact match against the actual column name: a non-match merely skips the flip
        // (keeps the always-correct NOT(...)), so being strict here can only cost a pruning opportunity.
        return node.type == ExpressionNode.LITERAL && Chars.equals(node.token, designatedTimestampColumn);
    }

    /**
     * Conservative "this expression can never evaluate to NULL" test, gating the null-unsafe timestamp flip
     * (see {@link #isNullSafeOrderingFlip}). Only an allowlist is treated as provably non-null: a non-null
     * constant literal; the nullary clock functions {@code now()/systimestamp()/sysdate()}; and the
     * null-preserving timestamp functions / arithmetic operators (e.g. {@code dateadd}, {@code date_trunc},
     * {@code timestamp_floor}, {@code +}, {@code -}, {@code *}) applied to provably-non-null operands.
     * Everything else — a column (LITERAL), bind variable, the NULL literal, {@code cast(...)},
     * {@code to_timestamp(...)}, or any unknown function — is treated as possibly-NULL, so the flip falls
     * back to the always-correct CASE form. Being conservative here only costs a partition-pruning
     * opportunity, never correctness. (Merely "references no column" is NOT enough: a column-free constant
     * such as {@code cast(null as timestamp)} is still NULL, and flipping {@code NOT(ts < NULL)} to
     * {@code ts >= NULL} would wrongly hide every row.)
     */
    private static boolean isOperandProvablyNonNull(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.CONSTANT) {
            return !SqlKeywords.isNullKeyword(node.token);
        }
        if (node.type == ExpressionNode.FUNCTION || node.type == ExpressionNode.OPERATION) {
            if (isNeverNullClockFunction(node.token)) {
                return true;
            }
            if (!isNullPreservingTimestampExpr(node.token)) {
                return false;
            }
            // null-preserving: non-null iff every operand is non-null (and there is at least one operand).
            boolean hasOperand = false;
            if (node.lhs != null) {
                if (!isOperandProvablyNonNull(node.lhs)) {
                    return false;
                }
                hasOperand = true;
            }
            if (node.rhs != null) {
                if (!isOperandProvablyNonNull(node.rhs)) {
                    return false;
                }
                hasOperand = true;
            }
            for (int i = 0, n = node.args.size(); i < n; i++) {
                if (!isOperandProvablyNonNull(node.args.getQuick(i))) {
                    return false;
                }
                hasOperand = true;
            }
            return hasOperand;
        }
        return false;
    }

    private static boolean isNeverNullClockFunction(CharSequence token) {
        return Chars.equalsIgnoreCase(token, "now")
                || Chars.equalsIgnoreCase(token, "systimestamp")
                || Chars.equalsIgnoreCase(token, "sysdate");
    }

    private static boolean isNullPreservingTimestampExpr(CharSequence token) {
        // null-in -> null-out, non-null-in -> non-null-out, so the result is non-null iff all operands are.
        return Chars.equalsIgnoreCase(token, "dateadd")
                || Chars.equalsIgnoreCase(token, "date_trunc")
                || Chars.equalsIgnoreCase(token, "timestamp_floor")
                || Chars.equalsIgnoreCase(token, "to_timezone")
                || Chars.equalsIgnoreCase(token, "to_utc")
                || (token != null && token.length() == 1
                && (token.charAt(0) == '+' || token.charAt(0) == '-' || token.charAt(0) == '*'));
    }

    /**
     * Returns the EXPIRE ROWS predicate for the given table token, or null if the token is null, is not a
     * materialized view (the only object type that can carry a policy), or carries no policy. Uses the
     * in-memory metadata cache
     * ({@link io.questdb.cairo.MetadataCache#readLock()} + map lookup): a shared read lock plus a
     * hash-map get, no pool borrow, no file I/O on a cache hit. See class/PR notes for the
     * cache-miss caveat.
     */
    private String lookupExpiryPredicate(TableToken tableToken) {
        expiryPolicyTable = null;
        expiryTimestampColumnName = null;
        // EXPIRE ROWS is materialized-view-only; require isMatView() (not merely !isView()) so a policy that
        // ever leaks onto a plain table cannot silently hide its rows. Defense-in-depth: the compiler gate is
        // the primary enforcement, this is the read-side last line.
        if (tableToken == null || !tableToken.isMatView()) {
            return null;
        }
        final MetadataCache metadataCache = cairoEngine.getMetadataCache();
        final boolean isUpdatePending = metadataCache.isExpiryPolicyUpdatePending(tableToken);
        // During SET/DROP the cache deliberately retains the previous policy until the authoritative _meta/_txn
        // publish the new one. Bypass that stale entry while the transition is pending. A concurrent mark after
        // this check advances the policy epoch, so the compiler rejects and reparses any decision made here.
        if (!isUpdatePending) {
            try (MetadataCacheReader metadataRO = metadataCache.readLock()) {
                final CairoTable table = metadataRO.getTable(tableToken);
                if (table != null) {
                    final String predicate = table.getExpiryPredicate();
                    if (predicate == null || predicate.isEmpty()) {
                        return null;
                    }
                    // Copy: the CairoTable's name view must not outlive the read lock we are about to release.
                    expiryTimestampColumnName = Chars.toString(table.getTimestampName());
                    expiryPolicyTable = table;
                    return predicate;
                }
            }
        }
        // Cache miss, or a policy transition in progress: fall back to authoritative table metadata. This
        // prevents a pending first/replacement SET or DROP from embedding the cache's previous policy state.
        try (TableMetadata metadata = cairoEngine.getTableMetadata(tableToken)) {
            if (isUpdatePending) {
                // The policy epoch counter ticks once before the metadata swap and once after it, so a compile
                // that reads the counter both before and after but entirely between those two ticks sees the
                // same value twice and cannot tell this pre-swap read from the new policy. The table's metadata
                // version changes exactly at the swap, so record the version this read saw; the optimiser then
                // rejects the compile if the reader opens a different one.
                pendingExpiryReadVersions.put(tableToken.getTableId(), metadata.getMetadataVersion());
            }
            final String predicate = metadata.getExpiryPredicate();
            if (predicate == null || predicate.isEmpty()) {
                return null;
            }
            final int tsIndex = metadata.getTimestampIndex();
            expiryTimestampColumnName = tsIndex >= 0 ? Chars.toString(metadata.getColumnName(tsIndex)) : null;
            return predicate;
        } catch (CairoException e) {
            if (metadataCache.isExpiryPolicyUpdatePending(tableToken)) {
                // Failing open while the previous policy is intentionally bypassed could permanently bind a
                // no-policy plan to the new policy. Propagate the transient failure; callers may retry, but they
                // must not expose rows silently.
                throw e;
            }
            // Table concurrently dropped/renamed, or its metadata is briefly unavailable: treat as no policy.
            return null;
        }
    }

    private CharSequence createColumnAlias(
            CharSequence token,
            int type,
            LowerCaseCharSequenceHashSet aliasToColumnMap
    ) {
        return SqlUtil.createColumnAlias(
                characterStore,
                unquote(token),
                Chars.indexOfLastUnquoted(token, '.'),
                aliasToColumnMap,
                aliasSequenceMap,
                type != ExpressionNode.LITERAL
        );
    }

    private CharSequence createConstColumnAlias(LowerCaseCharSequenceHashSet aliasToColumnMap) {
        final CharacterStoreEntry characterStoreEntry = characterStore.newEntry();

        characterStoreEntry.put(column);
        int len = characterStoreEntry.length();
        characterStoreEntry.put(digit);

        while (aliasToColumnMap.contains(characterStoreEntry.toImmutable())) {
            characterStoreEntry.trimTo(len);
            digit++;
            characterStoreEntry.put(digit);
        }
        return characterStoreEntry.toImmutable();
    }

    private GenericLexer createLexer() {
        final GenericLexer lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
        SqlCompilerImpl.configureLexer(lexer);
        return lexer;
    }

    private @NotNull CreateTableColumnModel ensureCreateTableColumnModel(CharSequence columnName, int columnNamePos) {
        CreateTableColumnModel touchUpModel = getCreateTableColumnModel(columnName);
        if (touchUpModel != null) {
            return touchUpModel;
        }
        try {
            return newCreateTableColumnModel(columnName, columnNamePos);
        } catch (SqlException e) {
            throw new AssertionError("createColumnModel should never fail here", e);
        }
    }

    private boolean expectBoolean(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "'true' or 'false'");
        if (isTrueKeyword(tok)) {
            return true;
        } else if (isFalseKeyword(tok)) {
            return false;
        } else {
            throw errUnexpected(lexer, tok);
        }
    }

    private void expectBy(GenericLexer lexer) throws SqlException {
        if (isByKeyword(tok(lexer, "'by'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'by' expected");
    }

    private double expectDouble(GenericLexer lexer) throws SqlException {
        CharSequence tok = GenericLexer.unquote(expectStringLiteral(lexer).token);
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "number");
        } else {
            negative = false;
        }
        try {
            double result = Numbers.parseDouble(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, tok, "bad number");
        }
    }

    private ExpressionNode expectExpr(GenericLexer lexer, SqlParserCallback sqlParserCallback, LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        final ExpressionNode n = expr(lexer, null, sqlParserCallback, decls);
        if (n != null) {
            return n;
        }
        throw SqlException.$(lexer.hasUnparsed() ? lexer.lastTokenPosition() : lexer.getPosition(), "Expression expected");
    }

    private ExpressionNode expectExpr(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        return expectExpr(lexer, sqlParserCallback, null);
    }

    private int expectInt(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "integer");
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "integer");
        } else {
            negative = false;
        }
        try {
            int result = Numbers.parseInt(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, tok, "bad integer");
        }
    }

    /**
     * Parses an interval literal like "5s", "-2m", "+10h". Handles optional leading sign.
     */
    private ExpressionNode expectIntervalLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "interval");
        int pos = lexer.lastTokenPosition();

        // Check for optional sign
        if (Chars.equals(tok, '-') || Chars.equals(tok, '+')) {
            char sign = tok.charAt(0);
            CharSequence valueTok = tok(lexer, "interval value");
            // Combine sign with value: "-" + "2s" -> "-2s"
            CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(sign).put(valueTok);
            return expressionNodePool.next().of(ExpressionNode.CONSTANT, entry.toImmutable(), 0, pos);
        }

        return expressionNodePool.next().of(ExpressionNode.CONSTANT, GenericLexer.immutableOf(tok), 0, pos);
    }

    private ExpressionNode expectLiteral(GenericLexer lexer) throws SqlException {
        return expectLiteral(lexer, null);
    }

    private ExpressionNode expectLiteral(GenericLexer lexer, @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        assertNameIsQuotedOrNotAKeyword(tok, pos);
        validateLiteral(pos, tok);
        return rewriteDeclaredVariables(nextLiteral(GenericLexer.immutableOf(GenericLexer.unquote(tok)), pos), decls, null);
    }

    private long expectLong(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "long integer");
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "long integer");
        } else {
            negative = false;
        }
        try {
            long result = Numbers.parseLong(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, tok, "bad long integer");
        }
    }

    private void expectObservation(GenericLexer lexer) throws SqlException {
        if (isObservationKeyword(tok(lexer, "'observation'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'observation' expected");
    }

    private void expectOffset(GenericLexer lexer) throws SqlException {
        if (isOffsetKeyword(tok(lexer, "'offset'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'offset' expected");
    }

    private void expectSample(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        final ExpressionNode n = expr(lexer, null, sqlParserCallback, model.getDecls());
        if (isFullSampleByPeriod(n)) {
            model.setSampleBy(n);
            return;
        }

        // this is complex expression of sample by period. It must follow time unit interval
        // lets preempt the problem where time unit interval is missing, and we hit keyword instead
        final int pos = lexer.lastTokenPosition();
        final CharSequence tok = tok(lexer, "time interval unit");

        if (isValidSampleByPeriodLetter(tok)) {
            model.setSampleBy(n, SqlUtil.nextLiteral(expressionNodePool, tok, pos));
            return;
        }
        throw SqlException.$(pos, "one letter sample by period unit expected");
    }

    private ExpressionNode expectStringLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        assertNameIsQuotedOrNotAKeyword(tok, pos);
        return nextLiteral(GenericLexer.immutableOf(tok), pos);
    }

    private CharSequence expectTableNameOrSubQuery(GenericLexer lexer) throws SqlException {
        return tok(lexer, "table name or sub-query");
    }

    private void expectTo(GenericLexer lexer) throws SqlException {
        if (isToKeyword(tok(lexer, "'to'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'to' expected");
    }

    private void expectTok(GenericLexer lexer, CharSequence tok, CharSequence expected) throws SqlException {
        if (tok == null || !Chars.equalsLowerCaseAscii(tok, expected)) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(expected).put("' expected");
        }
    }

    private void expectTok(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(lexer, tok, expected);
    }

    private void expectTok(GenericLexer lexer, char expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(tok, lexer.lastTokenPosition(), expected);
    }

    private void expectTok(CharSequence tok, int pos, char expected) throws SqlException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw SqlException.position(pos).put('\'').put(expected).put("' expected");
        }
    }

    private void expectZone(GenericLexer lexer) throws SqlException {
        if (isZoneKeyword(tok(lexer, "'zone'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'zone' expected");
    }

    private void generateColumnAlias(GenericLexer lexer, QueryColumn qc, boolean hasFrom) throws SqlException {
        CharSequence token = qc.getAst().token;
        if (qc.getAst().isWildcard() && !hasFrom) {
            throw err(lexer, null, "'from' expected");
        }

        CharSequence alias;
        if (configuration.isColumnAliasExpressionEnabled()) {
            CharacterStoreEntry entry = characterStore.newEntry();
            qc.getAst().toSink(entry);
            alias = SqlUtil.createExprColumnAlias(
                    characterStore,
                    entry.toImmutable(),
                    aliasMap,
                    aliasSequenceMap,
                    configuration.getColumnAliasGeneratedMaxSize(),
                    qc.getAst().type != ExpressionNode.LITERAL
            );
        } else {
            if (qc.getAst().type == ExpressionNode.CONSTANT && Chars.indexOfLastUnquoted(token, '.') != -1) {
                alias = createConstColumnAlias(aliasMap);
            } else {
                CharSequence tokenAlias = qc.getAst().token;
                if (qc.isWindowExpression() && ((WindowExpression) qc).isIgnoreNulls()) {
                    CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(tokenAlias);
                    cse.put("_ignore_nulls");
                    tokenAlias = cse.toImmutable();
                }
                alias = createColumnAlias(tokenAlias, qc.getAst().type, aliasMap);
            }
        }
        qc.setAlias(alias, QueryColumn.SYNTHESIZED_ALIAS_POSITION);
        aliasMap.add(alias);
    }

    private @Nullable CreateTableColumnModel getCreateTableColumnModel(CharSequence columnName) {
        return createTableOperationBuilder.getColumnModel(columnName);
    }

    private boolean isCurrentRow(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isCurrentKeyword(tok)) {
            tok = tok(lexer, "'row'");
            if (isRowKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'row' expected");
        }
        return false;
    }

    private boolean isExcludePrevailing(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isExcludeKeyword(tok)) {
            tok = tok(lexer, "'prevailing'");
            if (isPrevailingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'prevailing' expected");
        }
        return false;
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private boolean isIncludePrevailing(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isIncludeKeyword(tok)) {
            tok = tok(lexer, "'prevailing'");
            if (isPrevailingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'prevailing' expected");
        }
        return false;
    }

    private boolean isUnboundedPreceding(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isUnboundedKeyword(tok)) {
            tok = tok(lexer, "'preceding'");
            if (isPrecedingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'preceding' expected");
        }
        return false;
    }

    private boolean isUnexpectedRightParenInTopLevelSelect(CharSequence tok) {
        return Chars.equals(tok, ')') && !(subQueryMode || createTableMode || copyMode || createViewMode);
    }

    private ExpressionNode literal(GenericLexer lexer, CharSequence name) {
        return literal(name, lexer.lastTokenPosition());
    }

    private ExpressionNode literal(CharSequence name, int position) {
        // this can never be null in its current contexts
        // every time this function is called is after lexer.unparse(), which ensures non-null token.
        return expressionNodePool.next().of(ExpressionNode.LITERAL, unquote(name), 0, position);
    }

    private @NotNull CreateTableColumnModel newCreateTableColumnModel(
            CharSequence columnName,
            int columnNamePos
    ) throws SqlException {
        if (createTableOperationBuilder.getColumnModel(columnName) != null) {
            throw SqlException.duplicateColumn(columnNamePos, columnName);
        }
        CreateTableColumnModel model = createTableColumnModelPool.next();
        model.setColumnNamePos(columnNamePos);
        createTableOperationBuilder.addColumnModel(columnName, model);
        return model;
    }

    private ExpressionNode nextLiteral(CharSequence token, int position) {
        return SqlUtil.nextLiteral(expressionNodePool, token, position);
    }

    private CharSequence notTermTok(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "')' or ','");
        if (isFieldTerm(tok)) {
            throw err(lexer, tok, "missing column definition");
        }
        return tok;
    }

    private CharSequence optTok(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')') && !pivotMode)) {
            return null;
        }
        return tok;
    }

    private IQueryModel parseAsSubQueryAndExpectClosingBrace(
            GenericLexer lexer,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final IQueryModel model = parseAsSubQuery(lexer, withClauses, useTopLevelWithClauses, sqlParserCallback, decls, false);
        expectTok(lexer, ')');
        return model;
    }

    private ExecutionModel parseCompileView(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "view");

        CharSequence tok = tok(lexer, "view name");
        final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
        if (tt == null) {
            throw SqlException.viewDoesNotExist(lexer.lastTokenPosition(), tok);
        }
        if (!tt.isView()) {
            throw SqlException.$(lexer.lastTokenPosition(), "view expected, got table");
        }

        final CompileViewModel model = compileViewModelPool.next();
        model.setTableNameExpr(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = optTok(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw errUnexpected(lexer, tok);
        }

        final IQueryModel queryModel = queryModelPool.next();
        model.setQueryModel(queryModel);

        compileViewQuery(queryModel, tt, lexer.lastTokenPosition());
        return model;
    }

    private ExecutionModel parseCopy(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        @Nullable ExpressionNode target = null;
        @Nullable CharSequence selectText = null;
        CharSequence tok = tok(lexer, "copy source");
        int startOfSelect = 0;

        if (tok.length() == 1 && tok.charAt(0) == '(') {
            startOfSelect = lexer.getPosition();
            copyMode = true;
            try {
                parseDml(lexer, startOfSelect, sqlParserCallback);
                final int endOfSelect = lexer.getPosition() - 1;
                selectText = lexer.getContent().subSequence(startOfSelect, endOfSelect);
                expectTok(lexer, ')');
            } finally {
                copyMode = false;
            }
        } else {
            lexer.unparseLast();
            target = expectExpr(lexer, sqlParserCallback);
        }

        tok = tok(lexer, "'from' or 'to' or 'cancel'");

        ExportModel model = copyModelPool.next();
        if (isCancelKeyword(tok)) {
            model.setCancel(true);
            model.setTarget(target);

            tok = optTok(lexer);
            // no more tokens or ';' should indicate end of statement
            if (tok == null || Chars.equals(tok, ';')) {
                return model;
            }

            throw errUnexpected(lexer, tok);
        }

        if (isFromKeyword(tok) || isToKeyword(tok)) {
            tok = GenericLexer.immutableOf(tok);
            final ExpressionNode fileName = expectExpr(lexer, sqlParserCallback);
            if (fileName.token.length() < 3 && Chars.startsWith(fileName.token, '\'')) {
                throw SqlException.$(fileName.position, "file name expected");
            }

            model.setTarget(target);
            model.setSelectText(selectText, startOfSelect);
            model.setFileName(fileName);
        }

        if (isFromKeyword(tok)) {
            if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
                throw SqlException.$(lexer.lastTokenPosition(), "COPY is disabled ['cairo.sql.copy.root' is not set?]");
            }
            if (selectText != null) {
                throw SqlException.$(startOfSelect, "subqueries are not supported for `COPY-FROM`");
            }

            model.setType(ExportModel.COPY_TYPE_FROM);

            tok = optTok(lexer);
            if (tok != null && isWithKeyword(tok)) {
                tok = tok(lexer, "copy option");
                while (tok != null && !isSemicolon(tok)) {
                    if (isHeaderKeyword(tok)) {
                        model.setHeader(isTrueKeyword(tok(lexer, "'true' or 'false'")));
                        tok = optTok(lexer);
                    } else if (isPartitionKeyword(tok)) {
                        expectTok(lexer, "by");
                        tok = tok(lexer, "year month day hour none");
                        int partitionBy = PartitionBy.fromString(tok);
                        if (partitionBy < 0) {
                            throw SqlException.$(lexer.getPosition(), "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
                        }
                        model.setPartitionBy(partitionBy);
                        tok = optTok(lexer);
                    } else if (isTimestampKeyword(tok)) {
                        tok = tok(lexer, "timestamp column name expected");
                        CharSequence columnName = GenericLexer.immutableOf(unquote(tok));
                        if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                            throw SqlException.$(lexer.getPosition(), "timestamp column name contains invalid characters");
                        }
                        model.setTimestampColumnName(columnName);
                        tok = optTok(lexer);
                    } else if (isFormatKeyword(tok)) {
                        tok = tok(lexer, "timestamp format expected");
                        CharSequence format = GenericLexer.immutableOf(unquote(tok));
                        model.setTimestampFormat(format);
                        tok = optTok(lexer);
                    } else if (isOnKeyword(tok)) {
                        expectTok(lexer, "error");
                        tok = tok(lexer, "skip_column skip_row abort");
                        if (Chars.equalsIgnoreCase(tok, "skip_column")) {
                            model.setAtomicity(Atomicity.SKIP_COL);
                        } else if (Chars.equalsIgnoreCase(tok, "skip_row")) {
                            model.setAtomicity(Atomicity.SKIP_ROW);
                        } else if (Chars.equalsIgnoreCase(tok, "abort")) {
                            model.setAtomicity(Atomicity.SKIP_ALL);
                        } else {
                            throw SqlException.$(lexer.getPosition(), "invalid 'on error' copy option found");
                        }
                        tok = optTok(lexer);
                    } else if (isDelimiterKeyword(tok)) {
                        tok = tok(lexer, "timestamp character expected");
                        CharSequence delimiter = GenericLexer.immutableOf(unquote(tok));
                        if (delimiter == null || delimiter.length() != 1) {
                            throw SqlException.$(lexer.getPosition(), "delimiter is empty or contains more than 1 character");
                        }
                        char delimiterChar = delimiter.charAt(0);
                        if (delimiterChar > 127) {
                            throw SqlException.$(lexer.getPosition(), "delimiter is not an ascii character");
                        }
                        model.setDelimiter((byte) delimiterChar);
                        tok = optTok(lexer);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected option");
                    }
                }
            } else if (tok != null && !isSemicolon(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
            return model;
        }

        if (isToKeyword(tok)) {
            // Disable COPY TO when export root is not configured
            if (Chars.isBlank(configuration.getSqlCopyExportRoot())) {
                throw SqlException.$(lexer.lastTokenPosition(), "COPY TO is disabled ['cairo.sql.copy.export.root' is not set?]");
            }

            tok = optTok(lexer);
            model.setType(ExportModel.COPY_TYPE_TO);
            if (tok == null || isSemicolon(tok)) {
                return model;
            }
            if (!isWithKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
            tok = tok(lexer, "copy option");
            while (tok != null && !isSemicolon(tok)) {
                final int optionCode = ExportModel.getExportOption(tok);
                switch (optionCode) {
                    case ExportModel.COPY_OPTION_FORMAT:
                        // only support parquet for now
                        tok = tok(lexer, "'parquet'");
                        if (isParquetKeyword(tok)) {
                            model.setFormat(ExportModel.COPY_FORMAT_PARQUET);
                            model.setParquetDefaults(configuration);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "unsupported format, only 'parquet' is supported");
                        }
                        break;
                    case ExportModel.COPY_OPTION_PARTITION_BY:
                        final ExpressionNode partitionByExpr = expectLiteral(lexer);
                        final int partitionBy = PartitionBy.fromString(partitionByExpr.token);
                        if (partitionBy < 0) {
                            throw SqlException.$(lexer.lastTokenPosition(), "invalid partition by option: ").put(partitionByExpr.token);
                        }
                        model.setPartitionBy(partitionBy);
                        break;
                    case ExportModel.COPY_OPTION_SIZE_LIMIT:
                        // todo: add this when table writer has appropriate support for it
                        throw SqlException.$(lexer.lastTokenPosition(), "size limit is not yet supported");
                    case ExportModel.COPY_OPTION_COMPRESSION_CODEC:
                        ExpressionNode codecExpr = expectLiteral(lexer);
                        int codec = ParquetCompression.getCompressionCodec(codecExpr.token);
                        if (codec < 0) {
                            SqlException e = SqlException.$(codecExpr.position, "invalid compression codec[").put(codecExpr.token).put("], expected one of: ");
                            ParquetCompression.addCodecNamesToException(e);
                            throw e;
                        }
                        model.setCompressionCodec(codec);
                        break;
                    case ExportModel.COPY_OPTION_COMPRESSION_LEVEL:
                        model.setCompressionLevel(expectInt(lexer), lexer.lastTokenPosition());
                        break;
                    case ExportModel.COPY_OPTION_ROW_GROUP_SIZE:
                        model.setRowGroupSize(expectInt(lexer));
                        break;
                    case ExportModel.COPY_OPTION_DATA_PAGE_SIZE:
                        model.setDataPageSize(expectInt(lexer));
                        break;
                    case ExportModel.COPY_OPTION_RAW_ARRAY_ENCODING:
                        model.setRawArrayEncoding(expectBoolean(lexer));
                        break;
                    case ExportModel.COPY_OPTION_STATISTICS_ENABLED:
                        model.setStatisticsEnabled(expectBoolean(lexer));
                        break;
                    case ExportModel.COPY_OPTION_PARQUET_VERSION:
                        int parquetVersion = expectInt(lexer);
                        if (parquetVersion != ExportModel.PARQUET_VERSION_V1 && parquetVersion != ExportModel.PARQUET_VERSION_V2) {
                            throw SqlException.$(lexer.lastTokenPosition(), "invalid parquet version: ").put(parquetVersion).put(", expected 1 or 2");
                        }
                        model.setParquetVersion(parquetVersion);
                        break;
                    case ExportModel.COPY_OPTION_BLOOM_FILTER_COLUMNS:
                        ExpressionNode bloomFilterColumnsExpr = expectStringLiteral(lexer);
                        model.setBloomFilterColumns(GenericLexer.unquote(bloomFilterColumnsExpr.token), Chars.isQuoted(bloomFilterColumnsExpr.token) ? bloomFilterColumnsExpr.position + 1 : bloomFilterColumnsExpr.position);
                        break;
                    case ExportModel.COPY_OPTION_BLOOM_FILTER_FPP:
                        double fpp = expectDouble(lexer);
                        if (!Double.isFinite(fpp) || fpp <= 0 || fpp >= 1) {
                            throw SqlException.$(lexer.lastTokenPosition(), "bloom_filter_fpp must be between 0 and 1 (exclusive)");
                        }
                        model.setBloomFilterFpp(fpp);
                        break;
                    case ExportModel.COPY_OPTION_UNKNOWN:
                        throw SqlException.$(lexer.lastTokenPosition(), "unrecognised option [option=")
                                .put(tok).put(']');
                }
                tok = optTok(lexer);
            }
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private ExecutionModel parseCreate(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CharSequence tok = tok(lexer, "'atomic' or 'table' or 'batch' or 'materialized' or 'view' or 'or replace'");
        if (isOrKeyword(tok)) {
            // we need to skip OR REPLACE, it is handled in an executor
            expectTok(lexer, "replace");
            tok = tok(lexer, "'view'");
        }
        if (isViewKeyword(tok)) {
            return parseCreateView(lexer, executionContext, sqlParserCallback);
        }
        if (isLiveKeyword(tok)) {
            if (!configuration.isLiveViewEnabled()) {
                throw SqlException.$(lexer.lastTokenPosition(), "live views are disabled");
            }
            // A view created with no refresh worker would never seed, never drain and never serve
            // a row, while WalPurgeJob would have to hold the base WAL from its genesis seqTxn
            // forever on its behalf. Refuse it here rather than hand back a view that only looks
            // created. buildViewGraphs applies the same predicate to views already on disk.
            if (configuration.getLiveViewRefreshWorkerCount() < 1) {
                throw SqlException.$(lexer.lastTokenPosition(), "live view refresh is disabled, set ")
                        .put(PropertyKey.LIVE_VIEW_REFRESH_WORKER_COUNT.getPropertyPath())
                        .put(" to a positive value");
            }
            // The CREATE body is the one place ANCHOR is written by hand, and it
            // parses with isLiveViewCompile() still false (only the later re-compile
            // of the stored SELECT sets that). Restore the flag afterwards so a
            // failed CREATE cannot leave ANCHOR enabled for the next expr() call.
            expressionParser.setAnchorAllowed(true);
            try {
                return parseCreateLiveView(lexer, executionContext, sqlParserCallback);
            } finally {
                expressionParser.setAnchorAllowed(executionContext.isLiveViewCompile());
            }
        }
        if (isMaterializedKeyword(tok)) {
            if (!configuration.isMatViewEnabled()) {
                throw SqlException.$(lexer.lastTokenPosition(), "materialized views are disabled");
            }
            return parseCreateMatView(lexer, executionContext, sqlParserCallback);
        }
        return parseCreateTable(lexer, tok, executionContext, sqlParserCallback);
    }

    private ExecutionModel parseCreateLiveView(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CreateLiveViewOperationBuilderImpl builder = createLiveViewOperationBuilder;
        builder.clear();

        expectTok(lexer, "view");

        // optional IF NOT EXISTS
        CharSequence tok = tok(lexer, "live view name");
        if (isIfKeyword(tok)) {
            expectTok(lexer, "not");
            expectTok(lexer, "exists");
            builder.setIgnoreIfExists(true);
            tok = tok(lexer, "live view name");
        }

        // view name - apply the same normalization as CREATE TABLE / MATERIALIZED
        // VIEW: strip a leading public. schema, reject an unquoted keyword name, and
        // reject dots/slashes. Without this the live-view path diverged (accepting
        // keyword names and a public. prefix that the other CREATE paths normalize).
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        builder.setViewName(Chars.toString(assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition())));
        builder.setViewNamePosition(lexer.lastTokenPosition());

        // FLUSH EVERY <duration> -- required
        tok = tok(lexer, "'flush'");
        if (!isFlushKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'flush every <duration>' expected");
        }
        expectTok(lexer, "every");
        CharSequence flushTok = tok(lexer, "flush every duration");
        int flushPos = lexer.lastTokenPosition();
        long flushValue = LiveViewDefinition.parseDurationValue(flushTok, flushPos);
        char flushUnit = LiveViewDefinition.parseDurationUnit(flushTok, flushPos);
        long flushMicros = LiveViewDefinition.toMicrosChecked(flushValue, flushUnit, flushPos);
        if (flushValue == 0 || flushMicros < 100_000) {
            throw SqlException.$(flushPos, "live view FLUSH EVERY must be at least 100ms");
        }
        builder.setFlushEveryInterval(flushValue);
        builder.setFlushEveryIntervalUnit(flushUnit);

        // Defaults: IN MEMORY = FLUSH EVERY; PARTITION BY = base table's scheme.
        // IN MEMORY is the user-facing knob for the in-memory tier's retention
        // window. Parsed, bounded by cairo.live.view.in.memory.max, and persisted
        // into _lv.
        long inMemoryValue = flushValue;
        char inMemoryUnit = flushUnit;
        long inMemoryMicros = flushMicros;
        boolean inMemorySpecified = false;
        boolean partitionBySpecified = false;
        boolean startFromSpecified = false;

        // Clauses: IN MEMORY <duration>, PARTITION BY <unit>, START FROM <start>.
        // Any of the three may appear, in any order, before AS, but each at most
        // once - a repeat is rejected so a typo'd second clause does not silently
        // overwrite the first. START FROM is the only mandatory one; the check for
        // it sits below, once AS terminates the clause list.
        tok = tok(lexer, "'in', 'partition', 'start', or 'as'");
        while (true) {
            if (isInKeyword(tok)) {
                if (inMemorySpecified) {
                    throw SqlException.$(lexer.lastTokenPosition(), "live view IN MEMORY clause specified more than once");
                }
                expectTok(lexer, "memory");
                CharSequence memTok = tok(lexer, "in memory duration");
                int memPos = lexer.lastTokenPosition();
                inMemoryValue = LiveViewDefinition.parseDurationValue(memTok, memPos);
                inMemoryUnit = LiveViewDefinition.parseDurationUnit(memTok, memPos);
                inMemoryMicros = LiveViewDefinition.toMicrosChecked(inMemoryValue, inMemoryUnit, memPos);
                if (inMemoryMicros < flushMicros) {
                    SqlException ex = SqlException.position(memPos)
                            .put("live view IN MEMORY must be at least FLUSH EVERY (")
                            .put(flushValue)
                            .put(displayDurationUnit(flushUnit))
                            .put(')');
                    throw ex;
                }
                if (inMemoryMicros > configuration.getLiveViewInMemoryMaxMicros()) {
                    SqlException ex = SqlException.position(memPos)
                            .put("live view IN MEMORY must be at most cairo.live.view.in.memory.max (");
                    appendDurationFromMicros(ex, configuration.getLiveViewInMemoryMaxMicros());
                    ex.put(')');
                    throw ex;
                }
                inMemorySpecified = true;
                builder.setInMemoryInterval(inMemoryValue);
                builder.setInMemoryIntervalUnit(inMemoryUnit);
                tok = tok(lexer, "next clause or 'as'");
            } else if (isPartitionKeyword(tok)) {
                if (partitionBySpecified) {
                    throw SqlException.$(lexer.lastTokenPosition(), "live view PARTITION BY clause specified more than once");
                }
                expectTok(lexer, "by");
                tok = tok(lexer, "year month week day hour");
                int partPos = lexer.lastTokenPosition();
                int partitionBy = PartitionBy.fromString(tok);
                if (partitionBy < 0) {
                    throw SqlException.$(partPos, "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
                }
                // The LV's on-disk tier is a WAL-backed table, and WAL tables
                // require a partition scheme. Explicit PARTITION BY NONE would
                // fail downstream with a confusing "WAL is only supported for
                // partitioned tables" error; reject up front with an LV-specific
                // message instead.
                if (partitionBy == PartitionBy.NONE) {
                    throw SqlException.$(partPos,
                            "live view PARTITION BY NONE is not supported; live views must be partitioned");
                }
                builder.setPartitionBy(partitionBy);
                partitionBySpecified = true;
                tok = tok(lexer, "next clause or 'as'");
            } else if (isStartKeyword(tok)) {
                if (startFromSpecified) {
                    throw SqlException.$(lexer.lastTokenPosition(), "live view START FROM clause specified more than once");
                }
                expectTok(lexer, "from");
                tok = tok(lexer, "'now', 'beginning' or a timestamp literal");
                final int startPos = lexer.lastTokenPosition();
                if (isNowKeyword(tok)) {
                    builder.setStartFromNow();
                    startFromSpecified = true;
                    tok = tok(lexer, "next clause or 'as'");
                    // NOW is grammar, not the now() function: the view resolves it to a single
                    // clock reading at CREATE and persists that. Reject the call syntax rather
                    // than letting the '(' fall through to a bare "'as' expected".
                    if (Chars.equals(tok, '(')) {
                        throw SqlException.$(lexer.lastTokenPosition(), "live view START FROM NOW does not take arguments");
                    }
                } else if (isBeginningKeyword(tok)) {
                    builder.setStartFromBeginning();
                    startFromSpecified = true;
                    tok = tok(lexer, "next clause or 'as'");
                } else if (isNullKeyword(tok)) {
                    throw SqlException.$(startPos, "live view START FROM does not accept NULL");
                } else if (tok.length() > 1 && tok.charAt(0) == '\'' && tok.charAt(tok.length() - 1) == '\'') {
                    // Single quotes only: a double-quoted or back-quoted token is an identifier
                    // in QuestDB SQL, and the boundary is a constant, not a name.
                    //
                    // The literal is parsed at CREATE, not here: its precision follows the base
                    // table's designated timestamp type (MICRO or NANO), which the parser cannot
                    // see. CairoEngine.createLiveView resolves it against the base's driver.
                    builder.setStartFromTimestamp(Chars.toString(GenericLexer.unquote(tok)), startPos);
                    startFromSpecified = true;
                    tok = tok(lexer, "next clause or 'as'");
                } else {
                    throw SqlException.$(startPos, "'now', 'beginning' or a quoted timestamp literal expected");
                }
            } else if (isBackfillKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "live view BACKFILL is not supported, use START FROM BEGINNING");
            } else {
                break;
            }
        }

        if (!inMemorySpecified) {
            // IN MEMORY defaults to FLUSH EVERY when omitted, so the same
            // cairo.live.view.in.memory.max cap that bounds the explicit clause
            // must bound the default too. Otherwise a large FLUSH EVERY (which
            // has no upper bound of its own) silently retains more than the cap
            // in the in-memory tier.
            if (inMemoryMicros > configuration.getLiveViewInMemoryMaxMicros()) {
                SqlException ex = SqlException.position(flushPos)
                        .put("live view FLUSH EVERY must be at most cairo.live.view.in.memory.max (");
                appendDurationFromMicros(ex, configuration.getLiveViewInMemoryMaxMicros());
                ex.put(") because IN MEMORY defaults to FLUSH EVERY");
                throw ex;
            }
            builder.setInMemoryInterval(inMemoryValue);
            builder.setInMemoryIntervalUnit(inMemoryUnit);
        }

        // expect AS
        if (!isAsKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'as' expected");
        }

        // START FROM decides which base rows the view ever contains, and the answer
        // differs by orders of magnitude between NOW and BEGINNING. There is no
        // defensible default, so the clause is mandatory: point at the AS that closed
        // the clause list, which is where the missing clause belongs.
        if (!startFromSpecified) {
            throw SqlException.$(lexer.lastTokenPosition(),
                    "live view requires a START FROM clause, one of 'START FROM NOW', 'START FROM BEGINNING' or 'START FROM <timestamp>'");
        }

        // parse SELECT
        int selectStart = lexer.getPosition();
        tok = tok(lexer, "'(' or 'select'");
        boolean hasParens = Chars.equals(tok, '(');
        if (hasParens) {
            // Skip past the opening parenthesis so the captured SELECT text stays
            // balanced; otherwise the stored SQL keeps the leading '(' but drops
            // the trailing ')', and recompiling it later fails with "')' expected".
            selectStart = lexer.getPosition();
        } else {
            lexer.unparseLast();
        }
        IQueryModel queryModel = parseDml(lexer, lexer.getPosition(), sqlParserCallback);
        if (hasParens) {
            expectTok(lexer, ")");
        }
        // A live view freezes its output schema at CREATE, but persists the SELECT text
        // verbatim and recompiles it whenever the base metadata drifts. A wildcard in the
        // top-level projection would re-expand against the new base metadata, so a base
        // ADD COLUMN - which the view otherwise treats as transparent - would widen the
        // projection past the frozen on-disk schema and the row copier would write the new
        // column into the slot of the one after it. Reject it at CREATE, mirroring the ban
        // SAMPLE BY carries for exactly the same reason (see SqlOptimiser.rewriteSampleBy).
        // The top-level projection is the only one to check: it alone fixes the view's schema,
        // and a subquery in FROM - the one shape that could hide another projection - is
        // already rejected below ("live view requires a single base table in FROM clause").
        final ObjList<QueryColumn> projection = queryModel.getColumns();
        for (int i = 0, n = projection.size(); i < n; i++) {
            final ExpressionNode ast = projection.getQuick(i).getAst();
            if (ast.isWildcard()) {
                throw SqlException.$(ast.position, "wildcard column select is not allowed in live view queries");
            }
        }
        // Trim whitespace between the query and any wrapping parentheses so the
        // captured SELECT text round-trips cleanly. SHOW CREATE LIVE VIEW re-emits
        // the definition as "AS (\n<sql>\n)"; without trimming, re-parsing that
        // output would fold the surrounding newlines into the stored SQL and
        // accumulate more whitespace on every round-trip.
        final CharSequence content = lexer.getContent();
        int selectTextStart = selectStart;
        int selectTextEnd = lexer.getPosition() - (hasParens ? 1 : 0);
        while (selectTextStart < selectTextEnd && isLexerWhitespace(content.charAt(selectTextStart))) {
            selectTextStart++;
        }
        while (selectTextEnd > selectTextStart && isLexerWhitespace(content.charAt(selectTextEnd - 1))) {
            selectTextEnd--;
        }
        builder.setSelectSql(Chars.toString(content, selectTextStart, selectTextEnd));
        builder.setSelectModel(queryModel);

        // extract base table name from query model
        IQueryModel from = queryModel.getNestedModel() != null ? queryModel.getNestedModel() : queryModel;
        if (from.getTableName() == null) {
            // The user named one table. If that table carries an EXPIRE ROWS policy, the read
            // filter has already swapped it for a sub-query, and the check below would blame the
            // user for a FROM clause they never wrote. Say what actually happened instead.
            // Refusing is right either way: a live view reads its base raw, so it would take in
            // the very rows the policy expires.
            final ExpressionNode fromAlias = from.getAlias();
            if (fromAlias != null && expiryExpandedTables.contains(unquote(fromAlias.token))) {
                throw SqlException.$(fromAlias.position, "cannot create a live view over '")
                        .put(unquote(fromAlias.token))
                        .put("': it carries an EXPIRE ROWS policy (the view would copy expired rows on refresh)");
            }
            throw SqlException.$(selectStart, "live view requires a single base table in FROM clause");
        }
        builder.setBaseTableName(Chars.toString(from.getTableName()));
        // Position of the base table name in the source SQL; engine-side
        // validation rules that reject based on the base table (DEDUP keys,
        // missing designated timestamp, live-on-live) point at this offset.
        final ExpressionNode baseNameExpr = from.getTableNameExpr();
        builder.setBaseTableNamePosition(baseNameExpr != null ? baseNameExpr.position : selectStart);

        // Validate ORDER BY on each named window: CREATE-time validation requires
        // the ORDER BY column to be the base table's designated timestamp,
        // ascending. Caught at parse time so the LV never reaches the engine with a
        // shape its WAL-row-order processing can't honor.
        validateLiveViewWindowOrderBy(queryModel, from.getTableName());

        // Validate ANCHOR usage on each named window. Inline anchor expressions
        // attached to anonymous OVER (...) clauses inside SELECT columns are also
        // captured by the parser but live in the SELECT-column WindowExpressions;
        // we walk the named-window map here.
        validateLiveViewAnchors(queryModel);

        // Enforce the bare-unbounded-window rule, which validateLiveViewAnchors
        // used to carry: a PARTITION-BY-keyed window over the default frame needs
        // an ANCHOR to bound its per-partition state. Resolved per window-function
        // call rather than per window definition, because the state the rule is
        // about belongs to the calls.
        rejectBareUnboundedWindows(queryModel);

        // Defense-in-depth lead() reject. The factory-side check inside
        // CairoEngine only fires when the planner picks a window factory
        // that exposes lead - a future planner path that bypasses both
        // CachedWindowRecordCursorFactory and WindowRecordCursorFactory
        // would silently accept lead-only LVs. Surface it at the parser
        // level too. Runs before the finite-influence gate so a lead() over
        // the default frame is named for what actually disqualifies it: lead
        // reads forward and ignores the frame entirely, so "bound the frame"
        // would be advice that cannot help.
        rejectLeadInSelect(queryModel);

        // Enforce the finite-influence scope cut: unanchored ranking functions
        // (row_number / rank / dense_rank) and unbounded frame starts have no
        // finite forward influence and are rejected at CREATE. Runs after
        // validateLiveViewAnchors so the named-window anchor kinds it inspects
        // are already validated.
        validateLiveViewFiniteInfluence(queryModel);

        // Capture the (at most one) anchored named WINDOW for persistence in _lv.
        // The runtime side reads this back to compile the anchor expression and
        // build the LiveViewWindow without re-parsing the SELECT.
        builder.setAnchorSpec(captureAnchoredWindow(queryModel));

        // Hand any trailing token to the edition grammar hook, as CREATE TABLE / VIEW /
        // MATERIALIZED VIEW already do. Enterprise consumes OWNED BY '<principal>' here; the OSS
        // default rejects whatever is left over, which is what a bare trailing token did before
        // this hook existed. SHOW CREATE LIVE VIEW emits the same clause, so its output has to
        // parse back through this call.
        tok = optTok(lexer);
        return parseCreateLiveViewExt(lexer, executionContext, sqlParserCallback, tok, builder);
    }

    private LiveViewDefinition.LvAnchorSpec captureAnchoredWindow(IQueryModel queryModel) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> named = queryModel.getNamedWindows();
        ObjList<CharSequence> keys = named.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence keyCs = keys.getQuick(i);
            WindowExpression w = named.get(keyCs);
            if (w == null || w.getAnchorKind() == WindowExpression.ANCHOR_KIND_NONE) {
                continue;
            }
            // Per validateLiveViewAnchors, at most one anchored window survives.
            String windowName = Chars.toString(keyCs);
            byte anchorKind = w.getAnchorKind();
            String anchorExpressionSql = null;
            if (anchorKind == WindowExpression.ANCHOR_KIND_EXPRESSION) {
                ExpressionNode expr = w.getAnchorExpression();
                if (expr != null) {
                    StringSink anchorSink = Misc.getThreadLocalSink();
                    expr.toSink(anchorSink);
                    anchorExpressionSql = anchorSink.toString();
                }
            } else if (anchorKind == WindowExpression.ANCHOR_KIND_DAILY) {
                // Desugar ANCHOR DAILY 'HH:MM' [tz] into the equivalent
                // timestamp_floor / timestamp_floor_utc expression so the runtime
                // path that compiles ANCHOR EXPRESSION can drive resetPartition
                // dispatch identically. The original DAILY fields stay persisted
                // for round-tripping in SHOW CREATE LIVE VIEW.
                anchorExpressionSql = desugarDailyAnchor(w);
            }
            ObjList<String> partitionColumnNames = new ObjList<>(w.getPartitionBy().size());
            for (int j = 0, k = w.getPartitionBy().size(); j < k; j++) {
                ExpressionNode pNode = w.getPartitionBy().getQuick(j);
                if (pNode.type != ExpressionNode.LITERAL) {
                    throw SqlException.$(pNode.position,
                            "live view ANCHOR currently requires PARTITION BY to reference base columns directly");
                }
                partitionColumnNames.add(Chars.toString(pNode.token));
            }
            return new LiveViewDefinition.LvAnchorSpec(
                    windowName,
                    anchorKind,
                    anchorExpressionSql,
                    w.getAnchorDailyTimeUs(),
                    w.getAnchorDailyTimeZone() == null ? null : Chars.toString(w.getAnchorDailyTimeZone()),
                    w.getAnchorPosition(),
                    partitionColumnNames
            );
        }
        return null;
    }

    /**
     * Builds the desugared {@code timestamp_floor} / {@code timestamp_floor_utc}
     * expression text equivalent to {@code ANCHOR DAILY 'HH:MM' [tz]}. The runtime
     * side feeds this through the same {@code ensureAnchorFunction} path that
     * {@code ANCHOR EXPRESSION} uses, so the actual reset dispatch is identical.
     * <ul>
     *     <li>UTC midnight (no tz or {@code 'UTC'}): {@code timestamp_floor('1d', <ts>)} — a UTC tz at zero offset adds no information, so the two forms collapse into the same desugared expression.</li>
     *     <li>No-tz non-midnight: {@code timestamp_floor('1d', <ts>, '1970-01-01THH:MM:00.000000Z'::timestamp)}.</li>
     *     <li>Tz-aware: {@code timestamp_floor_utc('1d', <ts>, '1970-01-01THH:MM:00.000000Z'::timestamp, '+00:00', '<tz>')}
     *     using the UTC-encoded variant so DST fall-back keeps bucket distinctness.</li>
     * </ul>
     */
    private static String desugarDailyAnchor(WindowExpression w) throws SqlException {
        ObjList<ExpressionNode> orderBy = w.getOrderBy();
        if (orderBy.size() == 0) {
            throw SqlException.$(w.getAnchorPosition(), "ANCHOR DAILY requires ORDER BY <timestamp column>");
        }
        ExpressionNode tsNode = orderBy.getQuick(0);
        if (tsNode.type != ExpressionNode.LITERAL) {
            throw SqlException.$(tsNode.position, "ANCHOR DAILY requires ORDER BY a base timestamp column");
        }
        long timeUs = w.getAnchorDailyTimeUs();
        CharSequence tz = w.getAnchorDailyTimeZone();
        // A 'UTC' tz at zero offset is a no-op: tz='UTC' and tz=null produce
        // the same buckets and the same desugared form. Collapse the UTC
        // case into the no-tz branch so the persisted anchor expression
        // skips the unnecessary timestamp_floor_utc call on the hot path.
        final boolean tzIsUtc = tz != null && Chars.equalsIgnoreCase("UTC", tz);
        StringSink sink = Misc.getThreadLocalSink();
        if ((tz == null || tzIsUtc) && timeUs == 0) {
            sink.put("timestamp_floor('1d', ").put(tsNode.token).put(')');
        } else if (tz == null) {
            sink.put("timestamp_floor('1d', ").put(tsNode.token).put(", '1970-01-01T");
            putHHMM(sink, timeUs);
            sink.put(":00.000000Z'::timestamp)");
        } else {
            sink.put("timestamp_floor_utc('1d', ").put(tsNode.token).put(", '1970-01-01T");
            putHHMM(sink, timeUs);
            sink.put(":00.000000Z'::timestamp, '+00:00', '").put(tz).put("')");
        }
        return sink.toString();
    }

    /**
     * Renders a duration in microseconds onto an asserted-wording error message,
     * picking the largest unit that divides cleanly. Mirrors the user-facing
     * grammar units accepted by {@link LiveViewDefinition#parseDurationUnit} so
     * the rendered string can be copy-pasted back into a CREATE.
     */
    private static void appendDurationFromMicros(SqlException ex, long micros) {
        if (micros > 0 && micros % Micros.HOUR_MICROS == 0) {
            ex.put(micros / Micros.HOUR_MICROS).put('h');
        } else if (micros > 0 && micros % Micros.MINUTE_MICROS == 0) {
            ex.put(micros / Micros.MINUTE_MICROS).put('m');
        } else if (micros > 0 && micros % Micros.SECOND_MICROS == 0) {
            ex.put(micros / Micros.SECOND_MICROS).put('s');
        } else if (micros > 0 && micros % Micros.MILLI_MICROS == 0) {
            ex.put(micros / Micros.MILLI_MICROS).put("ms");
        } else {
            ex.put(micros).put("us");
        }
    }

    /**
     * Maps the internal duration-unit char ({@code 's'}, {@code 'm'}, {@code 'h'},
     * {@code 'd'}, {@code 'T'} for milliseconds) back to the grammar string a user
     * would type. Used to render values in CREATE-time error messages.
     */
    private static String displayDurationUnit(char unit) {
        return switch (unit) {
            case 's' -> "s";
            case 'm' -> "m";
            case 'h' -> "h";
            case 'd' -> "d";
            case 'T' -> "ms";
            default -> String.valueOf(unit);
        };
    }

    private static int positionOfWindow(WindowExpression w, ExpressionNode fallback) {
        if (w.getAnchorPosition() > 0) {
            return w.getAnchorPosition();
        }
        if (w.getPartitionBy().size() > 0) {
            return w.getPartitionBy().getQuick(0).position;
        }
        if (w.getOrderBy().size() > 0) {
            return w.getOrderBy().getQuick(0).position;
        }
        return fallback != null ? fallback.position : 0;
    }

    private static void putHHMM(StringSink sink, long timeUs) {
        long totalSeconds = timeUs / 1_000_000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        if (hours < 10) {
            sink.put('0');
        }
        sink.put(hours);
        sink.put(':');
        if (minutes < 10) {
            sink.put('0');
        }
        sink.put(minutes);
    }

    /**
     * Validates the ORDER BY clause of every named WINDOW in a live-view SELECT
     * against the requirement that windows order rows by the base table's
     * designated timestamp ascending. The base table is resolved via
     * {@link CairoEngine#getTableTokenIfExists(CharSequence)}; if the base can't
     * be resolved (e.g. concurrent DROP, mistyped name) this validator skips the
     * column-name match so the engine surfaces the primary "base does not exist"
     * error rather than a misleading ORDER-BY message.
     */
    private void validateLiveViewWindowOrderBy(IQueryModel queryModel, CharSequence baseTableName) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> named = queryModel.getNamedWindows();
        if (named.size() == 0) {
            return;
        }
        String designatedTsName = null;
        if (baseTableName != null) {
            final TableToken baseToken = cairoEngine.getTableTokenIfExists(baseTableName);
            if (baseToken != null) {
                try (MetadataCacheReader metaRO = cairoEngine.getMetadataCache().readLock()) {
                    final CairoTable baseTable = metaRO.getTable(baseToken);
                    if (baseTable != null) {
                        CharSequence n = baseTable.getTimestampName();
                        if (n != null) {
                            designatedTsName = Chars.toString(n);
                        }
                    }
                }
            }
        }
        if (designatedTsName == null) {
            return;
        }

        ObjList<CharSequence> keys = named.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            WindowExpression w = named.get(keys.getQuick(i));
            if (w == null) {
                continue;
            }
            ObjList<ExpressionNode> orderBy = w.getOrderBy();
            IntList orderDir = w.getOrderByDirection();
            int fallbackPos = w.getAnchorPosition();
            if (fallbackPos <= 0 && w.getPartitionBy().size() > 0) {
                fallbackPos = w.getPartitionBy().getQuick(0).position;
            }
            if (orderBy.size() == 0) {
                throw SqlException.$(fallbackPos,
                        "live view named WINDOW must ORDER BY ").put(designatedTsName);
            }
            if (orderBy.size() > 1) {
                throw SqlException.$(orderBy.getQuick(1).position,
                                "live view named WINDOW must ORDER BY a single column (")
                        .put(designatedTsName).put(')');
            }
            ExpressionNode tsNode = orderBy.getQuick(0);
            if (tsNode.type != ExpressionNode.LITERAL
                    || !Chars.equalsIgnoreCase(tsNode.token, designatedTsName)) {
                throw SqlException.$(tsNode.position,
                        "live view named WINDOW must ORDER BY ").put(designatedTsName);
            }
            if (orderDir.size() > 0
                    && orderDir.getQuick(0) == IQueryModel.ORDER_DIRECTION_DESCENDING) {
                throw SqlException.$(tsNode.position,
                        "live view named WINDOW must ORDER BY ").put(designatedTsName).put(" ASC");
            }
        }
    }

    private static void validateLiveViewAnchors(IQueryModel queryModel) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> named = queryModel.getNamedWindows();
        ObjList<CharSequence> keys = named.keys();
        int anchoredCount = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            WindowExpression w = named.get(keys.getQuick(i));
            if (w == null) {
                continue;
            }
            if (w.getAnchorKind() == WindowExpression.ANCHOR_KIND_NONE) {
                // An unanchored window is this validator's business only through the
                // bare-unbounded rule, which reads the calls over the window rather
                // than the definition and so runs in rejectBareUnboundedWindows.
                continue;
            }
            anchoredCount++;
            if (anchoredCount > 1) {
                // The LiveViewWindow runtime supports a single anchored WINDOW per
                // LV. Multi-window LVs with different anchors would need per-WINDOW
                // dispatch of resetPartition, which is not implemented yet.
                throw SqlException.$(w.getAnchorPosition(),
                        "live view supports at most one anchored WINDOW in V1");
            }
            if (w.getPartitionBy().size() == 0) {
                // resetPartition is keyed on the partition; the LiveViewWindow
                // anchor map cannot be built without at least one partition
                // column, so the per-partition reset would never dispatch and
                // window state would silently never reset at anchor boundaries.
                throw SqlException.$(w.getAnchorPosition(),
                        "live view anchored WINDOW requires PARTITION BY");
            }
            if (w.isNonDefaultFrame()) {
                throw SqlException.$(w.getAnchorPosition(),
                        "ANCHOR is incompatible with bounded frames; use a separate WINDOW without ANCHOR for ROWS / RANGE windows");
            }
            if (w.getAnchorKind() == WindowExpression.ANCHOR_KIND_EXPRESSION) {
                ExpressionNode expr = w.getAnchorExpression();
                if (expr != null && expr.type == ExpressionNode.CONSTANT) {
                    throw SqlException.$(expr.position,
                            "ANCHOR EXPRESSION must not be a constant");
                }
                walkAnchorExpressionForPurity(expr);
            }
        }

        // Inline OVER (...) clauses attached to SELECT-column function calls.
        // A column may either be an inline WindowExpression itself (e.g. SELECT
        // sum(price) OVER (...) FROM t) or carry a nested inline OVER inside an
        // arithmetic / function tree (e.g. sum(price) OVER (...) + 1). Walk both.
        // One check fires here, the inline-ANCHOR reject: the runtime AnchorSpec
        // is captured only from named WINDOW clauses, so an inline anchor parses
        // but never wires through to the reset path - reject up front and
        // point the user at the named-window form.
        ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                validateInlineWindow((WindowExpression) qc, qc.getAst());
            }
            walkInlineWindows(qc.getAst());
        }
    }

    /**
     * Enforces the bare-unbounded-window rule: a window carrying the default frame
     * ({@code RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}, spelled or left
     * implicit) together with a {@code PARTITION BY} keeps per-partition state for a
     * partition count that grows without bound, so it must carry an ANCHOR to reset.
     * Bounded frames stay allowed without one; so does a single-partition window
     * ({@code OVER ()}), whose state is O(1).
     * <p>
     * The rule reads the calls over a window rather than the window itself, because
     * the state it is about is the calls'.
     * {@link #hasStatelessCurrentRowShape} names the one family that keeps none:
     * {@code last_value} respecting nulls over a frame ending at the current row,
     * whose {@code computeNext} reads the row it was handed and whose partitioned
     * implementation is constructed with no map at all. A window every call of which
     * is that family is admitted; one call that is not takes the reject for the whole
     * window, since a single growing map is enough.
     * <p>
     * The carve-out additionally requires the window to ORDER BY. An unordered
     * default RANGE frame makes every row a peer of every other and compiles to the
     * whole-partition {@code last_value} - a per-partition map after all - so it keeps
     * this reject rather than being handed on to a downstream one.
     * <p>
     * A named window no call references keeps the reject too. Vacuously, every one of
     * its zero calls is stateless, but admitting a definition on the strength of
     * having no user would relax more than the shape this carve-out proves.
     * <p>
     * The same unreferenced-definition rule covers anchored windows, for a different
     * reason. An ANCHOR bounds the state of the calls over its window, so a definition
     * no call references anchors nothing: the runtime would capture the anchor spec,
     * find no function for {@code resetPartition} to dispatch to, and fail every refresh
     * cycle until the flush-retry budget invalidates the view. Refusing it at CREATE
     * reports the mistake where the user can still fix it, and keeps the runtime's
     * "an anchored window always has at least one function" invariant load-bearing.
     */
    private static void rejectBareUnboundedWindows(IQueryModel queryModel) throws SqlException {
        final LowerCaseCharSequenceObjHashMap<WindowExpression> named = queryModel.getNamedWindows();
        final ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        // Named definitions a stateless call has vouched for. Collected during the
        // walk and read after it, because a definition is only cleared by its calls.
        final ObjList<WindowExpression> vouchedFor = new ObjList<>();
        // Named definitions some call resolves to, whether or not that call clears the
        // bare-unbounded rule. Read after the walk by the unreferenced-definition arms.
        final ObjList<WindowExpression> referenced = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                rejectBareUnboundedWindowCall(qc.getAst(), (WindowExpression) qc, named, vouchedFor, referenced);
            }
            // Window calls nested in an arithmetic / function tree carry their OVER
            // clause on the function node itself; walk for those too.
            walkForBareUnboundedWindow(qc.getAst(), named, vouchedFor, referenced);
        }
        ObjList<CharSequence> keys = named.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            WindowExpression w = named.get(keys.getQuick(i));
            if (w == null) {
                continue;
            }
            if (w.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE) {
                if (referenced.indexOf(w) < 0) {
                    throw SqlException.$(positionOfWindow(w, null), "live view anchored WINDOW '")
                            .put(keys.getQuick(i))
                            .put("' is not referenced by any window function; an ANCHOR bounds the state of the calls over its window, so it has nothing to reset. A window inheriting from it, e.g. WINDOW w2 AS (")
                            .put(keys.getQuick(i))
                            .put(" ORDER BY ts), does not carry its ANCHOR either - the call has to name it directly, e.g. OVER ")
                            .put(keys.getQuick(i));
                }
                continue;
            }
            if (isBareUnboundedWindow(w) && vouchedFor.indexOf(w) < 0) {
                throw bareUnboundedWindowReject(positionOfWindow(w, null));
            }
        }
    }

    /**
     * Recursive AST walk for the nested case of {@link #rejectBareUnboundedWindows}:
     * a window function with an inline {@code OVER (...)} embedded inside a larger
     * expression carries its window on {@code node.windowExpression}.
     */
    private static void walkForBareUnboundedWindow(
            ExpressionNode node,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named,
            ObjList<WindowExpression> vouchedFor,
            ObjList<WindowExpression> referenced
    ) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            rejectBareUnboundedWindowCall(node, node.windowExpression, named, vouchedFor, referenced);
        }
        if (node.paramCount < 3) {
            walkForBareUnboundedWindow(node.lhs, named, vouchedFor, referenced);
            walkForBareUnboundedWindow(node.rhs, named, vouchedFor, referenced);
        } else if (node.args != null) {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                walkForBareUnboundedWindow(node.args.getQuick(i), named, vouchedFor, referenced);
            }
        }
    }

    /**
     * Applies the bare-unbounded-window rule to one window call, recording the
     * definition in {@code vouchedFor} when the call clears it. See
     * {@link #rejectBareUnboundedWindows}.
     */
    private static void rejectBareUnboundedWindowCall(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named,
            ObjList<WindowExpression> vouchedFor,
            ObjList<WindowExpression> referenced
    ) throws SqlException {
        if (fn == null || fn.type != ExpressionNode.FUNCTION || fn.token == null) {
            return;
        }
        // Record the definition this call resolves to before the anchored short-circuit
        // below: an anchored definition is exempt from the bare-unbounded rule but still
        // has to have a user, and this is the only walk that sees the calls.
        if (window != null && window.isNamedWindowReference()) {
            final WindowExpression def = named.get(window.getWindowName());
            if (def != null && referenced.indexOf(def) < 0) {
                referenced.add(def);
            }
        }
        if (isAnchoredWindow(window, named)) {
            return;
        }
        // A named reference carries neither frame nor PARTITION BY of its own, so
        // both halves of the shape are read off the definition it resolves to.
        final WindowExpression frame = resolveFrameWindow(window, named);
        if (frame == null || !isBareUnboundedWindow(frame)) {
            return;
        }
        if (frame.getOrderBy().size() > 0 && hasStatelessCurrentRowShape(fn, window, named)) {
            vouchedFor.add(frame);
            return;
        }
        throw bareUnboundedWindowReject(positionOfWindow(frame, fn));
    }

    /**
     * Reports whether {@code window} is a PARTITION-BY-keyed window over the default
     * frame - the shape {@link #rejectBareUnboundedWindows} governs. Takes the
     * definition a named reference resolves to, not the reference.
     */
    private static boolean isBareUnboundedWindow(WindowExpression window) {
        return !window.isNonDefaultFrame() && window.getPartitionBy().size() > 0;
    }

    private static SqlException bareUnboundedWindowReject(int position) {
        return SqlException.$(position,
                "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported. Add an ANCHOR to bound per-partition state, e.g. ANCHOR EXPRESSION timestamp_floor('1d', ts)");
    }

    /**
     * Parser-side half of the finite-influence scope cut (see
     * {@code io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind}).
     * The localized out-of-order repair the checkpoint timeline relies on can
     * only bound its work when every window function has a finite forward
     * influence boundary {@code H}. Two shapes have none, and both are rejected
     * at CREATE, naming the function:
     * <ul>
     *     <li>Ranking functions - {@code row_number()}, {@code rank()},
     *     {@code dense_rank()} - running unanchored: an out-of-order row shifts
     *     every following row's rank without bound.</li>
     *     <li>Any window function over a frame starting at UNBOUNDED PRECEDING:
     *     an out-of-order row joins the frame of every following row, so it can
     *     move every later value the function produces. That is plainly true of
     *     an accumulator, and true of the value functions too - a row inserted
     *     below a partition's current earliest row becomes the
     *     {@code first_value} of every frame above it, and shifts what
     *     {@code nth_value} counts to.</li>
     * </ul>
     * The rule reads the frame rather than the function, so a window function
     * added later is covered without being listed anywhere. It still costs the
     * shapes whose influence is in fact finite, and two of those are now proven
     * and carved out, both of them {@code last_value} respecting nulls:
     * {@link #hasHighBoundStateExtent} admits {@code ROWS ... AND K PRECEDING},
     * which accumulates nothing, so its state is the {@code K} values behind it
     * and a late row moves only the {@code K} outputs above it; and
     * {@link #hasStatelessCurrentRowShape} admits a frame ending at
     * {@code CURRENT ROW}, which reads the row it is handed and moves nothing at
     * all. Every other unbounded start keeps the reject, because an unproven
     * bound means a late row replays the whole history rather than an interval,
     * and a frame the planner can bound is the price of admission.
     * <p>
     * The anchored, per-segment-reset forms have a finite {@code H} (the
     * segment end) and stay eligible; they route through the fixed-anchor
     * dependency kind.
     * <p>
     * Partitioned-but-unanchored ranking (e.g. {@code row_number() OVER
     * (PARTITION BY sym ORDER BY ts)}) is already turned away by
     * {@link #rejectBareUnboundedWindows}; this closes the remaining
     * single-partition {@code OVER ()} / {@code OVER (ORDER BY ts)} hole, which
     * that rule deliberately leaves open for O(1)-state single-partition
     * windows. The frame reject closes that same
     * hole plus the one an explicit frame opens: a window declaring
     * {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW} is a non-default
     * frame, so the bare-unbounded rule skips it however it is partitioned.
     */
    private static void validateLiveViewFiniteInfluence(IQueryModel queryModel) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> named = queryModel.getNamedWindows();
        ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                rejectUnboundedInfluence(qc.getAst(), (WindowExpression) qc, named);
            }
            // Window calls nested in an arithmetic / function tree carry their
            // OVER clause on the function node itself; walk for those too.
            walkForUnboundedInfluence(qc.getAst(), named);
        }
    }

    /**
     * Recursive AST walk for the nested case of {@link #validateLiveViewFiniteInfluence}:
     * a window function with an inline {@code OVER (...)} embedded inside a larger
     * expression carries its window on {@code node.windowExpression}.
     */
    private static void walkForUnboundedInfluence(
            ExpressionNode node,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            rejectUnboundedInfluence(node, node.windowExpression, named);
        }
        if (node.paramCount < 3) {
            walkForUnboundedInfluence(node.lhs, named);
            walkForUnboundedInfluence(node.rhs, named);
        } else if (node.args != null) {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                walkForUnboundedInfluence(node.args.getQuick(i), named);
            }
        }
    }

    /**
     * Applies both finite-influence rejects to one window call. See
     * {@link #validateLiveViewFiniteInfluence}.
     */
    private static void rejectUnboundedInfluence(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) throws SqlException {
        if (fn == null || fn.type != ExpressionNode.FUNCTION || fn.token == null) {
            return;
        }
        rejectUnanchoredRanking(fn, window, named);
        rejectUnboundedFrameStart(fn, window, named);
    }

    /**
     * Throws when {@code fn} reads from an unbounded frame start over a window
     * no anchor resets. See {@link #validateLiveViewFiniteInfluence}.
     */
    private static void rejectUnboundedFrameStart(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) throws SqlException {
        if (isAnchoredWindow(window, named)
                || !hasUnboundedFrameStart(window, named)
                || hasHighBoundStateExtent(fn, window, named)
                || hasStatelessCurrentRowShape(fn, window, named)) {
            return;
        }
        throw SqlException.$(fn.position, "live view select cannot use ")
                .put(fn.token)
                .put("() over a frame starting at UNBOUNDED PRECEDING; it has no finite out-of-order influence boundary, ")
                .put("so a late row would replay the whole history. ")
                .put("Bound the frame, e.g. ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW, or add an ANCHOR to reset per segment, ")
                .put("e.g. WINDOW w AS (PARTITION BY <key> ORDER BY <ts> ANCHOR EXPRESSION timestamp_floor('1d', <ts>))");
    }

    /**
     * Reports whether the frame governing {@code window} starts at UNBOUNDED
     * PRECEDING. A frame start is bounded only when it names a row or time
     * offset ({@code N PRECEDING}) or the current row; the parser leaves that
     * offset in {@code rowsLoExpr}, so a PRECEDING start with no expression is
     * the unbounded one. This is also the shape a window declaring no frame
     * carries, since the SQL default is RANGE BETWEEN UNBOUNDED PRECEDING AND
     * CURRENT ROW, and the shape {@code CUMULATIVE} desugars to.
     */
    private static boolean hasUnboundedFrameStart(
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) {
        final WindowExpression frame = resolveFrameWindow(window, named);
        if (frame == null) {
            // An unresolvable reference carries no frame this parse can read;
            // the default it would inherit is unbounded, so treat it as such.
            return true;
        }
        return frame.getRowsLoKind() == WindowExpression.PRECEDING && frame.getRowsLoExpr() == null;
    }

    /**
     * Reports whether {@code fn} is the one call whose state the frame's <b>end</b>
     * bounds rather than its start, and which therefore keeps a finite forward
     * influence over an unbounded frame start: {@code last_value} respecting
     * nulls over {@code ROWS BETWEEN ... AND K PRECEDING}.
     * <p>
     * It emits the row {@code K} back and accumulates nothing, so its state is
     * the {@code K} values behind the current row however far back the frame
     * says it starts, and a late row shifts only the {@code K} outputs above it.
     * That is what the repair planner reads as the descriptor's state extent,
     * and the compiler applies the same three narrowings this does.
     * {@code IGNORE NULLS} scans the whole frame for the last non-null, so it is
     * bounded by the frame's start like an accumulator; and a frame end at the
     * current row leaves no ring at all - it compiles to a stateless per-row
     * projection, a family whose admission needs a stateless window function to
     * be able to declare itself.
     * <p>
     * A RANGE end keeps the reject for a reason of its own: it is a timestamp
     * offset rather than a row, so it names no row for the state to be. The
     * emitted value is the newest base row at or below {@code t - V}, which an
     * unbounded start lets reach arbitrarily far back, and a row inserted at
     * {@code m} moves every output from {@code m + V} up to the {@code + V} of
     * whichever base row supersedes it next. Both distances are the data's
     * rather than the lag's, so no bound follows from {@code V} - rows at 0s,
     * 100s and 200s under a one-second lag move the output at 100s from a change
     * at 50s. The bounded RANGE start needs none of this: its own width bounds
     * the state and the forward influence alike.
     * <p>
     * The shape is read syntactically, because the parser has neither folded
     * frame bound expressions to numbers nor picked a factory yet. So a couple
     * of spellings pass here and are turned away further on for carrying no
     * checkpoint surface: {@code AND 0 PRECEDING}, which folds to the stateless
     * family, and a window with no {@code PARTITION BY}, whose ROWS-frame
     * implementation has no checkpoint state whatever its frame starts at. For
     * those this decides which reject names them, not whether they are one.
     */
    private static boolean hasHighBoundStateExtent(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) {
        // IGNORE NULLS lives on the call rather than on the named definition, so
        // the two halves are read from the windows that carry them.
        if (window.isIgnoreNulls() || !Chars.equalsLowerCaseAscii(fn.token, "last_value")) {
            return false;
        }
        final WindowExpression frame = resolveFrameWindow(window, named);
        return frame != null
                && frame.getFramingMode() == WindowExpression.FRAMING_ROWS
                && ((frame.getRowsHiKind() == WindowExpression.PRECEDING && frame.getRowsHiExpr() != null)
                // EXCLUDE CURRENT ROW is the same shape with the smallest lag: the runtime
                // rewrites the frame end to one row below the current one before any factory
                // sees it, so the ring holds a single value.
                || (frame.getRowsHiKind() == WindowExpression.CURRENT
                && frame.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW));
    }

    /**
     * Reports whether {@code fn} is the call that reads no history at all over an
     * unbounded frame start: {@code last_value} respecting nulls over a frame
     * ending at {@code CURRENT ROW}.
     * <p>
     * Its whole {@code computeNext} is a read of the argument off the row it was
     * handed, so it accumulates nothing, keeps nothing, and moves no output but
     * the changed row's own. That holds however far back the frame says it
     * starts - an unbounded start and a bounded one compile to one class - which
     * is what makes the reject an over-rejection here rather than a scope cut.
     * <p>
     * The two narrowings match the family the factory dispatches to.
     * {@code IGNORE NULLS} keeps the last non-null across rows and so is bounded
     * by the frame's start like an accumulator. {@code EXCLUDE CURRENT ROW}
     * rewrites the frame end to one row below the current one before any factory
     * sees it, which is a ring of one value rather than no ring, and
     * {@link #hasHighBoundStateExtent} is what admits that shape.
     * <p>
     * Read syntactically like its sibling, so a spelling that folds to some other
     * family passes here and is turned away downstream instead: a {@code RANGE}
     * default frame with no {@code ORDER BY} makes every row a peer of every
     * other and compiles to the whole-partition or whole-result-set
     * {@code last_value}, whose influence really is unbounded, and the
     * per-function checkpoint gate or the factory-shape one names it. This
     * decides which reject such a query gets, not whether it is one.
     * <p>
     * A PARTITION-BY-keyed window carrying the default frame - {@code OVER
     * (PARTITION BY <key> ORDER BY <ts>)} and its explicit {@code RANGE BETWEEN
     * UNBOUNDED PRECEDING AND CURRENT ROW} spelling, which
     * {@code WindowExpression.isNonDefaultFrame()} reads as the same thing - answers
     * to the bare-unbounded-window rule as well, and this predicate is what clears
     * it there too: {@link #rejectBareUnboundedWindows} admits such a window when
     * every call over it has this shape, on the strength of the same no-map
     * implementation. It adds one narrowing of its own, an ORDER BY, because an
     * unordered default RANGE frame compiles to the whole-partition family instead.
     */
    private static boolean hasStatelessCurrentRowShape(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) {
        if (window.isIgnoreNulls() || !Chars.equalsLowerCaseAscii(fn.token, "last_value")) {
            return false;
        }
        final WindowExpression frame = resolveFrameWindow(window, named);
        return frame != null
                && frame.getRowsHiKind() == WindowExpression.CURRENT
                && frame.getExclusionKind() != WindowExpression.EXCLUDE_CURRENT_ROW;
    }

    /**
     * Resolves the window whose frame governs {@code window}: an {@code OVER w}
     * reference carries no frame of its own and takes the named definition's.
     * <p>
     * Base-window inheritance ({@code WINDOW w2 AS (w1 ...)}) is not followed,
     * because a live view cannot reach it: the optimizer expands an inherited
     * window into a cached, multi-pass factory, which the live-view eligibility
     * gate turns away before this frame ever matters. Were that to change, an
     * inheriting window would read as its own default frame - unbounded - and
     * be rejected, which is the conservative direction.
     */
    private static WindowExpression resolveFrameWindow(
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) {
        if (window == null) {
            return null;
        }
        return window.isNamedWindowReference() ? named.get(window.getWindowName()) : window;
    }

    /**
     * Throws when {@code fn} is a ranking window function ({@code row_number} /
     * {@code rank} / {@code dense_rank}) whose window {@code window} is not
     * anchored. See {@link #validateLiveViewFiniteInfluence}.
     */
    private static void rejectUnanchoredRanking(
            ExpressionNode fn,
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) throws SqlException {
        if (!isRankingFunctionToken(fn.token)) {
            return;
        }
        if (isAnchoredWindow(window, named)) {
            return;
        }
        throw SqlException.$(fn.position, "live view select cannot use ")
                .put(fn.token)
                .put("() without an anchored WINDOW; it has no finite out-of-order influence boundary. ")
                .put("Add an ANCHOR to reset per segment, e.g. WINDOW w AS (PARTITION BY <key> ORDER BY <ts> ANCHOR EXPRESSION timestamp_floor('1d', <ts>))");
    }

    /**
     * Resolves whether {@code window} carries an ANCHOR clause. A named-window
     * reference ({@code OVER w}) inherits the anchor kind of its definition in
     * {@code named}; an inline window carries its own.
     */
    private static boolean isAnchoredWindow(
            WindowExpression window,
            LowerCaseCharSequenceObjHashMap<WindowExpression> named
    ) {
        if (window == null) {
            return false;
        }
        if (window.isNamedWindowReference()) {
            WindowExpression def = named.get(window.getWindowName());
            return def != null && def.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE;
        }
        return window.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE;
    }

    private static boolean isRankingFunctionToken(CharSequence token) {
        return token != null
                && (Chars.equalsLowerCaseAscii(token, "row_number")
                || Chars.equalsLowerCaseAscii(token, "rank")
                || Chars.equalsLowerCaseAscii(token, "dense_rank"));
    }

    /**
     * Walks the SELECT columns and inline OVER trees looking for any
     * {@code lead(...)} function call. The factory-side reject inside
     * {@code CairoEngine} only fires when the planner picks a window factory
     * exposing lead; a future planner change could bypass both factories for
     * a lead-only query. This walk is the parser-level safety net.
     */
    private static void rejectLeadInSelect(IQueryModel queryModel) throws SqlException {
        ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            walkForLeadCall(columns.getQuick(i).getAst());
        }
    }

    private static void validateInlineWindow(WindowExpression w, ExpressionNode fallback) throws SqlException {
        // An OVER <named-window> reference inherits all checks from the named
        // definition (already validated upstream in this method).
        if (w.isNamedWindowReference()) {
            return;
        }
        // Inline OVER (... ANCHOR ...) parses but the runtime AnchorSpec is
        // captured only from named WINDOW clauses, so an inline anchor would
        // silently never reset. Reject up front and direct the user at the
        // named-window form.
        if (w.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE) {
            throw SqlException.$(positionOfWindow(w, fallback),
                    "ANCHOR is only supported on named WINDOW clauses; declare the window with WINDOW <name> AS (...) and reference it from the SELECT");
        }
        // The bare-unbounded reject an inline window also answers to lives in
        // rejectBareUnboundedWindows, which reads the call the window belongs to.
    }

    /**
     * Recursive AST walk implementing the parser-side half of the anchor-expression
     * validator. Rejects subqueries, bind variables, and function calls that the planner
     * would later resolve to runtime-state ({@code now}, {@code current_timestamp},
     * {@code systimestamp}) or random ({@code rnd_*}) functions. The function-property
     * checks (constant-fold, isGroupBy, isRandom, isRuntimeConstant, isNonDeterministic)
     * are Pass 2; they need the compiled {@code io.questdb.cairo.sql.Function} tree
     * and live in {@code CairoEngine.validateAnchorPurity} (called at CREATE time
     * after the SELECT factory has been compiled).
     */
    private static void walkAnchorExpressionForPurity(ExpressionNode node) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.QUERY) {
            throw SqlException.$(node.position, "ANCHOR EXPRESSION must not contain subqueries");
        }
        if (node.type == ExpressionNode.BIND_VARIABLE) {
            throw SqlException.$(node.position, "ANCHOR EXPRESSION must not reference bind variables");
        }
        if (node.type == ExpressionNode.FUNCTION) {
            CharSequence token = node.token;
            if (token != null) {
                if (Chars.startsWithLowerCase(token, "rnd_")) {
                    throw SqlException.$(node.position,
                            "ANCHOR EXPRESSION must be deterministic; ").put(token).put("() is not allowed");
                }
                if (SqlKeywords.isNowKeyword(token)
                        || isCurrentTimestampToken(token)
                        || isSystimestampToken(token)) {
                    throw SqlException.$(node.position,
                            "ANCHOR EXPRESSION must be deterministic; ").put(token).put("() is not allowed");
                }
            }
        }
        if (node.lhs != null) {
            walkAnchorExpressionForPurity(node.lhs);
        }
        if (node.rhs != null) {
            walkAnchorExpressionForPurity(node.rhs);
        }
        if (node.args != null) {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                walkAnchorExpressionForPurity(node.args.getQuick(i));
            }
        }
    }

    /**
     * Recursive AST walk for the parser-side lead() reject. Any function
     * node whose token equals "lead" is rejected at its position with the
     * same wording the factory-side reject in CairoEngine uses.
     */
    private static void walkForLeadCall(ExpressionNode node) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.FUNCTION && node.token != null
                && Chars.equalsLowerCaseAscii(node.token, "lead")) {
            throw SqlException.$(node.position, "lead() is not supported in live views; use lag() for lookback");
        }
        if (node.paramCount < 3) {
            walkForLeadCall(node.lhs);
            walkForLeadCall(node.rhs);
        } else if (node.args != null) {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                walkForLeadCall(node.args.getQuick(i));
            }
        }
    }

    private static void walkInlineWindows(ExpressionNode node) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            validateInlineWindow(node.windowExpression, node);
        }
        if (node.paramCount < 3) {
            walkInlineWindows(node.lhs);
            walkInlineWindows(node.rhs);
        } else if (node.args != null) {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                walkInlineWindows(node.args.getQuick(i));
            }
        }
    }

    private static boolean isCurrentTimestampToken(CharSequence token) {
        return token.length() == 17
                && (token.charAt(0) | 32) == 'c'
                && (token.charAt(1) | 32) == 'u'
                && (token.charAt(2) | 32) == 'r'
                && (token.charAt(3) | 32) == 'r'
                && (token.charAt(4) | 32) == 'e'
                && (token.charAt(5) | 32) == 'n'
                && (token.charAt(6) | 32) == 't'
                && token.charAt(7) == '_'
                && (token.charAt(8) | 32) == 't'
                && (token.charAt(9) | 32) == 'i'
                && (token.charAt(10) | 32) == 'm'
                && (token.charAt(11) | 32) == 'e'
                && (token.charAt(12) | 32) == 's'
                && (token.charAt(13) | 32) == 't'
                && (token.charAt(14) | 32) == 'a'
                && (token.charAt(15) | 32) == 'm'
                && (token.charAt(16) | 32) == 'p';
    }

    private static boolean isSystimestampToken(CharSequence token) {
        return token.length() == 12
                && (token.charAt(0) | 32) == 's'
                && (token.charAt(1) | 32) == 'y'
                && (token.charAt(2) | 32) == 's'
                && (token.charAt(3) | 32) == 't'
                && (token.charAt(4) | 32) == 'i'
                && (token.charAt(5) | 32) == 'm'
                && (token.charAt(6) | 32) == 'e'
                && (token.charAt(7) | 32) == 's'
                && (token.charAt(8) | 32) == 't'
                && (token.charAt(9) | 32) == 'a'
                && (token.charAt(10) | 32) == 'm'
                && (token.charAt(11) | 32) == 'p';
    }

    private ExecutionModel parseCreateMatView(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CreateMatViewOperationBuilderImpl mvOpBuilder = createMatViewOperationBuilder;
        final CreateTableOperationBuilderImpl tableOpBuilder = mvOpBuilder.getCreateTableOperationBuilder();
        mvOpBuilder.clear(); // clears tableOpBuilder too
        tableOpBuilder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        tableOpBuilder.setMaxUncommittedRows(configuration.getMaxUncommittedRows());
        tableOpBuilder.setWalEnabled(true); // mat view is always WAL-enabled

        expectTok(lexer, "view");
        CharSequence tok = tok(lexer, "view name or 'if'");
        if (isIfKeyword(tok)) {
            if (isNotKeyword(tok(lexer, "'not'")) && isExistsKeyword(tok(lexer, "'exists'"))) {
                tableOpBuilder.setIgnoreIfExists(true);
                tok = tok(lexer, "view name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        }
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        tableOpBuilder.setTableNameExpr(nextLiteral(
                assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()
        ));

        tok = tok(lexer, "'as' or 'with' or 'refresh'");
        CharSequence baseTableName = null;
        int baseTableNamePos = 0;
        if (isWithKeyword(tok)) {
            expectTok(lexer, "base");
            tok = tok(lexer, "base table");
            baseTableName = sansPublicSchema(tok, lexer);
            assertNameIsQuotedOrNotAKeyword(baseTableName, lexer.lastTokenPosition());
            baseTableName = unquote(baseTableName);
            baseTableNamePos = lexer.lastTokenPosition();
            tok = tok(lexer, "'as' or 'refresh'");
        }

        boolean refreshDefined = false;
        int refreshType = MatViewDefinition.REFRESH_TYPE_IMMEDIATE;
        boolean deferred = false;
        if (isRefreshKeyword(tok)) {
            refreshDefined = true;
            tok = tok(lexer, "'immediate' or 'manual' or 'period' or 'every' or 'as'");
            int every = 0;
            char everyUnit = 0;
            // 'incremental' is obsolete, replaced with 'immediate'
            if (isIncrementalKeyword(tok)) {
                tok = tok(lexer, "'as'");
            } else if (isImmediateKeyword(tok)) {
                tok = tok(lexer, "'deferred' or 'period' or 'as'");
            } else if (isManualKeyword(tok)) {
                refreshType = MatViewDefinition.REFRESH_TYPE_MANUAL;
                tok = tok(lexer, "'deferred' or 'period' or 'as'");
            } else if (isEveryKeyword(tok)) {
                tok = tok(lexer, "interval");
                every = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                everyUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                validateMatViewEveryUnit(everyUnit, lexer.lastTokenPosition());
                refreshType = MatViewDefinition.REFRESH_TYPE_TIMER;
                tok = tok(lexer, "'deferred' or 'start' or 'period' or 'as'");
            }

            if (isDeferredKeyword(tok)) {
                deferred = true;
                if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                    tok = tok(lexer, "'start' or 'period' or 'as'");
                } else {
                    tok = tok(lexer, "'period' or 'as'");
                }
            }

            // Timer uses microsecond precision for start time calculation
            if (isPeriodKeyword(tok)) {
                expectTok(lexer, "(");
                tok = tok(lexer, "'length' or 'sample'");
                if (isLengthKeyword(tok)) {
                    // REFRESH ... PERIOD(LENGTH <interval> [TIME ZONE '<timezone>'] [DELAY <interval>])
                    tok = tok(lexer, "LENGTH interval");
                    final int length = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                    final char lengthUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                    validateMatViewPeriodLength(length, lengthUnit, lexer.lastTokenPosition());
                    final TimestampSampler periodSamplerMicros = TimestampSamplerFactory.getInstance(
                            MicrosTimestampDriver.INSTANCE,
                            length,
                            lengthUnit,
                            lexer.lastTokenPosition()
                    );
                    tok = tok(lexer, "'time zone' or 'delay' or ')'");

                    TimeZoneRules tzRulesMicros = null;
                    String tz = null;
                    if (isTimeKeyword(tok)) {
                        expectTok(lexer, "zone");
                        tok = tok(lexer, "TIME ZONE name");
                        if (Chars.equals(tok, ')') || isDelayKeyword(tok)) {
                            throw SqlException.position(lexer.lastTokenPosition()).put("TIME ZONE name expected");
                        }
                        tz = unquote(tok).toString();
                        try {
                            tzRulesMicros = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                        } catch (CairoException e) {
                            throw SqlException.position(lexer.lastTokenPosition()).put(e.getFlyweightMessage());
                        }
                        tok = tok(lexer, "'delay' or ')'");
                    }

                    int delay = 0;
                    char delayUnit = 0;
                    if (isDelayKeyword(tok)) {
                        tok = tok(lexer, "DELAY interval");
                        delay = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                        delayUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                        validateMatViewPeriodDelay(length, lengthUnit, delay, delayUnit, lexer.lastTokenPosition());
                        tok = tok(lexer, "')'");
                    }

                    // Period timer start is at the boundary of the current period.
                    final long nowMicros = configuration.getMicrosecondClock().getTicks();
                    final long nowLocalMicros = tzRulesMicros != null ? nowMicros + tzRulesMicros.getOffset(nowMicros) : nowMicros;
                    final long startUs = periodSamplerMicros.round(nowLocalMicros);

                    mvOpBuilder.setTimer(tz, startUs, every, everyUnit);
                    mvOpBuilder.setPeriodLength(length, lengthUnit, delay, delayUnit);
                } else if (isSampleKeyword(tok)) {
                    // REFRESH ... PERIOD(SAMPLE BY INTERVAL)
                    expectTok(lexer, "by");
                    expectTok(lexer, "interval");
                    tok = tok(lexer, "')'");

                    mvOpBuilder.setTimer(null, 0, every, everyUnit);
                    // Set length to -1 to define the period later, once we parse the query.
                    mvOpBuilder.setPeriodLength(-1, (char) 0, 0, (char) 0);
                } else {
                    throw SqlException.position(lexer.lastTokenPosition()).put("'length' or 'sample' expected");
                }

                if (!Chars.equals(tok, ')')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
                tok = tok(lexer, "'as'");
            } else if (!isAsKeyword(tok)) {
                // REFRESH EVERY <interval> [START '<datetime>' [TIME ZONE '<timezone>']]
                if (refreshType != MatViewDefinition.REFRESH_TYPE_TIMER) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'as' expected");
                }
                // Use the current time as the start timestamp if it wasn't specified.
                long startUs = configuration.getMicrosecondClock().getTicks();
                String tz = null;
                if (isStartKeyword(tok)) {
                    tok = tok(lexer, "START timestamp");
                    try {
                        startUs = MicrosTimestampDriver.INSTANCE.parseFloorLiteral(GenericLexer.unquote(tok));
                    } catch (NumericException e) {
                        throw SqlException.$(lexer.lastTokenPosition(), "invalid START timestamp value");
                    }
                    tok = tok(lexer, "'time zone' or 'as'");

                    if (isTimeKeyword(tok)) {
                        expectTok(lexer, "zone");
                        tok = tok(lexer, "TIME ZONE name");
                        tz = unquote(tok).toString();
                        tok = tok(lexer, "'as'");
                    }
                }
                mvOpBuilder.setTimer(tz, startUs, every, everyUnit);
            } else if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // REFRESH EVERY <interval> AS
                // Don't forget to set timer params.
                final long startUs = configuration.getMicrosecondClock().getTicks();
                mvOpBuilder.setTimer(null, startUs, every, everyUnit);
            }
        }
        mvOpBuilder.setRefreshType(refreshType);
        mvOpBuilder.setDeferred(deferred);

        boolean enclosedInParentheses;
        if (isAsKeyword(tok)) {
            int startOfQuery = lexer.getPosition();
            tok = tok(lexer, "'(' or 'with' or 'select'");
            enclosedInParentheses = Chars.equals(tok, '(');
            if (enclosedInParentheses) {
                startOfQuery = lexer.getPosition();
                tok = tok(lexer, "'with' or 'select'");
            }

            // Parse SELECT for the sake of basic SQL validation.
            // It'll be compiled and optimized later, at the execution phase.
            if (isWithKeyword(tok)) {
                parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, null);
                // CTEs require SELECT to be specified
                expectTok(lexer, "select");
            }
            lexer.unparseLast();
            final IQueryModel queryModel = parseDml(lexer, lexer.getPosition(), sqlParserCallback);
            final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

            tableNames.clear();
            tableNamePositions.clear();
            SqlUtil.collectAllTableNames(queryModel, tableNames, tableNamePositions);

            // Find base table name if not set explicitly.
            if (baseTableName == null) {
                if (tableNames.size() < 1) {
                    throw SqlException.$(startOfQuery, "missing base table, materialized views have to be based on a table");
                }
                if (tableNames.size() > 1) {
                    throw SqlException.$(startOfQuery, "query references multiple tables (views are expanded to their underlying physical tables), use 'WITH BASE' to explicitly select the base table");
                }
                baseTableName = Chars.toString(tableNames.getAny());
                baseTableNamePos = tableNamePositions.getQuick(0);
            }

            mvOpBuilder.setBaseTableNamePosition(baseTableNamePos);
            final String baseTableNameStr = Chars.toString(baseTableName);
            mvOpBuilder.setBaseTableName(baseTableNameStr);

            // Basic validation - check all nested models that read from the base table for window functions, unions, FROM-TO, or FILL.
            if (!tableNames.contains(baseTableNameStr)) {
                final TableToken baseTableToken = cairoEngine.getTableTokenIfExists(baseTableNameStr);
                if (baseTableToken != null && baseTableToken.isView()) {
                    throw SqlException.position(baseTableNamePos)
                            .put("base table should be a physical table, cannot be a view: ").put(baseTableName);
                }
                throw SqlException.position(baseTableNamePos)
                        .put("base table is not referenced in materialized view query: ").put(baseTableName);
            }
            validateMatViewQuery(queryModel, baseTableNameStr);

            final IQueryModel nestedModel = queryModel.getNestedModel();
            if (nestedModel != null) {
                if (nestedModel.getSampleByTimezoneName() != null) {
                    mvOpBuilder.setTimeZone(unquote(nestedModel.getSampleByTimezoneName().token).toString());
                }
                if (nestedModel.getSampleByOffset() != null) {
                    mvOpBuilder.setTimeZoneOffset(unquote(nestedModel.getSampleByOffset().token).toString());
                }
            }

            final String matViewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);
            tableOpBuilder.setSelectText(matViewSql, startOfQuery);
            tableOpBuilder.setSelectModel(queryModel); // transient model, for toSink() purposes only

            if (enclosedInParentheses) {
                expectTok(lexer, ')');
            } else {
                // We expect nothing more when there are no parentheses. Trailing clauses such as
                // EXPIRE ROWS / TIMESTAMP / PARTITION BY / TTL are only recognised when the SELECT is
                // wrapped in parentheses ("AS ( SELECT ... ) EXPIRE ROWS ..."); without them the SELECT
                // parser greedily consumes the trailing EXPIRE keyword as a table alias and reports the
                // following token (ROWS) as unexpected, which still signals the malformed statement.
                tok = optTok(lexer);
                if (tok != null && !Chars.equals(tok, ';')) {
                    throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
                }
                return mvOpBuilder;
            }
        } else {
            if (refreshDefined) {
                throw SqlException.position(lexer.lastTokenPosition()).put("'as' expected");
            }
            throw SqlException.position(lexer.lastTokenPosition()).put("'refresh' or 'as' expected");
        }

        // Optional clauses that go after the parentheses.

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index'");
            if (isIndexKeyword(tok)) {
                parseCreateTableIndexDef(lexer, false);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        final ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            tableOpBuilder.setTimestampExpr(timestamp);
            tok = optTok(lexer);
        }

        final ExpressionNode partitionByExpr = parseCreateTablePartition(lexer, tok);
        int partitionBy = -1;
        if (partitionByExpr != null) {
            partitionBy = PartitionBy.fromString(partitionByExpr.token);
            if (partitionBy == -1) {
                throw SqlException.$(partitionByExpr.position, "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                throw SqlException.position(partitionByExpr.position).put("materialized view has to be partitioned");
            }
            tableOpBuilder.setPartitionByExpr(partitionByExpr);
            tok = optTok(lexer);
        }

        tok = sqlParserCallback.parseTtlSettings(lexer, tok, partitionBy, tableOpBuilder, true);

        // Optional: EXPIRE ROWS WHEN <predicate> [CLEANUP EVERY <duration>]. Mirrors CREATE TABLE:
        // it sits after the TTL/partition clauses and feeds the SAME CreateTableOperationBuilder
        // fields (which the underlying CreateTableOperation persists to _meta), exactly like TTL.
        // The predicate is captured here as raw text and validated structurally before the view is
        // created (SqlCompilerImpl.validateCreateExpiryPredicate, against the SELECT's output columns).
        if (tok != null && isExpireKeyword(tok)) {
            tok = parseCreateTableExpireRows(lexer, tableOpBuilder);
        }

        if (tok != null && isInKeyword(tok)) {
            parseInVolume(lexer, tableOpBuilder);
            tok = optTok(lexer);
        }

        return parseCreateMatViewExt(lexer, executionContext, sqlParserCallback, tok, mvOpBuilder);
    }

    private ExecutionModel parseCreateTable(
            GenericLexer lexer,
            CharSequence tok,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CreateTableOperationBuilderImpl builder = createTableOperationBuilder;
        builder.clear();
        builder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        CharSequence tableName;
        // default to non-atomic, batched, creation
        builder.setBatchSize(configuration.getInsertModelBatchSize());
        boolean atomicSpecified = false;
        boolean batchSpecified = false;
        boolean isDirectCreate = true;

        // if it's a CREATE ATOMIC, we don't accept BATCH
        if (isAtomicKeyword(tok)) {
            atomicSpecified = true;
            builder.setBatchSize(-1);
            expectTok(lexer, "table");
            tok = tok(lexer, "table name or 'if'");
        } else if (isBatchKeyword(tok)) {
            batchSpecified = true;

            long val = expectLong(lexer);
            if (val > 0) {
                builder.setBatchSize(val);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "batch size must be positive integer");
            }

            tok = tok(lexer, "table or o3MaxLag");
            if (isO3MaxLagKeyword(tok)) {
                int pos = lexer.getPosition();
                builder.setBatchO3MaxLag(SqlUtil.expectMicros(tok(lexer, "lag value"), pos));
                expectTok(lexer, "table");
            }
            tok = tok(lexer, "table name or 'if'");
        } else if (isTableKeyword(tok)) {
            tok = tok(lexer, "table name or 'if'");
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'atomic' or 'table' or 'batch' expected");
        }

        if (isIfKeyword(tok)) {
            if (isNotKeyword(tok(lexer, "'not'")) && isExistsKeyword(tok(lexer, "'exists'"))) {
                builder.setIgnoreIfExists(true);
                tableName = tok(lexer, "table name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        } else {
            tableName = tok;
        }
        tableName = sansPublicSchema(tableName, lexer);
        assertNameIsQuotedOrNotAKeyword(tableName, lexer.lastTokenPosition());

        builder.setTableNameExpr(nextLiteral(
                assertNoDotsAndSlashes(unquote(tableName), lexer.lastTokenPosition()), lexer.lastTokenPosition()
        ));

        tok = tok(lexer, "'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "like");
            if (isLikeKeyword(tok)) {
                builder.setBatchSize(-1);
                parseCreateTableLikeTable(lexer);
                tok = optTok(lexer);
                return parseCreateTableExt(lexer, executionContext, sqlParserCallback, tok, builder);
            } else {
                lexer.unparseLast();
                parseCreateTableColumns(lexer);
            }
        } else if (isAsKeyword(tok)) {
            isDirectCreate = false;
            parseCreateTableAsSelect(lexer, sqlParserCallback);
        } else {
            throw errUnexpected(lexer, tok);
        }

        // if not CREATE ... AS SELECT, make it atomic
        if (isDirectCreate) {
            builder.setBatchSize(-1);
            builder.setBatchO3MaxLag(-1);

            // if we use atomic or batch keywords, then throw an error
            if (atomicSpecified || batchSpecified) {
                throw SqlException.$(
                        lexer.lastTokenPosition(),
                        "'atomic' or 'batch' keywords can only be used in CREATE ... AS SELECT statements."
                );
            }
        }

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index' or 'cast'");
            if (isIndexKeyword(tok)) {
                parseCreateTableIndexDef(lexer, isDirectCreate);
            } else if (isCastKeyword(tok)) {
                parseCreateTableCastDef(lexer);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            if (isDirectCreate) {
                CreateTableColumnModel model = builder.getColumnModel(timestamp.token);
                if (model == null) {
                    throw SqlException.position(timestamp.position)
                            .put("invalid designated timestamp column [name=").put(timestamp.token).put(']');
                }
                if (!ColumnType.isTimestamp(model.getColumnType())) {
                    throw SqlException
                            .position(timestamp.position)
                            .put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(model.getColumnType()))
                            .put(", columnName=").put(timestamp.token)
                            .put(']');
                }
            }
            builder.setTimestampExpr(timestamp);
            tok = optTok(lexer);
        }

        int walSetting = WAL_NOT_SET;
        boolean formatSeen = false;

        final ExpressionNode partitionByExpr = parseCreateTablePartition(lexer, tok);
        if (partitionByExpr != null) {
            // timestamp may be inferred from select query.
            if (builder.getSelectText() == null && builder.getTimestampExpr() == null) {
                throw SqlException.$(partitionByExpr.position, "partitioning is possible only on tables with designated timestamps");
            }
            final int partitionBy = PartitionBy.fromString(partitionByExpr.token);
            if (partitionBy == -1) {
                throw SqlException.$(partitionByExpr.position, "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            builder.setPartitionByExpr(partitionByExpr);
            tok = optTok(lexer);

            tok = sqlParserCallback.parseTtlSettings(lexer, tok, partitionBy, builder, false);

            // FORMAT can appear before WAL: ... PARTITION BY DAY FORMAT PARQUET WAL ...
            if (tok != null && isFormatKeyword(tok)) {
                tok = parseCreateTableFormat(lexer, builder);
                formatSeen = true;
            }

            if (tok != null) {
                if (isWalKeyword(tok)) {
                    if (!PartitionBy.isPartitioned(builder.getPartitionByFromExpr())) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("WAL Write Mode can only be used on partitioned tables");
                    }
                    walSetting = WAL_ENABLED;
                    tok = optTok(lexer);
                } else if (isBypassKeyword(tok)) {
                    tok = optTok(lexer);
                    if (tok != null && isWalKeyword(tok)) {
                        walSetting = WAL_DISABLED;
                        tok = optTok(lexer);
                    } else {
                        throw SqlException.position(tok == null ? lexer.getPosition() : lexer.lastTokenPosition())
                                .put(" invalid syntax, should be BYPASS WAL but was BYPASS ")
                                .put(tok != null ? tok : "");
                    }
                }
            }

            // FORMAT can also appear after WAL: ... PARTITION BY DAY WAL FORMAT PARQUET ...
            if (tok != null && isFormatKeyword(tok)) {
                if (formatSeen) {
                    throw SqlException.$(lexer.lastTokenPosition(), "duplicate FORMAT clause");
                }
                tok = parseCreateTableFormat(lexer, builder);
                formatSeen = true;
            }
        }

        // EXPIRE ROWS is only supported on materialized views (see CREATE MATERIALIZED VIEW); base tables use
        // TTL + storage policies for retention. Checked here (outside the PARTITION BY block) so the specific
        // message also fires for an un-partitioned CREATE TABLE / CTAS, where that block is skipped and EXPIRE
        // would otherwise fall through to a generic "unexpected token".
        if (tok != null && isExpireKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "EXPIRE ROWS is only supported on materialized views");
        }
        final boolean isWalEnabled = configuration.isWalSupported()
                && PartitionBy.isPartitioned(builder.getPartitionByFromExpr())
                && ((walSetting == WAL_NOT_SET && configuration.getWalEnabledDefault()) || walSetting == WAL_ENABLED);
        builder.setWalEnabled(isWalEnabled);

        int maxUncommittedRows = configuration.getMaxUncommittedRows();
        long o3MaxLag = configuration.getO3MaxLag();

        if (tok != null && isWithKeyword(tok)) {
            ExpressionNode expr;
            while ((expr = expr(lexer, (IQueryModel) null, sqlParserCallback)) != null) {
                if (Chars.equals(expr.token, '=')) {
                    if (isMaxUncommittedRowsKeyword(expr.lhs.token)) {
                        try {
                            maxUncommittedRows = Numbers.parseInt(expr.rhs.token);
                        } catch (NumericException e) {
                            throw SqlException.position(lexer.getPosition())
                                    .put(" could not parse maxUncommittedRows value \"").put(expr.rhs.token).put('"');
                        }
                    } else if (isO3MaxLagKeyword(expr.lhs.token)) {
                        o3MaxLag = SqlUtil.expectMicros(expr.rhs.token, lexer.getPosition());
                    } else {
                        throw SqlException.position(lexer.getPosition()).put(" unrecognized ")
                                .put(expr.lhs.token).put(" after WITH");
                    }
                    tok = optTok(lexer);
                    if (tok != null && Chars.equals(tok, ',')) {
                        CharSequence peek = optTok(lexer);
                        if (peek != null && isInKeyword(peek)) { // in volume
                            tok = peek;
                            break;
                        }
                        lexer.unparseLast();
                        continue;
                    }
                    break;
                }
                throw SqlException.position(lexer.getPosition()).put(" expected parameter after WITH");
            }
        }
        builder.setMaxUncommittedRows(maxUncommittedRows);
        builder.setO3MaxLag(o3MaxLag);

        if (tok != null && isInKeyword(tok)) {
            parseInVolume(lexer, builder);
            tok = optTok(lexer);
        }

        if (tok != null && (isDedupKeyword(tok) || isDeduplicateKeyword(tok))) {
            if (!builder.isWalEnabled()) {
                throw SqlException.position(lexer.getPosition()).put("deduplication is possible only on WAL tables");
            }

            tok = optTok(lexer);
            if (tok == null || !isUpsertKeyword(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("expected 'upsert'");
            }

            tok = optTok(lexer);
            if (tok == null || !isKeysKeyword(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("expected 'keys'");
            }

            boolean timestampColumnFound = false;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, '(')) {
                tok = optTok(lexer);
                int columnListPos = lexer.lastTokenPosition();

                while (tok != null && !Chars.equals(tok, ')')) {
                    validateLiteral(lexer.lastTokenPosition(), tok);
                    final CharSequence columnName = unquote(tok);
                    CreateTableColumnModel model = getCreateTableColumnModel(columnName);
                    if (model == null) {
                        if (isDirectCreate) {
                            throw SqlException.position(lexer.lastTokenPosition())
                                    .put("deduplicate key column not found [column=").put(columnName).put(']');
                        }
                        model = newCreateTableColumnModel(columnName, lexer.lastTokenPosition());
                    } else if (model.isDedupKey() && isDirectCreate) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("duplicate dedup column [column=").put(columnName).put(']');
                    } else if (ColumnType.isArray(model.getColumnType())) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("dedup key columns cannot include ARRAY [column=")
                                .put(columnName).put(", type=")
                                .put(ColumnType.nameOf(model.getColumnType())).put(']');
                    }
                    model.setIsDedupKey();
                    int colIndex = builder.getColumnIndex(columnName);
                    if (colIndex == builder.getTimestampIndex()) {
                        timestampColumnFound = true;
                    }

                    tok = optTok(lexer);
                    if (tok != null && Chars.equals(tok, ',')) {
                        tok = optTok(lexer);
                    }
                }

                if (!timestampColumnFound && isDirectCreate) {
                    throw SqlException.position(columnListPos).put("deduplicate key list must include dedicated timestamp column");
                }

                tok = optTok(lexer);
            } else {
                throw SqlException.position(lexer.getPosition()).put("column list expected");
            }
        }

        // FORMAT can also appear after DEDUP: ... DEDUP UPSERT KEYS(ts) FORMAT PARQUET
        if (tok != null && isFormatKeyword(tok)) {
            if (formatSeen) {
                throw SqlException.$(lexer.lastTokenPosition(), "duplicate FORMAT clause");
            }
            tok = parseCreateTableFormat(lexer, builder);
        }

        if (builder.getTableFormat() == TableUtils.TABLE_FORMAT_PARQUET && !isWalEnabled) {
            throw SqlException.$(builder.getTableFormatPosition(), "FORMAT PARQUET is only supported on WAL tables");
        }

        return parseCreateTableExt(lexer, executionContext, sqlParserCallback, tok, builder);
    }

    private void parseCreateTableAsSelect(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        expectTok(lexer, '(');
        final int startOfSelect = lexer.getPosition();
        // Parse SELECT for the sake of basic SQL validation.
        // It'll be compiled and optimized later, at the execution phase.
        IQueryModel selectModel;
        createTableMode = true;
        try {
            selectModel = parseDml(lexer, startOfSelect, sqlParserCallback);
        } finally {
            createTableMode = false;
        }
        final int endOfSelect = lexer.getPosition() - 1;
        final String selectText = Chars.toString(lexer.getContent(), startOfSelect, endOfSelect);
        createTableOperationBuilder.setSelectText(selectText, startOfSelect);
        createTableOperationBuilder.setSelectModel(selectModel); // transient model, for toSink() purposes only
        expectTok(lexer, ')');
    }

    private void parseCreateTableCastDef(GenericLexer lexer) throws SqlException {
        if (createTableOperationBuilder.getSelectText() == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "cast is only supported in 'create table as ...' context");
        }
        expectTok(lexer, '(');
        final ExpressionNode columnName = expectLiteral(lexer);

        CreateTableColumnModel model = ensureCreateTableColumnModel(columnName.token, columnName.position);
        if (model.getColumnType() != ColumnType.UNDEFINED) {
            throw SqlException.$(lexer.lastTokenPosition(), "duplicate cast");
        }
        expectTok(lexer, "as");

        final ExpressionNode columnType = expectLiteral(lexer);
        final int type = toColumnType(lexer, columnType.token);
        model.setCastType(type, columnType.position);

        if (ColumnType.isSymbol(type)) {
            CharSequence tok = tok(lexer, "'capacity', 'nocache', 'cache' or ')'");

            int symbolCapacity;
            int capacityPosition;
            if (isCapacityKeyword(tok)) {
                capacityPosition = lexer.getPosition();
                symbolCapacity = parseSymbolCapacity(lexer);
                tok = tok(lexer, "'nocache', 'cache' or ')'");
            } else {
                capacityPosition = 0;
                symbolCapacity = configuration.getDefaultSymbolCapacity();
            }
            model.setSymbolCapacity(symbolCapacity);

            final boolean isCached;
            if (isNoCacheKeyword(tok)) {
                isCached = false;
            } else if (isCacheKeyword(tok)) {
                isCached = true;
            } else {
                isCached = configuration.getDefaultSymbolCacheFlag();
                lexer.unparseLast();
            }
            model.setSymbolCacheFlag(isCached);

            if (isCached) {
                TableUtils.validateSymbolCapacityCached(true, symbolCapacity, capacityPosition);
            }
        }
        expectTok(lexer, ')');
    }

    private void parseCreateTableColumns(GenericLexer lexer) throws SqlException {
        while (true) {
            CharSequence tok = notTermTok(lexer);
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            final CharSequence columnName = GenericLexer.immutableOf(unquote(tok));
            final int columnPosition = lexer.lastTokenPosition();
            final int columnType = toColumnType(lexer, notTermTok(lexer));

            if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                throw SqlException.$(columnPosition, " new column name contains invalid characters");
            }

            CreateTableColumnModel model = newCreateTableColumnModel(columnName, columnPosition);
            model.setColumnType(columnType);
            model.setSymbolCapacity(configuration.getDefaultSymbolCapacity());

            if (ColumnType.isSymbol(columnType)) {
                tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

                int symbolCapacity;
                if (isCapacityKeyword(tok)) {
                    // when capacity is not set explicitly, it will default via configuration
                    model.setSymbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                    tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
                } else {
                    symbolCapacity = -1;
                }

                final boolean cacheFlag;
                if (isNoCacheKeyword(tok)) {
                    cacheFlag = false;
                } else if (isCacheKeyword(tok)) {
                    cacheFlag = true;
                } else {
                    cacheFlag = configuration.getDefaultSymbolCacheFlag();
                    lexer.unparseLast();
                }
                model.setSymbolCacheFlag(cacheFlag);
                if (cacheFlag && symbolCapacity != -1) {
                    TableUtils.validateSymbolCapacityCached(true, symbolCapacity, lexer.lastTokenPosition());
                }
                tok = parseCreateTableInlineIndexDef(lexer, model);
            } else {
                tok = null;
            }

            if (tok == null) {
                // check for dodgy array syntax
                CharSequence tempTok = optTok(lexer);
                if (tempTok != null && Chars.equals(tempTok, ']')) {
                    throw SqlException.position(columnPosition).put(columnName).put(" has an unmatched `]` - were you trying to define an array?");
                } else {
                    lexer.unparseLast();
                }
                tok = tok(lexer, "',' or ')'");
            }

            if (isParquetKeyword(tok)) {
                tok = parseCreateTableParquetProperties(lexer, model);
            }

            // ignore `PRECISION`
            if (isPrecisionKeyword(tok)) {
                tok = tok(lexer, "'NOT' or 'NULL' or ',' or ')'");
            }

            // ignore `NULL` and `NOT NULL`
            if (isNotKeyword(tok)) {
                tok = tok(lexer, "'NULL'");
            }

            if (isNullKeyword(tok)) {
                tok = tok(lexer, "','");
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(lexer, tok, "',' or ')' expected");
            }
        }
    }

    private CharSequence parseCreateTableFormat(GenericLexer lexer, CreateTableOperationBuilderImpl builder) throws SqlException {
        final int formatPos = lexer.getPosition();
        final CharSequence tok = tok(lexer, "'parquet' or 'native'");
        final int format;
        if (isParquetKeyword(tok)) {
            format = TableUtils.TABLE_FORMAT_PARQUET;
        } else if (isNativeKeyword(tok)) {
            format = TableUtils.TABLE_FORMAT_NATIVE;
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'parquet' or 'native' expected");
        }
        builder.setTableFormat(format);
        builder.setTableFormatPosition(formatPos);
        return optTok(lexer);
    }

    private void parseCreateTableIndexDef(GenericLexer lexer, boolean isDirectCreate) throws SqlException {
        expectTok(lexer, '(');
        final ExpressionNode columnName = expectLiteral(lexer);
        final int columnNamePosition = lexer.lastTokenPosition();

        CreateTableColumnModel model = getCreateTableColumnModel(columnName.token);
        if (model == null) {
            if (isDirectCreate) {
                throw SqlException.invalidColumn(columnNamePosition, columnName.token);
            }
            model = newCreateTableColumnModel(columnName.token, columnName.position);
        } else if (model.isIndexed()) {
            throw SqlException.$(columnNamePosition, "duplicate index clause");
        }
        if (isDirectCreate && model.getColumnType() != ColumnType.SYMBOL) {
            throw SqlException
                    .position(columnNamePosition)
                    .put("indexes are supported only for SYMBOL columns [columnName=").put(columnName.token)
                    .put(", columnType=").put(ColumnType.nameOf(model.getColumnType()))
                    .put(']');
        }

        // Parse optional index type and/or capacity: INDEX(col TYPE POSTING) or INDEX(col CAPACITY n)
        byte indexType = configuration.getDefaultSymbolIndexType();
        boolean typeExplicit = false;
        int indexValueBlockSize = configuration.getIndexValueBlockSize();
        CharSequence tok = tok(lexer, "'type', 'capacity' or ')'");
        if (isTypeKeyword(tok)) {
            typeExplicit = true;
            tok = tok(lexer, "index type name");
            int typePosition = lexer.lastTokenPosition();
            indexType = IndexType.valueOf(tok);
            if (indexType == IndexType.NONE) {
                throw SqlException.position(typePosition).put("unknown index type: ").put(tok);
            }
            if (indexType == IndexType.POSTING) {
                tok = tok(lexer, "'delta', 'ef' or ')'");
                if (SqlKeywords.isDeltaKeyword(tok)) {
                    indexType = IndexType.POSTING_DELTA;
                } else if (SqlKeywords.isEfKeyword(tok)) {
                    indexType = IndexType.POSTING_EF;
                } else {
                    lexer.unparseLast();
                }
            }
            tok = tok(lexer, IndexType.isPosting(indexType) ? "')'" : "'capacity' or ')'");
        }
        if (isCapacityKeyword(tok)) {
            if (!typeExplicit) {
                indexType = IndexType.BITMAP;
            } else if (indexType != IndexType.BITMAP) {
                throw SqlException.position(lexer.lastTokenPosition())
                        .put("CAPACITY is only supported for BITMAP index type");
            }
            int errorPosition = lexer.getPosition();
            indexValueBlockSize = expectInt(lexer);
            TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
            indexValueBlockSize = Numbers.ceilPow2(indexValueBlockSize);
        } else {
            lexer.unparseLast();
        }
        model.setIndexType(indexType, columnNamePosition, indexValueBlockSize);
        expectTok(lexer, ')');
    }

    private CharSequence parseCreateTableInlineIndexDef(GenericLexer lexer, CreateTableColumnModel model) throws SqlException {
        CharSequence tok = tok(lexer, "')', 'index' or 'parquet'");

        if (isFieldTerm(tok) || isParquetKeyword(tok)) {
            model.setIndexType(IndexType.NONE, -1, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "index");
        int indexColumnPosition = lexer.lastTokenPosition();

        if (isFieldTerm(tok = tok(lexer, ") | , expected")) || isParquetKeyword(tok)) {
            model.setIndexType(configuration.getDefaultSymbolIndexType(), indexColumnPosition, configuration.getIndexValueBlockSize());
            return tok;
        }

        // Parse optional index type: INDEX TYPE POSTING
        byte indexType = configuration.getDefaultSymbolIndexType();
        boolean typeExplicit = false;
        if (isTypeKeyword(tok)) {
            typeExplicit = true;
            tok = tok(lexer, "index type name");
            int typePosition = lexer.lastTokenPosition();
            indexType = IndexType.valueOf(tok);
            if (indexType == IndexType.NONE) {
                throw SqlException.position(typePosition).put("unknown index type: ").put(tok);
            }
            if (indexType == IndexType.POSTING) {
                tok = tok(lexer, ") | , expected");
                if (SqlKeywords.isDeltaKeyword(tok)) {
                    indexType = IndexType.POSTING_DELTA;
                } else if (SqlKeywords.isEfKeyword(tok)) {
                    indexType = IndexType.POSTING_EF;
                } else {
                    lexer.unparseLast();
                }
            }
            tok = tok(lexer, ") | , expected");
            if (isFieldTerm(tok) || isParquetKeyword(tok)) {
                model.setIndexType(indexType, indexColumnPosition, configuration.getIndexValueBlockSize());
                return tok;
            }
        }

        if (SqlKeywords.isIncludeKeyword(tok)) {
            if (!typeExplicit) {
                indexType = IndexType.POSTING;
            } else if (!IndexType.isPosting(indexType)) {
                throw SqlException.position(lexer.lastTokenPosition())
                        .put("INCLUDE is only supported for POSTING index type");
            }
            expectTok(lexer, '(');
            tok = tok(lexer, "column name");
            if (Chars.equals(tok, ')')) {
                throw SqlException.$(lexer.lastTokenPosition(), "at least one column name expected in INCLUDE");
            }
            do {
                model.addCoveringColumnName(GenericLexer.immutableOf(unquote(tok)), lexer.lastTokenPosition());
                tok = tok(lexer, "',' or ')'");
                if (Chars.equals(tok, ',')) {
                    tok = tok(lexer, "column name");
                }
            } while (!Chars.equals(tok, ')'));
            model.setIndexType(indexType, indexColumnPosition, configuration.getIndexValueBlockSize());
            tok = optTok(lexer);
            if (tok == null || isFieldTerm(tok) || isParquetKeyword(tok)) {
                return tok;
            }
        }

        expectTok(lexer, tok, "capacity");
        if (!typeExplicit) {
            indexType = IndexType.BITMAP;
        } else if (indexType != IndexType.BITMAP) {
            throw SqlException.position(lexer.lastTokenPosition())
                    .put("CAPACITY is only supported for BITMAP index type");
        }

        int errorPosition = lexer.getPosition();
        int indexValueBlockSize = expectInt(lexer);
        TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
        model.setIndexType(indexType, indexColumnPosition, Numbers.ceilPow2(indexValueBlockSize));
        return null;
    }

    private void parseCreateTableLikeTable(GenericLexer lexer) throws SqlException {
        // todo: validate keyword usage
        CharSequence tok = tok(lexer, "table name");
        tok = sansPublicSchema(tok, lexer);
        createTableOperationBuilder.setLikeTableNameExpr(
                nextLiteral(
                        assertNoDotsAndSlashes(
                                unquote(tok),
                                lexer.lastTokenPosition()
                        ),
                        lexer.lastTokenPosition()
                )
        );
        tok = tok(lexer, ")");
        if (!Chars.equals(tok, ')')) {
            throw errUnexpected(lexer, tok);
        }
    }

    private CharSequence parseCreateTableParquetProperties(GenericLexer lexer, CreateTableColumnModel model) throws SqlException {
        model.setParquetEncodingConfig(SqlUtil.parseParquetConfig(lexer, model.getColumnType()));
        return tok(lexer, "',' or ')'");
    }

    private ExpressionNode parseCreateTablePartition(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && isPartitionKeyword(tok)) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private ExecutionModel parseCreateView(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CreateViewOperationBuilderImpl vOpBuilder = createViewOperationBuilder;
        final CreateTableOperationBuilderImpl tableOpBuilder = vOpBuilder.getCreateTableOperationBuilder();
        vOpBuilder.clear(); // clears tableOpBuilder too
        tableOpBuilder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        tableOpBuilder.setMaxUncommittedRows(configuration.getMaxUncommittedRows());
        tableOpBuilder.setWalEnabled(true); // view is always WAL

        CharSequence tok = tok(lexer, "view name or 'if'");
        if (isIfKeyword(tok)) {
            if (isNotKeyword(tok(lexer, "'not'")) && isExistsKeyword(tok(lexer, "'exists'"))) {
                tableOpBuilder.setIgnoreIfExists(true);
                tok = tok(lexer, "view name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        }
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        tableOpBuilder.setTableNameExpr(nextLiteral(
                assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()
        ));

        tok = tok(lexer, "'as'");
        if (!isAsKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'as' expected");
        }

        int startOfQuery = lexer.getPosition();
        tok = tok(lexer, "'(' or 'with' or 'select'");
        boolean enclosedInParentheses = Chars.equals(tok, '(');
        if (enclosedInParentheses) {
            startOfQuery = lexer.getPosition();
            tok = tok(lexer, "'with' or 'select'");
        }

        // Parse SELECT for the sake of basic SQL validation.
        // It'll be compiled and optimized later, at the execution phase.
        if (isWithKeyword(tok)) {
            parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, null);
            // CTEs require SELECT to be specified
            expectTok(lexer, "select");
        }
        lexer.unparseLast();
        final IQueryModel queryModel;
        try {
            createViewMode = true;
            queryModel = parseDml(lexer, lexer.getPosition(), sqlParserCallback);
        } finally {
            createViewMode = false;
        }
        final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

        final String viewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);
        tableOpBuilder.setSelectText(viewSql, startOfQuery);
        tableOpBuilder.setSelectModel(queryModel); // transient model, for toSink() purposes only

        SqlUtil.collectTableAndColumnReferences(cairoEngine, queryModel, vOpBuilder.getDependencies());

        if (enclosedInParentheses) {
            expectTok(lexer, ')');
        } else {
            // We expect nothing more when there are no parentheses.
            tok = optTok(lexer);
            if (tok != null && !Chars.equals(tok, ';')) {
                throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
            }
            return vOpBuilder;
        }

        tok = optTok(lexer);
        return parseCreateViewExt(lexer, executionContext, sqlParserCallback, tok, vOpBuilder);
    }

    private void parseDeclare(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        int contentLength = lexer.getContent().length();
        while (lexer.getPosition() < contentLength) {
            int pos = lexer.getPosition();

            CharSequence tok = optTok(lexer);

            if (tok == null) {
                break;
            }

            if (tok.charAt(0) == ',') {
                continue;
            }

            if (isDeclareKeyword(tok)) {
                throw errUnexpected(lexer, tok, "Multiple DECLARE statements are not allowed. Use single DECLARE block: DECLARE @a := 1, @b := 1, @c := 1");
            }

            boolean isOverridable = false;
            if (isOverridableKeyword(tok)) {
                isOverridable = true;
                pos = lexer.getPosition();
                tok = optTok(lexer);
                if (tok == null || tok.charAt(0) != '@') {
                    throw SqlException.$(pos, "variable name expected after OVERRIDABLE");
                }
            }

            if (isSelectKeyword(tok) || !(tok.charAt(0) == '@')) {
                lexer.unparseLast();
                break;
            }

            CharacterStoreEntry cse = characterStore.newEntry();
            cse.put(tok);
            tok = cse.toImmutable();

            CharSequence expectWalrus = optTok(lexer);

            if (expectWalrus == null || !Chars.equals(expectWalrus, ":=")) {
                throw errUnexpected(lexer, expectWalrus, "expected variable assignment operator `:=`");
            }

            lexer.goToPosition(pos);

            ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls(), tok);

            if (expr == null) {
                throw errUnexpected(lexer, tok, "declaration was empty or could not be parsed");
            }

            if (!Chars.equalsIgnoreCase(expr.lhs.token, tok)) {
                // could be a `DECLARE @x := (1,2,3)` situation
                throw errUnexpected(lexer, tok, "unexpected bind expression - bracket lists are not supported");
            }

            model.getDecls().put(tok, expr);
            if (isOverridable) {
                model.getOverridableDecls().add(tok);
            }
        }
    }

    private IQueryModel parseDml(
            GenericLexer lexer,
            int modelPosition,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        return parseDml(lexer, null, modelPosition, true, sqlParserCallback, null, false);
    }

    private IQueryModel parseDml(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            int modelPosition,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        IQueryModel model = null;
        IQueryModel prevModel = null;

        while (true) {
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses = prevModel != null ? prevModel.getWithClauses() : withClauses;
            LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses = useTopLevelWithClauses && model == null ? topLevelWithModel : null;
            // Propagate DECLARE variables from previous UNION branch, similar to how WITH clauses are propagated
            LowerCaseCharSequenceObjHashMap<ExpressionNode> parentDecls = prevModel != null ? prevModel.getDecls() : decls;

            IQueryModel unionModel = parseDml0(lexer, parentWithClauses, topWithClauses, modelPosition, sqlParserCallback, parentDecls, overrideDeclare);
            if (prevModel == null) {
                model = unionModel;
                prevModel = model;
            } else {
                prevModel.setUnionModel(unionModel);
                prevModel = unionModel;
            }

            CharSequence tok = optTok(lexer);
            if (tok == null || Chars.equals(tok, ';') || setOperations.excludes(tok)) {
                lexer.unparseLast();
                return model;
            }

            if (prevModel.getNestedModel() != null) {
                if (prevModel.getNestedModel().getOrderByPosition() > 0) {
                    throw SqlException.$(prevModel.getNestedModel().getOrderByPosition(), "unexpected token 'order'");
                }
                if (prevModel.getNestedModel().getLimitPosition() > 0) {
                    throw SqlException.$(prevModel.getNestedModel().getLimitPosition(), "unexpected token 'limit'");
                }
            }

            if (isUnionKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_UNION_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_UNION);
                    if (isDistinctKeyword(tok)) {
                        // union distinct is equal to just union, we only consume to 'distinct' token and we are good
                        modelPosition = lexer.getPosition();
                    } else {
                        lexer.unparseLast();
                        modelPosition = lexer.lastTokenPosition();
                    }
                }
            }

            if (isExceptKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_EXCEPT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_EXCEPT);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
            }

            if (isIntersectKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_INTERSECT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_INTERSECT);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
            }

            // check for decls
            if (prevModel.getDecls() != null && prevModel.getDecls().size() > 0 && decls == null) {
                decls = prevModel.getDecls();
            }
        }
    }

    @NotNull
    private IQueryModel parseDml0(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses,
            int modelPosition,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        CharSequence tok;
        IQueryModel model = queryModelPool.next();
        model.setModelPosition(modelPosition);

        if (parentWithClauses != null) {
            model.getWithClauses().putAll(parentWithClauses);
        }

        tok = tok(lexer, "'select', 'with', 'declare' or table name expected");

        // [declare]
        if (isDeclareKeyword(tok)) {
            parseDeclare(lexer, model, sqlParserCallback);
            tok = tok(lexer, "'select', 'with', or table name expected");
        }

        // Merge external declares with the query's own declares.
        // When there is a naming conflict, overrideDeclare controls the behavior:
        //   - true: external declares override the query's own declares
        //   - false: query's own declares take precedence over the external ones
        // Currently set to true at view boundaries only, allowing callers to parameterize views.
        model.copyDeclsFrom(decls, overrideDeclare);

        // [with]
        if (isWithKeyword(tok)) {
            parseWithClauses(lexer, model.getWithClauses(), sqlParserCallback, model.getDecls());
            tok = tok(lexer, "'select' or table name expected");
        } else if (topWithClauses != null) {
            model.getWithClauses().putAll(topWithClauses);
        }

        // [select]
        if (isSelectKeyword(tok)) {
            parseSelectClause(lexer, model, sqlParserCallback);

            tok = optTok(lexer);

            if (tok != null && setOperations.contains(tok)) {
                tok = null;
            }

            if (tok == null || Chars.equals(tok, ';') || Chars.equals(tok, ')')) { // token can also be ';' on query boundary
                IQueryModel nestedModel = queryModelPool.next();
                nestedModel.setModelPosition(modelPosition);
                ExpressionNode tableNameExpr = expressionNodePool.next().of(ExpressionNode.FUNCTION, "long_sequence", 0, lexer.lastTokenPosition());
                tableNameExpr.paramCount = 1;
                tableNameExpr.rhs = ONE;
                nestedModel.setTableNameExpr(tableNameExpr);
                model.setSelectModelType(IQueryModel.SELECT_MODEL_VIRTUAL);
                model.setNestedModel(nestedModel);
                lexer.unparseLast();
                return model;
            }
        } else if (isShowKeyword(tok)) {
            model.setSelectModelType(IQueryModel.SELECT_MODEL_SHOW);
            int showKind = -1;
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null) {
                // show tables
                // show columns from tab
                // show partitions from tab
                // show transaction isolation level
                // show transaction_isolation
                // show max_identifier_length
                // show standard_conforming_strings
                // show search_path
                // show datestyle
                // show time zone
                // show create table tab
                // show create materialized view mv
                // show create view v
                if (isTablesKeyword(tok)) {
                    showKind = IQueryModel.SHOW_TABLES;
                } else if (isColumnsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = IQueryModel.SHOW_COLUMNS;
                } else if (isPartitionsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = IQueryModel.SHOW_PARTITIONS;
                } else if (isTransactionKeyword(tok)) {
                    showKind = IQueryModel.SHOW_TRANSACTION;
                    validateShowTransactions(lexer);
                } else if (isTransactionIsolation(tok)) {
                    showKind = IQueryModel.SHOW_TRANSACTION_ISOLATION_LEVEL;
                } else if (isDefaultTransactionReadOnly(tok)) {
                    showKind = IQueryModel.SHOW_DEFAULT_TRANSACTION_READ_ONLY;
                } else if (isMaxIdentifierLength(tok)) {
                    showKind = IQueryModel.SHOW_MAX_IDENTIFIER_LENGTH;
                } else if (isStandardConformingStrings(tok)) {
                    showKind = IQueryModel.SHOW_STANDARD_CONFORMING_STRINGS;
                } else if (isSearchPath(tok)) {
                    showKind = IQueryModel.SHOW_SEARCH_PATH;
                } else if (isDateStyleKeyword(tok)) {
                    showKind = IQueryModel.SHOW_DATE_STYLE;
                } else if (isTimeKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isZoneKeyword(tok)) {
                        showKind = IQueryModel.SHOW_TIME_ZONE;
                    }
                } else if (isParametersKeyword(tok)) {
                    showKind = IQueryModel.SHOW_PARAMETERS;
                } else if (isServerVersionKeyword(tok)) {
                    showKind = IQueryModel.SHOW_SERVER_VERSION;
                } else if (isServerVersionNumKeyword(tok)) {
                    showKind = IQueryModel.SHOW_SERVER_VERSION_NUM;
                } else if (isCreateKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isTableKeyword(tok)) {
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_TABLE;
                    } else if (tok != null && isMaterializedKeyword(tok)) {
                        expectTok(lexer, "view");
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_MAT_VIEW;
                    } else if (tok != null && isLiveKeyword(tok)) {
                        expectTok(lexer, "view");
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_LIVE_VIEW;
                    } else if (tok != null && isViewKeyword(tok)) {
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_VIEW;
                    } else if (tok != null && isDatabaseKeyword(tok)) {
                        showKind = IQueryModel.SHOW_CREATE_DATABASE;
                        model.setShowCreateDatabaseInclude(parseShowCreateDatabaseInclude(lexer));
                    } else {
                        throw SqlException.position(lexer.lastTokenPosition()).put("expected 'TABLE' or 'VIEW' or 'MATERIALIZED VIEW' or 'LIVE VIEW' or 'DATABASE'");
                    }
                } else {
                    showKind = sqlParserCallback.parseShowSql(lexer, model, tok, expressionNodePool);
                }
            }

            if (showKind == -1) {
                throw SqlException.position(lexer.getPosition()).put("expected ")
                        .put("'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', ")
                        .put("'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', ")
                        .put("'max_identifier_length', 'standard_conforming_strings', ")
                        .put("'parameters', 'server_version', 'server_version_num', ")
                        .put("'search_path', 'datestyle', or 'time zone'");
            } else {
                model.setShowKind(showKind);
            }
        } else {
            lexer.unparseLast();
            SqlUtil.addSelectStar(
                    model,
                    queryColumnPool,
                    expressionNodePool
            );
        }

        if (model.getSelectModelType() != IQueryModel.SELECT_MODEL_SHOW) {
            IQueryModel nestedModel = queryModelPool.next();
            nestedModel.setModelPosition(modelPosition);

            nestedModel = parseFromClause(lexer, nestedModel, model, sqlParserCallback);
            if (nestedModel.getLimitHi() != null || nestedModel.getLimitLo() != null) {
                model.setLimit(nestedModel.getLimitLo(), nestedModel.getLimitHi());
                nestedModel.setLimit(null, null);
            }
            model.setSelectModelType(IQueryModel.SELECT_MODEL_CHOOSE);
            model.setNestedModel(nestedModel);
            final ExpressionNode n = nestedModel.getAlias();
            if (n != null) {
                model.setAlias(n);
            }
        }
        return model;
    }

    private IQueryModel parseDmlUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        // Update IQueryModel structure is
        // IQueryModel with SET column expressions (updateQueryModel)
        // |-- nested IQueryModel of select-virtual or select-choose of data selected for update (fromModel)
        //     |-- nested IQueryModel with selected data (nestedModel)
        //         |-- join QueryModels to represent FROM clause
        CharSequence tok;
        final int modelPosition = lexer.getPosition();

        IQueryModel updateQueryModel = queryModelPool.next();
        updateQueryModel.setModelType(ExecutionModel.UPDATE);
        updateQueryModel.setModelPosition(modelPosition);
        IQueryModel fromModel = queryModelPool.next();
        fromModel.setModelPosition(modelPosition);
        updateQueryModel.setIsUpdate(true);
        fromModel.setIsUpdate(true);
        tok = tok(lexer, "UPDATE, WITH or table name expected");

        // [update]
        if (isUpdateKeyword(tok)) {
            // parse SET statements into updateQueryModel and rhs of SETs into fromModel to select
            parseUpdateClause(lexer, updateQueryModel, fromModel, sqlParserCallback);

            // create nestedModel IQueryModel to source rowids for the update
            IQueryModel nestedModel = queryModelPool.next();
            nestedModel.setTableNameExpr(fromModel.getTableNameExpr());
            nestedModel.setAlias(updateQueryModel.getAlias());
            nestedModel.setIsUpdate(true);

            // nest nestedModel inside fromModel
            fromModel.setTableNameExpr(null);
            fromModel.setNestedModel(nestedModel);

            // Add WITH clauses if they exist into fromModel
            fromModel.getWithClauses().putAll(topLevelWithModel);

            tok = optTok(lexer);

            // [from]
            if (tok != null && isFromKeyword(tok)) {
                tok = ","; // FROM in Postgres UPDATE statement means cross join
                int joinType;
                int i = 0;
                while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
                    if (i++ == 1) {
                        throw SqlException.$(lexer.lastTokenPosition(), "JOIN is not supported on UPDATE statement");
                    }
                    // expect multiple [[inner | outer | cross] join]
                    nestedModel.addJoinModel(parseJoin(lexer, fromModel, tok, joinType, topLevelWithModel, sqlParserCallback, decls));
                    tok = optTok(lexer);
                }
            } else if (tok != null && isSemicolon(tok)) {
                tok = null;
            } else if (tok != null && !isWhereKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "FROM, WHERE or EOF expected");
            }

            // [where]
            if (tok != null && isWhereKeyword(tok)) {
                ExpressionNode expr = expr(lexer, fromModel, sqlParserCallback, decls);
                if (expr != null) {
                    nestedModel.setWhereClause(expr);
                } else {
                    throw SqlException.$((lexer.lastTokenPosition()), "empty where clause");
                }
            } else if (tok != null && !isSemicolon(tok)) {
                throw errUnexpected(lexer, tok);
            }

            // No row-expiry read filter is applied to an UPDATE target: the read filter is
            // materialized-view-only (isMatView()), and a materialized view cannot be the target of an
            // UPDATE (rejected downstream with "cannot modify materialized view"). Plain tables never carry
            // an expiry policy (rejected at CREATE/ALTER), so there is nothing to AND into the WHERE here.
            updateQueryModel.setNestedModel(fromModel);
        }
        return updateQueryModel;
    }

    // doesn't allow copy, rename
    private ExecutionModel parseExplain(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CharSequence tok = tok(lexer, "'create', 'format', 'insert', 'update', 'select' or 'with'");

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, null);
        }

        if (isCreateKeyword(tok)) {
            return parseCreate(lexer, executionContext, sqlParserCallback);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, null);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, null);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer, sqlParserCallback, null);
        }

        if (isDropKeyword(tok) || isAlterKeyword(tok) || isRefreshKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put(
                    "'create', 'format', 'insert', 'update', 'select' or 'with'"
            ).put(" expected");
        }

        return parseSelect(lexer, sqlParserCallback, null);
    }

    private int parseExplainOptions(GenericLexer lexer, CharSequence prevTok) throws SqlException {
        int parenthesisPos = lexer.getPosition();
        CharSequence explainTok = GenericLexer.immutableOf(prevTok);
        CharSequence tok = tok(lexer, "'create', 'insert', 'update', 'select', 'with' or '('");
        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "'format'");
            if (isFormatKeyword(tok)) {
                tok = tok(lexer, "'text' or 'json'");
                if (isTextKeyword(tok) || isJsonKeyword(tok)) {
                    int format = isJsonKeyword(tok) ? ExplainModel.FORMAT_JSON : ExplainModel.FORMAT_TEXT;
                    tok = tok(lexer, "')'");
                    if (!Chars.equals(tok, ')')) {
                        throw SqlException.$((lexer.lastTokenPosition()), "unexpected explain option found");
                    }
                    return format;
                } else {
                    throw SqlException.$((lexer.lastTokenPosition()), "unexpected explain format found");
                }
            } else {
                lexer.backTo(parenthesisPos, explainTok);
                return ExplainModel.FORMAT_TEXT;
            }
        } else {
            lexer.unparseLast();
            return ExplainModel.FORMAT_TEXT;
        }
    }

    private IQueryModel parseFromClause(GenericLexer lexer, IQueryModel model, IQueryModel masterModel, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok = expectTableNameOrSubQuery(lexer);

        // copy decls down
        model.copyDeclsFrom(masterModel, false);

        // standalone UNNEST: FROM UNNEST(...)
        if (isUnnestKeyword(tok)) {
            // synthesize long_sequence(1) as the base model
            ExpressionNode longSeq = expressionNodePool.next().of(ExpressionNode.FUNCTION, "long_sequence", 0, 0);
            longSeq.paramCount = 1;
            longSeq.rhs = ONE;
            model.setTableNameExpr(longSeq);

            IQueryModel unnestModel = parseUnnest(lexer, model, model.getDecls(), sqlParserCallback);
            unnestModel.setStandaloneUnnest(true);
            model.addJoinModel(unnestModel);

            tok = optTok(lexer);
        } else {
            IQueryModel proposedNested = null;
            ExpressionNode variableExpr;

            // check for variable as subquery
            if (tok.charAt(0) == '@'
                    && (variableExpr = model.getDecls().get(tok)) != null
                    && variableExpr.rhs != null
                    && variableExpr.rhs.queryModel != null) {
                proposedNested = variableExpr.rhs.queryModel;
            }

            final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
            if (tt != null && tt.isView()) {
                compileViewQuery(model, tt, lexer.lastTokenPosition());
                tok = setModelAliasAndTimestamp(lexer, model);
                // expect "(" in case of sub-query
            } else if (Chars.equals(tok, '(') || proposedNested != null) {
                if (proposedNested == null) {
                    proposedNested = parseAsSubQueryAndExpectClosingBrace(lexer, masterModel.getWithClauses(), true, sqlParserCallback, model.getDecls());
                }

                tok = optTok(lexer);

                // do not collapse aliased sub-queries or those that have timestamp()
                // select * from (table) x
                if (tok == null || (tableAliasStop.contains(tok) && !isTimestampKeyword(tok))) {
                    final IQueryModel target = proposedNested.getNestedModel();
                    // when * is artificial, there is no union, there is no "where" clause inside sub-query,
                    // e.g. there was no "select * from" we should collapse sub-query to a regular table
                    if (
                            proposedNested.isArtificialStar()
                                    && proposedNested.getUnionModel() == null
                                    && target.getWhereClause() == null
                                    && target.getOrderBy().size() == 0
                                    && target.getLatestBy().size() == 0
                                    && target.getNestedModel() == null
                                    && target.getSampleBy() == null
                                    && target.getGroupBy().size() == 0
                                    && proposedNested.getLimitLo() == null
                                    && proposedNested.getLimitHi() == null
                                    && target.getPivotForColumns().size() == 0
                    ) {
                        model.setTableNameExpr(target.getTableNameExpr());
                        model.setAlias(target.getAlias());
                        model.setTimestamp(target.getTimestamp());

                        int n = target.getJoinModels().size();
                        for (int i = 1; i < n; i++) {
                            model.addJoinModel(target.getJoinModels().getQuick(i));
                        }
                        proposedNested = null;
                    } else {
                        lexer.unparseLast();
                    }
                } else {
                    lexer.unparseLast();
                }

                if (proposedNested != null) {
                    model.setNestedModel(proposedNested);
                    model.setNestedModelIsSubQuery(true);
                    tok = setModelAliasAndTimestamp(lexer, model);
                }
            } else {
                lexer.unparseLast();
                parseSelectFrom(lexer, model, masterModel.getWithClauses(), sqlParserCallback);
                tok = setModelAliasAndTimestamp(lexer, model);

                // expect [latest by] (deprecated syntax)
                if (tok != null && isLatestKeyword(tok)) {
                    parseLatestBy(lexer, model);
                    tok = optTok(lexer);
                }
            }
        }

        // expect multiple [[inner | outer | cross] join]
        int joinType;
        boolean hasWindowJoin = false;
        boolean hasHorizonJoin = false;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            // Check if this is a WINDOW clause (named window definitions) rather than WINDOW JOIN
            // WINDOW clause pattern: WINDOW name AS (...)
            // WINDOW JOIN pattern: WINDOW JOIN table ON ... or WINDOW table ON ...
            if (isWindowKeyword(tok)) {
                // Save lexer state before lookahead
                int windowLastPos = lexer.lastTokenPosition();
                CharSequence windowTok = tok;

                // Lookahead: read two tokens after WINDOW to distinguish
                // WINDOW clause (WINDOW name AS ...) from WINDOW JOIN (WINDOW JOIN table ON ...).
                // We always check both tokens because join keywords like "join", "cross",
                // "left" etc. could theoretically be quoted window names.
                CharSequence nextTok = SqlUtil.fetchNext(lexer);
                boolean isWindowClause = false;
                if (nextTok != null) {
                    if (isAsKeyword(nextTok)) {
                        // WINDOW AS (...) - missing window name
                        lexer.backTo(windowLastPos, windowTok);
                        tok = optTok(lexer);
                        break;
                    }
                    CharSequence afterName = SqlUtil.fetchNext(lexer);
                    if (afterName != null && isAsKeyword(afterName)) {
                        isWindowClause = true;
                    }
                }

                // Restore lexer to start of "window" token so it can be re-read
                lexer.backTo(windowLastPos, windowTok);

                if (isWindowClause) {
                    // Break out of join loop - WINDOW clause will be parsed after the loop
                    // Re-read "window" so tok is valid for WINDOW clause parsing
                    tok = optTok(lexer);
                    break;
                }
                // WINDOW JOIN - re-read "window" so tok is valid for parseJoin
                tok = optTok(lexer);
            }
            if (hasWindowJoin && joinType != IQueryModel.JOIN_WINDOW) {
                throw SqlException.$((lexer.lastTokenPosition()), "no other join types allowed after window join");
            }
            if (hasHorizonJoin && joinType != IQueryModel.JOIN_HORIZON) {
                throw SqlException.$((lexer.lastTokenPosition()), "only horizon joins can follow a horizon join");
            }
            if (joinType == IQueryModel.JOIN_HORIZON && !hasHorizonJoin && model.getJoinModels().size() > 1) {
                throw SqlException.$((lexer.lastTokenPosition()), "horizon join cannot be combined with other joins");
            }
            hasWindowJoin = joinType == IQueryModel.JOIN_WINDOW;
            hasHorizonJoin = joinType == IQueryModel.JOIN_HORIZON;
            model.addJoinModel(parseJoin(lexer, model, tok, joinType, masterModel.getWithClauses(), sqlParserCallback, model.getDecls()));
            tok = optTok(lexer);
        }

        // expect [where]

        if (tok != null && isWhereKeyword(tok)) {
            if (model.getLatestByType() == IQueryModel.LATEST_BY_NEW) {
                throw SqlException.$((lexer.lastTokenPosition()), "unexpected where clause after 'latest on'");
            }
            ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls());
            if (expr != null) {
                model.setWhereClause(expr);
                tok = optTok(lexer);
            } else {
                throw SqlException.$((lexer.lastTokenPosition()), "empty where clause");
            }
        }

        // expect [latest by] (new syntax)

        if (tok != null && isLatestKeyword(tok)) {
            if (model.getLatestByType() == IQueryModel.LATEST_BY_DEPRECATED) {
                throw SqlException.$((lexer.lastTokenPosition()), "mix of new and deprecated 'latest by' syntax");
            }
            expectTok(lexer, "on");
            parseLatestByNew(lexer, model);
            tok = optTok(lexer);
        }

        // expect [pivot]
        // PIVOT operates on the result of a full subquery.
        // Syntax: SELECT ... FROM <subquery> WHERE <condition> PIVOT (agg FOR col IN (...))
        // The pivot transformation wraps the current model as a nested subquery.
        boolean hasPivot = false;
        if (tok != null && isPivotKeyword(tok)) {
            try {
                pivotMode = true;
                IQueryModel pivotModel = queryModelPool.next();
                pivotModel.setModelPosition(lexer.lastTokenPosition());
                pivotModel.setNestedModel(model);
                tok = parsePivot(lexer, pivotModel, sqlParserCallback);
                hasPivot = true;
                model = pivotModel;
            } finally {
                pivotMode = false;
            }
        }

        // expect [sample by]
        if (tok != null && isSampleKeyword(tok)) {
            if (hasPivot) {
                IQueryModel parentModel = queryModelPool.next();
                parentModel.setNestedModel(model);
                model = parentModel;
            }
            expectBy(lexer);
            expectSample(lexer, model, sqlParserCallback);
            tok = optTok(lexer);

            ExpressionNode fromNode = null, toNode = null;
            // support `SAMPLE BY 5m FROM foo TO bah`
            if (tok != null && isFromKeyword(tok)) {
                fromNode = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (fromNode == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'timestamp' expression expected");
                }
                tok = optTok(lexer);
            }

            if (tok != null && isToKeyword(tok)) {
                toNode = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (toNode == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'timestamp' expression expected");
                }
                tok = optTok(lexer);
            }

            model.setSampleByFromTo(fromNode, toNode);

            if (tok != null && isFillKeyword(tok)) {
                expectTok(lexer, '(');
                do {
                    final ExpressionNode fillNode = expr(lexer, model, sqlParserCallback, model.getDecls());
                    if (fillNode == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "'none', 'prev', 'mid', 'null' or number expected");
                    }
                    model.addSampleByFill(fillNode);
                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    expectTok(tok, lexer.lastTokenPosition(), ',');
                } while (true);

                tok = optTok(lexer);
            }

            if (tok != null && isAlignKeyword(tok)) {
                expectTo(lexer);

                tok = tok(lexer, "'calendar' or 'first observation'");

                if (isCalendarKeyword(tok)) {
                    tok = optTok(lexer);
                    if (tok == null) {
                        model.setSampleByTimezoneName(null);
                        model.setSampleByOffset(ZERO_OFFSET);
                    } else if (isTimeKeyword(tok)) {
                        expectZone(lexer);
                        model.setSampleByTimezoneName(expectExpr(lexer, sqlParserCallback, model.getDecls()));
                        tok = optTok(lexer);
                        if (tok != null && isWithKeyword(tok)) {
                            tok = parseWithOffset(lexer, model, sqlParserCallback);
                        } else {
                            model.setSampleByOffset(ZERO_OFFSET);
                        }
                    } else if (isWithKeyword(tok)) {
                        tok = parseWithOffset(lexer, model, sqlParserCallback);
                    } else {
                        model.setSampleByTimezoneName(null);
                        model.setSampleByOffset(ZERO_OFFSET);
                    }
                } else if (isFirstKeyword(tok)) {
                    expectObservation(lexer);

                    if (model.getSampleByTo() != null || model.getSampleByFrom() != null) {
                        throw SqlException.$(lexer.getPosition(), "ALIGN TO FIRST OBSERVATION is incompatible with FROM-TO");
                    }

                    model.setSampleByTimezoneName(null);
                    model.setSampleByOffset(null);
                    tok = optTok(lexer);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'calendar' or 'first observation' expected");
                }
            } else {
                // Set offset according to default config
                if (configuration.getSampleByDefaultAlignmentCalendar()) {
                    model.setSampleByOffset(ZERO_OFFSET);
                } else {
                    model.setSampleByOffset(null);
                }
            }
        }

        // expect [group by]

        if (tok != null && isGroupKeyword(tok)) {
            if (hasPivot) {
                IQueryModel parentModel = queryModelPool.next();
                parentModel.setNestedModel(model);
                model = parentModel;
            }
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();
                ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (n == null || (n.type != ExpressionNode.LITERAL && n.type != ExpressionNode.CONSTANT && n.type != ExpressionNode.FUNCTION && n.type != ExpressionNode.OPERATION)) {
                    throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "literal expected");
                }

                model.addGroupBy(n);

                tok = optTok(lexer);
            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [window]
        // WINDOW clause for named window definitions: WINDOW w AS (PARTITION BY ... ORDER BY ...)
        // SQL standard places WINDOW between HAVING/GROUP BY and ORDER BY.
        if (tok != null && isWindowKeyword(tok)) {
            do {
                // Parse window name
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "window name expected after 'window'");
                }
                if (isAsKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "window name expected after 'window'");
                }
                validateIdentifier(lexer, tok);
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

                // Intern the window name immediately before any more lexer operations
                // (the lexer reuses its buffer, so tok would be overwritten)
                CharacterStoreEntry cse = characterStore.newEntry();
                cse.put(GenericLexer.unquote(tok));
                CharSequence windowName = cse.toImmutable();
                int windowNamePos = lexer.lastTokenPosition();

                // Check for duplicate window name in the outer (master) model
                if (masterModel.getNamedWindows().keyIndex(windowName) < 0) {
                    throw SqlException.$(windowNamePos, "duplicate window name");
                }

                // Expect AS
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !isAsKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'as' expected after window name");
                }

                // Expect '('
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || tok.charAt(0) != '(') {
                    throw SqlException.$(lexer.lastTokenPosition(), "'(' expected after 'as'");
                }

                // Create WindowExpression and parse the specification
                WindowExpression windowSpec = windowExpressionPool.next();
                windowSpec.clear();
                expressionParser.parseWindowSpec(lexer, windowSpec, sqlParserCallback, model.getDecls());
                rewriteWindowExpression(windowSpec);

                // Validate base window reference (window inheritance):
                // the base must be defined earlier in the same WINDOW clause (no forward references)
                if (windowSpec.hasBaseWindow()) {
                    CharSequence baseName = windowSpec.getBaseWindowName();
                    if (masterModel.getNamedWindows().keyIndex(baseName) > -1) {
                        throw SqlException.$(windowSpec.getBaseWindowNamePosition(), "window '")
                                .put(baseName).put("' is not defined");
                    }
                }

                // Store named window in the outer (master) model where the SELECT columns are defined,
                // not the FROM model. The window functions in SELECT reference these named windows.
                masterModel.getNamedWindows().put(windowName, windowSpec);

                tok = optTok(lexer);
            } while (tok != null && Chars.equals(tok, ','));
        }

        // Validate that all named window references in SELECT columns are defined.
        // Fail fast here rather than waiting for the optimizer.
        validateNamedWindowReferences(masterModel);

        // expect [order by]

        if (tok != null && isOrderKeyword(tok)) {
            model.setOrderByPosition(lexer.lastTokenPosition());
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();

                ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (n == null || (n.type == ExpressionNode.QUERY || n.type == ExpressionNode.SET_OPERATION)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "literal or expression expected");
                }

                // token can sometimes be null, like during parsing of CASE clause
                if ((n.type == ExpressionNode.CONSTANT && Chars.equals("''", n.token))
                        || (n.type == ExpressionNode.LITERAL && (n.token == null || n.token.isEmpty()))) {
                    throw SqlException.$(lexer.lastTokenPosition(), "non-empty literal or expression expected");
                }

                tok = optTok(lexer);

                if (tok != null && isDescKeyword(tok)) {
                    model.addOrderBy(n, IQueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok(lexer);
                } else {
                    model.addOrderBy(n, IQueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && isAscKeyword(tok)) {
                        tok = optTok(lexer);
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err(lexer, tok, "Too many columns");
                }
            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && isLimitKeyword(tok)) {
            model.setLimitPosition(lexer.lastTokenPosition());
            ExpressionNode lo = expr(lexer, model, sqlParserCallback, model.getDecls());
            ExpressionNode hi = null;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr(lexer, model, sqlParserCallback, model.getDecls());
            } else {
                lexer.unparseLast();
            }
            // questdb accepts open-ended limits like 'LIMIT 5,' and 'LIMIT ,5'.
            // so reject only when neither side of the LIMIT clause parsed.
            if (lo == null && hi == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "limit expression expected");
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparseLast();
        }
        return model;
    }

    private void parseFromTable(GenericLexer lexer, IQueryModel model) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isFromKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'from'");
        }
        parseTableName(lexer, model);
    }

    private void parseHints(GenericLexer lexer, IQueryModel model) {
        CharSequence hintToken;
        boolean parsingParams = false;
        CharSequence hintKey = null;
        CharacterStoreEntry hintValuesEntry = null;
        boolean error = false;
        while ((hintToken = SqlUtil.fetchNextHintToken(lexer)) != null) {
            if (error) {
                // if in error state, just consume the rest of hints, but ignore them
                // since in error state we cannot reliably parse them
                continue;
            }

            if (Chars.equals(hintToken, '(')) {
                if (parsingParams) {
                    // hints cannot be nested
                    error = true;
                    continue;
                }
                if (hintKey == null) {
                    // missing key
                    error = true;
                    continue;
                }
                parsingParams = true;
                continue;
            }

            if (Chars.equals(hintToken, ')')) {
                if (!parsingParams) {
                    // unexpected closing parenthesis
                    error = true;
                    continue;
                }
                if (hintValuesEntry == null) {
                    // store last parameter-less hint, e.g. KEY()
                    model.addHint(hintKey, null);
                } else {
                    // ok, there are some parameters
                    model.addHint(hintKey, hintValuesEntry.toImmutable());
                    hintValuesEntry = null;
                }
                hintKey = null;
                parsingParams = false;
                continue;
            }

            if (parsingParams) {
                if (hintValuesEntry == null) {
                    // store first parameter
                    hintValuesEntry = characterStore.newEntry();
                } else {
                    hintValuesEntry.put(SqlHints.HINTS_PARAMS_DELIMITER);
                }
                hintValuesEntry.put(GenericLexer.unquote(hintToken));
                continue;
            }

            if (hintKey != null) {
                // store previous parameter-less hint
                model.addHint(hintKey, null);
            }
            CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(hintToken);
            hintKey = entry.toImmutable();
        }
        if (!error && !parsingParams && hintKey != null) {
            // store the last parameter-less hint
            // why only when not parsingParams? dangling parsingParams indicates a syntax error and in this case
            // we don't want to store the hint
            model.addHint(hintKey, null);
        }
    }

    private void parseInVolume(GenericLexer lexer, CreateTableOperationBuilderImpl tableOpBuilder) throws SqlException {
        int volumeKwPos = lexer.getPosition();
        expectTok(lexer, "volume");
        CharSequence tok = tok(lexer, "path for volume");
        if (Os.isWindows()) {
            throw SqlException.position(volumeKwPos).put("'in volume' is not supported on Windows");
        }
        tableOpBuilder.setVolumeAlias(GenericLexer.unquote(tok), lexer.lastTokenPosition());
    }

    private ExecutionModel parseInsert(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final InsertModel model = insertModelPool.next();
        CharSequence tok = tok(lexer, "atomic or into or batch");
        model.setBatchSize(configuration.getInsertModelBatchSize());
        boolean atomicSpecified = false;

        if (isAtomicKeyword(tok)) {
            atomicSpecified = true;
            model.setBatchSize(-1);
            tok = tok(lexer, "into");
        }

        if (isBatchKeyword(tok)) {
            long val = expectLong(lexer);
            if (val > 0) {
                model.setBatchSize(val);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "batch size must be positive integer");
            }

            tok = tok(lexer, "into or o3MaxLag");
            if (isO3MaxLagKeyword(tok)) {
                int pos = lexer.getPosition();
                model.setO3MaxLag(SqlUtil.expectMicros(tok(lexer, "lag value"), pos));
                tok = tok(lexer, "into");
            }
        }

        if (!isIntoKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'into' expected");
        }

        tok = tok(lexer, "table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTableName(nextLiteral(assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()));

        tok = tok(lexer, "'(' or 'select'");

        if (Chars.equals(tok, '(')) {
            do {
                tok = tok(lexer, "column");
                if (Chars.equals(tok, ')')) {
                    throw err(lexer, tok, "missing column name");
                }

                assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                model.addColumn(unquote(tok), lexer.lastTokenPosition());
            } while (Chars.equals((tok = tok(lexer, "','")), ','));

            expectTok(tok, lexer.lastTokenPosition(), ')');
            tok = optTok(lexer);
        }

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "'select' or 'values' expected");
        }

        if (isSelectKeyword(tok)) {
            model.setSelectKeywordPosition(lexer.lastTokenPosition());
            lexer.unparseLast();
            final IQueryModel queryModel = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls, false);
            model.setQueryModel(queryModel);
            tok = optTok(lexer);
            // no more tokens or ';' should indicate end of statement
            if (tok == null || Chars.equals(tok, ';')) {
                return model;
            }
            throw errUnexpected(lexer, tok);
        }

        // if not INSERT INTO SELECT, make it atomic (select returns early)
        model.setBatchSize(-1);

        // if they used atomic or batch keywords, then throw an error
        if (atomicSpecified) {
            throw SqlException.$(lexer.lastTokenPosition(), "'atomic' keyword can only be used in INSERT INTO SELECT statements.");
        }

        if (isValuesKeyword(tok)) {
            do {
                expectTok(lexer, '(');
                ObjList<ExpressionNode> rowValues = new ObjList<>();
                do {
                    rowValues.add(expectExpr(lexer, sqlParserCallback));
                } while (Chars.equals((tok = tok(lexer, "','")), ','));
                expectTok(tok, lexer.lastTokenPosition(), ')');
                model.addRowTupleValues(rowValues);
                model.addEndOfRowTupleValuesPosition(lexer.lastTokenPosition());
                tok = optTok(lexer);
                // no more tokens or ';' should indicate end of statement
                if (tok == null || Chars.equals(tok, ';')) {
                    return model;
                }
                expectTok(tok, lexer.lastTokenPosition(), ',');
            } while (true);
        }

        throw err(lexer, tok, "'select' or 'values' expected");
    }

    private IQueryModel parseJoin(
            GenericLexer lexer,
            IQueryModel model,
            CharSequence tok,
            int joinType,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parent,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        int errorPos = lexer.lastTokenPosition();

        if (isNotJoinKeyword(tok) && !Chars.equals(tok, ',')) {
            // not already a join?
            // was it "left", "right", "full" or window?
            if (isLeftKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_LEFT_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isRightKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_RIGHT_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isFullKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_FULL_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isWindowKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_WINDOW;
            } else if (isHorizonKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_HORIZON;
            } else if (isLateralKeyword(tok)) {
                joinType = IQueryModel.JOIN_LATERAL_CROSS;
            } else {
                tok = tok(lexer, "join");
            }
            if (joinType != IQueryModel.JOIN_LATERAL_CROSS && isNotJoinKeyword(tok)) {
                throw SqlException.position(errorPos).put("'join' expected");
            }
        }

        tok = expectTableNameOrSubQuery(lexer);

        // UNNEST in comma position: FROM t, UNNEST(...)
        if (isUnnestKeyword(tok) && joinType == QueryModel.JOIN_CROSS) {
            return parseUnnest(lexer, model, decls, sqlParserCallback);
        }

        if (isLateralKeyword(tok) && joinType != IQueryModel.JOIN_LATERAL_CROSS) {
            joinType = switch (joinType) {
                case IQueryModel.JOIN_LEFT_OUTER -> IQueryModel.JOIN_LATERAL_LEFT;
                case IQueryModel.JOIN_INNER -> IQueryModel.JOIN_LATERAL_INNER;
                case IQueryModel.JOIN_CROSS -> IQueryModel.JOIN_LATERAL_CROSS;
                default -> throw SqlException.position(lexer.lastTokenPosition())
                        .put("LATERAL is only supported with INNER, LEFT, or CROSS joins");
            };
            tok = expectTableNameOrSubQuery(lexer);
        }

        if (IQueryModel.isLateralJoin(joinType) && !Chars.equals(tok, '(')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("LATERAL requires a subquery");
        }

        QueryModel joinModel = queryModelPool.next();
        joinModel.copyDeclsFrom(decls, false);
        joinModel.setJoinType(joinType);
        joinModel.setJoinKeywordPosition(errorPos);

        final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
        if (tt != null && tt.isView()) {
            compileViewQuery(joinModel, tt, lexer.lastTokenPosition());
        } else if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseAsSubQueryAndExpectClosingBrace(lexer, parent, true, sqlParserCallback, decls));
        } else {
            lexer.unparseLast();
            parseSelectFrom(lexer, joinModel, parent, sqlParserCallback);
        }

        tok = setModelAliasAndGetOptTok(lexer, joinModel);

        if ((joinType == IQueryModel.JOIN_CROSS || joinType == IQueryModel.JOIN_LATERAL_CROSS) && tok != null && isOnKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
        }

        boolean onClauseObserved = false;
        switch (joinType) {
            case IQueryModel.JOIN_ASOF:
            case IQueryModel.JOIN_LT:
            case IQueryModel.JOIN_SPLICE:
            case IQueryModel.JOIN_WINDOW:
            case IQueryModel.JOIN_HORIZON:
            case IQueryModel.JOIN_LATERAL_INNER:
            case IQueryModel.JOIN_LATERAL_LEFT:
                if (tok == null || !isOnKeyword(tok)) {
                    lexer.unparseLast();
                    break;
                }
                // intentional fall through
            case IQueryModel.JOIN_INNER:
            case IQueryModel.JOIN_LEFT_OUTER:
            case IQueryModel.JOIN_RIGHT_OUTER:
            case IQueryModel.JOIN_FULL_OUTER:
                expectTok(lexer, tok, "on");
                onClauseObserved = true;
                // A join nested in a lambda sub-query (e.g. "x IN (SELECT ... JOIN ... ON ...)")
                // leaves the outer operand on the shared arg stack; raise the floor so the drain
                // cannot consume it, and reject unsupported ON-clause sub-queries at any depth.
                expressionTreeBuilder.pushArgStackBottom();
                try {
                    expressionParser.parseExpr(lexer, expressionTreeBuilder, sqlParserCallback, decls);
                    ExpressionNode expr;
                    switch (expressionTreeBuilder.size()) {
                        case 0:
                            throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
                        case 1:
                            expr = expressionTreeBuilder.poll();
                            assert expr != null;
                            // Expand declared variables (and other known rewrites) up front, before the
                            // literal/criteria dispatch. A variable bound to a bare column then behaves
                            // exactly like an inline shorthand join column; one bound to a sub-query or
                            // expression flows into the criteria branch below, where the sub-query reject
                            // fires. So the declared form matches its inline expansion in every ON-clause
                            // position -- shorthand column and criteria alike -- not just operator forms.
                            expr = rewriteKnownStatements(expr, decls, null);
                            if (expr.type == ExpressionNode.LITERAL) {
                                do {
                                    joinModel.addJoinColumn(expr);
                                } while ((expr = expressionTreeBuilder.poll()) != null);
                            } else {
                                traversalAlgo.traverse(expr, rejectJoinSubQueryRef);
                                joinModel.setJoinCriteria(expr);
                            }
                            break;
                        default:
                            // "join on (a,b,c)", a list of shorthand join columns. Declared variables
                            // expand here too: one bound to a column joins like the inline column, while
                            // one bound to a sub-query is rejected (sub-queries are unsupported in ON
                            // clauses), matching the inline forms instead of leaking a raw "@q" literal.
                            while ((expr = expressionTreeBuilder.poll()) != null) {
                                expr = rewriteKnownStatements(expr, decls, null);
                                if (expr.type == ExpressionNode.QUERY) {
                                    throw SqlException.$(expr.position, "query is not allowed here");
                                }
                                if (expr.type != ExpressionNode.LITERAL) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "Column name expected");
                                }
                                joinModel.addJoinColumn(expr);
                            }
                            break;
                    }
                } catch (SqlException e) {
                    expressionTreeBuilder.reset();
                    throw e;
                } finally {
                    expressionTreeBuilder.popArgStackBottom();
                }
                break;
            default:
                lexer.unparseLast();
                break;
        }

        tok = optTok(lexer);
        if (joinType == IQueryModel.JOIN_WINDOW) {
            expectTok(lexer, tok, "range");
            tok = optTok(lexer);
            expectTok(lexer, tok, "between");
            tok = tok(lexer, "'unbounded', 'current' or expression");
            WindowJoinContext context = joinModel.getWindowJoinContext();

            // lo
            if (isUnboundedPreceding(lexer, tok)) {
                context.setLoKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
            } else if (isCurrentRow(lexer, tok)) {
                context.setLoKind(WindowJoinContext.CURRENT, lexer.lastTokenPosition());
            } else if (isPrecedingKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
            } else {
                int pos = lexer.lastTokenPosition();
                lexer.unparseLast();
                context.setLoExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                char timeUnit = parseTimeUnit(lexer);
                if (timeUnit != 0) {
                    context.setLoExprTimeUnit(timeUnit);
                }

                tok = tok(lexer, "'preceding' or 'following'");
                if (isPrecedingKeyword(tok)) {
                    context.setLoKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
                } else if (isFollowingKeyword(tok)) {
                    context.setLoKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                }
            }

            tok = optTok(lexer);
            expectTok(lexer, tok, "and");
            tok = tok(lexer, "'unbounded', 'current' or expression");
            // hi
            if (isUnboundedKeyword(tok)) {
                tok = tok(lexer, "'following'");
                if (isFollowingKeyword(tok)) {
                    context.setHiKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'following' expected");
                }
            } else if (isCurrentRow(lexer, tok)) {
                context.setHiKind(WindowJoinContext.CURRENT, lexer.lastTokenPosition());
            } else if (isPrecedingKeyword(tok) || isFollowingKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
            } else {
                int pos = lexer.lastTokenPosition();
                lexer.unparseLast();
                context.setHiExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                char timeUnit = parseTimeUnit(lexer);
                if (timeUnit != 0) {
                    context.setHiExprTimeUnit(timeUnit);
                }

                tok = tok(lexer, "'preceding'  'following'");
                if (isPrecedingKeyword(tok)) {
                    if (context.getLoKind() == WindowJoinContext.CURRENT) {
                        throw SqlException.$(lexer.lastTokenPosition(), "start row is CURRENT, end row must not be PRECEDING");
                    }
                    if (context.getLoKind() == WindowJoinContext.FOLLOWING) {
                        throw SqlException.$(lexer.lastTokenPosition(), "start row is FOLLOWING, end row must not be PRECEDING");
                    }
                    context.setHiKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
                } else if (isFollowingKeyword(tok)) {
                    context.setHiKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                }
            }

            tok = optTok(lexer);
            if (tok != null) {
                if (isIncludePrevailing(lexer, tok)) {
                    context.setIncludePrevailing(true);
                } else if (isExcludePrevailing(lexer, tok)) {
                    context.setIncludePrevailing(false);
                } else {
                    lexer.unparseLast();
                }
            } else {
                lexer.unparseLast();
            }
            return joinModel;
        }

        if (joinType == IQueryModel.JOIN_HORIZON) {
            HorizonJoinContext context = joinModel.getHorizonJoinContext();

            // RANGE/LIST clause is optional for non-last HORIZON JOINs in a multi-join chain.
            // If the next token is not range/list, this is a non-last HORIZON JOIN — return as-is.
            if (tok == null || (!isRangeKeyword(tok) && !isListKeyword(tok))) {
                lexer.unparseLast();
                return joinModel;
            }

            if (isRangeKeyword(tok)) {
                // RANGE FROM <interval> TO <interval> STEP <interval> AS <alias>
                context.setMode(HorizonJoinContext.MODE_RANGE);

                expectTok(lexer, "from");
                ExpressionNode fromExpr = expectIntervalLiteral(lexer);
                context.setRangeFrom(fromExpr, fromExpr.position);

                tok = tok(lexer, "'to'");
                expectTok(lexer, tok, "to");
                ExpressionNode toExpr = expectIntervalLiteral(lexer);
                context.setRangeTo(toExpr);

                tok = tok(lexer, "'step'");
                expectTok(lexer, tok, "step");
                ExpressionNode stepExpr = expectIntervalLiteral(lexer);
                context.setRangeStep(stepExpr, stepExpr.position);
            } else if (isListKeyword(tok)) {
                // LIST (<expr>, <expr>, ...) AS <alias>
                context.setMode(HorizonJoinContext.MODE_LIST);

                tok = tok(lexer, "'('");
                expectTok(lexer, tok, "(");

                // Parse list of offset expressions
                // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
                tok = tokIncludingLocalBrace(lexer, "expression");
                if (Chars.equals(tok, ')')) {
                    throw SqlException.$(lexer.lastTokenPosition(), "at least one offset expression expected");
                }
                lexer.unparseLast();

                while (true) {
                    ExpressionNode offsetExpr = expectIntervalLiteral(lexer);
                    context.addListOffset(offsetExpr);

                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
                    }
                }
            }

            // Expect AS <alias>
            tok = tok(lexer, "'as'");
            expectTok(lexer, tok, "as");
            tok = tok(lexer, "alias");
            int aliasPos = lexer.lastTokenPosition();
            ExpressionNode aliasNode = literal(tok, aliasPos);
            context.setAlias(aliasNode, aliasPos);

            // Create synthetic offset model for the horizon pseudo-table
            // This model represents the virtual table with offset/timestamp columns
            IQueryModel syntheticOffsetModel = queryModelPool.next();
            syntheticOffsetModel.setJoinType(IQueryModel.JOIN_CROSS);
            syntheticOffsetModel.setAlias(aliasNode);

            // Move HorizonJoinContext to the synthetic model
            // The synthetic model holds the range/list configuration
            HorizonJoinContext syntheticContext = syntheticOffsetModel.getHorizonJoinContext();
            syntheticContext.copyFrom(context);
            context.clear();

            // Add offset and timestamp columns to the synthetic model
            ExpressionNode offsetNode = expressionNodePool.next().of(ExpressionNode.LITERAL, "offset", 0, aliasPos);
            syntheticOffsetModel.addField(queryColumnPool.next().of("offset", offsetNode));

            ExpressionNode timestampNode = expressionNodePool.next().of(ExpressionNode.LITERAL, "timestamp", 0, aliasPos);
            syntheticOffsetModel.addField(queryColumnPool.next().of("timestamp", timestampNode));

            // Add synthetic model to parent's join models before the HORIZON JOIN model
            model.addJoinModel(syntheticOffsetModel);

            return joinModel;
        }

        if (tok == null || !SqlKeywords.isToleranceKeyword(tok)) {
            lexer.unparseLast();
            return joinModel;
        }
        if (joinType != IQueryModel.JOIN_ASOF && joinType != IQueryModel.JOIN_LT) {
            throw SqlException.$(lexer.lastTokenPosition(), "TOLERANCE is only supported for ASOF and LT joins");
        }

        final ExpressionNode n = expr(lexer, null, sqlParserCallback, decls);
        if (n == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE period expected");
        }
        if (n.type == ExpressionNode.OPERATION && n.token != null && Chars.equals(n.token, "-")) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE must be positive");
        }
        if (n.type != ExpressionNode.CONSTANT) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE must be a constant");
        }
        joinModel.setAsOfJoinTolerance(n);

        if (!onClauseObserved) {
            // no join clauses yet
            tok = optTok(lexer);
            if (tok != null && SqlKeywords.isOnKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'ON' clause must precede 'TOLERANCE' clause. " +
                        "Hint: put the ON condition right after the JOIN, then add TOLERANCE, " +
                        "e.g. … ASOF JOIN t2 ON t1.ts = t2.ts TOLERANCE 1h");
            }
            lexer.unparseLast();
        }
        return joinModel;
    }

    private void parseLatestBy(GenericLexer lexer, IQueryModel model) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null) {
            if (isByKeyword(tok)) {
                parseLatestByDeprecated(lexer, model);
                return;
            }
            if (isOnKeyword(tok)) {
                parseLatestByNew(lexer, model);
                return;
            }
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'on' or 'by' expected");
    }

    private void parseLatestByDeprecated(GenericLexer lexer, IQueryModel model) throws SqlException {
        // 'latest by' is already parsed at this point

        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer, model.getDecls()));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(IQueryModel.LATEST_BY_DEPRECATED);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    private void parseLatestByNew(GenericLexer lexer, IQueryModel model) throws SqlException {
        // 'latest on' is already parsed at this point

        // <timestamp>
        final ExpressionNode timestamp = expectLiteral(lexer, model.getDecls());
        model.setTimestamp(timestamp);
        // 'partition by'
        expectTok(lexer, "partition");
        expectTok(lexer, "by");
        // <columns>
        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer, model.getDecls()));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(IQueryModel.LATEST_BY_NEW);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    /**
     * Parses PIVOT clause with the following syntax:
     * <pre>
     * PIVOT (
     *     agg_func(col) [AS alias], ...
     *     FOR pivot_col IN (val1 [AS alias1], val2 [AS alias2], ... | subquery)
     *     [FOR pivot_col2 IN (...)]
     *     [GROUP BY col1, col2, ...] ) [alias]
     * </pre>
     * <p>
     * <b>Note:</b> ELSE clause is not supported in PIVOT FOR columns. Two-phase aggregates (e.g., avg requires
     * sum + count in phase 1) cannot be correctly rewritten for ELSE values. Inserting
     * appropriate expressions during the first aggregation phase is complex and would
     * significantly impact performance. This aligns with mainstream databases which also
     * do not support ELSE in PIVOT. For such requirements, user can use subqueries instead.
     */
    private CharSequence parsePivot(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok;
        expectTok(lexer, '(');

        // Parse aggregate functions.
        FunctionFactoryCache functionFactoryCache = cairoEngine.getFunctionFactoryCache();
        pivotAliasMap.clear();
        do {
            model.addPivotGroupByColumn(parsePivotAggregateColumn(lexer, model, functionFactoryCache, sqlParserCallback));
            tok = optTok(lexer);
        } while (tok != null && isNotForKeyword(tok) && isComma(tok));

        ObjList<QueryColumn> pivotGroupByCols = model.getPivotGroupByColumns();
        boolean hasNoAlias = false;
        for (int i = 0, n = pivotGroupByCols.size(); i < n; i++) {
            QueryColumn qc = pivotGroupByCols.getQuick(i);
            if (qc.getAlias() == null) {
                hasNoAlias = true;
                CharacterStoreEntry entry = characterStore.newEntry();
                qc.getAst().toSink(entry);
                CharSequence alias = SqlUtil.createExprColumnAlias(
                        characterStore,
                        entry.toImmutable(),
                        pivotAliasMap,
                        aliasSequenceMap,
                        configuration.getColumnAliasGeneratedMaxSize(),
                        true
                );
                pivotAliasMap.add(alias);
                qc.setAlias(alias, qc.getAst().position);
            }
        }
        model.setPivotGroupByColumnHasNoAlias(hasNoAlias && pivotGroupByCols.size() == 1);

        if (tok == null || isNotForKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "expected FOR");
        }

        // Parse FOR expressions (e.g., FOR region IN ('East', 'West') ELSE 'Other')
        // We parse "col IN (values)" separately (not as standard IN expression) because
        // PIVOT supports per-value aliases (e.g., 'value' AS alias) which regular IN doesn't.
        while (true) {
            ExpressionNode inColumnExpr;
            try {
                // Stop at top-level IN operator to handle values list with alias support
                expressionParser.setStopOnTopINOperator(true);
                inColumnExpr = expr(lexer, model, sqlParserCallback);
            } finally {
                expressionParser.setStopOnTopINOperator(false);
            }
            if (inColumnExpr == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "expected IN expression");
            }
            if (hasGroupByFunc(sqlNodeStack, functionFactoryCache, inColumnExpr)) {
                throw SqlException.$(inColumnExpr.position, "aggregate functions are not supported in PIVOT FOR expressions");
            }

            expectTok(lexer, "in");
            expectTok(lexer, '(');
            tok = tok(lexer, "'select' or constant");
            lexer.unparseLast();
            boolean isSubquery = isSelectKeyword(tok);
            final PivotForColumn pivotForColumn = pivotQueryColumnPool.next().of(inColumnExpr, !isSubquery);
            model.addPivotForColumn(pivotForColumn);

            if (isSubquery) { // IN list from subquery
                ExpressionNode expr = expr(lexer, model, sqlParserCallback);
                if (expr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "missing subquery");
                }
                pivotForColumn.setSelectSubqueryExpr(expr);
                expectTok(lexer, ')');
            } else {
                tempCharSequenceSet.clear();
                pivotAliasMap.clear();
                aliasSequenceMap.clear();
                do {
                    ExpressionNode expr = expr(lexer, model, sqlParserCallback);
                    if (expr == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "missing constant");
                    }
                    CharacterStoreEntry entry = characterStore.newEntry();
                    expr.toSink(entry);
                    CharSequence exprName = entry.toImmutable();
                    final int index = tempCharSequenceSet.keyIndex(exprName);
                    if (index < 0) {
                        throw SqlException.$(expr.position, "duplicate value in PIVOT IN list: ").put(exprName);
                    }
                    tempCharSequenceSet.addAtWithBorrowed(index, exprName);

                    CharSequence nextTok = tok(lexer, "',' or ')'");
                    CharSequence alias;
                    if (isNotForKeyword(nextTok) && columnAliasStop.excludes(nextTok)) {
                        assertNotDot(lexer, nextTok);
                        if (isAsKeyword(nextTok)) {
                            nextTok = tok(lexer, "alias");
                            SqlKeywords.assertNameIsQuotedOrNotAKeyword(nextTok, lexer.lastTokenPosition());
                            CharSequence aliasTok = GenericLexer.immutableOf(nextTok);
                            validateIdentifier(lexer, aliasTok);
                            alias = unquote(aliasTok);
                        } else {
                            validateIdentifier(lexer, nextTok);
                            SqlKeywords.assertNameIsQuotedOrNotAKeyword(nextTok, lexer.lastTokenPosition());
                            alias = GenericLexer.immutableOf(unquote(nextTok));
                        }
                        if (!pivotAliasMap.add(alias)) {
                            throw SqlException.$(lexer.lastTokenPosition(), "duplicate alias in PIVOT IN list: ").put(alias);
                        }
                    } else {
                        lexer.unparseLast();
                        alias = SqlUtil.createExprColumnAlias(
                                characterStore,
                                unquote(exprName),
                                pivotAliasMap,
                                aliasSequenceMap,
                                configuration.getColumnAliasGeneratedMaxSize(),
                                true
                        );
                        pivotAliasMap.add(alias);
                    }

                    pivotForColumn.addValue(expr, alias);
                    tok = tok(lexer, "constant list");
                } while (isComma(tok));

                if (!isRightParen(tok)) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
            }

            tok = optTok(lexer);
            if (tok == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "')' expected");
            }
            if (pivotForStop.contains(tok)) {
                break;
            } else {
                lexer.unparseLast();
            }
        }

        // Parse optional GROUP BY clause
        if (isGroupKeyword(tok)) {
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();
                ExpressionNode groupByExpr = expr(lexer, model, sqlParserCallback, model.getDecls());

                if (groupByExpr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "group by expression expected");
                }

                switch (groupByExpr.type) {
                    case ExpressionNode.LITERAL:
                    case ExpressionNode.CONSTANT:
                    case ExpressionNode.FUNCTION:
                    case ExpressionNode.OPERATION:
                        break;
                    default:
                        throw SqlException.$(lexer.lastTokenPosition(), "group by expression expected");
                }

                model.addGroupBy(groupByExpr);
                tok = optTok(lexer);
            } while (tok != null && !isRightParen(tok) && isComma(tok));
        }

        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "missing ')'");
        }

        if (!isRightParen(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "')' expected");
        }
        tok = setModelAliasAndGetOptTok(lexer, model);

        return tok;
    }

    private QueryColumn parsePivotAggregateColumn(GenericLexer lexer, IQueryModel model, FunctionFactoryCache functionFactoryCache, SqlParserCallback sqlParserCallback) throws SqlException {
        ExpressionNode expr = expr(lexer, model, sqlParserCallback);
        if (expr == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "missing aggregate function expression");
        }
        if (!hasGroupByFunc(sqlNodeStack, functionFactoryCache, expr)) {
            throw SqlException.$(expr.position, "expected aggregate function [col=").put(expr).put(']');
        }
        CharSequence tok = tok(lexer, "'FOR' or ',' or ')'");
        QueryColumn col = queryColumnPool.next().of(null, expr);

        if (isNotForKeyword(tok) && columnAliasStop.excludes(tok)) {
            CharSequence alias;
            assertNotDot(lexer, tok);
            if (isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                CharSequence aliasTok = GenericLexer.immutableOf(tok);
                validateIdentifier(lexer, aliasTok);
                alias = unquote(aliasTok);
            } else {
                validateIdentifier(lexer, tok);
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                alias = GenericLexer.immutableOf(unquote(tok));
            }
            col.setAlias(alias, lexer.lastTokenPosition());
        } else {
            lexer.unparseLast();
        }
        return col;
    }

    private ExecutionModel parseRenameStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        RenameTableModel model = renameTableModelPool.next();

        CharSequence tok = tok(lexer, "from table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

        model.setFrom(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = tok(lexer, "to");
        if (Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }
        lexer.unparseLast();

        expectTok(lexer, "to");

        tok = tok(lexer, "to table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTo(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = optTok(lexer);

        if (tok != null && Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }

        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "debris?");
        }

        return model;
    }

    private ExecutionModel parseSelect(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        lexer.unparseLast();
        final IQueryModel model = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls, false);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            model.recordViews(recordedViews);
            return model;
        }
        if (Chars.equals(tok, ":=")) {
            throw errUnexpected(lexer, tok, "perhaps `DECLARE` was misspelled?");
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseSelectClause(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        int pos = lexer.getPosition();
        CharSequence tok = SqlUtil.fetchNext(lexer, true);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')'))) {
            throw SqlException.position(pos).put("[distinct] column expected");
        }

        if (Chars.equals(tok, "/*+")) {
            parseHints(lexer, model);
            tok = tok(lexer, "[distinct] column");
        }

        ExpressionNode expr;
        if (isDistinctKeyword(tok)) {
            model.setDistinct(true);
        } else {
            lexer.unparseLast();
        }

        try {
            boolean hasFrom = false;
            while (true) {
                tok = tok(lexer, "column");
                if (Chars.equals(tok, '*')) {
                    expr = nextLiteral(GenericLexer.immutableOf(tok), lexer.lastTokenPosition());
                } else {
                    // cut off some obvious errors
                    if (isFromKeyword(tok)) {
                        if (accumulatedColumns.size() == 0) {
                            throw SqlException.$(lexer.lastTokenPosition(), "column expression expected");
                        }
                        hasFrom = true;
                        lexer.unparseLast();
                        break;
                    }

                    if (isSelectKeyword(tok)) {
                        throw SqlException.$(lexer.getPosition(), "reserved name");
                    }

                    if (isUnexpectedRightParenInTopLevelSelect(tok)) {
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [)]");
                    }

                    lexer.unparseLast();
                    expr = expr(lexer, model, sqlParserCallback, model.getDecls());

                    if (expr == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "missing expression");
                    }

                    if (Chars.endsWith(expr.token, '.') && expr.type == ExpressionNode.LITERAL) {
                        throw SqlException.$(expr.position + expr.token.length(), "'*' or column name expected");
                    }
                }

                tok = optTok(lexer);

                QueryColumn col;
                final int colPosition = lexer.lastTokenPosition();

                // ExpressionParser now handles window functions (func(...) OVER (...)) as part of
                // expression parsing. When windowContext is set, the OVER clause has already been
                // consumed from the lexer and we can use the parsed WindowColumn directly.
                if (expr.windowExpression != null) {
                    // ExpressionParser already parsed the window function with its OVER clause
                    col = expr.windowExpression;
                } else {
                    // Regular expression (non-window function)
                    if (expr.type == ExpressionNode.QUERY) {
                        throw SqlException.$(expr.position, "query is not expected, did you mean column?");
                    }
                    col = queryColumnPool.next().of(null, expr);
                }

                final CharSequence alias;
                final int aliasPosition;
                if (tok != null && columnAliasStop.excludes(tok)) {
                    assertNotDot(lexer, tok);
                    if (isAsKeyword(tok)) {
                        tok = tok(lexer, "alias");
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        CharSequence aliasTok = GenericLexer.immutableOf(tok);
                        validateIdentifier(lexer, aliasTok);
                        boolean unquoting = Chars.indexOf(aliasTok, '.') == -1;
                        alias = unquoting ? unquote(aliasTok) : aliasTok;
                    } else {
                        validateIdentifier(lexer, tok);
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        boolean unquoting = Chars.indexOf(tok, '.') == -1;
                        alias = GenericLexer.immutableOf(unquoting ? unquote(tok) : tok);
                    }
                    aliasPosition = lexer.lastTokenPosition();

                    if (col.getAst().isWildcard()) {
                        throw err(lexer, null, "wildcard cannot have alias");
                    }

                    tok = optTok(lexer);
                    aliasMap.add(alias);
                } else {
                    alias = null;
                    aliasPosition = QueryColumn.SYNTHESIZED_ALIAS_POSITION;
                }

                // correlated sub-queries do not have expr.token values (they are null)
                if (expr.type == ExpressionNode.QUERY) {
                    expr.token = alias;
                }

                if (alias != null) {
                    if (alias.isEmpty()) {
                        throw err(lexer, null, "column alias cannot be a blank string");
                    }
                    col.setAlias(alias, aliasPosition);
                }

                accumulatedColumns.add(col);
                accumulatedColumnPositions.add(colPosition);

                if (tok == null || Chars.equals(tok, ';')) {
                    lexer.unparseLast();
                    break;
                }

                if (Chars.equals(tok, ')')) {
                    if (isUnexpectedRightParenInTopLevelSelect(tok)) {
                        // it's an unbalanced ')' in top-level SELECT
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [)]");
                    } else {
                        // it's a balanced: ')'
                        lexer.unparseLast();
                        break;
                    }
                }

                if (isFromKeyword(tok)) {
                    hasFrom = true;
                    lexer.unparseLast();
                    break;
                }

                if (setOperations.contains(tok)) {
                    lexer.unparseLast();
                    break;
                }

                if (!Chars.equals(tok, ',')) {
                    if (isIgnoreWord(tok) || isRespectWord(tok)) {
                        throw err(lexer, tok, "',', 'nulls' or 'from' expected");
                    }
                    throw err(lexer, tok, "',', 'from' or 'over' expected");
                }
            }

            for (int i = 0, n = accumulatedColumns.size(); i < n; i++) {
                QueryColumn qc = accumulatedColumns.getQuick(i);
                if (qc.getAlias() == null) {
                    generateColumnAlias(lexer, qc, hasFrom);
                }
                model.addBottomUpColumn(accumulatedColumnPositions.getQuick(i), qc, false);
            }
        } finally {
            accumulatedColumns.clear();
            accumulatedColumnPositions.clear();
            aliasMap.clear();
            aliasSequenceMap.clear();
        }
    }

    private void parseSelectFrom(
            GenericLexer lexer,
            IQueryModel model,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> masterModel,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls());
        if (expr == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put("table name expected");
        }

        // subquery is expected to be handled outside
        if (expr.type != ExpressionNode.LITERAL && expr.type != ExpressionNode.CONSTANT && expr.type != ExpressionNode.FUNCTION) {
            throw SqlException.$(expr.position, "function, literal or constant is expected");
        }

        // check if it's a decl
        if (model.getDecls().contains(expr.token)) {
            if (expr.type == ExpressionNode.LITERAL) {
                // replace it if so
                expr = model.getDecls().get(expr.token).rhs;
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "expected literal table name or subquery");
            }
        }

        CharSequence tableName = expr.token;
        switch (expr.type) {
            case ExpressionNode.LITERAL:
            case ExpressionNode.CONSTANT:
                final WithClauseModel withClause = masterModel.get(tableName);
                if (withClause != null) {
                    IQueryModel cteModel = parseWith(lexer, withClause, sqlParserCallback, model.getDecls());
                    cteModel.setIsCteModel(true);
                    model.setNestedModel(cteModel);
                    model.setAlias(literal(tableName, expr.position));
                } else {
                    int dot = Chars.indexOfLastUnquoted(tableName, '.');
                    if (dot == -1) {
                        model.setTableNameExpr(literal(tableName, expr.position));
                    } else {
                        if (isPublicKeyword(tableName, 0, dot)) {
                            if (dot + 1 == tableName.length()) {
                                throw SqlException.$(expr.position, "table name expected");
                            }

                            BufferWindowCharSequence fs = (BufferWindowCharSequence) tableName;
                            fs.shiftLo(dot + 1);
                            model.setTableNameExpr(literal(tableName, expr.position + dot + 1));
                        } else {
                            model.setTableNameExpr(literal(tableName, expr.position));
                        }
                    }
                }
                break;
            case ExpressionNode.FUNCTION:
                model.setTableNameExpr(expr);
                break;
            default:
                throw SqlException.$(expr.position, "function, literal or constant is expected");
        }

        // Read-time row-expiry filter (approach A): if this resolved to a plain table (not a CTE,
        // not a table-function) that carries an EXPIRE ROWS policy, transparently rewrite the
        // reference into a nested "SELECT * FROM t WHERE <keep-filter>" so expired rows are hidden
        // from every read. This is the single chokepoint for plain-table resolution: both the FROM
        // branch (parseFromClause) and the JOIN branch funnel through parseSelectFrom, and the inner
        // tables of sub-queries/CTE bodies recurse back here too. The CTE *reference* case set a
        // nested model above (tableNameExpr stays null), so it is naturally skipped.
        final ExpressionNode resolvedTableNameExpr = model.getTableNameExpr();
        if (
                resolvedTableNameExpr != null
                        && resolvedTableNameExpr.type == ExpressionNode.LITERAL
                        && cairoEngine.getMetadataCache().mayHaveExpiryPolicy()
        ) {
            // Normalise to the unquoted name so the recursion guard matches regardless of quoting:
            // the outer reference may be "from t" while the synthetic inner is "from \"t\"".
            final CharSequence unquotedName = unquote(resolvedTableNameExpr.token);
            // Guard against the synthetic inner "SELECT * FROM t" re-expanding (infinite recursion).
            if (!expiringTablesBeingExpanded.contains(unquotedName)) {
                final TableToken tt = cairoEngine.getTableTokenIfExists(unquotedName);
                final String predicate;
                if (tt != null && !tt.isView()
                        && isExpiryReadFilterEnabledFor(tt)
                        && cairoEngine.getMetadataCache().mayTableHaveExpiryPolicy(tt)
                        && (predicate = lookupExpiryPredicate(tt)) != null) {
                    final CharSequence designatedTimestampColumn = expiryTimestampColumnName;
                    final int position = resolvedTableNameExpr.position;
                    model.setTableNameExpr(null);
                    // The set stores references, not copies, and unquote() of a quoted token yields a
                    // view over the (transient) lexer buffer; store a stable String, like
                    // viewsBeingCompiled does, so the key survives the nested parse.
                    final String guardKey = Chars.toString(unquotedName);
                    expiringTablesBeingExpanded.add(guardKey);
                    expiryExpandedTables.add(guardKey);
                    try {
                        expandExpiringTable(model, guardKey, predicate, designatedTimestampColumn, position, sqlParserCallback);
                    } finally {
                        expiringTablesBeingExpanded.remove(guardKey);
                    }
                }
            }
        }
    }

    // The read-filter decision for one resolved table: the context's per-table refinement when a
    // context is present (the mat-view refresh context keeps the filter on every table except the
    // base), the parse-global flag otherwise.
    private boolean isExpiryReadFilterEnabledFor(TableToken tableToken) {
        return expiryFilterExecutionContext == null
                ? rowExpiryReadFilterEnabled
                : expiryFilterExecutionContext.isExpiryReadFilterEnabled(tableToken);
    }

    private int parseSymbolCapacity(GenericLexer lexer) throws SqlException {
        final int errorPosition = lexer.getPosition();
        final int symbolCapacity = expectInt(lexer);
        TableUtils.validateSymbolCapacity(errorPosition, symbolCapacity);
        return Numbers.ceilPow2(symbolCapacity);
    }

    private void parseTableName(GenericLexer lexer, IQueryModel model) throws SqlException {
        CharSequence tok = tok(lexer, "expected a table name");
        tok = sansPublicSchema(tok, lexer);
        final CharSequence tableName = assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition());
        ExpressionNode tableNameExpr = expressionNodePool.next().of(ExpressionNode.LITERAL, tableName, 0, lexer.lastTokenPosition());
        tableNameExpr = rewriteDeclaredVariables(tableNameExpr, model.getDecls(), null);
        model.setTableNameExpr(tableNameExpr);
    }

    private char parseTimeUnit(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "'preceding' or time unit");
        char unit = 0;
        if (isNanosecondsKeyword(tok) || isNanosecondKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_NANOSECOND;
        } else if (isMicrosecondKeyword(tok) || isMicrosecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MICROSECOND;
        } else if (isMillisecondKeyword(tok) || isMillisecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MILLISECOND;
        } else if (isSecondKeyword(tok) || isSecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_SECOND;
        } else if (isMinuteKeyword(tok) || isMinutesKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MINUTE;
        } else if (isHourKeyword(tok) || isHoursKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_HOUR;
        } else if (isDayKeyword(tok) || isDaysKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_DAY;
        }
        if (unit == 0) {
            lexer.unparseLast();
        }
        return unit;
    }

    private ExpressionNode parseTimestamp(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && isTimestampKeyword(tok)) {
            expectTok(lexer, '(');
            final ExpressionNode result = expectLiteral(lexer);
            tokIncludingLocalBrace(lexer, "')'");
            return result;
        }
        return null;
    }

    private IQueryModel parseUnnest(
            GenericLexer lexer,
            IQueryModel parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        // Temporarily disable subQueryMode so that optTok() does not swallow
        // the ')' tokens that belong to UNNEST's own parentheses.
        boolean savedSubQueryMode = subQueryMode;
        subQueryMode = false;
        try {
            return parseUnnest0(lexer, parent, decls, sqlParserCallback);
        } finally {
            subQueryMode = savedSubQueryMode;
        }
    }

    private IQueryModel parseUnnest0(
            GenericLexer lexer,
            IQueryModel parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryModel unnestModel = queryModelPool.next();
        unnestModel.copyDeclsFrom(decls, false);
        unnestModel.setJoinType(QueryModel.JOIN_UNNEST);
        unnestModel.setJoinKeywordPosition(lexer.lastTokenPosition());

        expectTok(lexer, '(');
        // parse comma-separated expressions, each optionally followed by
        // COLUMNS(name TYPE, ...) for JSON UNNEST sources
        do {
            ExpressionNode expression = expr(lexer, parent, sqlParserCallback, decls);
            if (expression == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "expression expected");
            }
            unnestModel.getUnnestExpressions().add(expression);
            CharSequence tok = tok(lexer, "'COLUMNS', ',' or ')'");
            if (isColumnsKeyword(tok)) {
                expectTok(lexer, '(');
                ObjList<CharSequence> colNames = new ObjList<>();
                IntList colTypes = new IntList();
                do {
                    CharSequence colNameTok = tok(lexer, "column name");
                    assertNameIsQuotedOrNotAKeyword(colNameTok, lexer.lastTokenPosition());
                    // A dotted name keeps its dots as content, not a table.column separator (matches
                    // the SELECT-alias convention). Normalize to the protective DOUBLE-quote form
                    // regardless of the user's quote style: only double quotes are recognized
                    // downstream (Chars.indexOfLastUnquoted / SqlUtil.isQuoteProtectedAlias handle '"'
                    // only), so a retained single quote or backtick would leave the dot to mis-split
                    // into a spurious table.column reference and fail to resolve at compile time.
                    final CharSequence unquotedColName = unquote(colNameTok);
                    final CharSequence colName;
                    if (Chars.indexOf(unquotedColName, '.') == -1) {
                        colName = GenericLexer.immutableOf(unquotedColName);
                    } else {
                        // A dotted name is re-wrapped in double quotes to keep its dots as content; an
                        // embedded double quote would break that quote parity (isQuoteProtectedAlias and
                        // Chars.indexOfLastUnquoted toggle on '"'), leaking a malformed name or, for a JSON
                        // COLUMNS key, silently matching nothing. Reject it cleanly instead.
                        if (Chars.indexOf(unquotedColName, '"') != -1) {
                            throw SqlException.$(lexer.lastTokenPosition(), "dotted UNNEST column name cannot contain a double quote");
                        }
                        final CharacterStoreEntry colNameEntry = characterStore.newEntry();
                        colNameEntry.put('"').put(unquotedColName).put('"');
                        colName = colNameEntry.toImmutable();
                    }
                    CharSequence typeName = tok(lexer, "column type");
                    int type = ColumnType.typeOf(typeName);
                    if (type == -1) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(), "unknown type: ")
                                .put(typeName);
                    }
                    if (!isJsonUnnestSupportedType(type)) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(),
                                        "unsupported type for JSON UNNEST: ")
                                .put(typeName);
                    }
                    colNames.add(colName);
                    colTypes.add(type);
                    tok = tok(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(),
                                        "',' or ')' expected");
                    }
                } while (true);
                unnestModel.getUnnestJsonColumnNames().add(colNames);
                unnestModel.getUnnestJsonColumnTypes().add(colTypes);
                tok = tok(lexer, "',' or ')'");
            } else {
                // array source - null marker
                unnestModel.getUnnestJsonColumnNames().add(null);
                unnestModel.getUnnestJsonColumnTypes().add(null);
            }
            if (Chars.equals(tok, ')')) {
                break;
            }
            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
            }
        } while (true);

        // optional WITH ORDINALITY
        CharSequence tok = optTok(lexer);
        if (tok != null && isWithKeyword(tok)) {
            tok = tok(lexer, "'ordinality'");
            if (!isOrdinalityKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'ordinality' expected");
            }
            unnestModel.setUnnestOrdinality(true);
            tok = optTok(lexer);
        }

        // optional AS alias
        if (tok != null && isAsKeyword(tok)) {
            tok = tok(lexer, "alias");
            unnestModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        } else if (tok != null && tableAliasStop.excludes(tok) && !Chars.equals(tok, '(')) {
            unnestModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        }

        // optional column aliases: (col1, col2, ...)
        int firstExcessAliasPos = -1;
        if (tok != null && Chars.equals(tok, '(')) {
            int maxAliases = unnestModel.getUnnestOutputColumnCount()
                    + (unnestModel.isUnnestOrdinality() ? 1 : 0);
            do {
                tok = tok(lexer, "column alias");
                int aliasPos = lexer.lastTokenPosition();
                assertNameIsQuotedOrNotAKeyword(tok, aliasPos);
                // A dotted alias keeps its dots as content (see the COLUMNS field-name note above):
                // normalize any quote style to the protective double-quote form so downstream lookups
                // treat the dots as content instead of a table.column separator.
                final CharSequence unquotedAlias = unquote(tok);
                final CharSequence aliasName;
                if (Chars.indexOf(unquotedAlias, '.') == -1) {
                    aliasName = GenericLexer.immutableOf(unquotedAlias);
                } else {
                    // see the COLUMNS field-name note: an embedded double quote breaks the protective
                    // re-wrap, so reject a dotted alias that carries one rather than leak a malformed name.
                    if (Chars.indexOf(unquotedAlias, '"') != -1) {
                        throw SqlException.$(aliasPos, "dotted UNNEST column alias cannot contain a double quote");
                    }
                    final CharacterStoreEntry aliasEntry = characterStore.newEntry();
                    aliasEntry.put('"').put(unquotedAlias).put('"');
                    aliasName = aliasEntry.toImmutable();
                }
                unnestModel.getUnnestColumnAliases().add(aliasName);
                if (firstExcessAliasPos == -1
                        && unnestModel.getUnnestColumnAliases().size() > maxAliases) {
                    firstExcessAliasPos = aliasPos;
                }
                tok = tok(lexer, "',' or ')'");
                if (Chars.equals(tok, ')')) {
                    break;
                }
                if (!Chars.equals(tok, ',')) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
                }
            } while (true);
        } else if (tok != null) {
            lexer.unparseLast();
        }

        if (firstExcessAliasPos != -1) {
            throw SqlException.$(
                    firstExcessAliasPos,
                    "too many column aliases for UNNEST"
            );
        }

        return unnestModel;
    }

    private ExecutionModel parseUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        lexer.unparseLast();
        final IQueryModel model = parseDmlUpdate(lexer, sqlParserCallback, decls);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseUpdateClause(
            GenericLexer lexer,
            IQueryModel updateQueryModel,
            IQueryModel fromModel,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CharSequence tok = tok(lexer, "table name or alias");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        CharSequence tableName = GenericLexer.immutableOf(unquote(tok));
        ExpressionNode tableNameExpr = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, tableName, 0, 0);
        updateQueryModel.setTableNameExpr(tableNameExpr);
        fromModel.setTableNameExpr(tableNameExpr);

        tok = tok(lexer, "AS, SET or table alias expected");
        if (isAsKeyword(tok)) {
            tok = tok(lexer, "table alias expected");
            if (isSetKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "table alias expected");
            }
        }

        if (!isAsKeyword(tok) && !isSetKeyword(tok)) {
            // This is table alias
            CharSequence tableAlias = GenericLexer.immutableOf(tok);
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            ExpressionNode tableAliasExpr = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, tableAlias, 0, 0);
            updateQueryModel.setAlias(tableAliasExpr);
            tok = tok(lexer, "SET expected");
        }

        if (!isSetKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "SET expected");
        }

        while (true) {
            // Column
            tok = tok(lexer, "column name");
            CharSequence col = GenericLexer.immutableOf(unquote(tok));
            int colPosition = lexer.lastTokenPosition();

            expectTok(lexer, "=");

            // Value expression
            ExpressionNode expr = expr(lexer, (IQueryModel) null, sqlParserCallback);
            ExpressionNode setColumnExpression = expressionNodePool.next().of(ExpressionNode.LITERAL, col, 0, colPosition);
            updateQueryModel.getUpdateExpressions().add(setColumnExpression);

            QueryColumn valueColumn = queryColumnPool.next().of(col, expr);
            fromModel.addBottomUpColumn(colPosition, valueColumn, false, "in SET clause");

            tok = optTok(lexer);
            if (tok == null) {
                break;
            }

            if (tok.length() != 1 || tok.charAt(0) != ',') {
                lexer.unparseLast();
                break;
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    @NotNull
    private ExecutionModel parseWith(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, decls);
        CharSequence tok = tok(lexer, "'select', 'update' or name expected");
        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, decls);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, decls);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, decls);
        }

        throw SqlException.$(lexer.lastTokenPosition(), "'select' | 'update' | 'insert' expected");
    }

    private IQueryModel parseWith(
            GenericLexer lexer,
            WithClauseModel wcm,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        IQueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        lexer.stash();
        lexer.goToPosition(wcm.getPosition());
        // this will not throw exception because this is second pass over the same sub-query
        // we wouldn't be here is syntax was wrong
        m = parseAsSubQueryAndExpectClosingBrace(lexer, wcm.getWithClauses(), false, sqlParserCallback, decls);
        lexer.unstash();
        return m;
    }

    private void parseWithClauses(
            GenericLexer lexer,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> model,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        do {
            ExpressionNode name = expectLiteral(lexer);
            if (name.token.isEmpty()) {
                throw SqlException.$(name.position, "empty common table expression name");
            }

            if (model.get(name.token) != null) {
                throw SqlException.$(name.position, "duplicate name");
            }

            expectTok(lexer, "as");
            expectTok(lexer, '(');
            int lo = lexer.lastTokenPosition();
            WithClauseModel wcm = withClauseModelPool.next();
            // todo: review passing non-null here
            wcm.of(lo + 1, model, parseAsSubQueryAndExpectClosingBrace(lexer, model, true, sqlParserCallback, decls));
            model.put(name.token, wcm);

            CharSequence tok = optTok(lexer);
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparseLast();
                break;
            }
        } while (true);
    }

    private CharSequence parseWithOffset(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok;
        expectOffset(lexer);
        ExpressionNode offsetExpr = expectExpr(lexer, sqlParserCallback, model.getDecls());
        // Normalize explicit zero offsets ('00:00', '+00:00', '-00:00') to the canonical
        // ZERO_OFFSET singleton so that identity checks against ZERO_OFFSET work consistently in the optimizer.
        model.setSampleByOffset(isZeroOffsetToken(offsetExpr.token) ? ZERO_OFFSET : offsetExpr);
        tok = optTok(lexer);
        return tok;
    }

    // Join ON-clause sub-queries are unsupported and rejected during expression parsing, but
    // declared variables are literals at parse time and only expand to their definition later, in
    // rewriteKnownStatements. A variable bound to a sub-query (e.g. "@q := (SELECT ...)" used as
    // "ON x IN @q") would therefore slip past the parse-time block and compile to surprising
    // cross-join semantics. parseJoin now expands declared variables before dispatching the ON
    // clause, then uses this visitor to walk the rewritten criteria and reject any sub-query node;
    // the shorthand column branches reject expanded QUERY nodes directly. So a declared sub-query
    // errors the same as the literal one at every nesting depth and in every ON-clause position --
    // criteria, single-column shorthand, and multi-column lists alike.
    private void rejectJoinSubQuery(ExpressionNode node) throws SqlException {
        if (node.type == ExpressionNode.QUERY) {
            throw SqlException.$(node.position, "query is not allowed here");
        }
    }

    private void rewriteCase(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCaseKeyword(node.token)) {
            tempExprNodes.clear();
            ExpressionNode literal = null;
            ExpressionNode elseExpr;
            boolean convertToSwitch = true;
            final int paramCount = node.paramCount;

            final int lim;
            if ((paramCount & 1) == 0) {
                elseExpr = node.args.getQuick(0);
                lim = 0;
            } else {
                elseExpr = null;
                lim = -1;
            }

            // args are in inverted order, hence last list item is the first arg
            ExpressionNode first = node.args.getQuick(paramCount - 1);
            if (first.token != null) {
                // simple case of 'case' :) e.g.
                // case x
                //   when 1 then 'A'
                //   ...
                node.token = "switch";
                return;
            }
            int thenRemainder = elseExpr == null ? 0 : 1;
            for (int i = paramCount - 2; i > lim; i--) {
                if ((i & 1) == thenRemainder) {
                    // this is "then" clause, copy it as is
                    tempExprNodes.add(node.args.getQuick(i));
                    continue;
                }
                ExpressionNode where = node.args.getQuick(i);
                if (where.type == ExpressionNode.OPERATION && where.token.charAt(0) == '=') {
                    ExpressionNode thisConstant;
                    ExpressionNode thisLiteral;
                    if (where.lhs.type == ExpressionNode.CONSTANT && where.rhs.type == ExpressionNode.LITERAL) {
                        thisConstant = where.lhs;
                        thisLiteral = where.rhs;
                    } else if (where.lhs.type == ExpressionNode.LITERAL && where.rhs.type == ExpressionNode.CONSTANT) {
                        thisConstant = where.rhs;
                        thisLiteral = where.lhs;
                    } else {
                        convertToSwitch = false;
                        // not supported
                        break;
                    }

                    if (literal == null) {
                        literal = thisLiteral;
                        tempExprNodes.add(thisConstant);
                    } else if (Chars.equals(literal.token, thisLiteral.token)) {
                        tempExprNodes.add(thisConstant);
                    } else {
                        convertToSwitch = false;
                        // not supported
                        break;
                    }
                } else {
                    convertToSwitch = false;
                    // not supported
                    break;
                }
            }

            if (convertToSwitch) {
                int n = tempExprNodes.size();
                node.token = "switch";
                node.args.clear();
                // else expression may not have been provided,
                // in which case it needs to be synthesized
                if (elseExpr == null) {
                    elseExpr = SqlUtil.nextConstant(expressionNodePool, "null", node.position);
                }
                node.args.add(elseExpr);
                for (int i = n - 1; i > -1; i--) {
                    node.args.add(tempExprNodes.getQuick(i));
                }
                node.args.add(literal);
                node.paramCount = n + 2;
            } else {
                // remove the 'null' marker arg
                node.args.remove(paramCount - 1);
                node.paramCount = paramCount - 1;

                // 2 args 'case', e.g. case when x>0 then 1
                if (node.paramCount < 3) {
                    node.rhs = node.args.get(0);
                    node.lhs = node.args.get(1);
                    node.args.clear();
                }
            }
        }
    }

    private void rewriteConcat(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isConcatOperator(node.token)) {
            node.type = ExpressionNode.FUNCTION;
            node.token = CONCAT_FUNC_NAME;
            addConcatArgs(node.args, node.rhs);
            addConcatArgs(node.args, node.lhs);
            node.paramCount = node.args.size();
            if (node.paramCount > 2) {
                node.rhs = null;
                node.lhs = null;
            }
        }
    }

    /**
     * Rewrites count(*) expressions to count().
     *
     * @param node expression node, provided by tree walking algo
     */
    private void rewriteCount(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCountKeyword(node.token)) {
            if (node.paramCount == 1) {
                // special case, typically something like
                // case value else expression end
                // this can be simplified to "expression" only

                ExpressionNode that = node.rhs;
                if (Chars.equalsNc(that.token, '*')) {
                    if (that.rhs == null && node.lhs == null) {
                        that.paramCount = 0;
                        node.rhs = null;
                        node.paramCount = 0;
                    }
                }
            }
        }
    }

    private void rewriteCountAndWindowExpressions(ExpressionNode node) throws SqlException {
        if (node.windowExpression != null) {
            rewriteWindowExpression(node.windowExpression);
        }
        rewriteCount(node);
    }

    private ExpressionNode rewriteDeclaredVariables(
            ExpressionNode expr,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        if (decls == null || decls.size() == 0) { // short circuit null case
            return expr;
        }
        return recursiveReplace(
                expr,
                rewriteDeclaredVariablesInExpressionVisitor.of(decls, exprTargetVariableName)
        );
    }

    /**
     * Rewrites the following:
     * <p>
     * select json_extract(json,path)::varchar -> select json_extract(json,path)
     * select json_extract(json,path)::double -> select json_extract(json,path,double)
     * select json_extract(json,path)::uuid -> select json_extract(json,path)::uuid
     * <p>
     * Notes:
     * - varchar cast is rewritten in a special way, e.g. removed
     * - subset of types is handled more efficiently in the 3-arg function
     * - the remaining type casts are not rewritten, e.g. left as is
     */
    private void rewriteJsonExtractCast(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCastKeyword(node.token)) {
            if (node.lhs != null
                    && node.lhs.type == ExpressionNode.FUNCTION
                    && node.lhs.paramCount == 2
                    && node.lhs.token != null
                    && isJsonExtract(node.lhs.token)) {
                // rewrite cast such as
                // json_extract(json,path)::type -> json_extract(json,path,type)
                // the ::type is already rewritten as
                // cast(json_extract(json,path) as type)

                // we remove the outer cast and let json_extract() do the cast
                ExpressionNode jsonExtractNode = node.lhs;
                // check if the type is a valid symbol
                ExpressionNode typeNode = node.rhs;
                if (typeNode != null) {
                    int castType = ColumnType.typeOf(typeNode.token);
                    if (castType == ColumnType.VARCHAR) {
                        // redundant cast to varchar, just remove it
                        node.token = jsonExtractNode.token;
                        node.paramCount = jsonExtractNode.paramCount;
                        node.type = jsonExtractNode.type;
                        node.position = jsonExtractNode.position;
                        node.lhs = jsonExtractNode.lhs;
                        node.rhs = jsonExtractNode.rhs;
                        node.args.clear();
                    } else if (JsonExtractTypedFunctionFactory.isIntrusivelyOptimized(castType)) {
                        node.token = jsonExtractNode.token;
                        node.paramCount = 3;
                        node.type = jsonExtractNode.type;
                        node.position = jsonExtractNode.position;
                        node.lhs = null;
                        node.rhs = null;
                        node.args.clear();

                        // args are added in reverse order

                        // type integer
                        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                        characterStoreEntry.put(castType);
                        node.args.add(
                                expressionNodePool.next().of(
                                        ExpressionNode.CONSTANT,
                                        characterStoreEntry.toImmutable(),
                                        typeNode.precedence,
                                        typeNode.position
                                )
                        );
                        node.args.add(jsonExtractNode.rhs);
                        node.args.add(jsonExtractNode.lhs);
                    }
                }
            }
        }
    }

    private ExpressionNode rewriteKnownStatements(
            ExpressionNode parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        traversalAlgo.traverse(parent, rewriteCountAndWindowExpressionsRef);
        traversalAlgo.traverse(parent, rewriteCaseRef);
        traversalAlgo.traverse(parent, rewriteConcatRef);
        traversalAlgo.traverse(parent, rewritePgCastRef);
        traversalAlgo.traverse(parent, rewriteJsonExtractCastRef);
        traversalAlgo.traverse(parent, rewritePgNumericRef);
        return rewriteDeclaredVariables(parent, decls, exprTargetVariableName);
    }

    private void rewritePgCast(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isColonColon(node.token)) {
            node.token = "cast";
            node.type = ExpressionNode.FUNCTION;
            node.rhs.type = ExpressionNode.CONSTANT;
            // In PG x::float casts x to "double precision" type
            // also, we have to rewrite postgres types such as "float8" to our native "double" type
            // All of the above also applies to array types: "float8[]" -> "double[]"
            // or "double precision[][]" -> "double[][]"

            if (rewritePgCast0(node.rhs, "float", ColumnType.DOUBLE)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "float8", ColumnType.DOUBLE)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "float4", ColumnType.FLOAT)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int4", ColumnType.INT)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int8", ColumnType.LONG)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int2", ColumnType.SHORT)) {
                return;
            }
            rewritePgCast0(node.rhs, "double precision", ColumnType.DOUBLE);
        }
    }

    private boolean rewritePgCast0(ExpressionNode typeNode, String srcTypePrefix, short type) {
        CharSequence token = typeNode.token;
        if (!Chars.startsWithLowerCase(token, srcTypePrefix)) {
            return false;
        }

        int len = token.length();
        int prefixLen = srcTypePrefix.length();
        int rem = len - prefixLen;

        if (rem == 0) {
            // full match. e.g. replacing 'float8' with 'double'
            typeNode.token = ColumnType.nameOf(type);
            return true;
        }

        // src has a suffix. it could be an array suffix. consider 'float8[][]' -> 'double[][]'
        if (rem % 2 == 0) {
            // suffix must be even, since square brackets come in pairs
            int dims = rem / 2;
            String suffix = ColumnType.ARRAY_DIM_SUFFIX[dims];
            if (Chars.endsWith(token, suffix)) {
                typeNode.token = ColumnType.nameOf(ColumnType.encodeArrayType(type, dims));
                return true;
            }
        }
        return false;
    }

    /**
     * Rewrites the following:
     * <p>
     * select '123.456'::numeric::decimal(p, s) -> select '123.456'::decimal(p, s)
     */
    private void rewritePgNumeric(ExpressionNode node) {
        if (node.type != ExpressionNode.FUNCTION || !isCastKeyword(node.token)) {
            return;
        }

        ExpressionNode innerCastNode = node.lhs;
        if (innerCastNode == null || innerCastNode.type != ExpressionNode.FUNCTION || !isCastKeyword(innerCastNode.token)) {
            return;
        }

        ExpressionNode innerTypeNode = innerCastNode.rhs;
        if (innerTypeNode == null || innerTypeNode.type != ExpressionNode.CONSTANT || !isNumericKeyword(innerTypeNode.token)) {
            return;
        }

        ExpressionNode typeNode = node.rhs;
        if (typeNode == null || typeNode.type != ExpressionNode.CONSTANT || typeNode.token.length() < 7 || !startsWithDecimalKeyword(typeNode.token)) {
            return;
        }

        // At this point, we know that the expression is compatible with our rewrite.
        node.lhs = innerCastNode.lhs;
    }

    private void rewriteWindowExpression(WindowExpression windowExpression) throws SqlException {
        final ObjList<ExpressionNode> partitionBy = windowExpression.getPartitionBy();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            rewriteWindowSubExpression(partitionBy.getQuick(i));
        }
        final ObjList<ExpressionNode> orderBy = windowExpression.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            rewriteWindowSubExpression(orderBy.getQuick(i));
        }
        rewriteWindowSubExpression(windowExpression.getRowsLoExpr());
        rewriteWindowSubExpression(windowExpression.getRowsHiExpr());
    }

    private void rewriteWindowSubExpression(ExpressionNode node) throws SqlException {
        if (node == null) {
            return;
        }
        traversalAlgo.traverse(node, rewriteCountAndWindowExpressionsRef);
        traversalAlgo.traverse(node, rewriteCaseRef);
        traversalAlgo.traverse(node, rewriteConcatRef);
        traversalAlgo.traverse(node, rewritePgCastRef);
        traversalAlgo.traverse(node, rewriteJsonExtractCastRef);
        traversalAlgo.traverse(node, rewritePgNumericRef);
    }

    @NotNull
    private CharSequence sansPublicSchema(@NotNull CharSequence tok, GenericLexer lexer) throws SqlException {
        int lo = 0;
        int hi = tok.length();
        if (Chars.isQuoted(tok)) {
            lo = 1;
            hi--;
        }
        if (!isPublicKeyword(tok, lo, hi)) {
            return tok;
        }

        CharSequence savedTok = GenericLexer.immutableOf(tok);
        tok = optTok(lexer);
        if (tok == null) {
            return savedTok;
        }
        if (!Chars.equals(tok, '.')) {
            lexer.unparseLast();
            return savedTok;
        }

        tok = tok(lexer, "table name");
        return tok;
    }

    private CharSequence setModelAliasAndGetOptTok(GenericLexer lexer, IQueryModel joinModel) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null && tableAliasStop.excludes(tok)) {
            if (isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
            }
            if (tok.isEmpty() || isEmptyAlias(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Empty table alias");
            }
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            joinModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        }
        return tok;
    }

    private CharSequence setModelAliasAndTimestamp(GenericLexer lexer, IQueryModel model) throws SqlException {
        CharSequence tok;
        tok = setModelAliasAndGetOptTok(lexer, model);

        // expect [timestamp(column)]
        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            model.setTimestamp(timestamp);
            model.setExplicitTimestamp(true);
            tok = optTok(lexer);
        }
        return tok;
    }

    private int toColumnType(GenericLexer lexer, @NotNull CharSequence tok) throws SqlException {
        int typePosition = lexer.lastTokenPosition();
        if (Chars.equalsNc(tok, '[')) {
            // '[' is a wierd type name, it could be that someone is either:
            // 1. array dereferencing [x]
            // 2. inverting array definition, []type
            // 3. left out array definition (type), e.g. just []
            // 4. dangling [, e.g. there is no closing ]

            // we can be brave here, we will error out already, [ is not a type regardless of what we find
            tok = optTok(lexer);
            if (tok == null) {
                throw SqlException.position(typePosition).put("dangling '[' where column type is expected");
            }

            if (Chars.equals(tok, ']')) {
                // we have []
                // lets see if there is a type
                tok = optTok(lexer);
                if (tok == null) {
                    throw SqlException.position(typePosition).put("did you mean 'double[]'?");
                }
                if (!Chars.equals(tok, ')') && !Chars.equals(tok, ',') && !Chars.equals(tok, '(')) {
                    throw SqlException.position(typePosition).put("did you mean '").put(tok).put("[]'?");
                }
            }
            throw SqlException.position(typePosition).put("column type is expected here");
        }
        final int columnType = SqlUtil.toPersistedType(tok, typePosition);
        final int typeTagPosition = lexer.lastTokenPosition();

        // ignore precision keyword for DOUBLE column: 'double precision' is the same type as 'double'
        if (ColumnType.tagOf(columnType) == ColumnType.DOUBLE) {
            CharSequence next = optTok(lexer);
            if (next != null && !isPrecisionKeyword(next)) {
                lexer.unparseLast();
            }
        }

        int nDims = SqlUtil.parseArrayDimensionality(lexer, columnType, typeTagPosition);
        if (nDims > 0) {
            if (!ColumnType.isSupportedArrayElementType(columnType)) {
                throw SqlException.position(typePosition)
                        .put("unsupported array element type [type=")
                        .put(ColumnType.nameOf(columnType))
                        .put(']');
            }
            if (nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw SqlException.position(typePosition)
                        .put("too many array dimensions [nDims=").put(nDims)
                        .put(", maxNDims=").put(ColumnType.ARRAY_NDIMS_LIMIT)
                        .put(']');
            }
            return ColumnType.encodeArrayType(ColumnType.tagOf(columnType), nDims);
        }

        final short typeTag = ColumnType.tagOf(columnType);
        if (typeTag == ColumnType.GEOHASH) {
            expectTok(lexer, '(');
            final int bits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, expectLiteral(lexer).token);
            expectTok(lexer, ')');
            return ColumnType.getGeoHashTypeWithBits(bits);
        } else if (typeTag == ColumnType.DECIMAL) {
            return parseDecimalColumnType(lexer);
        }
        return columnType;
    }

    private int parseShowCreateDatabaseInclude(GenericLexer lexer) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
        }
        final boolean exclude;
        if (isIncludeKeyword(tok)) {
            exclude = false;
        } else if (isExcludeKeyword(tok)) {
            exclude = true;
        } else {
            // no INCLUDE/EXCLUDE clause; leave the token for the trailing-token check
            lexer.unparseLast();
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
        }
        tok = tok(lexer, "'ALL' or '('");
        if (isAllKeyword(tok)) {
            return exclude ? 0 : ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
        }
        if (!Chars.equals(tok, '(')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'ALL' or '(' expected");
        }
        int mask = 0;
        do {
            tok = tok(lexer, "category");
            mask |= showCreateDatabaseCategory(lexer, tok);
            tok = tok(lexer, "',' or ')'");
        } while (Chars.equals(tok, ','));
        if (!Chars.equals(tok, ')')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("',' or ')' expected");
        }
        return exclude ? (ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL & ~mask) : mask;
    }

    private int showCreateDatabaseCategory(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.equalsIgnoreCase(tok, "tables")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES;
        }
        if (Chars.equalsIgnoreCase(tok, "views")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_VIEWS;
        }
        if (Chars.equalsIgnoreCase(tok, "materialized_views")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_MATERIALIZED_VIEWS;
        }
        if (Chars.equalsIgnoreCase(tok, "live_views")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_LIVE_VIEWS;
        }
        if (Chars.equalsIgnoreCase(tok, "users")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_USERS;
        }
        if (Chars.equalsIgnoreCase(tok, "groups")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_GROUPS;
        }
        if (Chars.equalsIgnoreCase(tok, "service_accounts")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_SERVICE_ACCOUNTS;
        }
        if (Chars.equalsIgnoreCase(tok, "permissions")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_PERMISSIONS;
        }
        if (Chars.equalsIgnoreCase(tok, "schema")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_SCHEMA;
        }
        if (Chars.equalsIgnoreCase(tok, "acl")) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_ACL;
        }
        if (isAllKeyword(tok)) {
            return ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
        }
        throw SqlException.position(lexer.lastTokenPosition()).put("unexpected category [category=").put(tok)
                .put("], expected one of TABLES, VIEWS, MATERIALIZED_VIEWS, LIVE_VIEWS, USERS, GROUPS, SERVICE_ACCOUNTS, PERMISSIONS, SCHEMA, ACL, ALL");
    }

    private @NotNull CharSequence tok(GenericLexer lexer, String expectedList) throws SqlException {
        final int pos = lexer.getPosition();
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(pos).put(expectedList).put(" expected");
        }
        return tok;
    }

    private @NotNull CharSequence tokIncludingLocalBrace(GenericLexer lexer, String expectedList) throws SqlException {
        final int pos = lexer.getPosition();
        final CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(pos).put(expectedList).put(" expected");
        }
        return tok;
    }

    private void validateMatViewQuery(IQueryModel model, String baseTableName) throws SqlException {
        for (IQueryModel m = model; m != null; m = m.getNestedModel()) {
            tableNames.clear();
            tableNamePositions.clear();
            SqlUtil.collectAllTableNames(m, tableNames, null);
            final boolean baseTableQueried = tableNames.contains(baseTableName);
            final int queriedTableCount = tableNames.size();
            if (baseTableQueried) {
                if (m.getSampleBy() != null && m.getSampleByOffset() == null) {
                    throw SqlException.position(m.getSampleBy().position + m.getSampleBy().token.length() + 1)
                            .put("ALIGN TO FIRST OBSERVATION on base table is not supported for materialized views: ").put(baseTableName);
                }

                if ((m.getSampleByFrom() != null || m.getSampleByTo() != null)) {
                    final int position = m.getSampleByFrom() != null ? m.getSampleByFrom().position : m.getSampleByTo().position;
                    throw SqlException.position(position)
                            .put("FROM-TO on base table is not supported for materialized views: ").put(baseTableName);
                }

                final ObjList<ExpressionNode> sampleByFill = m.getSampleByFill();
                if (sampleByFill != null && sampleByFill.size() > 0) {
                    throw SqlException.position(sampleByFill.get(0).position)
                            .put("FILL on base table is not supported for materialized views: ").put(baseTableName);
                }

                // LIMIT is a global, order-dependent row cap. Incremental refresh re-evaluates the
                // defining query per changed timestamp slice, so the cap would apply to each slice
                // and the view contents would diverge from re-running the defining query.
                final ExpressionNode limitLo = m.getLimitLo();
                final ExpressionNode limitHi = m.getLimitHi();
                if (limitLo != null || limitHi != null) {
                    // The LIMIT keyword position is not propagated when a limit is hoisted between
                    // models (see parseSelectClause), so anchor the error at the limit value node.
                    throw SqlException.position(limitLo != null ? limitLo.position : limitHi.position)
                            .put("LIMIT on base table is not supported for materialized views: ").put(baseTableName);
                }

                ObjList<QueryColumn> columns = m.getColumns();
                int windowFuncPosition = -1;
                for (int i = 0, n = columns.size(); i < n; i++) {
                    QueryColumn column = columns.getQuick(i);
                    // A window function can hide anywhere in the column's expression tree, e.g.
                    // row_number() OVER (...) + 1 or (row_number() OVER (...))::long, where the
                    // top-level QueryColumn is a plain column rather than a WindowExpression. Walk
                    // the whole tree so nested windows are caught too.
                    if (windowFuncPosition < 0) {
                        windowFuncPosition = windowFunctionPosition(column.getAst());
                    }

                    if (!Chars.equals(column.getName(), '*') && !TableUtils.isValidColumnName(column.getName(), configuration.getMaxFileNameLength())) {
                        if (column.getAliasPosition() == QueryColumn.SYNTHESIZED_ALIAS_POSITION) {
                            throw SqlException
                                    .position(column.getAst().position)
                                    .put("column '").put(column.getName()).put("' requires an explicit alias. Use: ")
                                    .put(column.getName()).put(" AS your_column_name");
                        } else {
                            throw SqlException
                                    .position(column.getAliasPosition())
                                    .put("column alias '").put(column.getName()).put("' contains unsupported characters");
                        }
                    }
                }

                if (windowFuncPosition > -1) {
                    throw SqlException.position(windowFuncPosition)
                            .put("window function on base table is not supported for materialized views: ").put(baseTableName);
                }
            }

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                validateMatViewQuery(joinModel, baseTableName);
            }

            final IQueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                // allow self-UNION on base table, but disallow UNION on base table with any other tables
                if (baseTableQueried && queriedTableCount > 1) {
                    throw SqlException.position(m.getUnionModel().getModelPosition())
                            .put("union on base table is not supported for materialized views: ").put(baseTableName);
                }
                validateMatViewQuery(unionModel, baseTableName);
            }
        }
    }

    private void validateNamedWindowReferences(IQueryModel model) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = model.getNamedWindows();
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                WindowExpression wc = (WindowExpression) qc;
                if (wc.isNamedWindowReference() && namedWindows.keyIndex(wc.getWindowName()) > -1) {
                    throw SqlException.$(wc.getWindowNamePosition(), "window '").put(wc.getWindowName()).put("' is not defined");
                }
            }
            // Check nested expression trees for all columns, not just window expressions,
            // to catch cases like: row_number() OVER w + 1 (where top-level column is +)
            validateNamedWindowReferencesInExpr(qc.getAst(), namedWindows);
        }
    }

    private void validateNamedWindowReferencesInExpr(ExpressionNode node, LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null && node.windowExpression.isNamedWindowReference()) {
            CharSequence name = node.windowExpression.getWindowName();
            if (namedWindows.keyIndex(name) > -1) {
                throw SqlException.$(node.windowExpression.getWindowNamePosition(), "window '").put(name).put("' is not defined");
            }
        }
        if (node.paramCount < 3) {
            validateNamedWindowReferencesInExpr(node.lhs, namedWindows);
            validateNamedWindowReferencesInExpr(node.rhs, namedWindows);
        } else {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                validateNamedWindowReferencesInExpr(node.args.getQuick(i), namedWindows);
            }
        }
    }

    // Returns the source position of the first window function found anywhere in the expression
    // tree, or -1 if there is none. A window function can sit below an operator or cast (e.g.
    // row_number() OVER (...) + 1), so the whole tree is walked, not just the root.
    private int windowFunctionPosition(ExpressionNode node) {
        if (node == null) {
            return -1;
        }
        if (node.windowExpression != null) {
            return node.position;
        }
        if (node.paramCount < 3) {
            final int lhsPosition = windowFunctionPosition(node.lhs);
            if (lhsPosition > -1) {
                return lhsPosition;
            }
            return windowFunctionPosition(node.rhs);
        }
        for (int i = 0, n = node.paramCount; i < n; i++) {
            final int position = windowFunctionPosition(node.args.getQuick(i));
            if (position > -1) {
                return position;
            }
        }
        return -1;
    }

    static void validateIdentifier(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok == null || tok.isEmpty()) {
            throw SqlException.position(lexer.lastTokenPosition()).put("non-empty identifier expected");
        }

        if (Chars.isQuoted(tok)) {
            if (tok.length() == 2) {
                throw SqlException.position(lexer.lastTokenPosition()).put("non-empty identifier expected");
            }
            return;
        }

        char c = tok.charAt(0);

        if (!(Character.isLetter(c) || c == '_')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("identifier should start with a letter or '_'");
        }

        for (int i = 1, n = tok.length(); i < n; i++) {
            c = tok.charAt(i);
            if (!(Character.isLetter(c) ||
                    Character.isDigit(c) ||
                    c == '_' ||
                    c == '$')) {
                throw SqlException.position(lexer.lastTokenPosition()).put("identifier can contain letters, digits, '_' or '$'");
            }
        }
    }

    void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        expressionNodePool.clear();
        windowExpressionPool.clear();
        createViewOperationBuilder.clear();
        createMatViewOperationBuilder.clear();
        createTableOperationBuilder.clear();
        createTableColumnModelPool.clear();
        renameTableModelPool.clear();
        withClauseModelPool.clear();
        compileViewModelPool.clear();
        subQueryMode = false;
        createTableMode = false;
        copyMode = false;
        createViewMode = false;
        characterStore.clear();
        insertModelPool.clear();
        pivotQueryColumnPool.clear();
        expressionTreeBuilder.reset();
        copyModelPool.clear();
        topLevelWithModel.clear();
        explainModelPool.clear();
        viewLexers.clear();
        digit = 1;
        traversalAlgo.clear();
        tempCharSequenceSet.clear();
        aliasMap.clear();
        aliasSequenceMap.clear();
        pivotAliasMap.clear();
        clearRecordedViews();
        // Hygiene: parse() always re-derives these from the execution context, but reset them here too so a
        // reused parser never carries a stale row-expiry gate/timestamp between compilations.
        rowExpiryReadFilterEnabled = true;
        expiryFilterExecutionContext = null;
        expiryExpandedTables.clear();
        expiryPolicyTable = null;
        expiryTimestampColumnName = null;
        pendingExpiryReadVersions.clear();
    }

    ExpressionNode expr(
            GenericLexer lexer,
            IQueryModel model,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        try {
            expressionTreeBuilder.pushModel(model);
            expressionParser.parseExpr(lexer, expressionTreeBuilder, sqlParserCallback, decls);
            return rewriteKnownStatements(expressionTreeBuilder.poll(), decls, exprTargetVariableName);
        } catch (SqlException e) {
            expressionTreeBuilder.reset();
            throw e;
        } finally {
            expressionTreeBuilder.popModel();
        }
    }

    ExpressionNode expr(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback, @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        return expr(lexer, model, sqlParserCallback, decls, null);
    }

    ExpressionNode expr(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        return expr(lexer, model, sqlParserCallback, null, null);
    }

    // test only
    @TestOnly
    void expr(GenericLexer lexer, ExpressionParserListener listener, SqlParserCallback sqlParserCallback) throws SqlException {
        expressionParser.parseExpr(lexer, listener, sqlParserCallback, null);
    }

    IntLongHashMap getPendingExpiryReadVersions() {
        return pendingExpiryReadVersions;
    }

    ExecutionModel parse(GenericLexer lexer, SqlExecutionContext executionContext, SqlParserCallback sqlParserCallback) throws SqlException {
        // Capture the read-filter toggle for this whole parse (the row-expiry cleanup job disables it).
        // The context is also kept for the per-table refinement: the mat-view refresh context keeps the
        // filter on every table except the base.
        rowExpiryReadFilterEnabled = executionContext.isExpiryReadFilterEnabled();
        expiryFilterExecutionContext = executionContext;
        // ANCHOR is a live-view-only clause. A live-view re-compile (the refresh
        // worker, the startup graph build, CREATE's own validating compile of the
        // stored SELECT) parses the view's SELECT as a plain query with this flag
        // set; parseCreateLiveView turns it on for the CREATE body itself, where
        // the flag is still false. Every other statement rejects the clause.
        expressionParser.setAnchorAllowed(executionContext.isLiveViewCompile());
        final CharSequence tok = tok(lexer, "'create', 'rename' or 'select'");

        if (isExplainKeyword(tok)) {
            int format = parseExplainOptions(lexer, tok);
            ExecutionModel model = parseExplain(lexer, executionContext, sqlParserCallback);
            ExplainModel explainModel = explainModelPool.next();
            explainModel.setFormat(format);
            explainModel.setModel(model);
            return explainModel;
        }

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, null);
        }

        if (isCreateKeyword(tok)) {
            return parseCreate(lexer, executionContext, sqlParserCallback);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, null);
        }

        if (isRenameKeyword(tok)) {
            return parseRenameStatement(lexer);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, null);
        }

        if (isCopyKeyword(tok)) {
            return parseCopy(lexer, sqlParserCallback);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer, sqlParserCallback, null);
        }

        if (isCompileKeyword(tok)) {
            return parseCompileView(lexer);
        }

        if (isFromKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Did you mean 'select * from'?");
        }

        return parseSelect(lexer, sqlParserCallback, null);
    }

    IQueryModel parseAsSubQuery(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        IQueryModel model;
        this.subQueryMode = true;
        try {
            model = parseDml(lexer, withClauses, lexer.getPosition(), useTopLevelWithClauses, sqlParserCallback, decls, overrideDeclare);
        } finally {
            this.subQueryMode = false;
        }
        return model;
    }

    String parseViewSql(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        int startOfQuery = lexer.getPosition();
        CharSequence tok = tok(lexer, "'(' or 'with' or 'select'");
        boolean enclosedInParentheses = Chars.equals(tok, '(');
        if (enclosedInParentheses) {
            startOfQuery = lexer.getPosition();
            tok = tok(lexer, "'with' or 'select'");
        }

        // Parse SELECT for the sake of basic SQL validation.
        // It'll be compiled and optimized later, at the execution phase.
        if (isWithKeyword(tok)) {
            parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, null);
            // CTEs require SELECT to be specified
            expectTok(lexer, "select");
        }
        lexer.unparseLast();
        parseAsSubQuery(lexer, null, true, sqlParserCallback, null, false);
        final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

        final String viewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);

        if (enclosedInParentheses) {
            expectTok(lexer, ')');
        }
        tok = optTok(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return viewSql;
    }

    /**
     * Arms or disarms the live-view-only ANCHOR clause for the next expression parse.
     * {@link #parse} stamps it per statement; a caller that parses a bare expression
     * without going through {@code parse} stamps it here rather than inheriting the
     * previous statement's value.
     */
    void setAnchorAllowed(boolean anchorAllowed) {
        expressionParser.setAnchorAllowed(anchorAllowed);
    }

    /**
     * Result of {@link #captureKeepColumnList}: the raw PARTITION BY column list and the trailing boundary.
     */
    private static final class ColumnListCapture {
        String csv;
        boolean foundCleanup;
        CharSequence nextTok;
        int startPos;
    }

    /**
     * Result of {@link #parseExpireRowsClause(GenericLexer, boolean)}: the captured raw predicate
     * text, the cleanup interval in microseconds, and the next unconsumed token (the boundary
     * keyword for the CREATE TABLE caller; null/';' for the ALTER caller).
     */
    public static final class ExpireRowsClause {
        public final long cleanupIntervalMicros;
        public final CharSequence nextTok;
        public final String predicate;
        public final int predicatePos;

        public ExpireRowsClause(String predicate, int predicatePos, long cleanupIntervalMicros, CharSequence nextTok) {
            this.predicate = predicate;
            this.predicatePos = predicatePos;
            this.cleanupIntervalMicros = cleanupIntervalMicros;
            this.nextTok = nextTok;
        }
    }

    public interface ReplacingVisitor {
        ExpressionNode visit(ExpressionNode node) throws SqlException;
    }

    private static class RewriteDeclaredVariablesInExpressionVisitor implements ReplacingVisitor {
        public LowerCaseCharSequenceObjHashMap<ExpressionNode> decls;
        public CharSequence exprTargetVariableName;
        public boolean hasAtChar;

        @Override
        public ExpressionNode visit(ExpressionNode node) throws SqlException {
            if (node.token == null) {
                return node;
            }

            if ((hasAtChar = node.token.charAt(0) == '@') && exprTargetVariableName != null && (Chars.equalsIgnoreCase(node.token, exprTargetVariableName))) {
                return node;
            }

            if (node.token != null && node.type == ExpressionNode.LITERAL && decls.contains(node.token)) {
                return decls.get(node.token).rhs;
            } else if (hasAtChar) {
                throw SqlException.$(node.position, "tried to use undeclared variable `" + node.token + '`');
            }

            return node;
        }

        ReplacingVisitor of(
                @NotNull LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
                @Nullable CharSequence exprTargetVariableName
        ) {
            this.decls = decls;
            this.exprTargetVariableName = exprTargetVariableName;
            return this;
        }
    }

    static {
        tableAliasStop.add("where");
        tableAliasStop.add("latest");
        tableAliasStop.add("join");
        tableAliasStop.add("inner");
        tableAliasStop.add("left");
        tableAliasStop.add("outer");
        tableAliasStop.add("asof");
        tableAliasStop.add("splice");
        tableAliasStop.add("lt");
        tableAliasStop.add("cross");
        tableAliasStop.add("sample");
        tableAliasStop.add("order");
        tableAliasStop.add("on");
        tableAliasStop.add("timestamp");
        tableAliasStop.add("limit");
        tableAliasStop.add(",");
        tableAliasStop.add(")");
        tableAliasStop.add(";");
        tableAliasStop.add("union");
        tableAliasStop.add("group");
        tableAliasStop.add("except");
        tableAliasStop.add("intersect");
        tableAliasStop.add("from");
        tableAliasStop.add("pivot");
        tableAliasStop.add("tolerance");
        tableAliasStop.add("right");
        tableAliasStop.add("full");
        tableAliasStop.add("range");
        tableAliasStop.add("window");
        tableAliasStop.add("horizon");
        tableAliasStop.add("unnest");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        columnAliasStop.add("union");
        columnAliasStop.add("except");
        columnAliasStop.add("intersect");
        columnAliasStop.add(")");
        columnAliasStop.add(";");
        columnAliasStop.add("FOR");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("left", IQueryModel.JOIN_INNER);
        joinStartSet.put("right", IQueryModel.JOIN_INNER);
        joinStartSet.put("full", IQueryModel.JOIN_INNER);
        joinStartSet.put("join", IQueryModel.JOIN_INNER);
        joinStartSet.put("inner", IQueryModel.JOIN_INNER);
        joinStartSet.put("left", IQueryModel.JOIN_LEFT_OUTER);
        joinStartSet.put("window", IQueryModel.JOIN_WINDOW);
        joinStartSet.put("right", IQueryModel.JOIN_RIGHT_OUTER);
        joinStartSet.put("full", IQueryModel.JOIN_FULL_OUTER);
        joinStartSet.put("cross", IQueryModel.JOIN_CROSS);
        joinStartSet.put("asof", IQueryModel.JOIN_ASOF);
        joinStartSet.put("splice", IQueryModel.JOIN_SPLICE);
        joinStartSet.put("lt", IQueryModel.JOIN_LT);
        joinStartSet.put("horizon", IQueryModel.JOIN_HORIZON);
        joinStartSet.put("lateral", IQueryModel.JOIN_LATERAL_CROSS);
        joinStartSet.put(",", IQueryModel.JOIN_CROSS);
        //
        setOperations.add("union");
        setOperations.add("except");
        setOperations.add("intersect");
        //
        pivotForStop.add("group");
        pivotForStop.add(";");
        pivotForStop.add(")");
        pivotForStop.add("order");
        pivotForStop.add("limit");
    }
}
