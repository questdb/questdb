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

package io.questdb.griffin.bind;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.ExpiryPolicyParser;
import io.questdb.griffin.ExpiryValidationResult;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlParserCallback;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.DateExpressionEvaluator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.GenericLexer.unquote;

/**
 * Parses, binds and classifies the scalar EXPIRE ROWS policy of a materialized view. DDL validates a policy with it
 * before storing it, and the cleanup job and {@code materialized_views()} classify a stored one. A validation parses
 * the policy text once and binds that parse in each shape the policy takes:
 * <ul>
 *     <li>the bare predicate, whose root binds as the overload it selects, which gives the type and rejects an
 *     aggregate root, see {@link SqlCompilerImpl#bindStatementExpressionType};</li>
 *     <li>the operand of a bare {@code <column> = <constant>} and the threshold of an ordering comparison of the
 *     designated timestamp, each on its own, when the predicate has that shape;</li>
 *     <li>at DDL only, the keep-filter every read of the view binds, through the read path itself, see
 *     {@link #bindKeepFilter}.</li>
 * </ul>
 * One shape parses text of its own: the cleanup sweep's keep-filter in its strict form, see {@link #bindSweepShape}.
 * The sweep and every read embed the stored text in SQL of their own, and only a parse of such text refuses a text
 * they cannot embed.
 * <p>
 * The classification ({@link ExpiryValidationResult}) comes from the parsed predicate rather than from a bound tree,
 * because binding folds constant subtrees away with the evidence they hold: {@code false AND ts > now()} binds to the
 * constant {@code false}, which reads no clock, no column and no session value, while the classification has to see
 * every clock read, date variable, bind variable, sub-query, column and function the text names. Each rule below says
 * what else a bound tree would lose.
 * <p>
 * The binder is created once with the compiler's binder and holds only references and temporaries. The parsed
 * predicate lives in the compiler's node pool until the compiler clears, so {@link #bindKeepFilter} binds the parse
 * of the {@link #validate} before it.
 */
public final class ExpiryPolicyBinder implements Mutable {
    // Operators and functions whose result depends only on their arguments. A row-expiry predicate built
    // only from these, the table's columns and constants gives each row the same result in every
    // execution and every session, so the cleanup job may reclaim under it. Any other operator or
    // function leaves the policy FILTER_ONLY: that costs disk, while the read filter keeps results correct.
    private static final LowerCaseCharSequenceHashSet ROW_ONLY_FUNCTIONS = new LowerCaseCharSequenceHashSet();
    private final SqlBinder binder;
    private final SqlCompilerImpl compiler;
    private final BindContext ctx;
    // The message of a failure that an invalid-policy error carries: SqlException reuses a thread-local instance, so
    // the message is copied out before the error that wraps it is created.
    private final StringSink failure = new StringSink();
    // The columns of the view, as a read's scan lists them, which the keep-filter DDL binds reads.
    private final OutputSchema keepFilterInput = new OutputSchema();
    private final ExpiryPolicyParser policyParser;
    private final ExpiryReadBinder readBinder;
    // The predicate the last validation parsed, which the keep-filter DDL binds next negates.
    private ExpressionNode predicate;

    ExpiryPolicyBinder(BindContext ctx, SqlBinder binder, ExpiryReadBinder readBinder, ExpiryPolicyParser policyParser, SqlCompilerImpl compiler) {
        this.ctx = ctx;
        this.binder = binder;
        this.readBinder = readBinder;
        this.policyParser = policyParser;
        this.compiler = compiler;
    }

    // Lets a test put a hook function of its own into a predicate that has to reclaim.
    @TestOnly
    public static void addRowOnlyFunctionForTesting(CharSequence name) {
        ROW_ONLY_FUNCTIONS.add(name);
    }

    @TestOnly
    public static void removeRowOnlyFunctionForTesting(CharSequence name) {
        ROW_ONLY_FUNCTIONS.remove(name);
    }

    /**
     * Binds the keep-filter of the policy {@link #validate} parsed last through the read path itself,
     * {@link ExpiryReadBinder#bindKeepFilter(ExpressionNode, OutputSchema, CharSequence, SqlExecutionContext)}, the
     * call every read of the view binds the policy with, over the columns of {@code viewMetadata} as a read's scan
     * lists them, with qualified names resolved against {@code viewName}, and refuses the policy when that fails, with
     * the read's error. No text spells the read's shape here: the read parses the stored predicate into
     * {@code NOT (<predicate>)}, pushes the negation down and binds the result as a filter of the scan, whose rules a
     * bind of the bare predicate does not apply. A negation that cancels another leaves its operand at the root, where
     * a NULL constant is no boolean expression, so {@code NOT NULL} binds on its own and fails every read.
     * <p>
     * At CREATE the view does not exist yet, so the filter reads the columns the view is created with, and no table
     * stands behind them. The caller closes what the bind prepared, since nothing generates the bound filter. It runs
     * last: pushing the negation down rewrites the parsed predicate, which nothing reads afterwards.
     */
    public void bindKeepFilter(
            RecordMetadata viewMetadata,
            CharSequence viewName,
            int position,
            SqlParserCallback parserCallback,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert predicate != null;
        try {
            binder.setParserCallback(parserCallback);
            final int timestampIndex = viewMetadata.getTimestampIndex();
            for (int i = 0, n = viewMetadata.getColumnCount(); i < n; i++) {
                binder.bindTableColumn(keepFilterInput, viewMetadata.getColumnName(i), viewMetadata.getColumnType(i),
                        viewMetadata.isSymbolTableStatic(i), i == timestampIndex);
            }
            readBinder.bindKeepFilter(predicate, keepFilterInput, viewName, executionContext);
        } catch (SqlException | CairoException | ImplicitCastException e) {
            throw invalidPredicate(position, e);
        } finally {
            predicate = null;
            keepFilterInput.clear();
        }
    }

    @Override
    public void clear() {
        predicate = null;
        keepFilterInput.clear();
    }

    /**
     * The keep-filter DDL binds for {@code predicate}, before the read path pushes its negation down, see
     * {@link #bindKeepFilter}. It lives until the compiler clears.
     */
    @TestOnly
    public ExpressionNode parseKeepFilterForTesting(CharSequence predicate, SqlParserCallback parserCallback) throws SqlException {
        return SqlUtil.negate(policyParser.parsePredicate(predicate, parserCallback), ctx.bindingExpressions);
    }

    /**
     * Fast-path support for the row-expiry cleanup job, see {@link io.questdb.griffin.SqlCompiler#expiryTimestampThreshold}:
     * the threshold {@code T} of {@code <ts> < T}, {@code <ts> <= T}, {@code T > <ts>} or {@code T >= <ts>} on the
     * designated timestamp, in the unit of that column, when {@code T} references no column, or
     * {@link Numbers#LONG_NULL} for any other shape. A failure to parse, bind or evaluate the threshold throws, and
     * the caller reads it as no threshold.
     * <p>
     * A partition floor uses the unit of the column, but a {@code now()}-based T is in micros, so
     * {@link #toTimestampUnit} converts T into the unit of the column. That conversion rounds T DOWN. A smaller T can
     * only make the set of fully expired partitions smaller, so the job never drops a partition that holds a live row.
     * A conversion that overflows throws.
     */
    public long timestampThreshold(
            RecordMetadata metadata,
            CharSequence predicate,
            CharSequence timestampColumn,
            SqlParserCallback parserCallback,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (timestampColumn == null) {
            return Numbers.LONG_NULL;
        }
        final int tsIndex = metadata.getColumnIndexQuiet(timestampColumn);
        if (tsIndex < 0) {
            return Numbers.LONG_NULL;
        }
        final int tsType = metadata.getColumnType(tsIndex);
        if (!ColumnType.isTimestamp(tsType)) {
            return Numbers.LONG_NULL;
        }
        final ExpressionNode node = policyParser.parseExpression(predicate, parserCallback);
        if (node == null || node.type != ExpressionNode.OPERATION || node.paramCount != 2
                || node.lhs == null || node.rhs == null) {
            return Numbers.LONG_NULL;
        }
        // Accept both the canonical "<ts> < T | <ts> <= T" (timestamp on the left) and the equivalent
        // symmetric "T > <ts> | T >= <ts>" (timestamp on the right). Both mean "expire everything below
        // T"; the threshold node is the side that is NOT the timestamp column. A "T < <ts>" / "<ts> > T"
        // ("expire recent, keep old") shape is deliberately NOT accepted here: its DROP direction is the
        // opposite of the partition-bounds fast path.
        // Resolve the operand by column index (unquote, strip qualifier, case-insensitive), the same way
        // the monotonicity classifier does in timestampThresholdNode. A case- or quote-mismatched
        // spelling of the timestamp column must take the same bounds fast path; otherwise the classifier
        // proves the policy monotonic while this method returns LONG_NULL, and the cleanup job falls to
        // the survivor scan with the SKIP generation cache on.
        final boolean tsOnLeft = node.lhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.lhs.token) == tsIndex;
        final boolean tsOnRight = node.rhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.rhs.token) == tsIndex;
        final ExpressionNode thresholdNode;
        if (tsOnLeft && (Chars.equals(node.token, "<") || Chars.equals(node.token, "<="))) {
            thresholdNode = node.rhs;
        } else if (tsOnRight && (Chars.equals(node.token, ">") || Chars.equals(node.token, ">="))) {
            thresholdNode = node.lhs;
        } else {
            return Numbers.LONG_NULL;
        }
        if (referencesColumn(thresholdNode)) {
            return Numbers.LONG_NULL;
        }
        Function f = null;
        try {
            f = compiler.bindStatementExpression(thresholdNode, metadata, ColumnType.UNDEFINED, executionContext);
            if (f == null || !ColumnType.isTimestamp(f.getType()) || !(f.isConstant() || f.isRuntimeConstant())) {
                return Numbers.LONG_NULL;
            }
            f.init(null, executionContext);
            final long threshold = f.getTimestamp(null);
            if (threshold == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL; // a NULL T expires no row, and the survivor scan gives the same result
            }
            // Math.multiplyExact throws on overflow, and the caller turns that into "no fast path".
            return toTimestampUnit(threshold, f.getType(), tsType);
        } finally {
            Misc.free(f);
        }
    }

    /**
     * Parses {@code predicate} once and binds it over {@code metadata}, the columns of the view, and returns its
     * classification. The predicate has to be a boolean expression consuming the ENTIRE text, with no root-level
     * aggregate and no bind variables: the stored text is embedded verbatim into every read's generated SQL, so
     * anything that read-path parse would choke on is rejected here. Any parse or bind error is rewritten as a clear
     * "invalid EXPIRE ROWS predicate" positioned at {@code position}. It binds with the planner's binder, as reads and
     * the cleanup sweep do.
     * <p>
     * The bare predicate is not what a read binds: a read binds its keep-filter, {@code NOT (<predicate>)} with the
     * negation pushed down, which DDL binds next through the read path itself, see {@link #bindKeepFilter}. This binds
     * the shape the cleanup sweep compiles, see {@link #bindSweepShape}, and refuses a constant NULL threshold, see
     * {@link #rejectNullThreshold}. The caller clears the compiler before, and must not clear it before
     * {@link #bindKeepFilter} reads the parse.
     */
    public ExpiryValidationResult validate(
            RecordMetadata metadata,
            CharSequence predicate,
            int position,
            SqlParserCallback parserCallback,
            SqlExecutionContext executionContext
    ) throws SqlException {
        this.predicate = null;
        final ExpressionNode node;
        final int predicateType;
        try {
            node = policyParser.parsePredicate(predicate, parserCallback);
            predicateType = compiler.bindStatementExpressionType(node, metadata, executionContext);
        } catch (SqlException | CairoException | ImplicitCastException e) {
            // ImplicitCastException extends RuntimeException, not CairoException. A constant the bind
            // folds - `i = 'abc'` against an INT column - raises one here, and it has to read as an
            // invalid policy rather than escaping the DDL as a bare cast error.
            throw invalidPredicate(position, e);
        }
        if (!ColumnType.isBoolean(predicateType)) {
            throw SqlException.$(position, "invalid EXPIRE ROWS predicate: expected a boolean expression");
        }
        // A stored predicate has no statement to supply bind values, so `v > $1` would fail on
        // every read with "undefined bind variable"; reject it at definition time instead. A bound tree cannot
        // answer this: `false AND v = $1` folds to the constant false and drops the bind variable.
        if (hasBindVariable(node)) {
            throw SqlException.$(position, "invalid EXPIRE ROWS predicate: bind variables are not supported");
        }
        // The columns come from the text, in the order its nodes list them, and not from the bound tree: a fold drops
        // the columns of a constant subtree, and the security context names the first column a caller may not read,
        // so the order of the list is part of the error ALTER reports.
        final IntList referencedColumnIndexes = new IntList();
        collectReferencedColumns(node, metadata, referencedColumnIndexes);
        // A date variable in a string constant (`ts IN '$now - 1h..$now'`) reads the clock too. The
        // function that evaluates such a string reads it in init() but reports neither
        // isNonDeterministic() nor isRuntimeConstant(), so only the AST shows the clock read.
        final boolean hasClock = hasClock(node) || hasDateVariable(node);
        // The predicate counts as depending only on the row (isDeterministic) only when its AST proves
        // that every node gives each row the same value in every execution and session. The cleanup job
        // evaluates the predicate once per sweep under the root context, so a clock read, a session
        // value such as current_user(), or any other function whose value can change could make it
        // delete a row that a later read, or a reader in another session, keeps. The proof works on the
        // AST rather than on the bound expressions: a call reports NON_DETERMINISTIC, RUNTIME_CONSTANT or
        // RANDOM reliably only when the answer is true, both on the function and on its bound description,
        // which copies the function's flags, and a fold drops a session value under a constant operand
        // (`false AND owner = current_user()`) that the read still evaluates in an OR.
        // A subquery (e.g. `sym IN (SELECT s FROM blacklist)`) reads other tables whose contents can
        // change between evaluations, so a row expired now can un-expire later - physical cleanup
        // under such a predicate could delete rows the read filter must show again. The expression
        // parse above already rejects subqueries ("query is not allowed here": the policy parses with
        // no query model), so none can be stored via DDL. The QUERY-node check is a second layer of
        // protection: if a subquery ever does reach classification, the predicate is classified
        // non-monotonic and the cleanup job skips physical deletion for it.
        final boolean isDeterministic = !hasClock
                && !hasQuery(node)
                && isRowOnlyExpression(node, metadata);
        final int timestampIndex = metadata.getTimestampIndex();
        final CharSequence timestampColumn = timestampIndex >= 0 ? metadata.getColumnName(timestampIndex) : null;
        // The proof reads the threshold's literals from the text: binding folds `now() - 1_000_000 * 2` into
        // `now() - 2000000` and canonicalizes comparisons, so a proof over the bound tree would accept shapes this one
        // leaves unproven.
        final ExpressionNode thresholdNode = timestampThresholdNode(node, metadata, timestampColumn);
        final boolean isMonotonic = isDeterministic
                || isProvenAdvancingClockExpression(thresholdNode);
        final ExpiryValidationResult result =
                new ExpiryValidationResult(hasClock, isDeterministic, isMonotonic, referencedColumnIndexes);
        bindSweepShape(node, predicate, metadata, position, parserCallback, executionContext);
        rejectNullThreshold(node, metadata, timestampColumn, position, executionContext);
        this.predicate = node;
        return result;
    }

    private static void addRowOnlyFunctions(String... names) {
        for (String name : names) {
            ROW_ONLY_FUNCTIONS.add(name);
        }
    }

    private static void checkTextConvertsToColumnType(CharSequence value, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE -> SqlUtil.implicitCastStrAsByte(value);
            case ColumnType.SHORT -> SqlUtil.implicitCastStrAsShort(value);
            case ColumnType.INT -> SqlUtil.implicitCastStrAsInt(value);
            case ColumnType.LONG -> SqlUtil.implicitCastStrAsLong(value);
            case ColumnType.FLOAT -> SqlUtil.implicitCastStrAsFloat(value);
            case ColumnType.DOUBLE -> SqlUtil.implicitCastStrAsDouble(value);
            case ColumnType.CHAR -> SqlUtil.implicitCastStrAsChar(value);
            case ColumnType.UUID -> SqlUtil.implicitCastStrAsUuid(value, new Uuid());
            default -> {
            }
        }
    }

    private static void collectReferencedColumns(
            ExpressionNode node,
            RecordMetadata metadata,
            IntList referencedColumnIndexes
    ) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int columnIndex = resolveColumnIndex(metadata, node.token);
            if (columnIndex >= 0 && referencedColumnIndexes.indexOf(columnIndex, 0, referencedColumnIndexes.size()) < 0) {
                referencedColumnIndexes.add(columnIndex);
            }
        }
        collectReferencedColumns(node.lhs, metadata, referencedColumnIndexes);
        collectReferencedColumns(node.rhs, metadata, referencedColumnIndexes);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectReferencedColumns(node.args.getQuick(i), metadata, referencedColumnIndexes);
        }
    }

    /**
     * The duration of a {@code dateadd} unit in nanoseconds (the finest timestamp resolution, so the
     * overflow bound derived from it holds for micro timelines too), or 0 for a calendar unit (M/y)
     * or an unrecognized token. Accepts the quoted ({@code 'd'}) and bare ({@code d}) forms.
     */
    private static long fixedDateaddUnitNanos(CharSequence unitToken) {
        if (unitToken == null) {
            return 0;
        }
        final int len = unitToken.length();
        final char unit;
        if (len == 3 && unitToken.charAt(0) == '\'' && unitToken.charAt(2) == '\'') {
            unit = unitToken.charAt(1);
        } else if (len == 1) {
            unit = unitToken.charAt(0);
        } else {
            return 0;
        }
        return switch (unit) {
            case 'n' -> 1L;
            case 'u' -> 1_000L;
            case 'T' -> 1_000_000L;
            case 's' -> 1_000_000_000L;
            case 'm' -> 60_000_000_000L;
            case 'h' -> 3_600_000_000_000L;
            case 'd' -> 86_400_000_000_000L;
            case 'w' -> 604_800_000_000_000L;
            default -> 0;
        };
    }

    // True if the sub-tree contains a bind variable ($1 / :name). A stored predicate has no statement
    // to supply bind values, so one would fail on every read.
    private static boolean hasBindVariable(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.BIND_VARIABLE) {
            return true;
        }
        if (hasBindVariable(node.lhs) || hasBindVariable(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasBindVariable(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // True if the sub-tree names a clock function. A bound tree drops one that a fold removes
    // (`false AND ts > now()`), and the classification reports every clock the text reads.
    private static boolean hasClock(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.FUNCTION && SqlKeywords.isClockFunctionKeyword(node.token)) {
            return true;
        }
        if (hasClock(node.lhs) || hasClock(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasClock(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // True if a string constant in the sub-tree contains a date variable. The function that
    // evaluates such a string reads the clock in init() but reports neither isNonDeterministic()
    // nor isRuntimeConstant(), so only the AST shows the clock read.
    private static boolean hasDateVariable(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.CONSTANT && DateExpressionEvaluator.hasDateVariable(node.token)) {
            return true;
        }
        if (hasDateVariable(node.lhs) || hasDateVariable(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasDateVariable(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // True if the sub-tree contains a subquery (QUERY node). A subquery's result depends on another
    // table's current contents, so a predicate containing one is not stable across evaluations.
    private static boolean hasQuery(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.QUERY) {
            return true;
        }
        if (hasQuery(node.lhs) || hasQuery(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasQuery(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // True for a bare zero-arg wall-time clock: now(), now_ns(), sysdate(), systimestamp(), systimestamp_ns().
    private static boolean isBareClockFunction(ExpressionNode node) {
        return node != null
                && node.type == ExpressionNode.FUNCTION
                && node.paramCount == 0
                && SqlKeywords.isClockFunctionKeyword(node.token);
    }

    /**
     * True when {@code node} is structurally proven to be a non-decreasing function of wall-clock
     * time, so a {@code ts < node} threshold only ever moves forward and physical cleanup under it is
     * safe. Three shapes carry a proof:
     * <ul>
     *     <li>a bare clock ({@code now()} / {@code now_ns()} / {@code sysdate()} / {@code systimestamp()} /
     *         {@code systimestamp_ns()});</li>
     *     <li>{@code <clock> - c} for a constant {@code c >= 0}: with {@code t >= 0} and
     *         {@code c in [0, Long.MAX_VALUE]}, {@code t - c} can neither overflow nor underflow and
     *         advances exactly as {@code t} does;</li>
     *     <li>{@code dateadd('<fixed unit>', k, <clock>)} for a constant {@code k <= 0} and a
     *         fixed-duration unit (n/u/T/s/m/h/d/w), which is {@code <clock> - c} with
     *         {@code c = -k * unit}; the amount is bounded so the scaled offset cannot overflow even
     *         on a nanosecond timeline.</li>
     * </ul>
     * Everything else stays a conservative false — calendar units (M/y) shift by a variable amount,
     * a look-forward offset expires rows the passage of time un-expires, and arbitrary clock
     * arithmetic (e.g. {@code now() - now()::long * 2}) can decrease. Skipping reclamation is safe;
     * accepting one decreasing transform is not.
     */
    private static boolean isProvenAdvancingClockExpression(ExpressionNode node) {
        if (isBareClockFunction(node)) {
            return true;
        }
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION
                && node.paramCount == 2
                && Chars.equals(node.token, "-")
                && isBareClockFunction(node.lhs)) {
            try {
                return parseConstantLong(node.rhs) >= 0;
            } catch (NumericException e) {
                return false;
            }
        }
        if (node.type == ExpressionNode.FUNCTION
                && node.paramCount == 3
                && SqlKeywords.isDateaddKeyword(node.token)) {
            // function args are stored reversed: [timestamp, amount, unit]
            final ExpressionNode clockArg = node.args.getQuick(0);
            final ExpressionNode amountArg = node.args.getQuick(1);
            final ExpressionNode unitArg = node.args.getQuick(2);
            if (!isBareClockFunction(clockArg)) {
                return false;
            }
            final long unitNanos = fixedDateaddUnitNanos(unitArg.token);
            if (unitNanos <= 0) {
                return false;
            }
            try {
                final long amount = parseConstantLong(amountArg);
                // look-back only, with the offset provably in range on a nanosecond timeline
                return amount <= 0 && amount != Long.MIN_VALUE && -amount <= Long.MAX_VALUE / unitNanos;
            } catch (NumericException e) {
                return false;
            }
        }
        return false;
    }

    // True when the sub-tree proves that it gives each row the same value in every execution and session:
    // every node is a column of the table, a constant, or an operator or function on the
    // ROW_ONLY_FUNCTIONS list. An unknown column name counts as not proven too, because the
    // function parser could resolve it to something other than a column. Date variables inside string
    // constants are the caller's separate check.
    private static boolean isRowOnlyExpression(ExpressionNode node, RecordMetadata metadata) {
        if (node == null) {
            return true;
        }
        final boolean isNodeRowOnly = switch (node.type) {
            case ExpressionNode.CONSTANT -> true;
            case ExpressionNode.LITERAL -> resolveColumnIndex(metadata, node.token) >= 0;
            case ExpressionNode.OPERATION, ExpressionNode.SET_OPERATION, ExpressionNode.FUNCTION ->
                    ROW_ONLY_FUNCTIONS.contains(node.token);
            default -> false;
        };
        if (isNodeRowOnly
                && (node.type == ExpressionNode.FUNCTION || node.type == ExpressionNode.SET_OPERATION)
                && SqlKeywords.isInKeyword(node.token)) {
            return isRowOnlyInExpression(node, metadata);
        }
        if (!isNodeRowOnly
                || !isRowOnlyExpression(node.lhs, metadata)
                || !isRowOnlyExpression(node.rhs, metadata)) {
            return false;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (!isRowOnlyExpression(node.args.getQuick(i), metadata)) {
                return false;
            }
        }
        return true;
    }

    // An IN over the designated timestamp parses a string value as an interval at runtime, and that parse
    // expands date variables such as $today. A date variable can reach it through concat() or from a column,
    // where the scan of string constants does not see it, so an IN counts as proven only when each of its
    // values is a constant. Its left operand follows the general rule.
    private static boolean isRowOnlyInExpression(ExpressionNode node, RecordMetadata metadata) {
        final ObjList<ExpressionNode> args = node.args;
        final int argCount = args.size();
        if (argCount == 0) {
            return node.rhs != null
                    && node.rhs.type == ExpressionNode.CONSTANT
                    && isRowOnlyExpression(node.lhs, metadata);
        }
        // Function args are stored reversed: the left operand comes last.
        for (int i = 0; i < argCount - 1; i++) {
            if (args.getQuick(i).type != ExpressionNode.CONSTANT) {
                return false;
            }
        }
        return isRowOnlyExpression(args.getQuick(argCount - 1), metadata);
    }

    /**
     * The threshold operand of a designated-timestamp ordering comparison, in either direction and with the
     * timestamp on either side, or null when the predicate is not that shape or the other operand references
     * a column.
     * <p>
     * {@link #timestampThresholdNode} recognises only the expire-old direction, because that is the one
     * whose threshold drives the partition-bounds fast path. A NULL check has to span all four ordering
     * operators with the timestamp on either side: a NULL threshold makes the comparison false for every row
     * whichever way it points, and the negation of the keep-filter then turns into a predicate that hides every
     * row.
     */
    private static ExpressionNode orderingThresholdNode(
            ExpressionNode node,
            RecordMetadata metadata,
            CharSequence timestampColumn
    ) {
        if (node == null || timestampColumn == null || node.type != ExpressionNode.OPERATION || node.paramCount != 2
                || node.lhs == null || node.rhs == null) {
            return null;
        }
        if (!Chars.equals(node.token, "<") && !Chars.equals(node.token, "<=")
                && !Chars.equals(node.token, ">") && !Chars.equals(node.token, ">=")) {
            return null;
        }
        final int timestampIndex = metadata.getColumnIndexQuiet(timestampColumn);
        if (node.lhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.lhs.token) == timestampIndex) {
            return referencesColumn(node.rhs) ? null : node.rhs;
        }
        if (node.rhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.rhs.token) == timestampIndex) {
            return referencesColumn(node.lhs) ? null : node.lhs;
        }
        return null;
    }

    // Long value of a constant expression node: a plain CONSTANT (with an optional L suffix) or a
    // unary-minus over one. Throws NumericException for any other shape.
    private static long parseConstantLong(ExpressionNode node) throws NumericException {
        if (node == null) {
            throw NumericException.INSTANCE;
        }
        if (node.type == ExpressionNode.CONSTANT) {
            final CharSequence token = node.token;
            final int len = token.length();
            final int hi = len > 1 && (token.charAt(len - 1) == 'L' || token.charAt(len - 1) == 'l') ? len - 1 : len;
            return Numbers.parseLong(token, 0, hi);
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, "-")) {
            final long value = parseConstantLong(node.rhs);
            if (value == Long.MIN_VALUE) {
                throw NumericException.INSTANCE;
            }
            return -value;
        }
        throw NumericException.INSTANCE;
    }

    // True if the sub-tree references any column (LITERAL) or bind variable, i.e. it cannot be evaluated to
    // a single constant threshold independent of the row.
    private static boolean referencesColumn(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL || node.type == ExpressionNode.BIND_VARIABLE) {
            return true;
        }
        if (referencesColumn(node.lhs) || referencesColumn(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (referencesColumn(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    // Resolves a predicate LITERAL token to a column index in metadata. Handles a plain (possibly quoted)
    // column name AND a table/alias-qualified reference such as t.v or "t"."v": EXPIRE predicates are
    // single-table, so a qualifier is the table name — the column is the segment after the last unquoted
    // dot. Returns -1 when the token names no column. Without the qualified-name handling a predicate like
    // `t.v < 2` would be seen as referencing no column, and dropping v would silently brick every read.
    private static int resolveColumnIndex(RecordMetadata metadata, CharSequence token) {
        int idx = metadata.getColumnIndexQuiet(unquote(token));
        if (idx >= 0) {
            return idx;
        }
        final int dot = Chars.indexOfLastUnquoted(token, '.');
        if (dot >= 0 && dot + 1 < token.length()) {
            return metadata.getColumnIndexQuiet(unquote(token.subSequence(dot + 1, token.length())));
        }
        return -1;
    }

    private static ExpressionNode timestampThresholdNode(
            ExpressionNode node,
            RecordMetadata metadata,
            CharSequence timestampColumn
    ) {
        if (node == null || timestampColumn == null || node.type != ExpressionNode.OPERATION || node.paramCount != 2
                || node.lhs == null || node.rhs == null) {
            return null;
        }
        final int timestampIndex = metadata.getColumnIndexQuiet(timestampColumn);
        final boolean isTimestampOnLeft = node.lhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.lhs.token) == timestampIndex;
        final boolean isTimestampOnRight = node.rhs.type == ExpressionNode.LITERAL
                && resolveColumnIndex(metadata, node.rhs.token) == timestampIndex;
        if (isTimestampOnLeft && (Chars.equals(node.token, "<") || Chars.equals(node.token, "<="))) {
            return referencesColumn(node.rhs) ? null : node.rhs;
        }
        if (isTimestampOnRight && (Chars.equals(node.token, ">") || Chars.equals(node.token, ">="))) {
            return referencesColumn(node.lhs) ? null : node.lhs;
        }
        return null;
    }

    // Converts an expiry threshold from the unit of the expression that produced it into the unit of the
    // designated timestamp column, so the caller can compare it against a partition floor. Rounds DOWN, so
    // the result is never above the exact threshold. A smaller threshold can only make the set of fully
    // expired partitions smaller, so the job never drops a partition that holds a live row. The remainder
    // that the rounding discards is smaller than one unit of the column, so at most one partition takes the
    // survivor scan, or waits for a later sweep. Math.multiplyExact throws on overflow, and the caller reads
    // that as "no fast path".
    private static long toTimestampUnit(long timestamp, int fromType, int toType) {
        if (fromType == toType) {
            return timestamp;
        }
        return ColumnType.isTimestampNano(toType)
                ? Math.multiplyExact(timestamp, Micros.MICRO_NANOS)
                : Math.floorDiv(timestamp, Micros.MICRO_NANOS);
    }

    /**
     * Parses and binds the shape the cleanup sweep compiles and refuses the policy when that fails. The sweep compiles
     * {@link RowExpiryUtil#buildRowExpiryKeepFilter}, which binds whenever the strict form of
     * {@link RowExpiryUtil#appendKeepCase} does, so this binds the strict form. Its type rules refuse a cross-type
     * equality such as {@code s = 12345} on a {@code STRING} column, which binds on its own but casts every value to
     * {@code INT} once rows are evaluated: every read and every sweep of the view would fail on the first non-numeric
     * value, with an error that names neither the view's policy nor EXPIRE ROWS. They also refuse cross-type equalities
     * that only match nothing, such as {@code k = 12345} on a {@code SYMBOL} column, because telling the two apart would
     * mean evaluating rows.
     * <p>
     * The strict form has to be bound on its own: {@code SqlParser.rewriteCase} turns a {@code CASE} over a
     * {@code <column> = <constant>} into a {@code switch} whose key types and conversions differ from those of the
     * comparison, and a {@code CASE} around any other predicate constructs every function the predicate holds, which
     * the bind of the bare predicate leaves to generation.
     * <p>
     * The strict form is parsed from its text rather than built from the parsed predicate, and the text round trip is
     * load-bearing. The sweep and every read embed the stored text verbatim, followed by a closing parenthesis, and a
     * text that ends in a line comment or an open block comment swallows that parenthesis, so every read fails with
     * "unbalanced (". The parse of the predicate alone cannot see that, and the parse of this text refuses it, as it
     * refuses whatever else the embedding breaks. The keep-filter DDL binds next is then safe to build from the parsed
     * predicate: the read's text embeds the policy between the same characters, see {@link RowExpiryUtil#appendKeepFilter}.
     * <p>
     * A predicate whose implicit cast only fails once a row is evaluated and that is not a bare
     * {@code <column> = <constant>} - {@code v < 'abc'} on a {@code DOUBLE} column - still gets through,
     * here and in the bare bind. Catching it would mean evaluating a row at DDL time, which makes
     * acceptance depend on whether the view happens to hold data. Such a predicate fails as a plain
     * {@code WHERE} clause too, so it is wrong in a way the author sees immediately.
     */
    private void bindSweepShape(
            ExpressionNode node,
            CharSequence predicate,
            RecordMetadata metadata,
            int position,
            SqlParserCallback parserCallback,
            SqlExecutionContext executionContext
    ) throws SqlException {
        Function f = null;
        try {
            f = compiler.bindStatementExpression(policyParser.parseKeepCase(predicate, parserCallback), metadata, ColumnType.UNDEFINED, executionContext);
            f = Misc.free(f);
            // A bare <column> = <constant> has to compare values of one kind. A number compared with a text column
            // reads each value as a number, which fails on the first non-numeric value of a read or a sweep. Text
            // compared with a numeric, CHAR or UUID column has to be a value of the column: otherwise it matches no
            // row, or fails every read, as v = 'abc' does on a DOUBLE column.
            if (node.type == ExpressionNode.OPERATION && node.paramCount == 2 && Chars.equals(node.token, "=")
                    && (node.lhs.type == ExpressionNode.LITERAL) != (node.rhs.type == ExpressionNode.LITERAL)
                    && (node.lhs.type == ExpressionNode.CONSTANT || node.rhs.type == ExpressionNode.CONSTANT)) {
                final ExpressionNode column = node.lhs.type == ExpressionNode.LITERAL ? node.lhs : node.rhs;
                final ExpressionNode constant = node.lhs.type == ExpressionNode.CONSTANT ? node.lhs : node.rhs;
                final int columnIndex = metadata.getColumnIndexQuiet(GenericLexer.unquote(column.token));
                if (columnIndex > -1) {
                    final int columnType = metadata.getColumnType(columnIndex);
                    f = compiler.bindStatementExpression(constant, metadata, ColumnType.UNDEFINED, executionContext);
                    final int constantType = f.getType();
                    f = Misc.free(f);
                    final boolean isTextConstant = ColumnType.isStringyType(constantType) || ColumnType.isSymbol(constantType) || constantType == ColumnType.CHAR;
                    if (ColumnType.isSymbol(columnType) || ColumnType.isVarcharOrString(columnType)) {
                        if (!isTextConstant && constantType != ColumnType.NULL) {
                            throw SqlException.position(constant.position)
                                    .put("type mismatch [expected=").put(ColumnType.nameOf(columnType))
                                    .put(", actual=").put(ColumnType.nameOf(constantType))
                                    .put(']');
                        }
                    } else if (isTextConstant) {
                        checkTextConvertsToColumnType(GenericLexer.unquote(constant.token), columnType);
                    }
                }
            }
        } catch (SqlException | CairoException | ImplicitCastException e) {
            throw invalidPredicate(position, e);
        } finally {
            Misc.free(f);
        }
    }

    private SqlException invalidPredicate(int position, FlyweightMessageContainer e) {
        failure.clear();
        failure.put(e.getFlyweightMessage());
        return SqlException.$(position, "invalid EXPIRE ROWS predicate: ").put(failure);
    }

    /**
     * Rejects a policy whose threshold is a compile-time constant NULL, such as
     * {@code ts < cast(null as timestamp)}, or an arithmetic expression that overflows onto the NULL
     * sentinel, {@code ts < 4611686018427387904 * 2}. Such a policy expires nothing, which is always a
     * mistake, and it is one the author cannot see: the source text reads as an ordinary timestamp.
     * <p>
     * The threshold comes from {@link #orderingThresholdNode}, which spans all four ordering
     * operators with the timestamp on either side. A check that saw only {@code ts < T} would leave
     * {@code ts > T} storing a NULL threshold that reads as an empty view while every row stays on disk.
     * <p>
     * Binding the threshold answers this with the engine's own arithmetic. QuestDB types integer literals
     * and promotes products by rules that are not evident from the source text - {@code 86400*1000000}
     * comes out LONG and correct, {@code 1073741824*2} comes out INT and NULL - so a check that folded the
     * expression itself would have to reproduce those rules to stay in step with what reads compute. The
     * threshold binds on its own rather than being read from the bound predicate, where the comparison may fold
     * a NULL operand away together with the comparison.
     * <p>
     * Only {@link Function#isConstant()} counts. A runtime constant such as {@code now() - c} evaluates
     * against whatever clock this DDL happens to see, so a non-NULL answer now says nothing about a read
     * a month later; a NULL there reads as a comparison that holds for no row, so the read keeps every row.
     * <p>
     * {@link #timestampThreshold} evaluates a threshold too but cannot stand in for this: it
     * returns {@code LONG_NULL} for a dozen unrelated reasons - no designated timestamp, the opposite
     * comparison direction, a non-timestamp threshold type - so it cannot tell a NULL threshold from a
     * shape it simply does not handle.
     */
    private void rejectNullThreshold(
            ExpressionNode node,
            RecordMetadata metadata,
            CharSequence timestampColumn,
            int position,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode thresholdNode = orderingThresholdNode(node, metadata, timestampColumn);
        if (thresholdNode == null) {
            return;
        }
        final boolean isNullThreshold;
        try {
            isNullThreshold = compiler.isNullStatementExpression(thresholdNode, metadata, executionContext);
        } catch (SqlException | CairoException | ImplicitCastException e) {
            // The whole-predicate bind already reported anything that matters; a failure here only
            // means the threshold could not be evaluated, which is not itself a reason to reject.
            return;
        }
        // The caret carries the caller's position, as every other error here does: a node position of the parsed
        // policy is an offset into the predicate text rather than into the statement the user wrote.
        if (isNullThreshold) {
            throw SqlException.$(position, "invalid EXPIRE ROWS predicate: the threshold is NULL, so no row can ever expire");
        }
    }

    static {
        // operators
        addRowOnlyFunctions(
                "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "&", "|", "^", "::",
                "and", "or", "not", "in", "between", "like", "ilike", "~", "!~"
        );
        // functions
        addRowOnlyFunctions(
                "abs", "case", "cast", "ceil", "ceiling", "coalesce", "concat", "dateadd", "datediff", "day",
                "day_of_week", "floor", "hour", "left", "length", "lower", "minute", "month", "right", "round",
                "second", "starts_with", "substring", "timestamp_ceil", "timestamp_floor", "to_lowercase",
                "to_uppercase", "trim", "upper", "year"
        );
    }
}
