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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.SingleCharCharSequence;
import io.questdb.std.str.StringSink;

/**
 * Symbol key intrinsics of one native scan occurrence: the included and excluded key sets of a
 * single column, combined across conjuncts.
 */
public final class SymbolKeyExtractor implements Mutable {
    private static final ExpressionVisitor WITHIN_CALLS = expression -> expression instanceof FunctionExpression call
            && "within".equals(call.getName()) ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final KeySet excluded = new KeySet();
    private final ObjList<BoundExpression> excludedConjuncts;
    private final KeySet included = new KeySet();
    private final ObjList<BoundExpression> includedConjuncts;
    private final ObjHashSet<BoundExpression> intrinsics = new ObjHashSet<>();
    private final StringSink keyText = new StringSink();
    private final KeySet merged = new KeySet();
    private final StringSink otherKeyText = new StringSink();
    private final KeySet pending = new KeySet();
    private int columnId = -1;
    private boolean hasKey;
    private boolean isFalse;
    private CursorExpression subquery;

    /**
     * Records the conjuncts of each extraction in the given temporary lists.
     */
    public SymbolKeyExtractor(ObjList<BoundExpression> includedConjuncts, ObjList<BoundExpression> excludedConjuncts) {
        this.includedConjuncts = includedConjuncts;
        this.excludedConjuncts = excludedConjuncts;
    }

    /**
     * The caller owns the returned key function; bind values remain deferred until cursor open.
     */
    public static Function instantiateValue(BoundExpression value, OutputSchema input, RecordMetadata metadata,
                                            FunctionInstantiator instantiator, SqlExecutionContext executionContext) throws SqlException {
        if (value instanceof ConstantExpression constant) {
            return switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.CHAR -> constant.getLongValue() == 0 ? StrConstant.NULL
                        : StrConstant.fromValue(SingleCharCharSequence.get((char) constant.getLongValue()));
                default -> StrConstant.fromValue(textOf(constant, Misc.getThreadLocalSink()));
            };
        }
        final Function parameter = instantiator.instantiate(value, input, metadata, executionContext);
        final Function function = isIntegerKey(ColumnType.tagOf(parameter.getType())) ? new CastLongToStrFunctionFactory.Func(parameter) : parameter;
        try {
            function.init(null, executionContext);
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * A constant that {@code =} compares with a SYMBOL by its text.
     */
    public static boolean isKeyConstant(BoundExpression value) {
        return value instanceof ConstantExpression && isKeyValue(value);
    }

    /**
     * A constant or bind variable that {@code =} compares with a SYMBOL by its text.
     */
    public static boolean isKeyValue(BoundExpression value) {
        final int type = ColumnType.tagOf(value.getDataType());
        if (value instanceof ConstantExpression) {
            return switch (type) {
                case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.CHAR, ColumnType.NULL -> true;
                default -> isIntegerKey(type);
            };
        }
        return value instanceof BindVariableExpression parameter && parameter.isDirectReference()
                && (type == ColumnType.STRING || type == ColumnType.VARCHAR || isIntegerKey(type));
    }

    @Override
    public void clear() {
        columnId = -1;
        hasKey = false;
        isFalse = false;
        subquery = null;
        included.clear();
        excluded.clear();
        pending.clear();
        merged.clear();
        includedConjuncts.clear();
        excludedConjuncts.clear();
        intrinsics.clear();
    }

    public BoundExpression extract(BoundExpression predicate, int candidateColumnId, BoundExpressionRewriter rewriter) {
        clear();
        columnId = candidateColumnId;
        final boolean isKeyOrRewritable = columnId >= 0 && predicate != null && predicate.walk(WITHIN_CALLS);
        return extractKeys(isKeyOrRewritable ? keyEqualityOrsToIn(predicate, rewriter) : predicate, rewriter);
    }

    public BoundExpression extractIndexed(BoundExpression predicate, OutputSchema input, IntList indexes,
                                          TableAccessInfo table, BoundExpressionRewriter rewriter) {
        clear();
        selectColumn(predicate, input, indexes, table);
        return extractKeys(predicate, rewriter);
    }

    public int getColumnId() {
        return hasKey ? columnId : -1;
    }

    public ObjList<BoundExpression> getExcludedConjuncts() {
        return excludedConjuncts;
    }

    public ObjList<BoundExpression> getExcludedValues() {
        return excluded.values;
    }

    public CursorExpression getSubquery() {
        return isFalse ? null : subquery;
    }

    public ObjList<BoundExpression> getValues() {
        return included.values;
    }

    public boolean hasKey() {
        return hasKey && !isFalse && (included.values.size() > 0 || excluded.values.size() > 0);
    }

    public boolean isFalse() {
        return isFalse;
    }

    private static boolean isBind(BoundExpression value) {
        return value instanceof BindVariableExpression;
    }

    private static boolean isEquals(FunctionExpression call) {
        return call.getArgumentCount() == 2 && "=".equals(call.getName());
    }

    private static boolean isIn(FunctionExpression call) {
        return "in".equals(call.getName()) && call.getArgumentCount() > 1;
    }

    private static boolean isIntegerKey(int type) {
        return switch (type) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> true;
            default -> false;
        };
    }

    private static boolean isKeyOperand(BoundExpression expression) {
        return expression instanceof ColumnExpression column
                && column.isDirectReference() && ColumnType.isSymbol(column.getDataType());
    }

    private static boolean isKeySubquery(FunctionExpression call) {
        return isIn(call) && call.getArgumentCount() == 2 && call.argumentAt(1) instanceof CursorExpression cursor
                && !cursor.isBoolean() && cursor.getPlan().getOutput().getColumnCount() == 1
                && switch (ColumnType.tagOf(cursor.getPlan().getOutput().getColumnType(0))) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> true;
            default -> false;
        };
    }

    /**
     * A key value the IN function accepts in its list: any key value but an integer bind variable.
     */
    private static boolean isListKeyValue(BoundExpression value) {
        return isKeyValue(value) && !(isBind(value) && isIntegerKey(ColumnType.tagOf(value.getDataType())));
    }

    private static boolean isNot(FunctionExpression call) {
        return "not".equals(call.getName()) && call.getArgumentCount() == 1
                && call.argumentAt(0) instanceof FunctionExpression inner && isIn(inner);
    }

    private static boolean isNotEquals(FunctionExpression call) {
        final String name = call.getName();
        return call.getArgumentCount() == 2 && ("!=".equals(name) || "<>".equals(name));
    }

    private static ColumnExpression selectorColumn(FunctionExpression call) {
        if (isEquals(call) || isNotEquals(call)) {
            for (int i = 0; i < 2; i++) {
                if (isKeyOperand(call.argumentAt(i)) && isKeyValue(call.argumentAt(1 - i))) {
                    return (ColumnExpression) call.argumentAt(i);
                }
            }
            return null;
        }
        final FunctionExpression list = isNot(call) ? (FunctionExpression) call.argumentAt(0) : isIn(call) ? call : null;
        if (list == null || !isKeyOperand(list.argumentAt(0))) {
            return null;
        }
        if (list == call && isKeySubquery(call)) {
            return (ColumnExpression) call.argumentAt(0);
        }
        for (int i = 1, n = list.getArgumentCount(); i < n; i++) {
            if (!isListKeyValue(list.argumentAt(i))) {
                return null;
            }
        }
        return (ColumnExpression) list.argumentAt(0);
    }

    /**
     * The UTF-16 text a STRING, VARCHAR, SYMBOL or integer constant matches a SYMBOL by, the decimal text of an
     * integer as {@code =} formats it; null for a NULL value or any other type. An integer formats into {@code sink}.
     */
    private static CharSequence textOf(BoundExpression value, StringSink sink) {
        final ConstantExpression constant = (ConstantExpression) value;
        final int type = ColumnType.tagOf(constant.getDataType());
        if (isIntegerKey(type)) {
            final long key = type == ColumnType.INT ? Numbers.intToLong((int) constant.getLongValue()) : constant.getLongValue();
            if (key == Numbers.LONG_NULL) {
                return null;
            }
            sink.clear();
            sink.put(key);
            return sink;
        }
        return constant.getText();
    }

    private void addExcluded(FunctionExpression call, BoundExpression value) {
        excluded.add(value);
        markExcluded(call);
    }

    private void analyze(BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call)) {
            return;
        }
        if (call.isAnd()) {
            // A right leaf comes before its left subtree, so the last conjunct of a chain is the first key candidate.
            if (call.argumentAt(1) instanceof FunctionExpression nested && nested.isAnd()) {
                analyze(call.argumentAt(0));
                analyze(call.argumentAt(1));
            } else {
                analyze(call.argumentAt(1));
                analyze(call.argumentAt(0));
            }
            return;
        }
        if (subquery != null) {
            return;
        }
        if (isKeySubquery(call)) {
            if (LogicalPlans.isKeyColumn(call.argumentAt(0), columnId)) {
                startKey();
                subquery = (CursorExpression) call.argumentAt(1);
                mark(call);
            }
            return;
        }
        if (isEquals(call) || isNotEquals(call)) {
            for (int i = 0; i < 2; i++) {
                if (LogicalPlans.isKeyColumn(call.argumentAt(i), columnId)) {
                    final BoundExpression value = call.argumentAt(1 - i);
                    if (isKeyValue(value)) {
                        if (isEquals(call)) {
                            analyzeEquals(call, value);
                        } else {
                            analyzeNotEquals(call, value);
                        }
                    }
                    return;
                }
            }
        } else if (isIn(call)) {
            if (LogicalPlans.isKeyColumn(call.argumentAt(0), columnId)) {
                analyzeIn(call);
            }
        } else if (isNot(call)) {
            final FunctionExpression inner = (FunctionExpression) call.argumentAt(0);
            if (LogicalPlans.isKeyColumn(inner.argumentAt(0), columnId)) {
                analyzeNotIn(call, inner);
            }
        }
    }

    private void analyzeEquals(FunctionExpression call, BoundExpression value) {
        if (!hasKey) {
            startKey();
            included.add(value);
            markIncluded(call);
            return;
        }
        if (!included.isKnown || isBind(value) && included.size() > 0) {
            return;
        }
        if (included.contains(value)) {
            if (included.size() > 1) {
                included.clear();
                included.add(value);
                markIncluded(call);
            }
        } else if (included.size() > 0) {
            included.clear();
            mark(call);
            isFalse = true;
            return;
        }
        if (!isNullKey(value) && excluded.contains(value)) {
            excluded.clear();
            mark(call);
            isFalse = true;
            return;
        }
        included.add(value);
        markIncluded(call);
    }

    private void analyzeIn(FunctionExpression call) {
        final boolean isNewKey = !hasKey;
        if (!isNewKey && !included.isKnown) {
            return;
        }
        if (hasRejectedValue(call, !isNewKey && included.size() > 0)) {
            return;
        }
        collectPending(call);
        if (isNewKey) {
            startKey();
            included.addAll(pending);
            markIncluded(call);
            return;
        }
        if (included.size() == 0) {
            included.addAll(pending);
        } else if (!pending.isKnown) {
            return;
        }
        final boolean isKnown = included.isKnown && pending.isKnown;
        merged.clear();
        for (int i = 0, n = pending.size(); i < n; i++) {
            final BoundExpression value = pending.values.getQuick(i);
            if (included.contains(value)) {
                merged.add(value);
            }
        }
        included.clear();
        included.addAll(merged);
        included.isKnown = isKnown;
        if (included.size() == 0) {
            isFalse = true;
        }
        if (excluded.size() > 0 && (!included.isKnown || !excluded.isKnown)) {
            excluded.clear();
            revert(excludedConjuncts);
        }
        markIncluded(call);
    }

    private void analyzeNotEquals(FunctionExpression call, BoundExpression value) {
        if (!hasKey) {
            startKey();
            addExcluded(call, value);
            return;
        }
        if (excluded.contains(value)) {
            markExcluded(call);
        } else if (!isBind(value) && included.isKnown && included.contains(value)) {
            included.remove(value);
            removeIncludedConjuncts(value);
            if (included.size() == 0) {
                isFalse = true;
            }
            markExcluded(call);
        } else {
            addExcluded(call, value);
        }
    }

    private void analyzeNotIn(FunctionExpression notCall, FunctionExpression inCall) {
        if (hasRejectedValue(inCall, false)) {
            return;
        }
        collectPending(inCall);
        if (!hasKey) {
            startKey();
            excluded.addAll(pending);
            markExcluded(notCall);
            return;
        }
        if (excluded.size() == 0) {
            excluded.addAll(pending);
        }
        final boolean isKnown = excluded.isKnown && pending.isKnown;
        excluded.addAll(pending);
        excluded.isKnown = isKnown;
        markExcluded(notCall);
    }

    private void applyExclusions() {
        if (!hasKey || included.size() == 0 || excludedConjuncts.size() == 0) {
            return;
        }
        if (included.isKnown && excluded.isKnown) {
            for (int i = 0, n = excludedConjuncts.size(); i < n && !isFalse; i++) {
                final FunctionExpression conjunct = (FunctionExpression) excludedConjuncts.getQuick(i);
                final FunctionExpression call = isNot(conjunct) ? (FunctionExpression) conjunct.argumentAt(0) : conjunct;
                if (call.getArgumentCount() == 2) {
                    final BoundExpression value = LogicalPlans.isKeyColumn(call.argumentAt(0), columnId) ? call.argumentAt(1) : call.argumentAt(0);
                    excludeIncluded(value);
                } else {
                    for (int k = 1, m = call.getArgumentCount(); k < m && !isFalse; k++) {
                        excludeIncluded(call.argumentAt(k));
                    }
                }
                mark(conjunct);
            }
        }
        if (included.size() > 0 && excluded.size() > 0) {
            if (!included.isKnown || !excluded.isKnown) {
                revert(excludedConjuncts);
            }
            excluded.clear();
        }
    }

    /**
     * The code of a one-character constant, CHAR, STRING or VARCHAR, so that every spelling compares equal; -1 otherwise.
     */
    private int charCodeOf(BoundExpression value) {
        if (!(value instanceof ConstantExpression constant)) {
            return -1;
        }
        if (ColumnType.tagOf(constant.getDataType()) == ColumnType.CHAR) {
            return constant.getLongValue() == 0 ? -1 : (int) constant.getLongValue();
        }
        final CharSequence text = textOf(constant, keyText);
        return text != null && text.length() == 1 ? text.charAt(0) : -1;
    }

    private void collectPending(FunctionExpression call) {
        pending.clear();
        for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
            final BoundExpression value = call.argumentAt(i);
            pending.add(value);
        }
    }

    private void excludeIncluded(BoundExpression value) {
        included.remove(value);
        if (included.size() == 0) {
            isFalse = true;
        }
    }

    private BoundExpression extractKeys(BoundExpression predicate, BoundExpressionRewriter rewriter) {
        if (columnId < 0) {
            return predicate;
        }
        analyze(predicate);
        applyExclusions();
        return intrinsics.size() == 0 ? predicate : residual(predicate, rewriter);
    }

    private boolean hasRejectedValue(FunctionExpression call, boolean isBindRejected) {
        for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
            final BoundExpression value = call.argumentAt(i);
            if (!isListKeyValue(value) || isBindRejected && isBind(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean isNullKey(BoundExpression value) {
        return !isBind(value) && charCodeOf(value) < 0 && textOf(value, keyText) == null;
    }

    private boolean isSameKey(BoundExpression left, BoundExpression right) {
        if (isBind(left) || isBind(right)) {
            return isBind(left) && isBind(right)
                    && Chars.equals(((BindVariableExpression) left).getName(), ((BindVariableExpression) right).getName());
        }
        final int code = charCodeOf(left);
        if (code != charCodeOf(right)) {
            return false;
        }
        if (code >= 0) {
            return true;
        }
        final CharSequence leftText = textOf(left, keyText);
        final CharSequence rightText = textOf(right, otherKeyText);
        return leftText == null ? rightText == null : rightText != null && Chars.equals(leftText, rightText);
    }

    private BoundExpression keyEqualityOrsToIn(BoundExpression predicate, BoundExpressionRewriter rewriter) {
        if (!(predicate instanceof FunctionExpression call)) {
            return predicate;
        }
        if (call.isAnd()) {
            return rewriter.replaceConjunction(call, keyEqualityOrsToIn(call.argumentAt(0), rewriter),
                    keyEqualityOrsToIn(call.argumentAt(1), rewriter));
        }
        if (!call.isOr() || !LogicalPlans.isKeyEqualityOr(call, columnId)) {
            return predicate;
        }
        final FunctionExpression in = rewriter.keyEqualityOrToIn(call, columnId);
        return in != null ? in : predicate;
    }

    private void mark(BoundExpression conjunct) {
        intrinsics.add(conjunct);
    }

    private void markExcluded(BoundExpression conjunct) {
        mark(conjunct);
        excludedConjuncts.add(conjunct);
    }

    private void markIncluded(BoundExpression conjunct) {
        mark(conjunct);
        includedConjuncts.add(conjunct);
    }

    private void removeIncludedConjuncts(BoundExpression key) {
        for (int i = includedConjuncts.size() - 1; i > -1; i--) {
            if (includedConjuncts.getQuick(i) instanceof FunctionExpression call && call.getArgumentCount() == 2) {
                final BoundExpression value = LogicalPlans.isKeyColumn(call.argumentAt(0), columnId) ? call.argumentAt(1) : call.argumentAt(0);
                if (isKeyValue(value) && !isBind(value) && isSameKey(value, key)) {
                    includedConjuncts.remove(i);
                }
            }
        }
    }

    private BoundExpression residual(BoundExpression predicate, BoundExpressionRewriter rewriter) {
        if (intrinsics.contains(predicate)) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            return rewriter.replaceConjunction(call, residual(call.argumentAt(0), rewriter), residual(call.argumentAt(1), rewriter));
        }
        return predicate;
    }

    private void revert(ObjList<BoundExpression> conjuncts) {
        for (int i = 0, n = conjuncts.size(); i < n; i++) {
            intrinsics.remove(conjuncts.getQuick(i));
        }
        conjuncts.clear();
    }

    private void selectColumn(BoundExpression expression, OutputSchema input, IntList indexes, TableAccessInfo table) {
        if (expression instanceof FunctionExpression call) {
            if (call.isAnd()) {
                if (call.argumentAt(1) instanceof FunctionExpression nested && nested.isAnd()) {
                    selectColumn(call.argumentAt(0), input, indexes, table);
                    selectColumn(call.argumentAt(1), input, indexes, table);
                } else {
                    selectColumn(call.argumentAt(1), input, indexes, table);
                    selectColumn(call.argumentAt(0), input, indexes, table);
                }
                return;
            }
            final ColumnExpression column = selectorColumn(call);
            if (column != null && table.isIndexed(indexes.getQuick(input.getColumnIndexById(column.getColumnId())))) {
                if (columnId < 0) {
                    columnId = column.getColumnId();
                } else if (column.getColumnId() != columnId) {
                    final int candidate = indexes.getQuick(input.getColumnIndexById(column.getColumnId()));
                    final int selected = indexes.getQuick(input.getColumnIndexById(columnId));
                    final int count = table.getSymbolCount(candidate);
                    final int selectedCount = table.getSymbolCount(selected);
                    if (count > selectedCount || count == selectedCount && table.getSymbolCapacity(candidate) > table.getSymbolCapacity(selected)) {
                        columnId = column.getColumnId();
                    }
                }
            }
        }
    }

    private void startKey() {
        hasKey = true;
        included.clear();
        excluded.clear();
        revert(includedConjuncts);
        revert(excludedConjuncts);
    }

    private final class KeySet {
        private final CharSequenceHashSet binds = new CharSequenceHashSet();
        private final IntHashSet chars = new IntHashSet();
        private final CharSequenceHashSet constants = new CharSequenceHashSet();
        private final ObjList<BoundExpression> values = new ObjList<>();
        private boolean isKnown = true;

        void add(BoundExpression value) {
            final boolean isAdded;
            if (value instanceof BindVariableExpression parameter) {
                isAdded = addText(binds, parameter.getName());
            } else {
                final int code = charCodeOf(value);
                isAdded = code >= 0 ? chars.add(code) : addText(constants, textOf(value, keyText));
            }
            if (isAdded) {
                values.add(value);
                isKnown &= !isBind(value);
            }
        }

        void addAll(KeySet that) {
            for (int i = 0, n = that.size(); i < n; i++) {
                add(that.values.getQuick(i));
            }
        }

        void clear() {
            binds.clear();
            chars.clear();
            constants.clear();
            values.clear();
            isKnown = true;
        }

        boolean contains(BoundExpression value) {
            if (value instanceof BindVariableExpression parameter) {
                return binds.contains(parameter.getName());
            }
            final int code = charCodeOf(value);
            return code >= 0 ? chars.contains(code) : constants.contains(textOf(value, keyText));
        }

        void remove(BoundExpression value) {
            if (!contains(value)) {
                return;
            }
            for (int i = 0, n = values.size(); i < n; i++) {
                if (isSameKey(values.getQuick(i), value)) {
                    if (value instanceof BindVariableExpression parameter) {
                        binds.remove(parameter.getName());
                    } else {
                        final int code = charCodeOf(value);
                        if (code >= 0) {
                            chars.remove(code);
                        } else {
                            constants.remove(textOf(value, keyText));
                        }
                    }
                    values.remove(i);
                    return;
                }
            }
        }

        int size() {
            return values.size();
        }

        /**
         * Adds the text, a copy when it is the decoding sink; any other text lives as long as the plan.
         */
        private boolean addText(CharSequenceHashSet set, CharSequence text) {
            if (text == null) {
                return set.addNull();
            }
            final int index = set.keyIndex(text);
            if (index < 0) {
                return false;
            }
            if (text == keyText) {
                set.addAt(index, text);
            } else {
                set.addAtWithBorrowed(index, text);
            }
            return true;
        }
    }
}
