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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;


/**
 * Operation codes and condition holders for Parquet row group pruning via bloom filters,
 * min/max statistics, and null counts. {@code ParquetPushdownExtractor} extracts the conditions.
 */
public final class PushdownFilterExtractor {

    // Operation types for pushdown filter conditions.
    // Keep in sync with FILTER_OP_* constants in parquet_read/mod.rs.
    // Range semantics (LT/LE/GT/GE): skip row group if
    //   LT: min >= val, LE: min > val, GT: max <= val, GE: max < val.
    // BETWEEN: auto-swaps bounds, skip if max_stat < min(a,b) || min_stat > max(a,b).
    public static final int OP_BETWEEN = 7;
    public static final int OP_EQ = 0;
    public static final int OP_GE = 4;
    public static final int OP_GT = 3;
    public static final int OP_IS_NOT_NULL = 6;
    public static final int OP_IS_NULL = 5;
    public static final int OP_LE = 2;
    public static final int OP_LT = 1;
    // Not an op the native side knows about: it marks a condition that cannot be pushed down.
    public static final int OP_UNSUPPORTED = -1;

    /**
     * Reports whether a null predicate over this column type may drive row group pruning.
     * <p>
     * Pruning is exact only where the parquet null bit and the SQL NULL denote the same rows.
     * The parquet writer marks column-top rows - rows that predate the ADD COLUMN - with
     * definition level 0, and decides every other row's null bit through its {@code Nullable}
     * impl in {@code core/rust/qdbr/src/parquet_write/mod.rs}. Two type groups break the
     * correspondence, in opposite directions:
     * <ul>
     * <li>BOOLEAN, BYTE and SHORT carry no null sentinel, so {@code EqBooleanFunctionFactory},
     * {@code EqByteFunctionFactory} and {@code EqShortFunctionFactory} fold a null constant to
     * {@code BooleanConstant.FALSE} and every stored row is non-null - a column-top row reads
     * back as 0/false, a legitimate value. A row group built entirely from column-top rows
     * reports {@code null_count == num_values}, which {@link ParquetRowGroupFilter} would
     * discard for IS NOT NULL even though every one of its rows matches. Neither direction may
     * consult the file's null bit.</li>
     * <li>CHAR, FLOAT and DOUBLE call more values NULL than the writer marks null, so parquet
     * nulls are a strict subset of SQL NULLs. IS NOT NULL still prunes exactly - a row group
     * with {@code null_count == num_values} holds only NULLs, so no row matches - but IS NULL
     * does not: {@code null_count == 0} no longer implies the group holds no NULL. CHAR's null
     * is {@code Numbers.CHAR_NULL} while the writer's {@code Nullable} impl for {@code u16}
     * reports every stored value non-null; {@code Numbers.isNull(double)} masks
     * {@code EXP_BIT_MASK} and {@code isNull(float)} tests {@code isInfinite}, so both count
     * +/-Infinity as NULL, while the writer's impls for {@code f32}/{@code f64} test only
     * {@code is_nan()} and {@code simd.rs} compares strictly greater than the infinity bits.
     * A stored CHAR_NULL or infinity therefore reaches the file as a non-null value, and a row
     * group of them reports {@code null_count == 0} while every row matches IS NULL.</li>
     * </ul>
     * Every other type's null detection - a {@code Nullable} impl for the fixed-size types, a
     * length or key check for the variable-size ones, which do not implement that trait -
     * recognises the same values SQL does, so both directions stay exact. That includes IPv4,
     * whose in-band 0 the writer does map to a parquet null.
     * <p>
     * This gates on the column's <em>metadata</em> type; soundness also needs the file's stored
     * type to agree, which {@link ParquetRowGroupFilter} enforces separately by dropping any
     * condition whose parquet column type differs, before it reaches the null-op branch.
     * <p>
     * Relaxing either arm would recover no pruning, so the two {@code false} answers cost nothing.
     * The native side declines the same skips independently: {@code writer_undercounts_nulls}
     * refuses the {@code null_count == 0} skip for CHAR, FLOAT and DOUBLE, and
     * {@code is_null_free_type} refuses the {@code null_count == num_values} skip for BOOLEAN, BYTE
     * and SHORT, both in {@code parquet_read::row_groups} and its {@code parquet_metadata::skip}
     * twin. A newly pushed condition would therefore prune nothing and merely mark pushdown active,
     * which costs the page frame cursor its up-front {@code size()}. For BOOLEAN, BYTE and SHORT it
     * would also reopen the {@code filter == null} alongside active pushdown state that
     * {@code ParquetRowGroupPruningTest.testLimitOverConstantFoldedByteNullFilter} pins closed,
     * because {@code b IS NOT NULL} folds to a constant TRUE the code generator drops. The
     * remaining pair - IS NULL over those three - folds to a constant FALSE that
     * {@code SqlCodeGenerator} replaces with an empty factory, so no scan runs there to prune.
     */
    public static boolean isNullOpPushable(int columnType, int opType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT -> false;
            case ColumnType.CHAR, ColumnType.FLOAT, ColumnType.DOUBLE -> opType == OP_IS_NOT_NULL;
            default -> true;
        };
    }

    /**
     * Rebuilds a constant DECIMAL function so its storage tag and scale match the
     * column's, which is what {@link ParquetRowGroupFilter#prepareFilterList} relies
     * on when it dispatches {@code getDecimal<N>} based on the column tag and pushes
     * the raw bytes against parquet row group statistics. Returns {@code null} when
     * the constant cannot be expressed at the column's scale (lossy scale-down) or
     * does not fit in the column's storage size, signalling the caller to skip
     * pushdown for the condition. Returns the input unchanged when scale and tag
     * already match. On a successful rebuild the original function is closed and
     * the new constant takes its place.
     */
    public static Function rescaleDecimalForPushdown(
            Function f,
            int colType,
            SqlExecutionContext executionContext
    ) {
        final int colTag = ColumnType.tagOf(colType);
        final int litTag = ColumnType.tagOf(f.getType());
        final int colScale = ColumnType.getDecimalScale(colType);
        final int litScale = ColumnType.getDecimalScale(f.getType());
        if (colTag == litTag && colScale == litScale) {
            return f;
        }

        final int colPrecision = ColumnType.getDecimalPrecision(colType);
        final Decimal256 d256 = executionContext.getDecimal256();
        final Decimal128 d128 = executionContext.getDecimal128();
        DecimalUtil.load(d256, d128, f, null);

        if (d256.isNull()) {
            f.close();
            return DecimalUtil.createNullDecimalConstant(colPrecision, colScale);
        }

        try {
            d256.rescale(colScale);
        } catch (NumericException e) {
            return null;
        }

        final int colStorageSizePow2 = Decimals.getStorageSizePow2(colPrecision);
        if (!d256.fitsInStorageSizePow2(colStorageSizePow2)) {
            return null;
        }

        f.close();
        return DecimalUtil.createDecimalConstant(d256, colPrecision, colScale);
    }

    // Not pooled: conditions are passed to RecordCursorFactory and live for the duration of query execution.
    public static class PushdownFilterCondition implements QuietCloseable {
        private final CharSequence columnName;
        private final int columnType;
        // Stable writer index (column id) of the filtered column. The Parquet file stores
        // this id per column, so native-table row-group pruning resolves the Parquet column
        // by id rather than by name -- a rename leaves the frozen Parquet name stale, which
        // would otherwise resolve to the wrong column (or a name collision) and skip rows.
        private final int columnWriterIndex;
        private final int operationType;
        private final ObjList<Function> valueFunctions = new ObjList<>();
        private final ObjList<ExpressionNode> values = new ObjList<>();
        // Set when value serialization declined this condition AND every one of its values is a
        // compile-time constant, so re-running it cannot reach a different answer.
        // ParquetRowGroupFilter.prepareFilterList runs once per parquet partition and re-raised the
        // same ImplicitCastException every time - a string bound on a TIMESTAMP column arrives as a
        // StrConstant whose getLong() throws - which over thousands of partitions is thousands of
        // exceptions for one permanent answer. A bind variable is a RUNTIME constant, not a
        // constant, so a value that can be re-bound is never cached.
        private boolean isSerializationDeclined;

        public PushdownFilterCondition(CharSequence columnName, int columnWriterIndex, int columnType) {
            this(columnName, columnWriterIndex, columnType, OP_EQ);
        }

        public PushdownFilterCondition(CharSequence columnName, int columnWriterIndex, int columnType, int operationType) {
            this.columnName = Chars.toString(columnName);
            this.columnWriterIndex = columnWriterIndex;
            this.columnType = columnType;
            this.operationType = operationType;
        }

        public void addValue(ExpressionNode valueNode) {
            values.add(valueNode);
        }

        public void addValueFunction(Function valueFunction) {
            valueFunctions.add(valueFunction);
        }

        public void addValues(ObjList<ExpressionNode> values1) {
            values.addAll(values1);
        }

        @Override
        public void close() {
            Misc.freeObjListAndClear(valueFunctions);
        }

        public CharSequence getColumnName() {
            return columnName;
        }

        public int getColumnType() {
            return columnType;
        }

        public int getColumnWriterIndex() {
            return columnWriterIndex;
        }

        public int getOperationType() {
            return operationType;
        }

        public ObjList<Function> getValueFunctions() {
            return valueFunctions;
        }

        public ObjList<ExpressionNode> getValues() {
            return values;
        }

        /**
         * Reports whether every value is a compile-time constant, i.e. whether an outcome computed
         * from the values holds for the life of the condition. A bind variable is a runtime
         * constant and answers false.
         */
        public boolean hasConstantValuesOnly() {
            for (int i = 0, n = valueFunctions.size(); i < n; i++) {
                if (!valueFunctions.getQuick(i).isConstant()) {
                    return false;
                }
            }
            return true;
        }

        public void init(SqlExecutionContext executionContext) throws SqlException {
            for (int i = 0, n = valueFunctions.size(); i < n; i++) {
                valueFunctions.getQuick(i).init(null, executionContext);
            }
        }

        public boolean isSerializationDeclined() {
            return isSerializationDeclined;
        }

        public void setSerializationDeclined() {
            isSerializationDeclined = true;
        }
    }
}
