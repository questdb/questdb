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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * uniform(n) window function.
 * Boolean "keep this row?" flag that marks an evenly-spaced subset of {@code n} rows out of the
 * ordered partition, using the same evenly-spaced-index formula as SUBSAMPLE's uniform algorithm:
 * for target N over n rows (n &gt; N), divisor = N-1, range = n-1, half = divisor/2, and the i-th
 * (0-based) kept ordinal is {@code (i*range + half) / divisor}, de-duplicated. When n &lt;= N every
 * row is kept.
 */
public class UniformFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "uniform";
    // LONG signature so both INT literals (auto-widened) and LONG literals resolve; the value is
    // validated to fit a positive long target below.
    private static final String SIGNATURE = NAME + "(L)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "uniform() requires ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "uniform() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "uniform() does not support PARTITION BY");
        }

        Function targetArg = args.getQuick(0);
        if (!targetArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "target must be a constant");
        }
        long target = targetArg.getLong(null);
        if (target == Numbers.LONG_NULL || target < 1) {
            throw SqlException.$(argPositions.getQuick(0), "target must be a positive constant");
        }

        return new UniformFunction(target);
    }

    // uniform(n) over (order by xxx) - no partition by, no framing.
    static class UniformFunction extends BaseWindowFunction implements Reopenable {

        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
        private final long target;
        private long count;          // running row counter during pass1; becomes totalRows
        private boolean keepAll;
        private ObjList<ExpressionNode> orderBy;
        private long pass2Ordinal;   // running row counter during pass2 (same traversal order as pass1)
        private long selIdx;         // monotonic cursor into `selected` during pass2

        UniformFunction(long target) {
            super(null);
            this.target = target;
        }

        @Override
        public void close() {
            super.close();
            selected.close();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.BOOLEAN;
        }

        @Override
        public void initRecordComparator(
                SqlCodeGenerator sqlGenerator,
                RecordMetadata metadata,
                ArrayColumnTypes chainTypes,
                IntList orderIndices,
                ObjList<ExpressionNode> orderBy,
                IntList orderByDirection
        ) throws SqlException {
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            boolean keep;
            if (keepAll) {
                keep = true;
            } else {
                keep = selIdx < selected.size() && selected.get(selIdx) == pass2Ordinal;
                if (keep) {
                    selIdx++;
                }
            }
            pass2Ordinal++;
            // BOOLEAN is a 1-byte chain column (see ColumnType.TYPE_SIZE[BOOLEAN]); write a byte,
            // not a long, or we'd corrupt the next column's storage.
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), (byte) (keep ? 1 : 0));
        }

        @Override
        public void preparePass2() {
            long totalRows = count;
            selected.clear();
            selIdx = 0;
            pass2Ordinal = 0;
            if (totalRows <= target) {
                keepAll = true;
                return;
            }
            keepAll = false;
            if (target == 1) {
                // divisor (target - 1) would be 0 below; target=1 over >1 rows keeps a single,
                // roughly-middle row instead.
                selected.add((totalRows - 1) / 2);
                return;
            }
            long divisor = target - 1;
            long range = totalRows - 1;
            long half = divisor / 2;
            long prev = -1;
            for (long i = 0; i < target; i++) {
                long pos = (i * range + half) / divisor;
                if (pos != prev) { // positions are non-decreasing; dedup consecutive repeats
                    selected.add(pos);
                    prev = pos;
                }
            }
        }

        @Override
        public void reopen() {
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.reopen();
            selected.clear();
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(target).val(')');
            if (orderBy != null) {
                sink.val(" over (");
                sink.val("order by ");
                sink.val(orderBy);
                sink.val(')');
            } else {
                sink.val(" over ()");
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.clear();
        }
    }
}
