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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public interface WindowFunction extends Function {
    int ONE_PASS = 1;
    int TWO_PASS = 2;
    int ZERO_PASS = 0;

    default void computeNext(Record record) {
    }

    /**
     * Drops partition-by accumulator state whose last-seen row timestamp falls
     * below {@code cutoffTs}. Default no-op; partitioned window functions that
     * maintain per-key state override this to shed keys that retention has made
     * unreachable (their last row has fallen outside the retained window and no
     * warm-path replay can reach it).
     * <p>
     * Called from the live view refresh path after {@code applyRetention} has
     * advanced the retention cutoff. Non-partitioned window functions keep the
     * no-op default.
     */
    default void evictStalePartitionState(long cutoffTs) {
    }

    @Override
    default ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default double getDouble(Record rec) {
        // unused
        throw new UnsupportedOperationException();
    }

    @Override
    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the maximum number of microseconds the function needs to look back from
     * the current row. Used by live view cold-path classification: a late row arriving
     * with {@code ts < oldest_visible_ts - max(lookback)} across all functions cannot
     * affect any visible output and may be skipped.
     * <p>
     * Returns {@code -1} (the default) when the lookback is not expressible as a
     * timestamp delta, i.e.:
     * <ul>
     *     <li>UNBOUNDED PRECEDING frame (accumulator depends on all prior rows),</li>
     *     <li>ROWS N PRECEDING frame (lookback is row-count, not time-based),</li>
     *     <li>ranking/numbering functions whose implicit frame spans the whole
     *         partition up to the current row.</li>
     * </ul>
     * RANGE-bounded frames override this to return {@code abs(rowsLo)} micros.
     * The default is conservative — unknown = must not skip.
     */
    default long getMaxLookbackMicros() {
        return -1;
    }

    /**
     * @return pass1 scan direction.
     * Some {@link #ONE_PASS} and {@link #TWO_PASS} window functions may be more efficient when using a backward scan.
     */
    default Pass1ScanDirection getPass1ScanDirection() {
        return Pass1ScanDirection.FORWARD;
    }

    /**
     * Returns a pass-count-oriented optimization hint for window execution.
     * <p>
     * This value is also used by the planner as a streaming fast-path hint when the input cursor
     * already satisfies the window order. In that case, {@link #ZERO_PASS} functions are evaluated
     * row-by-row through {@link #computeNext(Record)}.
     * <p>
     * {@link #ZERO_PASS} is the strongest optimization hint, not a promise that cached execution
     * will skip this function. If the query is routed through the cached executor, every window
     * function, including {@link #ZERO_PASS}, must still implement
     * {@link #pass1(Record, long, WindowSPI)}. For a {@link #ZERO_PASS} function, {@code pass1()}
     * normally performs the cached equivalent of {@code computeNext(record)} and materializes the
     * current result into the output slot identified by {@link #setColumnIndex(int)}.
     *
     * @return cached execution pass count: {@link #ZERO_PASS}, {@link #ONE_PASS}, or {@link #TWO_PASS}
     */
    default int getPassCount() {
        return ONE_PASS;
    }

    @Override
    default RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    default void initRecordComparator(
            SqlCodeGenerator sqlGenerator,
            RecordMetadata metadata,
            ArrayColumnTypes chainTypes,
            IntList orderIndices,
            ObjList<ExpressionNode> orderBy,
            IntList orderByDirections
    ) throws SqlException {
    }

    default boolean isIgnoreNulls() {
        return false;
    }

    /**
     * Performs the primary cached traversal for this function.
     * <p>
     * The cached executor calls this method for every window function, including functions whose
     * {@link #getPassCount()} returns {@link #ZERO_PASS}. Implementations must therefore not rely
     * on {@link #ZERO_PASS} to avoid cached execution. One-pass and zero-pass functions should
     * materialize their final result for {@code recordOffset}; two-pass functions may instead
     * build state or store scratch values for {@link #pass2(Record, long, WindowSPI)}.
     */
    void pass1(Record record, long recordOffset, WindowSPI spi);

    /**
     * Performs the optional secondary cached traversal. The cached executor calls this only when
     * {@link #getPassCount()} is greater than {@link #ONE_PASS}.
     */
    default void pass2(Record record, long recordOffset, WindowSPI spi) {
    }

    /**
     * Prepares state before the optional secondary cached traversal.
     */
    default void preparePass2() {
    }

    /**
     * Releases native memory and resets internal state to default/initial.
     * It differs from close() in that it doesn't release memory held by metadata, e.g. partition by key functions.
     * This means function may still be used after calling reopen().
     **/
    void reset();

    /*
      Set index of record chain column used to store window function result.
     */
    void setColumnIndex(int columnIndex);

    enum Pass1ScanDirection {
        FORWARD, BACKWARD
    }
}
