/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface Function extends Closeable, StatefulAtom, Plannable {

    /**
     * Initializes each function in the list of clones. It is assumed by this method that "clones" are copies of
     * the same function.
     * <p>
     * Two-phase functions might need to perform certain transformations upfront, before SQL executes. This is to
     * avoid doing those transformations on for every row/invocation. These transformations are done during the
     * "init" phase.
     * <p>
     * During concurrent SQL execution it might be beneficial to split the "init" phase into per-SQL execution and
     * per-thread. For example "init" could be a heavy SQL execution itself, which would benefit from executing once and
     * copying state of this execution to clones, so that clones to not have to repeat that heavy SQL execution. The
     * "prototype" function is the one that has already been fully initialized, and it is ready to pass its state to
     * all the clones.
     * <p>
     * Even though the prototype will be trying to pass its state, the clones do not have to accept it and choose to
     * continue to calculate own state.
     *
     * @param clones            uniform function to initialize and accept state from the prototype, if prototype is not null
     * @param symbolTableSource symbol table source to perform symbol value to key conversion
     * @param executionContext  the execution context, bind variables etc
     * @param prototypeFunction the prototype function, ready to donate its state
     * @throws SqlException function are allowed to throw SQLException to indicate initialization error
     */
    static void init(
            ObjList<? extends Function> clones,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext,
            @Nullable Function prototypeFunction
    ) throws SqlException {
        if (prototypeFunction != null) {
            for (int i = 0, n = clones.size(); i < n; i++) {
                prototypeFunction.offerStateTo(clones.getQuick(i));
            }
        }
        for (int i = 0, n = clones.size(); i < n; i++) {
            clones.getQuick(i).init(symbolTableSource, executionContext);
        }
    }

    static void initNc(
            ObjList<? extends Function> args,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext,
            @Nullable Function prototypeFunction
    ) throws SqlException {
        if (args != null) {
            init(args, symbolTableSource, executionContext, prototypeFunction);
        }
    }

    default void assignType(int type, BindVariableService bindVariableService) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() {
    }

    default void cursorClosed() {
    }

    default FunctionExtension extendedOps() {
        return null;
    }

    ArrayView getArray(Record rec);

    BinarySequence getBin(Record rec);

    long getBinLen(Record rec);

    boolean getBool(Record rec);

    byte getByte(Record rec);

    char getChar(Record rec);

    long getDate(Record rec);

    /**
     * Sets the raw value of sink (the scale is caller saved)
     */
    void getDecimal128(Record rec, Decimal128 sink);

    short getDecimal16(Record rec);

    /**
     * Sets the raw value of sink (the scale is caller saved)
     */
    void getDecimal256(Record rec, Decimal256 sink);

    int getDecimal32(Record rec);

    long getDecimal64(Record rec);

    byte getDecimal8(Record rec);

    double getDouble(Record rec);

    float getFloat(Record rec);

    byte getGeoByte(Record rec);

    int getGeoInt(Record rec);

    long getGeoLong(Record rec);

    short getGeoShort(Record rec);

    int getIPv4(Record rec);

    int getInt(Record rec);

    @NotNull
    Interval getInterval(Record rec);

    long getLong(Record rec);

    long getLong128Hi(Record rec);

    long getLong128Lo(Record rec);

    void getLong256(Record rec, CharSink<?> sink);

    Long256 getLong256A(Record rec);

    Long256 getLong256B(Record rec);

    default RecordMetadata getMetadata() {
        return null;
    }

    /**
     * @return function name or symbol, e.g. concat or + .
     * r=
     */
    default String getName() {
        return getClass().getName();
    }

    // when function returns factory it becomes factory
    // on other words this is not a tear-away instance
    RecordCursorFactory getRecordCursorFactory();

    short getShort(Record rec);

    CharSequence getStrA(Record rec);

    CharSequence getStrB(Record rec);

    int getStrLen(Record rec);

    CharSequence getSymbol(Record rec);

    CharSequence getSymbolB(Record rec);

    long getTimestamp(Record rec);

    int getType();

    @Nullable
    Utf8Sequence getVarcharA(Record rec);

    @Nullable
    Utf8Sequence getVarcharB(Record rec);

    /**
     * @return size of the varchar value or {@link TableUtils#NULL_LEN} in case of NULL
     */
    int getVarcharSize(Record rec);

    /**
     * Returns true if function is constant, i.e. its value does not require
     * any input from the record.
     *
     * @return true if function is constant
     * @see #isRuntimeConstant()
     */
    default boolean isConstant() {
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    default boolean isConstantOrRuntimeConstant() {
        return isConstant() || isRuntimeConstant();
    }

    default boolean isNonDeterministic() {
        return false;
    }

    default boolean isNullConstant() {
        return false;
    }

    // used in generic toSink implementations
    default boolean isOperator() {
        return false;
    }

    default boolean isRandom() {
        return false;
    }

    /**
     * Declares that the function will maintain its value for all the rows during
     * {@link RecordCursor} traversal. However, between cursor traversals the function
     * value is liable to change.
     * <p>
     * In practice this means that function arguments that are runtime constants can be
     * evaluated in the functions {@link #init(ObjList, SymbolTableSource, SqlExecutionContext, Function)} call.
     * <p>
     * It has be noted that the function cannot be both {@link #isConstant()} and runtime constant.
     *
     * @return true when function is runtime constant.
     */
    default boolean isRuntimeConstant() {
        return false;
    }

    /**
     * Returns true if the function and all of its children functions are thread-safe
     * and, thus, can be called concurrently, false - otherwise. Used as a hint for
     * parallel SQL execution, thus this method makes sense only for functions
     * that are allowed in WHERE or GROUP BY clause.
     * <p>
     * In case of non-aggregate functions this flag means read thread-safety.
     * For decomposable (think, parallel) aggregate functions
     * ({@link io.questdb.griffin.engine.functions.GroupByFunction})
     * it means write thread-safety, i.e. whether it's safe to use single function
     * instance concurrently to aggregate across multiple threads.
     * <p>
     * If the function is not read thread-safe, it gets cloned for each worker thread.
     *
     * @return true if the function and all of its children functions are thread-safe
     */
    default boolean isThreadSafe() {
        return false;
    }

    default boolean isUndefined() {
        return getType() == ColumnType.UNDEFINED;
    }

    /**
     * This method is called exactly once per data row. It provides an opportunity for the function
     * to perform heavy or volatile computations, cache the results and ensure getXXX() methods use the case instead
     * of recomputing values.
     *
     * @param record the record for data access.
     */
    default void memoize(Record record) {
    }

    default void offerStateTo(Function that) {
    }

    /**
     * For exactly once per-row execution functions can declare themselves memoizable by
     * returning true out of this method. Typically, this is all what's required. FunctionParser will
     * wrap memoizable functions into type-specific Memoizers. These memoizers will implement {@see memoize} method.
     * Caching heavy or potentially volatile computations for each data row.
     *
     * @return if function is memoizable
     */
    default boolean shouldMemoize() {
        return false;
    }

    /**
     * Returns true if the function supports parallel execution, e.g. parallel filter
     * or GROUP BY. If the method returns false, single-threaded execution plan
     * must be chosen for the query.
     * <p>
     * Examples of parallelizable, but thread-unsafe function are regexp_replace() or min(str).
     * These functions need to maintain a char sink, so they can't be accessed concurrently.
     * On the other hand, regexp_replace() can be used in parallel filter and min(str)
     * supports parallel aggregation and subsequent merge.
     *
     * @return true if the function and all of its children functions support parallel execution
     */
    default boolean supportsParallelism() {
        return true;
    }

    default boolean supportsRandomAccess() {
        return true;
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val("()");
    }

    default void toTop() {
        // no-op
    }
}
