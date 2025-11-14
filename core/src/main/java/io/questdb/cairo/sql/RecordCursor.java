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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.std.DirectLongLongSortedList;

import java.io.Closeable;

/**
 * A cursor interface for sequential iteration over database records in QuestDB.
 * <p>
 * RecordCursor provides a standardized way to iterate through query results, supporting
 * various access patterns including forward iteration, random access, and optimized
 * operations like size calculation and top-K queries. It extends Closeable for proper
 * resource management and SymbolTableSource for symbol column access.
 * <p>
 * Key features:
 * <ul>
 * <li>Sequential record iteration with {@link #hasNext()} and {@link #getRecord()}</li>
 * <li>Dual record access with {@link #getRecord()} and {@link #getRecordB()} for comparisons</li>
 * <li>Random access positioning with {@link #recordAt(Record, long)}</li>
 * <li>Efficient row skipping with {@link #skipRows(Counter)}</li>
 * <li>Size calculation with circuit breaker support</li>
 * <li>Symbol table access for symbol columns</li>
 * <li>Optimized top-K operations for ORDER BY + LIMIT queries</li>
 * <li>Cursor reset functionality with {@link #toTop()}</li>
 * </ul>
 * <p>
 * <strong>Resource Management:</strong><br>
 * This interface extends {@link Closeable} and is not optionally-closeable.
 * The {@link #close()} method must be called after all operations are complete
 * to ensure proper resource cleanup.
 * <p>
 * <strong>Thread Safety:</strong><br>
 * RecordCursor implementations are generally not thread-safe. For concurrent
 * access, use {@link #newSymbolTable(int)} to create thread-local symbol table instances.
 * <p>
 * <strong>Exception Handling:</strong><br>
 * Many operations may throw {@link DataUnavailableException} when accessing
 * partitions stored in cold storage or when data is temporarily unavailable.
 */
public interface RecordCursor extends RecordRandomAccess, Closeable, SymbolTableSource {

    /**
     * Utility method to convert a boolean value to its long representation.
     * <p>
     * This is commonly used in QuestDB's internal operations where boolean
     * values need to be represented as numeric values for storage or comparison.
     *
     * @param b the boolean value to convert
     * @return 1L if the boolean is true, 0L if false
     */
    static long fromBool(boolean b) {
        return b ? 1L : 0L;
    }

    /**
     * Static utility method to skip a specified number of rows in a cursor.
     * <p>
     * This method advances the cursor position by the number of rows specified
     * in the counter, decrementing the counter as rows are skipped. The operation
     * stops when either the desired number of rows have been skipped or the
     * cursor reaches the end.
     *
     * @param cursor   the record cursor to skip rows in
     * @param rowCount a counter indicating how many rows to skip; this value is
     *                 decremented as rows are actually skipped
     * @throws DataUnavailableException if data is temporarily unavailable during the skip operation
     * @see #skipRows(Counter)
     */
    static void skipRows(RecordCursor cursor, Counter rowCount) throws DataUnavailableException {
        while (rowCount.get() > 0 && cursor.hasNext()) {
            rowCount.dec();
        }
    }

    /**
     * Counts the remaining number of records in this cursor, moving the cursor to the end.
     * <p>
     * This method iterates through all remaining records in the cursor and updates the
     * provided counter with the total count. The cursor position will be at the end
     * after this operation completes.
     * <p>
     * Note: This method should return a correct result even if interrupted by
     * {@link DataUnavailableException}. The number of rows counted so far is kept
     * in the counter parameter, allowing for resumption if needed.
     *
     * @param circuitBreaker circuit breaker to check for timeouts or stale connections;
     *                       may be null to disable circuit breaking
     * @param counter        counter object to store the partial or complete result
     * @throws DataUnavailableException if data is temporarily unavailable during counting
     * @see #size()
     */
    default void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (circuitBreaker != null) {
            while (hasNext()) {
                counter.inc();
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
        } else {
            while (hasNext()) {
                counter.inc();
            }
        }
    }

    /**
     * Closes the record cursor and releases any associated resources.
     * <p>
     * This method must be called after all other operations are complete to ensure
     * proper cleanup of resources such as file handles, memory buffers, and network
     * connections. Failure to close the cursor may result in resource leaks.
     * <p>
     * After calling this method, the cursor should not be used for any further operations.
     *
     * @throws RuntimeException if an error occurs during resource cleanup
     */
    @Override
    void close();

    /**
     * Returns the record at the current cursor position.
     * <p>
     * This method provides access to the current record's data. The returned Record
     * instance may be reused across multiple calls for performance reasons, so callers
     * should not retain references to it across cursor movements.
     * <p>
     * The cursor must be positioned on a valid record (i.e., {@link #hasNext()} should
     * have returned true and been followed by cursor advancement) before calling this method.
     *
     * @return the record at the current cursor position
     * @throws IllegalStateException if the cursor is not positioned on a valid record
     * @see #getRecordB()
     */
    Record getRecord();

    /**
     * Returns a second record instance at the current cursor position.
     * <p>
     * This method provides access to an alternative Record instance for the same
     * cursor position. It is primarily used for record comparisons, joins, and
     * other operations that require two different Record objects pointing to the
     * same data.
     * <p>
     * Like {@link #getRecord()}, the returned instance may be reused across calls
     * and should not be retained across cursor movements.
     *
     * @return a second record instance at the current cursor position
     * @throws IllegalStateException if the cursor is not positioned on a valid record
     * @see #getRecord()
     */
    Record getRecordB();

    /**
     * Returns a cached symbol table instance for the specified column.
     * <p>
     * This method provides access to symbol tables for symbol columns, enabling
     * efficient symbol-to-string and string-to-symbol conversions. The method
     * guarantees that the same symbol table instance is reused across multiple
     * invocations for performance optimization.
     * <p>
     * For non-symbol columns, this method returns null. The default implementation
     * throws UnsupportedOperationException, so cursor implementations should override
     * this method if they support symbol tables.
     *
     * @param columnIndex the zero-based numeric index of the column
     * @return the symbol table instance for the column, or null if the column is not a symbol column
     * @throws UnsupportedOperationException if symbol tables are not supported by this cursor
     * @throws IllegalArgumentException      if the column index is invalid
     * @see #newSymbolTable(int)
     */
    default SymbolTable getSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Checks if there are more records available in the cursor and advances to the next record if available.
     * <p>
     * This method combines the functionality of checking for availability and advancing
     * the cursor position. It returns true if a next record exists and the cursor has
     * been positioned on it, false if no more records are available.
     * <p>
     * After this method returns true, {@link #getRecord()} and {@link #getRecordB()}
     * can be called to access the current record's data.
     *
     * @return true if more records are available and the cursor has advanced, false otherwise
     * @throws DataUnavailableException when the queried partition is in cold storage or temporarily unavailable
     */
    boolean hasNext() throws DataUnavailableException;

    /**
     * Indicates whether this cursor is using an index for data access.
     * <p>
     * Index usage typically provides faster access patterns and more efficient
     * query execution, especially for filtered queries and range scans. This
     * information can be useful for query optimization and performance analysis.
     *
     * @return true if the cursor is using an index, false if it's performing a full table scan
     */
    default boolean isUsingIndex() {
        return false;
    }

    /**
     * Executes an optimized top-K operation for ORDER BY + LIMIT queries.
     * <p>
     * This method provides an efficient implementation for queries that need to find
     * the top K records based on a specific column's values. It uses a sorted list-based
     * approach to avoid sorting the entire result set when only the top K records
     * are needed.
     * <p>
     * The method is only supported by certain cursor implementations. Check
     * {@link RecordCursorFactory#recordCursorSupportsLongTopK(int)} before calling.
     *
     * @param list        a min or max sorted list (DirectLongLongSortedList) to store the top K records
     * @param columnIndex the zero-based index of the column to order by
     * @throws UnsupportedOperationException if the cursor does not support top-K optimization
     * @see RecordCursorFactory#recordCursorSupportsLongTopK(int)
     */
    default void longTopK(DirectLongLongSortedList list, int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a new instance of a symbol table for the specified column.
     * <p>
     * This method creates a clone or new instance of the symbol table typically
     * returned by {@link #getSymbolTable(int)}. Symbol table clones are essential
     * for concurrent SQL execution, where each thread requires its own symbol table
     * instance to avoid race conditions.
     * <p>
     * For immutable or empty symbol tables, this method may return the same instance
     * for performance optimization.
     *
     * @param columnIndex the zero-based numeric index of the column
     * @return a new symbol table instance, or the same instance if the table is immutable
     * @throws UnsupportedOperationException if symbol tables are not supported by this cursor
     * @throws IllegalArgumentException      if the column index is invalid
     * @see #getSymbolTable(int)
     */
    default SymbolTable newSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Calculates a numeric representation of the cursor's pre-computed internal state,
     * primarily for performance assertions.
     * <p>
     * The {@link #toTop()} method is designed to be a lightweight operation that resets the
     * cursor to its starting position. It should not discard the cursor's internal state
     * (e.g., cached data structures) which would cause expensive re-computation on the
     * next iteration.
     * <p>
     * This method provides a way to verify that behavior. The expected usage, typically
     * within an {@code assert} statement, is to compare the state size before and after
     * calling {@link #toTop()}. A correct implementation will not change its state,
     * so the values should be identical.
     *
     * <p><b>Usage Example (in testing):</b></p>
     * <pre>{@code
     * doSomeWorkWith(cursor);
     * long stateBefore = cursor.preComputedStateSize();
     * cursor.toTop();
     * long stateAfter = cursor.preComputedStateSize();
     *
     * // Assert that resetting the cursor did not discard its state
     * Assert.assertEquals("Cursor precomputed state should not change on toTop()", stateBefore, stateAfter);
     * }</pre>
     *
     * @return A long value representing the cursor's pre-computed state. This is not
     * a memory size in bytes, but a stable value used to detect state changes.
     */
    long preComputedStateSize();

    /**
     * Returns the total number of records available in this cursor.
     * <p>
     * Not all record cursor implementations can determine their size efficiently.
     * When the size cannot be determined or is not available, this method returns -1.
     * In such cases, the caller should continue iterating using {@link #hasNext()}
     * until no more records are available.
     * <p>
     * For cursors that do support size calculation, this method provides the total
     * count without consuming the cursor's position.
     *
     * @return the total number of records available, or -1 if the size cannot be determined
     * @throws DataUnavailableException when the queried partition is in cold storage or temporarily unavailable
     * @see #calculateSize(SqlExecutionCircuitBreaker, Counter)
     */
    long size() throws DataUnavailableException;

    /**
     * Attempts to efficiently skip the specified number of rows from the current cursor position.
     * <p>
     * This method tries to position the cursor at a location that is the specified number
     * of rows ahead of the current position. Rows are counted from the top of the table.
     * The method updates the rowCount parameter by subtracting the number of rows that
     * were actually skipped.
     * <p>
     * This optimization is supported by some record cursors that provide random access
     * capabilities, such as tables ordered by a designated timestamp. For cursors that
     * don't support efficient skipping, this method falls back to iterative advancement.
     *
     * @param rowCount a counter containing the number of rows to skip; this value is
     *                 decremented by the number of rows actually skipped
     * @throws DataUnavailableException when the queried partition is in cold storage or temporarily unavailable
     * @see #skipRows(RecordCursor, Counter)
     */
    default void skipRows(Counter rowCount) throws DataUnavailableException {
        while (rowCount.get() > 0 && hasNext()) {
            rowCount.dec();
        }
    }

    /**
     * Resets the cursor to its initial position for re-iteration over the same result set.
     * <p>
     * This method returns the cursor to its starting position without re-executing the
     * underlying query or triggering expensive recomputations. It is designed for
     * scenarios where multiple passes over the same data are needed, such as in
     * multi-phase algorithms or when computing aggregations.
     * <p>
     * The operation should be lightweight and preserve any internal optimizations
     * or cached state. It should not reload data from storage or invalidate
     * pre-computed structures.
     * <p>
     * After calling this method, {@link #hasNext()} can be used to start iterating
     * from the beginning again.
     *
     * @see #preComputedStateSize()
     */
    void toTop();

    /**
     * A simple counter utility class for tracking numeric values in RecordCursor operations.
     * <p>
     * This class provides a mutable long value with basic arithmetic operations.
     * It is commonly used for counting records, tracking skip counts, and maintaining
     * state during cursor operations like size calculation and row skipping.
     * <p>
     * The counter is not thread-safe and should be used within a single thread context.
     * For concurrent operations, each thread should maintain its own Counter instance.
     */
    class Counter {
        private long value;

        /**
         * Adds the specified value to the current counter value.
         *
         * @param val the value to add to the counter
         */
        public void add(long val) {
            value += val;
        }

        /**
         * Resets the counter value to zero.
         */
        public void clear() {
            value = 0;
        }

        /**
         * Decrements the counter value by 1.
         */
        public void dec() {
            value--;
        }

        /**
         * Decrements the counter value by the specified amount.
         *
         * @param val the value to subtract from the counter
         */
        public void dec(long val) {
            value -= val;
        }

        /**
         * Returns the current counter value.
         *
         * @return the current value of the counter
         */
        public long get() {
            return value;
        }

        /**
         * Increments the counter value by 1.
         */
        public void inc() {
            value++;
        }

        /**
         * Sets the counter to the specified value.
         *
         * @param val the new value for the counter
         */
        public void set(long val) {
            value = val;
        }
    }
}
