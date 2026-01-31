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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface TableWriterAPI extends Closeable {

    default void addColumn(@NotNull CharSequence columnName, int columnType) {
        addColumn(columnName, columnType, null);
    }

    void addColumn(@NotNull CharSequence columnName, int columnType, @Nullable SecurityContext securityContext);

    /**
     * Adds new column to table, which can be either empty or can have data already. When existing columns
     * already have data this function will create ".top" file in addition to column files. ".top" file contains
     * size of partition at the moment of column creation. It must be used to accurately position inside new
     * column when either appending or reading.
     *
     * <b>Failures</b>
     * Adding new column can fail in many situations. None of the failures affect integrity of data that is already in
     * the table but can leave instance of TableWriter in inconsistent state. When this happens function will throw CairoError.
     * Calling code must close TableWriter instance and open another when problems are rectified. Those problems would be
     * either with disk or memory or both.
     * <p>
     * Whenever function throws CairoException application code can continue using TableWriter instance and may attempt to
     * add columns again.
     *
     * <b>Transactions</b>
     * <p>
     * Pending transaction will be committed before function attempts to add column. Even when function is unsuccessful it may
     * still have committed transaction.
     *
     * @param columnName              of column either ASCII or UTF8 encoded.
     * @param symbolCapacity          when column columnType is SYMBOL this parameter specifies approximate capacity for symbol map.
     *                                It should be equal to number of unique symbol values stored in the table and getting this
     *                                value badly wrong will cause performance degradation. Must be power of 2
     * @param symbolCacheFlag         when set to true, symbol values will be cached on Java heap.
     * @param columnType              {@link ColumnType}
     * @param indexType               column index type, see {@link IndexType}
     * @param indexValueBlockCapacity approximation of number of rows for single index key, must be power of 2
     * @param isSequential            unused, should be false
     */
    void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            byte indexType,
            int indexValueBlockCapacity,
            boolean isSequential
    );

    long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException;

    long apply(UpdateOperation operation);

    @Override
    void close();

    void commit();

    TableRecordMetadata getMetadata();

    /**
     * Returns table structure version. Implementations must be thread-safe.
     *
     * @return table structure version
     */
    long getMetadataVersion();

    /**
     * Returns safe watermark for the symbol count stored in the given column.
     * The purpose of the watermark is to let ILP I/O threads (SymbolCache) to
     * use symbol codes when serializing row data to be handled by the writer.
     * <p>
     * If the implementation doesn't require symbol count watermarks (e.g.
     * TableWriter), it should return <code>-1</code>.
     * </p>
     * Implementations must be thread-safe.
     *
     * @param columnIndex column index
     * @return watermark for the symbol count
     */
    int getSymbolCountWatermark(int columnIndex);

    @NotNull
    TableToken getTableToken();

    long getUncommittedRowCount();

    /**
     * Intermediate commit. It provides the best effort guarantee to commit as much data from the RSS to storage.
     * However, it also takes into account O3 data overlap from the previous intermediate commits and adjust
     * the internal "lag" to absorb merges while data is in RSS rather than disk.
     * <p>
     * When data is in order and O3 area is empty, ic() equals to commit().
     */
    void ic();

    void ic(long o3MaxLag);

    TableWriter.Row newRow();

    TableWriter.Row newRow(long timestamp);

    TableWriter.Row newRowDeferTimestamp();

    void rollback();

    /**
     * Declares type of behaviour of the implementing class.
     *
     * @return true when multiple writers of this type can be used simultaneously against the same table, false otherwise.
     */
    boolean supportsMultipleWriters();

    /**
     * Truncates non-WAL table. This method has to be called when the
     * {@link CairoEngine#lockReaders(TableToken)} lock is held, i.e. when there are no readers reading from the table.
     *
     * @throws UnsupportedOperationException when called a WAL table.
     */
    void truncate();

    /**
     * Truncates table, but keeps symbol tables, i.e. internal symbol string to symbol int code maps.
     * Sometimes the symbols should be kept to make sure that DETACH/ATTACH PARTITION does not lose data
     * for symbol columns. For non-WAL tables, this method has to be called when the
     * {@link CairoEngine#lockReaders(TableToken)} lock is held, i.e. when there are no readers reading from the table.
     */
    void truncateSoft();
}
