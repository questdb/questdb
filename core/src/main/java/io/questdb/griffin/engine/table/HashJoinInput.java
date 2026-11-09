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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * One input of a parallel hash join, in a form that serves either role: the scan that the join
 * streams, its probe, or the scan that one of its stages hashes, a build. The input is a page frame
 * scan, possibly a projection of one, which the join reads frame by frame rather than through its
 * record cursor.
 * <p>
 * The input owns the scan's factory and the filter context that the join reads its frames through:
 * the memory pools of the owner and of every worker, and the scan's filter with its worker copies,
 * or no filter for an unfiltered scan. A filtered input is a filter that the planner stole from the
 * query's own filter over a page frame scan, with its JIT code and bind variables: the join runs it
 * over each frame, in place of the filter's own cursor, and keeps the rows that pass it. Its column
 * map names, for each column of the input as the query compiled it, the column of the frames that
 * holds it, so that a projection above the filter maps onto the scan's columns. The scan order is
 * the order the factory scans in, which the join walks the frames and their rows in, so that the
 * rows it produces and the duplicates it chains come in the order the input's own cursor yields
 * them.
 */
public final class HashJoinInput implements QuietCloseable {
    private final IntList columnMap;
    // The types of the input's columns as compiled, in the order the column map lists them.
    private final ArrayColumnTypes columnTypes = new ArrayColumnTypes();
    private final RecordCursorFactory factory;
    private final AsyncFilterContext filterContext;
    // ORDER_ASC or ORDER_DESC.
    private final int order;
    private AsyncFilterContext ownedFilterContext;
    private RecordCursorFactory ownedFactory;

    /**
     * Takes ownership of the factory and the filter context.
     *
     * @param factory       the input's page frame scan
     * @param columnMap     for each column of the input as compiled, the frame column that holds it
     * @param filterContext the filter and memory pools the join reads the scan's frames through
     */
    public HashJoinInput(RecordCursorFactory factory, IntList columnMap, AsyncFilterContext filterContext) {
        assert factory.supportsPageFrameCursor();
        this.factory = factory;
        this.ownedFactory = factory;
        this.columnMap = columnMap;
        this.filterContext = filterContext;
        this.ownedFilterContext = filterContext;
        this.order = factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final RecordMetadata metadata = factory.getMetadata();
        for (int i = 0, n = columnMap.size(); i < n; i++) {
            columnTypes.add(metadata.getColumnType(columnMap.getQuick(i)));
        }
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeBestEffort(null, ownedFilterContext);
        ownedFilterContext = null;
        failure = Misc.freeBestEffort(failure, ownedFactory);
        ownedFactory = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * For each column of the input as the query compiled it, the frame column that holds it.
     */
    public IntList getColumnMap() {
        return columnMap;
    }

    /**
     * The types of the input's columns as the query compiled them, in the order of
     * {@link #getColumnMap()}.
     */
    public ColumnTypes getColumnTypes() {
        return columnTypes;
    }

    /**
     * The page frame scan the join reads, which holds the columns {@link #getColumnMap()} names.
     */
    public RecordCursorFactory getFactory() {
        return factory;
    }

    public AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    /**
     * The metadata of the page frame scan's columns, which the frames hold, rather than of the input
     * as the query compiled it; see {@link #getColumnTypes()}.
     */
    public RecordMetadata getMetadata() {
        return factory.getMetadata();
    }

    /**
     * The order the join opens the scan's frames in and walks their rows in: {@code ORDER_DESC} for
     * a scan that runs backward, {@code ORDER_ASC} otherwise.
     */
    public int getOrder() {
        return order;
    }

    /**
     * True when the filter context holds a filter that the planner stole from the input's own filter,
     * so that the join keeps only the rows of each frame that pass it. A filtered input's cursor
     * reports no size, so the serial join never swaps its inputs over it.
     */
    public boolean hasStolenFilter() {
        return filterContext.getFilter(-1) != null;
    }

    /**
     * True when the stolen filter runs as JIT code, on every frame that has no column top and no
     * Parquet column that the record converts per row; see {@link #hasStolenFilter()}.
     */
    public boolean isFilterCompiled() {
        return filterContext.getCompiledFilter() != null;
    }

    /**
     * True when the input's scan, or one of the inputs it reads, reads data from outside the
     * database, such as {@code read_parquet()}.
     */
    public boolean usesExternalDataSource() {
        return factory.usesExternalDataSource();
    }
}
