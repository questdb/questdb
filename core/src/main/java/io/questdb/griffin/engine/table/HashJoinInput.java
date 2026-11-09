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

import io.questdb.cairo.CairoException;
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
 * or no filter for an unfiltered scan. Its column map names, for each column of the input as the
 * query compiled it, the column of the frames that holds it. The scan order is the order the
 * factory scans in, which the join walks the frames and their rows in, so that the rows it produces
 * and the duplicates it chains come in the order the input's own cursor yields them.
 */
public final class HashJoinInput implements QuietCloseable {
    private final IntList columnMap;
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

    public RecordCursorFactory getFactory() {
        return factory;
    }

    public AsyncFilterContext getFilterContext() {
        return filterContext;
    }

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
     * True when the input's scan, or one of the inputs it reads, reads data from outside the
     * database, such as {@code read_parquet()}.
     */
    public boolean usesExternalDataSource() {
        return factory.usesExternalDataSource();
    }
}
