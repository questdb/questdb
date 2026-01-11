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

package io.questdb.cutlass.line.http;

import io.questdb.cutlass.line.tcp.v4.IlpV4TableBuffer;
import io.questdb.std.ObjList;

import java.util.Map;

/**
 * Represents a pending flush operation containing buffered data to be sent.
 * <p>
 * This class is used in async flush mode to hold a snapshot of the buffer state
 * that will be sent to the server by a background thread. The buffers are
 * transferred (not copied) to this object, and the caller receives fresh empty
 * buffers for continued writing.
 * <p>
 * Thread safety: This object is created by the user thread and consumed by a
 * background flush thread. Once created, it should not be modified.
 */
public final class PendingFlush {

    private final Map<String, IlpV4TableBuffer> tableBuffers;
    private final ObjList<String> tableOrder;
    private final int pendingRows;
    private final int initialAddressIndex;
    private final boolean gorillaEnabled;
    private final boolean useSchemaRef;

    /**
     * Creates a new PendingFlush with the given buffer state.
     *
     * @param tableBuffers        map of table name to buffer (ownership transferred)
     * @param tableOrder          order of tables for encoding (ownership transferred)
     * @param pendingRows         total number of rows across all tables
     * @param initialAddressIndex the address index to use for this flush (for multi-host support)
     * @param gorillaEnabled      whether Gorilla timestamp compression is enabled
     * @param useSchemaRef        whether to use schema references
     */
    public PendingFlush(
            Map<String, IlpV4TableBuffer> tableBuffers,
            ObjList<String> tableOrder,
            int pendingRows,
            int initialAddressIndex,
            boolean gorillaEnabled,
            boolean useSchemaRef
    ) {
        this.tableBuffers = tableBuffers;
        this.tableOrder = tableOrder;
        this.pendingRows = pendingRows;
        this.initialAddressIndex = initialAddressIndex;
        this.gorillaEnabled = gorillaEnabled;
        this.useSchemaRef = useSchemaRef;
    }

    /**
     * Returns the table buffers map.
     */
    public Map<String, IlpV4TableBuffer> getTableBuffers() {
        return tableBuffers;
    }

    /**
     * Returns the table order list.
     */
    public ObjList<String> getTableOrder() {
        return tableOrder;
    }

    /**
     * Returns the total number of pending rows.
     */
    public int getPendingRows() {
        return pendingRows;
    }

    /**
     * Returns the initial address index for multi-host support.
     * This is the starting address index to use when sending this flush.
     */
    public int getInitialAddressIndex() {
        return initialAddressIndex;
    }

    /**
     * Returns whether Gorilla timestamp compression is enabled for this flush.
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Returns whether schema references should be used for this flush.
     */
    public boolean isUseSchemaRef() {
        return useSchemaRef;
    }

    /**
     * Returns the number of tables in this flush.
     */
    public int getTableCount() {
        return tableOrder.size();
    }

    /**
     * Returns the buffer for the given table name.
     *
     * @param tableName the table name
     * @return the buffer, or null if not found
     */
    public IlpV4TableBuffer getBuffer(String tableName) {
        return tableBuffers.get(tableName);
    }

    /**
     * Returns true if this flush has data to send.
     */
    public boolean hasData() {
        return pendingRows > 0 && tableOrder.size() > 0;
    }

    /**
     * Resets all buffers for potential reuse.
     * This should be called after the flush completes successfully.
     */
    public void resetBuffers() {
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            String tableName = tableOrder.get(i);
            IlpV4TableBuffer buffer = tableBuffers.get(tableName);
            if (buffer != null) {
                buffer.reset();
            }
        }
    }

    /**
     * Clears all buffers completely (including column definitions).
     * This should be called when discarding the buffers.
     */
    public void clearBuffers() {
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            String tableName = tableOrder.get(i);
            IlpV4TableBuffer buffer = tableBuffers.get(tableName);
            if (buffer != null) {
                buffer.clear();
            }
        }
        tableBuffers.clear();
        tableOrder.clear();
    }

    @Override
    public String toString() {
        return "PendingFlush{" +
                "tables=" + tableOrder.size() +
                ", rows=" + pendingRows +
                '}';
    }
}
