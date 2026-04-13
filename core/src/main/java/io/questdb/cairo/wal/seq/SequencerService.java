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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.ObjHashSet;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * Pluggable sequencer service interface. Manages table registry and per-table
 * WAL transaction sequencing.
 * <p>
 * Implementations:
 * <ul>
 *   <li>{@link LocalSequencerService} — file-based, single-node (current behavior)</li>
 *   <li>RemoteSequencerService — RPC to central sequencer (future, multi-primary)</li>
 * </ul>
 */
public interface SequencerService extends QuietCloseable {

    // ---- Table Registry ----

    /**
     * Register a new WAL table with the sequencer service.
     * <p>
     * The caller provides its known database version. The service validates
     * that the caller is up-to-date before accepting the registration.
     *
     * @param tableId              unique table identifier
     * @param structure            table schema definition
     * @param tableToken           table token with name, directory, flags
     * @param callerDatabaseVersion the caller's known database version
     * @return {@code >= 0}: success, value is the new database version.
     *         {@code < 0}: refused, {@code |value|} is the service's current
     *         database version that the caller must sync to before retrying.
     */
    long registerTable(int tableId, TableStructure structure, TableToken tableToken, long callerDatabaseVersion);

    /**
     * Drop a table from the sequencer registry.
     *
     * @return database version after this operation
     */
    long dropTable(TableToken tableToken);

    /**
     * Current database version — a monotonically increasing counter that
     * increments on every table registry change (create/drop/rename).
     * Local nodes track this to detect when they are out of sync.
     */
    long getDatabaseVersion();

    /**
     * Iterate all WAL tables known to the sequencer.
     */
    void forAllWalTables(ObjHashSet<TableToken> bucket, boolean includeDropped, TableSequencerCallback callback);

    // ---- Per-Table Sequencing ----

    /**
     * Allocate a globally unique WAL ID for the given table.
     * In multi-primary mode, WAL IDs are unique across all nodes.
     */
    int getNextWalId(TableToken tableToken);

    /**
     * Register a data transaction. Returns the committed sequencer transaction
     * number, or {@code NO_TXN} if {@code expectedSchemaVersion} does not match
     * the current schema version (caller must refresh metadata and retry).
     */
    long nextTxn(
            TableToken tableToken,
            int walId,
            long expectedSchemaVersion,
            int segmentId,
            int segmentTxn,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    );

    /**
     * Register a schema change (ALTER TABLE) transaction. Returns the committed
     * sequencer transaction number, or {@code NO_TXN} if {@code structureVersion}
     * does not match (optimistic concurrency — caller must refresh and reconcile).
     */
    long nextStructureTxn(TableToken tableToken, long structureVersion, AlterOperation alterOp);

    /**
     * Get the last committed transaction number for the given table.
     */
    long lastTxn(TableToken tableToken);

    // ---- Metadata ----

    /**
     * Copy current table metadata (schema, columns, etc.) into the provided sink.
     *
     * @return current transaction number
     */
    long getTableMetadata(TableToken tableToken, TableRecordMetadataSink sink);

    /**
     * Get metadata changes since the given structure version.
     */
    @NotNull
    TableMetadataChangeLog getMetadataChangeLog(TableToken tableToken, long structureVersionLo);

    // ---- Transaction Log ----

    /**
     * Get a cursor to read the transaction log starting from the given sequencer
     * transaction number. In multi-primary mode, the transaction log lives on
     * the central sequencer and is cached locally.
     */
    @NotNull
    TransactionLogCursor getCursor(TableToken tableToken, long seqTxn);

    // ---- Local Table Setup ----

    /**
     * Initialize local sequencer files for a newly created table.
     * Called AFTER the main table files have been created on disk.
     * <p>
     * For local mode: creates the transaction log, metadata, and WAL index files.
     * For remote mode: creates local cache of the remote sequencer state.
     */
    void initSequencerFiles(int tableId, TableStructure structure, TableToken tableToken);

    // ---- Table Lifecycle ----

    /**
     * Reload table token (e.g., after rename). Returns the updated token,
     * or {@code null} if the table has been dropped.
     */
    TableToken reload(TableToken tableToken);

    /**
     * Apply a table rename to the sequencer's internal state.
     */
    void applyRename(TableToken tableToken);

    // ---- Resource Management ----

    /**
     * Release sequencer resources for tables that have been idle beyond the
     * configured TTL. Implementation-specific: local releases pooled file
     * handles; remote may release connection state.
     */
    boolean releaseInactive();

    /**
     * Release all sequencer resources.
     */
    boolean releaseAll();

    // ---- Push Notifications ----

    /**
     * Set a listener for push notifications from the sequencer service.
     * In multi-primary mode, the central sequencer pushes table creation,
     * drop, rename, and new transaction events to all connected nodes.
     */
    void setListener(SequencerServiceListener listener);

    // ---- Constants ----

    long NO_TXN = Long.MIN_VALUE;

    // ---- Callback ----

    @FunctionalInterface
    interface TableSequencerCallback {
        void onTable(int tableId, TableToken tableName, long lastTxn);
    }
}
