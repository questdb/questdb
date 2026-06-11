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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.Operation;

/**
 * Single source of truth for the per-statement read-only boundary gate the pg-wire
 * ({@code PGPipelineEntry}) and HTTP /exec ({@code JsonQueryProcessor}) paths apply when the
 * engine reports {@code isReadOnlyMode()}.
 * <p>
 * The gate flips to true as the FIRST step of an in-place PRIMARY-to-REPLICA switch cascade,
 * before the security-context factory swaps to the replica side. A write or DDL submitted on a
 * connection authorized while the node was still PRIMARY would otherwise be executed and land on a
 * node that is already demoting (its WAL uploader may have closed), losing the write once the node
 * settles as a replica. Re-checking the live engine state per statement closes that window.
 * <p>
 * The set must cover EVERY state-mutating statement type, because nothing behind the gate refuses
 * writes: {@code isReadOnlyMode()} is not enforced at the engine/writer level, and a connection
 * authorized while the node was PRIMARY keeps a read-write security context across the demote, so
 * steady-state read-only security contexts do not apply to it. A type omitted here falls through to
 * the protocol's default execute path and mutates a demoting node (write-loss).
 * <p>
 * Keeping this predicate in one place removes the lockstep-by-comment maintenance burden the two
 * verbatim {@code ||}-chains in the pg-wire and HTTP gates previously carried -- the drift that
 * burden invited is precisely what let the pg-wire COMMIT path slip the fence.
 * <p>
 * CREATE_USER/ALTER_USER are intentionally NOT listed: they are ACL ops gated by the enterprise ACL
 * permission layer, not the table-write class this gate covers.
 * <p>
 * One DROP shape is exempt: a DROP that targets the HTTP parquet exporter's temp table. A read-only
 * replica still runs parquet export (it materializes a temp table behind a SELECT), and when its own
 * cleanup fails the admin drops the leftover temp table. That drop is a purely local operation the
 * replica security context already permits, so the eager gate must let it pass and rely on the
 * downstream {@code authorizeTableDrop} check, which still refuses every genuine client DROP.
 */
public final class ReadOnlyStatementGate {

    private ReadOnlyStatementGate() {
    }

    /**
     * Returns true when a statement of the given compiled-query {@code sqlType} must be refused on a
     * read-only (demoting/replica) node. The caller is expected to gate this behind a cheap
     * {@code engine.isReadOnlyMode()} read; this predicate answers only the type-classification half
     * of the gate so the pg-wire and HTTP callers cannot drift apart.
     *
     * @param sqlType       one of the {@link CompiledQuery} type constants
     * @param operation     the compiled native operation, or null; only inspected for the
     *                      export-temp-table DROP exemption
     * @param configuration provides the parquet-export temp-table name prefix used by the DROP
     *                      exemption
     * @return true if the statement type is a write/DDL refused on a read-only node (and is not the
     * exempt parquet-export temp-table DROP)
     */
    public static boolean isRefusedOnReadOnly(int sqlType, Operation operation, CairoConfiguration configuration) {
        return sqlType == CompiledQuery.INSERT
                || sqlType == CompiledQuery.INSERT_AS_SELECT
                || sqlType == CompiledQuery.UPDATE
                || sqlType == CompiledQuery.ALTER
                || sqlType == CompiledQuery.TRUNCATE
                || sqlType == CompiledQuery.RENAME_TABLE
                || sqlType == CompiledQuery.CREATE_TABLE
                || sqlType == CompiledQuery.CREATE_TABLE_AS_SELECT
                || sqlType == CompiledQuery.CREATE_MAT_VIEW
                || sqlType == CompiledQuery.REFRESH_MAT_VIEW
                || sqlType == CompiledQuery.CREATE_VIEW
                || sqlType == CompiledQuery.ALTER_VIEW
                || sqlType == CompiledQuery.ALTER_STORAGE_POLICY
                || (sqlType == CompiledQuery.DROP && !isExportTempTableDrop(operation, configuration));
    }

    private static boolean isExportTempTableDrop(Operation operation, CairoConfiguration configuration) {
        return operation instanceof GenericDropOperation
                && ((GenericDropOperation) operation).isExportTempTableDrop(configuration.getParquetExportTableNamePrefix());
    }
}
