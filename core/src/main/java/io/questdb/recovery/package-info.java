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

/**
 * Offline recovery CLI for inspecting QuestDB database files when the database
 * may be corrupted or unreadable by the normal engine. This module reads binary
 * storage files directly (bypassing the engine code paths that assume data
 * integrity) with configurable bounds on record counts to avoid crashes on
 * corrupt data.
 *
 * <h2>Architecture</h2>
 * <pre>
 *   RecoveryMain           CLI entry point, lock acquisition
 *     RecoverySession      REPL loop, command dispatch
 *       NavigationContext   Mutable hierarchy: root / table / partition / column
 *       CommandContext      Immutable service bundle for commands
 *       RecoveryCommand     Functional interface for commands
 *
 *   Readers (read binary files into state objects):
 *     AbstractBoundedReader   Shared read primitives with issue accumulation
 *     BoundedTxnReader        _txn  -> TxnState
 *     BoundedMetaReader       _meta -> MetaState
 *     BoundedColumnVersionReader  _cv  -> ColumnVersionState
 *     BoundedRegistryReader   tables.d.N -> RegistryState
 *
 *   Services:
 *     TableDiscoveryService   Filesystem scan + registry cross-reference
 *     PartitionScanService    Directory/txn partition matching
 *     ColumnCheckService      Column file validation
 *     ColumnValueReader       Single-value reading for all column types
 *     ConsoleRenderer         Formatted console output with optional ANSI color
 * </pre>
 *
 * <h2>Key design decisions</h2>
 * <ul>
 *   <li><b>Bounded reads</b> - every reader caps record counts to prevent OOM
 *       on corrupt files (e.g. max 100k partitions, max 10k columns).</li>
 *   <li><b>Issue accumulation</b> - readers collect {@link io.questdb.recovery.ReadIssue}
 *       objects instead of throwing, allowing partial results from damaged files.</li>
 *   <li><b>No engine dependencies</b> - reads raw file bytes via {@link io.questdb.std.FilesFacade}
 *       rather than using Cairo table readers, so it works on files that would
 *       crash the normal engine.</li>
 * </ul>
 */
package io.questdb.recovery;
