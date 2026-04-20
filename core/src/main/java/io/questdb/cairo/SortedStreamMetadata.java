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

/**
 * Minimal schema view the {@link SortedRowSink} contract exposes on
 * {@link SortedRowSink#onStart(SortedStreamMetadata, int, long)}. Covers
 * exactly the three accessors every shipping sink implementation needs:
 * column count, per-column name, and per-column QuestDB type id.
 * <p>
 * Both upstream pipelines provide an instance of this interface:
 * <ul>
 *   <li>The parquet-run phaseB passes a {@link
 *       io.questdb.griffin.engine.table.parquet.PartitionDecoder.Metadata},
 *       which implements this interface verbatim.</li>
 *   <li>The intermediate-table phaseB passes a wrapper over the target
 *       {@link io.questdb.cairo.sql.TableMetadata} (with the synthetic and
 *       sort-timestamp columns projected out) so the sink sees the same
 *       schema it would have seen under the parquet-run pipeline.</li>
 * </ul>
 * Instances are transient — do not retain them past the {@link
 * SortedRowSink#onStart} call that produced them.
 */
public interface SortedStreamMetadata {

    int getColumnCount();

    CharSequence getColumnName(int columnIndex);

    int getColumnType(int columnIndex);
}
