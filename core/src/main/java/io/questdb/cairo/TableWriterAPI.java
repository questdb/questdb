/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import java.io.Closeable;

public interface TableWriterAPI extends Closeable {
    long apply(AlterOperation operation, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException;

    long apply(UpdateOperation operation) ;

    @Override
    void close();

    long commit();

    long commitWithLag();

    long commitWithLag(long commitLag);

    TableRecordMetadata getMetadata();

    /**
     * Returns table structure version.
     * <p>
     * Implementations must be thread-safe.
     */
    long getStructureVersion();

    String getSystemTableName();

    CharSequence getTableName();

    long getUncommittedRowCount();

    TableWriter.Row newRow();

    TableWriter.Row newRow(long timestamp);

    void rollback();

    /**
     * Returns safe watermark for the symbol count stored in the given column.
     * The purpose of the watermark is to let ILP I/O threads (SymbolCache) to
     * use symbol codes when serializing row data to be handled by the writer.
     * <p>
     * If the implementation doesn't require symbol count watermarks (e.g.
     * TableWriter), it should return <code>-1</code>.
     * <p>
     * Implementations must be thread-safe.
     */
    int getSymbolCountWatermark(int columnIndex);

    void truncate();
}
