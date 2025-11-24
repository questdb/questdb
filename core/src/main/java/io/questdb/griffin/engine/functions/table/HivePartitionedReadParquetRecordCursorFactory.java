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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for reading multiple parquet files matching a glob pattern.
 * Iterates through files and returns their rows sequentially.
 */
public class HivePartitionedReadParquetRecordCursorFactory extends AbstractRecordCursorFactory {
    public final RecordCursorFactory globCursorFactory;
    public final CharSequence globbedRoot;
    public final CharSequence nonGlobbedRoot;
    private final CairoConfiguration configuration;

    public HivePartitionedReadParquetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory globCursorFactory,
            @NotNull CharSequence nonGlobbedRoot,
            @NotNull CharSequence globbedRoot,
            RecordMetadata metadata
    ) {
        super(metadata);
        this.configuration = configuration;
        this.globCursorFactory = globCursorFactory;
        this.nonGlobbedRoot = nonGlobbedRoot.toString();
        this.globbedRoot = globbedRoot.toString();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Get the cursor from the glob cursor factory
        RecordCursor globCursor = globCursorFactory.getCursor(executionContext);
        try {
            // Create a single-file parquet reader that we'll reuse for each file
            ReadParquetRecordCursor parquetCursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
                    getMetadata()
            );

            // Create the hive partitioned cursor that wraps both
            return new HivePartitionedReadParquetRecordCursor(
                    globCursor,
                    parquetCursor,
                    configuration.getFilesFacade(),
                    nonGlobbedRoot
            );
        } catch (Throwable e) {
            // If anything goes wrong after globCursor is created,
            // ensure it's closed to avoid leaking the directory iterator FD
            globCursor.close();
            throw e;
        }
    }

    public RecordCursorFactory getGlobCursorFactory() {
        return globCursorFactory;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Parquet Scan")
                .attr("glob").val(globbedRoot);
    }

    @Override
    protected void _close() {
        // globCursorFactory is managed by SqlOptimiser and may be shared with other factories
        // Do not close it here to avoid use-after-free
    }
}
