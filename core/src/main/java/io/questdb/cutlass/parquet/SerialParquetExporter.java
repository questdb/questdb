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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cutlass.text.SerialCsvFileImporter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

public class SerialParquetExporter implements Closeable {

    private static final Log LOG = LogFactory.getLog(SerialCsvFileImporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final CharSequence inputRoot;
    Path fromNative, toParquet;
    TableToken tableToken;
    TableReader tableReader;
    ExecutionCircuitBreaker circuitBreaker;
    CharSequence tableName;
    CharSequence fileName;
    long copyId;
    PhaseStatusReporter statusReporter;

    public SerialParquetExporter(CairoEngine engine) {
        this.cairoEngine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.inputRoot = this.configuration.getSqlCopyInputRoot();
    }

    public void of(@NotNull CharSequence tableName,
                   @NotNull CharSequence fileName,
                   long copyId,
                   ExecutionCircuitBreaker circuitBreaker,
                   PhaseStatusReporter statusReporter) {
        this.tableName = tableName;
        this.fileName = fileName;
        this.copyId = copyId;
        this.circuitBreaker = circuitBreaker;
        this.statusReporter = statusReporter;
    }


    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(byte phase, byte status, @Nullable final CharSequence msg, long errors);
    }


    @Override
    public void close() throws IOException {

    }

    public void process(SecurityContext securityContext) throws IOException {
        final int memoryTag = MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER;

        try (TableReader reader = cairoEngine.getReader(tableToken)) {
            final int partitionCount = reader.getPartitionCount();

            for (int i = 0; i < partitionCount; i++) {

            }

        }

        cairoEngine.getTableSequencerAPI()

        setPathForNativePartition(fromNative.)

        cairoEngine.
    }

}
