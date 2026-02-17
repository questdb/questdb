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

package io.questdb.griffin.engine.functions.table;

import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

public class ReadParquetFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(ReadParquetFunctionFactory.class);

    @Override
    public String getSignature() {
        return "read_parquet(s)";
    }

    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPos,
            CairoConfiguration configuration,
            SqlExecutionContext context
    ) throws SqlException {
        final CharSequence filePath;
        try {
            filePath = args.getQuick(0).getStrA(null);
        } catch (CairoException e) {
            throw SqlException.$(argPos.getQuick(0), e.getFlyweightMessage());
        }

        try {
            final Path path = Path.getThreadLocal2("");
            checkPathIsSafeToRead(path, filePath, argPos.getQuick(0), configuration);
            final FilesFacade ff = configuration.getFilesFacade();
            final long fd = TableUtils.openRO(ff, path.$(), LOG);
            long addr = 0;
            long fileSize = 0;
            try (PartitionDecoder decoder = new PartitionDecoder()) {
                fileSize = ff.length(fd);
                addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                // `read_parquet` function will request symbols to be converted to varchar
                decoder.metadata().copyToSansUnsupported(metadata, true);
                if (metadata.getColumnCount() == 0) {
                    throw SqlException.$(argPos.getQuick(0), "no supported columns found in parquet file: ").put(filePath);
                }

                context.storeTelemetry(TelemetryEvent.READ_PARQUET, TelemetryOrigin.NO_MATTERS);
                if (context.isParallelReadParquetEnabled()) {
                    return new CursorFunction(new ReadParquetPageFrameRecordCursorFactory(path, metadata));
                } else {
                    return new CursorFunction(new ReadParquetRecordCursorFactory(path, metadata));
                }
            } finally {
                ff.close(fd);
                if (addr != 0) {
                    ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                }
            }
        } catch (CairoException e) {
            throw SqlException.$(argPos.getQuick(0), "error reading parquet file ").put('[').put(e.getErrno()).put("]: ").put(e.getFlyweightMessage());
        } catch (Throwable e) {
            throw SqlException.$(argPos.getQuick(0), "failed to read parquet file: ").put(filePath).put(": ").put(e.getMessage());
        }
    }

    private void checkPathIsSafeToRead(Path path, CharSequence filePath, int position, CairoConfiguration config) throws SqlException {
        CharSequence sqlCopyInputRoot = config.getSqlCopyInputRoot();
        if (Chars.isBlank(sqlCopyInputRoot)) {
            throw SqlException.$(position, "parquet files can only be read from sql.copy.input.root, please add sql.copy.input.root=<path> to your configuration");
        }
        if (Chars.isBlank(filePath)) {
            throw SqlException.$(position, "parquet file path pattern is empty");
        }
        if (Chars.contains(filePath, "../") || Chars.contains(filePath, "..\\")) {
            throw SqlException.$(position, "relative path is not allowed");
        }

        // Absolute path allowed
        if (
                filePath.length() > sqlCopyInputRoot.length()
                        && (Chars.startsWith(filePath, sqlCopyInputRoot)
                        // Path is not case-sensitive on Windows and OSX
                        || ((Os.isWindows() || Os.isOSX()) && Chars.startsWithIgnoreCase(filePath, sqlCopyInputRoot)))
        ) {
            if (
                    sqlCopyInputRoot.charAt(sqlCopyInputRoot.length() - 1) == Files.SEPARATOR
                            || filePath.charAt(sqlCopyInputRoot.length()) == Files.SEPARATOR
                            // On Windows, it's acceptable to use / as a separator
                            || (Os.isWindows() && filePath.charAt(sqlCopyInputRoot.length()) == '/')
            ) {
                path.of(filePath);
                return;
            }
        }
        path.of(sqlCopyInputRoot).concat(filePath);
    }
}
