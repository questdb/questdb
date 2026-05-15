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

package io.questdb.griffin.engine.functions.table;

import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.catalogue.GlobFilesFunctionFactory;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

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

            final CharSequence nonGlobPrefix = GlobFilesFunctionFactory.extractNonGlobPrefix(filePath);
            if (!Chars.equals(nonGlobPrefix, filePath)) {
                return newGlobInstance(position, argPos, configuration, context, path, filePath);
            }

            final FilesFacade ff = configuration.getFilesFacade();
            final long fd = TableUtils.openRO(ff, path.$(), LOG);
            long addr = 0;
            long fileSize = 0;
            try (ParquetFileDecoder decoder = new ParquetFileDecoder()) {
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

    private Function newGlobInstance(
            int position,
            IntList argPos,
            CairoConfiguration configuration,
            SqlExecutionContext context,
            Path resolvedPath,
            CharSequence originalPattern
    ) throws SqlException {
        // checkPathIsSafeToRead has already resolved the pattern against sql.copy.input.root.
        // Hand the resolved pattern to GlobFilesFunctionFactory so it can enumerate matches.
        final String resolvedPattern = resolvedPath.toString();
        // Byte length of the non-glob prefix: the offset (in UTF-8 bytes) at which
        // hive partition segments begin in every matched file's path. Computed from
        // the resolved Path's underlying Utf8Sequence because file paths the glob
        // cursor returns are byte-oriented; a UTF-16 char count would mis-locate
        // partition segments under non-ASCII roots.
        final int nonGlobRootByteLen = GlobFilesFunctionFactory.extractNonGlobPrefixByteLength(resolvedPath);
        // Enumerate matched files once at planning time, save the list, and hand
        // ownership to the hive factory. Both cursor backends then iterate the
        // cached list instead of re-walking the directory tree on every
        // getCursor / size() / iteration call. Critical for remote-backed reads
        // where each enumeration round-trip is expensive.
        final ObjList<Function> globArgs = new ObjList<>();
        globArgs.add(new StrConstant(resolvedPattern));
        final IntList globArgPos = new IntList();
        globArgPos.add(argPos.getQuick(0));
        Function globFunc = new GlobFilesFunctionFactory().newInstance(position, globArgs, globArgPos, configuration, context);
        RecordCursorFactory globFactory = globFunc.getRecordCursorFactory();
        DirectUtf8StringList matchedFiles = new DirectUtf8StringList(128, 8);
        try {
            final GenericRecordMetadata parquetMetadata = new GenericRecordMetadata();
            final ObjList<String> partitionColumnNames = new ObjList<>();
            final IntList partitionColumnTypes = new IntList();
            try (RecordCursor globCursor = globFactory.getCursor(context)) {
                if (!globCursor.hasNext()) {
                    throw SqlException.$(argPos.getQuick(0), "glob did not match any files: ").put(originalPattern);
                }
                // Decode schema from the first matched file. The path stays referenced
                // through matchedFiles so we don't need to copy the bytes twice.
                final Utf8Sequence firstFile = globCursor.getRecord().getVarcharA(0);
                matchedFiles.put(firstFile);
                final FilesFacade ff = configuration.getFilesFacade();
                final Path firstFilePath = Path.getThreadLocal("").of(firstFile);
                final long fd = TableUtils.openRO(ff, firstFilePath.$(), LOG);
                long addr = 0;
                long fileSize = 0;
                try (ParquetFileDecoder decoder = new ParquetFileDecoder()) {
                    fileSize = ff.length(fd);
                    addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    decoder.metadata().copyToSansUnsupported(parquetMetadata, true);
                    if (parquetMetadata.getColumnCount() == 0) {
                        throw SqlException.$(argPos.getQuick(0), "no supported columns found in parquet file: ").put(firstFile);
                    }
                } finally {
                    ff.close(fd);
                    if (addr != 0) {
                        ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    }
                }
                // Take the union of key=value partition keys seen across all matched files,
                // and demote each column's inferred type to the loosest one that fits every
                // value observed. Files that happen to be enumerated first without partitions
                // don't hide the schema; names that would shadow real parquet columns are dropped.
                // We also save each path into matchedFiles so the cursor doesn't re-walk.
                parsePartitionColumns(firstFile, nonGlobRootByteLen, parquetMetadata,
                        partitionColumnNames, partitionColumnTypes);
                while (globCursor.hasNext()) {
                    Utf8Sequence path = globCursor.getRecord().getVarcharA(0);
                    matchedFiles.put(path);
                    parsePartitionColumns(
                            path,
                            nonGlobRootByteLen,
                            parquetMetadata,
                            partitionColumnNames,
                            partitionColumnTypes
                    );
                }
            } finally {
                // Drop the glob factory: we've drained it into matchedFiles and the hive
                // factory doesn't need it for runtime iteration.
                Misc.free(globFactory);
            }
            final GenericRecordMetadata wrappingMetadata = new GenericRecordMetadata();
            for (int i = 0, n = parquetMetadata.getColumnCount(); i < n; i++) {
                wrappingMetadata.add(parquetMetadata.getColumnMetadata(i));
            }
            for (int i = 0, n = partitionColumnNames.size(); i < n; i++) {
                wrappingMetadata.add(new TableColumnMetadata(
                        partitionColumnNames.getQuick(i),
                        partitionColumnTypes.getQuick(i)
                ));
            }
            context.storeTelemetry(TelemetryEvent.READ_PARQUET, TelemetryOrigin.NO_MATTERS);
            return new CursorFunction(new HivePartitionedReadParquetRecordCursorFactory(
                    configuration,
                    matchedFiles,
                    originalPattern,
                    nonGlobRootByteLen,
                    wrappingMetadata,
                    parquetMetadata,
                    partitionColumnNames,
                    partitionColumnTypes
            ));
        } catch (Throwable th) {
            Misc.free(matchedFiles);
            throw th;
        }
    }

    /**
     * Walks {@code key=value} segments of {@code path} below {@code rootLen}, registering
     * each new key in {@code outNames} (in order of first appearance) and demoting its
     * inferred type in {@code outTypes} (parallel list) to the loosest type that still
     * fits every value observed so far. Type chain is:
     * INT -> LONG -> DATE -> TIMESTAMP -> DOUBLE -> VARCHAR.
     * <p>
     * Keys that collide with existing parquet columns are skipped entirely.
     */
    private static void parsePartitionColumns(
            Utf8Sequence path,
            int rootLen,
            GenericRecordMetadata parquetMetadata,
            ObjList<String> outNames,
            IntList outTypes
    ) {
        final StringSink keySink = Misc.getThreadLocalSink();
        final int n = path.size();
        int segStart = Math.min(rootLen, n);
        while (segStart < n) {
            int segEnd = segStart;
            while (segEnd < n) {
                byte b = path.byteAt(segEnd);
                if (b == '/' || b == Files.SEPARATOR) {
                    break;
                }
                segEnd++;
            }
            int eqIdx = -1;
            for (int i = segStart; i < segEnd; i++) {
                if (path.byteAt(i) == '=') {
                    eqIdx = i;
                    break;
                }
            }
            if (eqIdx > segStart && eqIdx < segEnd - 1) {
                keySink.clear();
                Utf8s.utf8ToUtf16(path, segStart, eqIdx, keySink);
                final String key = keySink.toString();
                if (parquetMetadata.getColumnIndexQuiet(key) < 0) {
                    int idx = outNames.indexOf(key);
                    if (idx < 0) {
                        outNames.add(key);
                        outTypes.add(ColumnType.INT);
                        idx = outNames.size() - 1;
                    }
                    int currentType = outTypes.getQuick(idx);
                    int demotedType = demoteTypeToFit(path, eqIdx + 1, segEnd, currentType);
                    if (demotedType != currentType) {
                        outTypes.setQuick(idx, demotedType);
                    }
                }
            }
            if (segEnd >= n) {
                break;
            }
            segStart = segEnd + 1;
        }
    }

    private static int demoteTypeToFit(Utf8Sequence path, int lo, int hi, int currentType) {
        if (currentType == ColumnType.VARCHAR) {
            return ColumnType.VARCHAR;
        }
        // Uses a local sink to avoid coupling with the thread-local sink that
        // parsePartitionColumns holds the key string in; planning-time path so
        // the allocation is acceptable.
        final StringSink sink = new StringSink();
        Utf8s.utf8ToUtf16(path, lo, hi, sink);
        int type = currentType;
        while (type != ColumnType.VARCHAR) {
            if (canParseAs(sink, type)) {
                return type;
            }
            type = nextLooserType(type);
        }
        return ColumnType.VARCHAR;
    }

    private static boolean canParseAs(CharSequence cs, int type) {
        try {
            switch (type) {
                case ColumnType.INT:
                    Numbers.parseInt(cs);
                    return true;
                case ColumnType.LONG:
                    Numbers.parseLong(cs);
                    return true;
                case ColumnType.DATE:
                    DateFormatUtils.parseDate(cs);
                    return true;
                case ColumnType.TIMESTAMP:
                    MicrosFormatUtils.parseTimestamp(cs);
                    return true;
                case ColumnType.DOUBLE:
                    Numbers.parseDouble(cs);
                    return true;
                default:
                    return false;
            }
        } catch (NumericException e) {
            return false;
        }
    }

    private static int nextLooserType(int type) {
        switch (type) {
            case ColumnType.INT:
                return ColumnType.LONG;
            case ColumnType.LONG:
                return ColumnType.DATE;
            case ColumnType.DATE:
                return ColumnType.TIMESTAMP;
            case ColumnType.TIMESTAMP:
                return ColumnType.DOUBLE;
            case ColumnType.DOUBLE:
            default:
                return ColumnType.VARCHAR;
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
