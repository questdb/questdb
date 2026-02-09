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

import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

/**
 * Migration tool for converting legacy table metadata files to BlockFile format.
 * <p>
 * Usage:
 * <pre>
 * TableMetadataMigration migration = new TableMetadataMigration(ff, configuration);
 * MigrationResult result = migration.migrateDatabase(dbRoot);
 * </pre>
 * <p>
 * The migration process for each table:
 * <ol>
 *   <li>Read legacy _meta file</li>
 *   <li>Write new format to _meta.new</li>
 *   <li>Validate new file by reading it back</li>
 *   <li>Atomic rename: _meta.new -> _meta</li>
 * </ol>
 */
public class TableMetadataMigration {
    private static final Log LOG = LogFactory.getLog(TableMetadataMigration.class);
    private static final String META_FILE_NAME = "_meta";
    private static final String META_NEW_FILE_NAME = "_meta.new";

    private final CairoConfiguration configuration;
    private final FilesFacade ff;

    public TableMetadataMigration(@NotNull FilesFacade ff, @NotNull CairoConfiguration configuration) {
        this.ff = ff;
        this.configuration = configuration;
    }

    /**
     * Migrates all tables in the database root directory.
     *
     * @param dbRoot path to the database root directory
     * @return migration result with statistics
     */
    public MigrationResult migrateDatabase(@NotNull Path dbRoot) {
        MigrationResult result = new MigrationResult();
        int rootLen = dbRoot.size();
        Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();

        LOG.info().$("starting metadata migration [dbRoot=").$(dbRoot).I$();

        long findPtr = ff.findFirst(dbRoot.$());
        if (findPtr == 0) {
            LOG.error().$("cannot open database directory [path=").$(dbRoot).I$();
            result.errors++;
            return result;
        }

        try {
            do {
                if (ff.isDirOrSoftLinkDirNoDots(dbRoot, rootLen, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                    // Skip QuestDB system directories (starting with _)
                    if (dirNameSink.size() > 0 && dirNameSink.byteAt(0) == '_') {
                        dbRoot.trimTo(rootLen);
                        continue;
                    }

                    migrateTable(dbRoot, result);
                    dbRoot.trimTo(rootLen);
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
            dbRoot.trimTo(rootLen);
        }

        LOG.info().$("metadata migration complete [migrated=").$(result.tablesMigrated)
                .$(", skipped=").$(result.tablesSkipped)
                .$(", errors=").$(result.errors).I$();

        return result;
    }

    /**
     * Migrates a single table's metadata file.
     *
     * @param tablePath path to the table directory (will be modified and restored)
     * @param result    result object to update
     */
    public void migrateTable(@NotNull Path tablePath, @NotNull MigrationResult result) {
        int pathLen = tablePath.size();

        try {
            tablePath.concat(META_FILE_NAME).$();

            if (!ff.exists(tablePath.$())) {
                // Not a table directory
                tablePath.trimTo(pathLen);
                return;
            }

            // Check if already in BlockFile format
            if (isBlockFileFormat(tablePath)) {
                LOG.debug().$("table already migrated [path=").$(tablePath).I$();
                result.tablesSkipped++;
                tablePath.trimTo(pathLen);
                return;
            }

            // Read legacy format
            LegacyMetadata legacy = readLegacyMetadata(tablePath);
            tablePath.trimTo(pathLen);

            // Write new format to temp file
            tablePath.concat(META_NEW_FILE_NAME).$();
            writeBlockFileMetadata(tablePath, legacy);

            // Validate new file
            validateBlockFileMetadata(tablePath, legacy);

            // Atomic rename: _meta.new -> _meta
            Path metaPath = Path.getThreadLocal2(tablePath);
            metaPath.trimTo(pathLen).concat(META_FILE_NAME).$();

            TableUtils.renameOrFail(ff, tablePath.$(), metaPath.$());

            LOG.info().$("migrated table metadata [path=").$(metaPath).I$();
            result.tablesMigrated++;

        } catch (CairoException e) {
            LOG.error().$("failed to migrate table [path=").$(tablePath).$(", error=").$(e.getMessage()).I$();
            result.errors++;
            // Clean up temp file if it exists
            tablePath.trimTo(pathLen).concat(META_NEW_FILE_NAME).$();
            ff.removeQuiet(tablePath.$());
        } finally {
            tablePath.trimTo(pathLen);
        }
    }

    private boolean isBlockFileFormat(Path metaPath) {
        // BlockFile format has version field at offset 0 as a long
        // Legacy format has column count at offset 0 as an int (small value)
        // BlockFile version starts at 1 and increments, but the dual-region
        // design means the file is at least 40 bytes header + region data
        // We can detect by checking if the file starts with a valid BlockFile header

        long fd = ff.openRO(metaPath.$());
        if (fd == -1) {
            return false;
        }

        try {
            // Read first 8 bytes (version field in BlockFile, or column_count + partition_by in legacy)
            long version = ff.readNonNegativeLong(fd, 0);
            if (version < 0) {
                return false;
            }

            // Legacy format: first 4 bytes is column count (typically < 1000)
            // followed by partition mode (0-5)
            // BlockFile format: first 8 bytes is version (starts at 1, increments)
            // The key difference is that legacy files have small values at offset 0

            // Check if this looks like a BlockFile by examining the structure
            // BlockFile has: version(8) + regionA_offset(8) + regionA_length(8) + regionB_offset(8) + regionB_length(8) = 40 bytes header
            // Then region data follows

            // If version is 0, file is uninitialized BlockFile
            // If version > 0 and < 100000, could be either format
            // Legacy: column_count is at offset 0 (int), partition_by at offset 4 (int)

            // Read as two ints to check legacy pattern
            int firstInt = (int) version;
            int secondInt = (int) (version >>> 32);

            // Legacy format constraints:
            // - column count > 0 and typically < 10000
            // - partition mode is 0-5 (NONE, DAY, MONTH, YEAR, HOUR, WEEK)
            if (firstInt > 0 && firstInt < 10000 && secondInt >= 0 && secondInt <= 5) {
                // This looks like legacy format
                return false;
            }

            // Additional check: BlockFile version is odd for region A, even for region B
            // and the file should have proper region offsets
            if (version > 0) {
                // Try to read region offset to verify it's a valid BlockFile
                long fileLen = ff.length(fd);
                if (fileLen >= 40) {
                    // This could be a BlockFile
                    return true;
                }
            }

            return false;
        } finally {
            ff.close(fd);
        }
    }

    private LegacyMetadata readLegacyMetadata(Path metaPath) {
        LegacyMetadata legacy = new LegacyMetadata();

        try (MemoryCMR mem = Vm.getCMRInstance()) {
            mem.of(ff, metaPath.$(), ff.getPageSize(), ff.length(metaPath.$()), MemoryTag.MMAP_DEFAULT);

            // Validate legacy format
            int version = mem.getInt(TableUtils.META_OFFSET_VERSION);
            if (version != ColumnType.VERSION) {
                throw CairoException.critical(CairoException.METADATA_VALIDATION)
                        .put("invalid metadata version [expected=").put(ColumnType.VERSION)
                        .put(", actual=").put(version)
                        .put(", path=").put(metaPath).put(']');
            }

            // Read header fields
            legacy.columnCount = mem.getInt(TableUtils.META_OFFSET_COUNT);
            legacy.partitionBy = mem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            legacy.timestampIndex = mem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            legacy.tableId = mem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            legacy.maxUncommittedRows = mem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
            legacy.o3MaxLag = mem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
            legacy.metadataVersion = mem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);
            legacy.walEnabled = mem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);
            legacy.ttlHoursOrMonths = TableUtils.getTtlHoursOrMonths(mem);

            // Read column metadata
            legacy.columns = new ObjList<>(legacy.columnCount);
            long nameOffset = TableUtils.getColumnNameOffset(legacy.columnCount);

            for (int i = 0; i < legacy.columnCount; i++) {
                int columnType = TableUtils.getColumnType(mem, i);
                long flags = TableUtils.getColumnFlags(mem, i);
                int indexBlockCapacity = TableUtils.getIndexBlockCapacity(mem, i);
                int symbolCapacity = TableUtils.getSymbolCapacity(mem, i);
                int replacingIndex = TableUtils.getReplacingColumnIndex(mem, i);

                boolean indexed = (flags & TableUtils.META_FLAG_BIT_INDEXED) != 0;
                boolean symbolCached = (flags & TableUtils.META_FLAG_BIT_SYMBOL_CACHE) != 0;
                boolean dedupKey = (flags & TableUtils.META_FLAG_BIT_DEDUP_KEY) != 0;

                CharSequence name = mem.getStrA(nameOffset);
                nameOffset += Vm.getStorageLength(name);

                TableColumnMetadata column = new TableColumnMetadata(
                        name.toString(),
                        columnType,
                        indexed,
                        indexBlockCapacity,
                        true, // symbolTableStatic
                        null, // metadata
                        i,    // writerIndex
                        dedupKey,
                        replacingIndex,
                        symbolCached,
                        symbolCapacity
                );
                legacy.columns.add(column);
            }
        }

        return legacy;
    }

    private void writeBlockFileMetadata(Path metaPath, LegacyMetadata legacy) {
        try (BlockFileWriter writer = new BlockFileWriter(ff, CommitMode.SYNC)) {
            writer.of(metaPath.$());

            TableMetadataFileBlock.write(
                    writer,
                    ColumnType.VERSION,
                    legacy.tableId,
                    legacy.partitionBy,
                    legacy.timestampIndex,
                    legacy.walEnabled,
                    legacy.maxUncommittedRows,
                    legacy.o3MaxLag,
                    legacy.ttlHoursOrMonths,
                    legacy.columns
            );
        }
    }

    private void validateBlockFileMetadata(Path metaPath, LegacyMetadata expected) {
        try (BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(metaPath.$());

            TableMetadataFileBlock.MetadataHolder actual = new TableMetadataFileBlock.MetadataHolder();
            TableMetadataFileBlock.read(reader, actual, metaPath.$());

            // Validate core fields
            if (actual.tableId != expected.tableId) {
                throw validationError("tableId", expected.tableId, actual.tableId, metaPath);
            }
            if (actual.partitionBy != expected.partitionBy) {
                throw validationError("partitionBy", expected.partitionBy, actual.partitionBy, metaPath);
            }
            if (actual.timestampIndex != expected.timestampIndex) {
                throw validationError("timestampIndex", expected.timestampIndex, actual.timestampIndex, metaPath);
            }
            if (actual.columnCount != expected.columnCount) {
                throw validationError("columnCount", expected.columnCount, actual.columnCount, metaPath);
            }
            if (actual.metadataVersion != expected.metadataVersion) {
                throw validationError("metadataVersion", expected.metadataVersion, actual.metadataVersion, metaPath);
            }
            if (actual.walEnabled != expected.walEnabled) {
                throw validationError("walEnabled", expected.walEnabled, actual.walEnabled, metaPath);
            }

            // Validate settings
            if (actual.maxUncommittedRows != expected.maxUncommittedRows) {
                throw validationError("maxUncommittedRows", expected.maxUncommittedRows, actual.maxUncommittedRows, metaPath);
            }
            if (actual.o3MaxLag != expected.o3MaxLag) {
                throw validationError("o3MaxLag", expected.o3MaxLag, actual.o3MaxLag, metaPath);
            }
            if (actual.ttlHoursOrMonths != expected.ttlHoursOrMonths) {
                throw validationError("ttlHoursOrMonths", expected.ttlHoursOrMonths, actual.ttlHoursOrMonths, metaPath);
            }

            // Validate columns
            if (actual.columns.size() != expected.columns.size()) {
                throw validationError("columns.size", expected.columns.size(), actual.columns.size(), metaPath);
            }

            for (int i = 0; i < expected.columns.size(); i++) {
                TableColumnMetadata exp = expected.columns.getQuick(i);
                TableColumnMetadata act = actual.columns.getQuick(i);

                if (!exp.getColumnName().equals(act.getColumnName())) {
                    throw validationError("column[" + i + "].name", exp.getColumnName(), act.getColumnName(), metaPath);
                }
                if (exp.getColumnType() != act.getColumnType()) {
                    throw validationError("column[" + i + "].type", exp.getColumnType(), act.getColumnType(), metaPath);
                }
                if (exp.isSymbolIndexFlag() != act.isSymbolIndexFlag()) {
                    throw validationError("column[" + i + "].indexed", exp.isSymbolIndexFlag(), act.isSymbolIndexFlag(), metaPath);
                }
                if (exp.isDedupKeyFlag() != act.isDedupKeyFlag()) {
                    throw validationError("column[" + i + "].dedupKey", exp.isDedupKeyFlag(), act.isDedupKeyFlag(), metaPath);
                }
            }
        }
    }

    private CairoException validationError(String field, Object expected, Object actual, Path path) {
        return CairoException.critical(CairoException.METADATA_VALIDATION)
                .put("metadata validation failed [field=").put(field)
                .put(", expected=").put(expected.toString())
                .put(", actual=").put(actual.toString())
                .put(", path=").put(path).put(']');
    }

    /**
     * Holder for legacy metadata read from the old format.
     */
    private static class LegacyMetadata {
        int columnCount;
        int partitionBy;
        int timestampIndex;
        int tableId;
        int maxUncommittedRows;
        long o3MaxLag;
        long metadataVersion;
        boolean walEnabled;
        int ttlHoursOrMonths;
        ObjList<TableColumnMetadata> columns;
    }

    /**
     * Result of migration operation.
     */
    public static class MigrationResult {
        public int tablesMigrated;
        public int tablesSkipped;
        public int errors;

        public boolean hasErrors() {
            return errors > 0;
        }

        @Override
        public String toString() {
            StringSink sink = new StringSink();
            sink.put("MigrationResult{migrated=").put(tablesMigrated)
                    .put(", skipped=").put(tablesSkipped)
                    .put(", errors=").put(errors).put('}');
            return sink.toString();
        }
    }
}
