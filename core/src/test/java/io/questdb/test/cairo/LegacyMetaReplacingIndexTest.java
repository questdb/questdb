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

package io.questdb.test.cairo;

import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Uses a real {@code _meta} taken from a {@code telemetry_config} created before QuestDB 6.1 and
 * migrated by {@code Mig608}. That migration grew column entries from 16 to 32 bytes and left the
 * old file contents in the bytes it did not write, so four of the eight columns hold junk where the
 * replacing column index later went. Every later rewrite copied the junk forward. QuestDB 10.0.0
 * was the first version to follow that index, and crashed on startup reading this file.
 */
public class LegacyMetaReplacingIndexTest extends AbstractCairoTest {

    private static final String CREATE_TABLE = "CREATE TABLE legacy_config (" +
            "id long256, enabled boolean, version symbol, os symbol, package symbol, " +
            "instance_name symbol, instance_type symbol, instance_desc symbol)";
    private static final String INSERT_ROW = "INSERT INTO legacy_config " +
            "(enabled, version, os, package, instance_name, instance_type, instance_desc) " +
            "VALUES (true, '9.3.5', 'Linux', 'deb', 'node-a', 'primary', 'first')";
    private static final String META_RESOURCE = "/meta/telemetry_config_mig608/_meta";
    private static final int REPLACING_INDEX_OFFSET = 24;
    // what the file holds at column-entry offset 24, checked so the fixture cannot go stale unnoticed
    private static final int[] STALE_REPLACING_RAW = {2056561992, 357933561, 1875551274, 0, 1090898432, 0, 0, 0};
    private static final String TABLE_NAME = "legacy_config";

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithLegacyMeta();
            execute(INSERT_ROW);

            execute("ALTER TABLE legacy_config ADD COLUMN region symbol");
            execute("INSERT INTO legacy_config (enabled, version, region) VALUES (false, '10.0.0', 'eu-west')");

            assertQuery("SELECT enabled, version, region FROM legacy_config")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            enabled\tversion\tregion
                            true\t9.3.5\t
                            false\t10.0.0\teu-west
                            """);
            assertOnDiskReplacingIndexes(9, -1, -1);
        });
    }

    @Test
    public void testChangeColumnType() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithLegacyMeta();
            execute(INSERT_ROW);

            execute("ALTER TABLE legacy_config ALTER COLUMN version TYPE varchar");

            // the data survives and the converted column keeps its position
            assertQuery(TABLE_NAME)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tenabled\tversion\tos\tpackage\tinstance_name\tinstance_type\tinstance_desc
                            \ttrue\t9.3.5\tLinux\tdeb\tnode-a\tprimary\tfirst
                            """);
            // the new "version" is column 8 and replaces column 2, which is the only chain left
            assertOnDiskReplacingIndexes(9, 8, 2);
        });
    }

    @Test
    public void testInsert() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithLegacyMeta();

            execute(INSERT_ROW);

            assertQuery("SELECT enabled, version, os, instance_name FROM legacy_config")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            enabled\tversion\tos\tinstance_name
                            true\t9.3.5\tLinux\tnode-a
                            """);
        });
    }

    @Test
    public void testSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithLegacyMeta();

            // no column dropped, none moved out of place
            assertQuery(TABLE_NAME)
                    .noLeakCheck()
                    .returns("id\tenabled\tversion\tos\tpackage\tinstance_name\tinstance_type\tinstance_desc\n");
        });
    }

    /**
     * Asserts the column block on disk: every column reads "no replacement" except
     * {@code replacementColumn}, which points at {@code replacedColumn}. Pass -1 for both when no
     * chain is expected. Stored 1-based, 0 means none.
     */
    private void assertOnDiskReplacingIndexes(int columnCount, int replacementColumn, int replacedColumn) {
        Assert.assertEquals(columnCount, readMetaInt(TableUtils.META_OFFSET_COUNT));
        for (int i = 0; i < columnCount; i++) {
            Assert.assertEquals("column " + i, i == replacementColumn ? replacedColumn + 1 : 0, readMetaInt(replacingIndexOffset(i)));
        }
    }

    private void createTableWithLegacyMeta() throws SqlException {
        execute(CREATE_TABLE);

        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        final long metadataVersion = readMetaLong(TableUtils.META_OFFSET_METADATA_VERSION);
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

        try (Path src = new Path(); Path dst = new Path()) {
            src.of(Files.getResourcePath(getClass().getResource(META_RESOURCE)));
            metaPath(dst);
            ff.removeQuiet(dst.$());
            Assert.assertTrue(ff.copy(src.$(), dst.$()) > -1);

            // The file comes from another instance, so its table id and metadata version would be
            // rejected before the column entries are ever read. Put back the two fields the fresh
            // table had and re-stamp their checksum, keeping the file's own minor version. The
            // column entries, which is what this test is about, stay as captured.
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, dst.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_TABLE_ID, tableToken.getTableId());
                mem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, metadataVersion);
                mem.putInt(
                        TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                        Numbers.encodeLowHighShorts(
                                TableUtils.checksumForMetaFormatMinorVersionField(metadataVersion, mem.getInt(TableUtils.META_OFFSET_COUNT)),
                                Numbers.decodeHighShort(mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION))
                        )
                );
            }
        }

        // drop everything the CREATE hydrated, so the next open reads the file just written
        engine.releaseInactive();
        try (MetadataCacheWriter ignore = engine.getMetadataCache().writeLock()) {
            ignore.clearCache();
        }

        for (int i = 0; i < STALE_REPLACING_RAW.length; i++) {
            Assert.assertEquals("column " + i, STALE_REPLACING_RAW[i], readMetaInt(replacingIndexOffset(i)));
        }
    }

    private void metaPath(Path path) {
        path.of(root).concat(engine.verifyTableName(TABLE_NAME)).concat(TableUtils.META_FILE_NAME);
    }

    private int readMetaInt(long offset) {
        try (Path path = new Path(); MemoryCMR mem = Vm.getCMRInstance()) {
            metaPath(path);
            mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(offset);
        }
    }

    private long readMetaLong(long offset) {
        try (Path path = new Path(); MemoryCMR mem = Vm.getCMRInstance()) {
            metaPath(path);
            mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getLong(offset);
        }
    }

    private long replacingIndexOffset(int columnIndex) {
        return TableUtils.META_OFFSET_COLUMN_TYPES + columnIndex * TableUtils.META_COLUMN_DATA_SIZE + REPLACING_INDEX_OFFSET;
    }
}
