/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo.mig;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.*;

public class EngineMigration {
    public static final int VERSION_TX_STRUCT_UPDATE_1 = 418;
    public static final int VERSION_TBL_META_COMMIT_LAG = 419;
    public static final int VERSION_COLUMN_TYPE_ENCODING_CHANGED = 420;
    public static final int VERSION_VAR_COLUMN_CHANGED = 421;
    public static final int VERSION_THAT_ADDED_TABLE_ID = 417;

    // All offsets hardcoded here in case TableUtils offset calculation changes
    // in future code version
    public static final long TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT = 72;
    public static final long TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY = 4;
    public static final String TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME = "_archive";

    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final ObjList<MigrationAction> MIGRATIONS = new ObjList<>();
    private static final IntList MIGRATIONS_CRITICALITY = new IntList();
    private static final int MIGRATIONS_LIST_OFFSET = VERSION_THAT_ADDED_TABLE_ID;

    public static void migrateEngineTo(CairoEngine engine, int latestVersion, boolean force) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final CairoConfiguration configuration=  engine.getConfiguration();
        int tempMemSize = 8;
        long mem = Unsafe.malloc(tempMemSize, MemoryTag.NATIVE_DEFAULT);

        try (
                MemoryARW virtualMem = Vm.getARWInstance(ff.getPageSize(), 8, MemoryTag.NATIVE_DEFAULT);
                Path path = new Path();
                MemoryMARW rwMemory = Vm.getMARWInstance()
        ) {

            MigrationContext context = new MigrationContext(engine, mem, tempMemSize, virtualMem, rwMemory);
            path.of(configuration.getRoot());

            // check if all tables have been upgraded already
            path.concat(TableUtils.UPGRADE_FILE_NAME).$();
            final boolean existed = !force && ff.exists(path);
            long upgradeFd = openFileRWOrFail(ff, path);
            LOG.debug()
                    .$("open [fd=").$(upgradeFd)
                    .$(", path=").$(path)
                    .$(']').$();
            if (existed) {
                long readLen = ff.read(upgradeFd, mem, Integer.BYTES, 0);
                if (readLen == Integer.BYTES && Unsafe.getUnsafe().getInt(mem) >= latestVersion) {
                    LOG.info().$("table structures are up to date").$();
                    ff.close(upgradeFd);
                    upgradeFd = -1;
                }
            }

            if (upgradeFd != -1) {
                try {
                    LOG.info().$("upgrading database [version=").$(latestVersion).I$();
                    if (upgradeTables(context, latestVersion)) {
                        Unsafe.getUnsafe().putInt(mem, latestVersion);
                        long writeLen = ff.write(upgradeFd, mem, Integer.BYTES, 0);
                        if (writeLen < Integer.BYTES) {
                            LOG.error().$("could not write to ").$(UPGRADE_FILE_NAME)
                                    .$(" [fd=").$(upgradeFd).$(",errno=").$(ff.errno()).I$();
                        }
                    }
                } finally {
                    ff.close(upgradeFd);
                }
            }
        } finally {
            Unsafe.free(mem, tempMemSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    static MigrationAction getMigrationToVersion(int version) {
        return MIGRATIONS.getQuick(version - MIGRATIONS_LIST_OFFSET);
    }

    private static int getMigrationToVersionCriticality(int version) {
        return MIGRATIONS_CRITICALITY.getQuick(version - MIGRATIONS_LIST_OFFSET);
    }

    private static void setByVersion(int version, MigrationAction action, int criticality) {
        MIGRATIONS.setQuick(version - MIGRATIONS_LIST_OFFSET, action);
        MIGRATIONS_CRITICALITY.extendAndSet(version - MIGRATIONS_LIST_OFFSET, criticality);
    }

    static void backupFile(FilesFacade ff, Path src, Path toTemp, String backupName, int version) {
        // make a copy
        int copyPathLen = toTemp.length();
        try {
            toTemp.concat(backupName).put(".v").put(version);
            for (int i = 1; ff.exists(toTemp.$()); i++) {
                // if backup file already exists
                // add .<num> at the end until file name is unique
                LOG.info().$("back up file exists, [path=").$(toTemp).I$();
                toTemp.trimTo(copyPathLen);
                toTemp.concat(backupName).put(".v").put(version).put(".").put(i);
            }

            LOG.info().$("back up coping file [from=").$(src).$(",to=").$(toTemp).I$();
            if (ff.copy(src.$(), toTemp.$()) < 0) {
                throw CairoException.instance(ff.errno()).put("Cannot backup transaction file [to=").put(toTemp).put(']');
            }
        } finally {
            toTemp.trimTo(copyPathLen);
        }
    }

    static int readIntAtOffset(FilesFacade ff, Path path, long tempMem4b, long fd) {
        if (ff.read(fd, tempMem4b, Integer.BYTES, EngineMigration.TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY) != Integer.BYTES) {
            throw CairoException.instance(ff.errno()).put("Cannot read: ").put(path);
        }
        return Unsafe.getUnsafe().getInt(tempMem4b);
    }

    private static boolean upgradeTables(MigrationContext context, int latestVersion) {
        final FilesFacade ff = context.getFf();
        final CharSequence root = context.getConfiguration().getRoot();
        long mem = context.getTempMemory(8);
        final AtomicBoolean updateSuccess = new AtomicBoolean(true);

        try (Path path = new Path(); Path copyPath = new Path()) {
            path.of(root);
            copyPath.of(root);
            final int rootLen = path.length();

            final NativeLPSZ nativeLPSZ = new NativeLPSZ();
            ff.iterateDir(path.$(), (name, type) -> {
                if (type == Files.DT_DIR) {
                    nativeLPSZ.of(name);
                    if (Chars.notDots(nativeLPSZ)) {
                        path.trimTo(rootLen);
                        path.concat(nativeLPSZ);
                        copyPath.trimTo(rootLen);
                        copyPath.concat(nativeLPSZ);
                        final int plen = path.length();
                        path.concat(TableUtils.META_FILE_NAME);

                        if (ff.exists(path.$())) {
                            final long fd = openFileRWOrFail(ff, path);
                            try {
                                if (ff.read(fd, mem, Integer.BYTES, META_OFFSET_VERSION) == Integer.BYTES) {
                                    int currentTableVersion = Unsafe.getUnsafe().getInt(mem);

                                    if (currentTableVersion < latestVersion) {
                                        LOG.info().$("upgrading [path=").$(path).$(",fromVersion=").$(currentTableVersion)
                                                .$(",toVersion=").$(latestVersion).I$();

                                        copyPath.trimTo(plen);
                                        backupFile(ff, path, copyPath, TableUtils.META_FILE_NAME, currentTableVersion);

                                        path.trimTo(plen);
                                        context.of(path, copyPath, fd);

                                        for (int i = currentTableVersion + 1; i <= latestVersion; i++) {
                                            MigrationAction migration = getMigrationToVersion(i);
                                            try {
                                                if (migration != null) {
                                                    LOG.info().$("upgrading table [path=").$(path).$(",toVersion=").$(i).I$();
                                                    migration.migrate(context);
                                                    path.trimTo(plen);
                                                }
                                            } catch (Exception e) {
                                                LOG.error().$("failed to upgrade table path=")
                                                        .$(path.trimTo(plen))
                                                        .$(", exception: ")
                                                        .$(e).$();

                                                if (getMigrationToVersionCriticality(i) != 0) {
                                                    throw e;
                                                }
                                                updateSuccess.set(false);
                                                return;
                                            }

                                            Unsafe.getUnsafe().putInt(mem, i);
                                            if (ff.write(fd, mem, Integer.BYTES, META_OFFSET_VERSION) != Integer.BYTES) {
                                                // Table is migrated but we cannot write new version
                                                // to meta file
                                                // This is critical, table potentially left in unusable state
                                                throw CairoException.instance(ff.errno())
                                                        .put("failed to write updated version to table Metadata file [path=")
                                                        .put(path.trimTo(plen))
                                                        .put(",latestVersion=")
                                                        .put(i)
                                                        .put(']');
                                            }
                                        }
                                    }
                                    return;
                                }
                                // todo: change message, log is unclear why we are here
                                updateSuccess.set(false);
                                throw CairoException.instance(ff.errno()).put("Could not update table [path=").put(path).put(']');
                            } finally {
                                ff.close(fd);
                                path.trimTo(plen);
                                copyPath.trimTo(plen);
                            }
                        }
                    }
                }
            });
            LOG.info().$("upgraded tables to ").$(latestVersion).$();
        }
        return updateSuccess.get();
    }

    static {
        MIGRATIONS.extendAndSet(ColumnType.VERSION - MIGRATIONS_LIST_OFFSET, null);
        setByVersion(VERSION_THAT_ADDED_TABLE_ID, MigrationActions::assignTableId, 1);
        setByVersion(VERSION_TX_STRUCT_UPDATE_1, MigrationActions::rebuildTransactionFile, 0);
        setByVersion(VERSION_TBL_META_COMMIT_LAG, MigrationActions::addTblMetaCommitLag, 0);
        setByVersion(VERSION_COLUMN_TYPE_ENCODING_CHANGED, MigrationActions::updateColumnTypeIds, 1);
        setByVersion(VERSION_VAR_COLUMN_CHANGED, MigrationActions::bumpVarColumnIndex, 1);
    }
}
