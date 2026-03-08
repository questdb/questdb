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

package io.questdb.cairo.mig;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.TableUtils.META_OFFSET_VERSION;
import static io.questdb.cairo.TableUtils.openFileRWOrFail;

public class EngineMigration {

    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final IntObjHashMap<MigrationAction> MIGRATIONS = new IntObjHashMap<>();

    /**
     * This method scans root db directory and applies necessary migrations to all tables.
     *
     * @param engine                 CairoEngine instance
     * @param latestTableVersion     storage compatibility version. This version is stored in table _meta file
     *                               and is enforced by QuestDB to match to ColumnType.VERSION on querying and writing.
     * @param latestMigrationVersion some migrations are forward compatible. When a forward compatible
     *                               migration runs it does not change _meta file version so that older QuestDB versions
     *                               can still read / write if downgraded.
     *                               Such compatible migration version is stored in table _upgrade.d file only,
     *                               and it must be greater or equal to latestTableVersion.
     *                               Migration version is used to determine if migration run is required.
     * @param force                  if true, migration will be run even if _upgrade.d file exists, and it is up-to-date.
     */
    public static void migrateEngineTo(CairoEngine engine, int latestTableVersion, int latestMigrationVersion, boolean force) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final CairoConfiguration configuration = engine.getConfiguration();
        int tempMemSize = Long.BYTES;
        long mem = Unsafe.malloc(tempMemSize, MemoryTag.NATIVE_MIG);

        try (
                MemoryARW virtualMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path path = new Path();
                MemoryMARW rwMemory = Vm.getCMARWInstance()
        ) {
            MigrationContext context = new MigrationContext(engine, mem, tempMemSize, virtualMem, rwMemory);
            path.of(configuration.getDbRoot());

            // check if all tables have been upgraded already
            path.concat(TableUtils.UPGRADE_FILE_NAME);
            final boolean existed = !force && ff.exists(path.$());
            long upgradeFd = openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            LOG.debug()
                    .$("open [fd=").$(upgradeFd)
                    .$(", path=").$(path)
                    .I$();
            if (existed) {
                int currentTableVersion = ff.readNonNegativeInt(upgradeFd, 0);

                if (currentTableVersion > latestTableVersion) {
                    ff.close(upgradeFd);
                    LOG.critical().$("database storage is marked as upgraded to an incompatible version, ")
                            .$(". Upgrade the database or ")
                            .$("remove file ")
                            .$(TableUtils.UPGRADE_FILE_NAME)
                            .$((" to force proceed"))
                            .$(" [storageVersion=").$(currentTableVersion)
                            .$(", databaseVersion=").$(latestTableVersion).I$();

                    throw CairoException.critical(0)
                            .put("database storage is marked as upgraded to an incompatible version, ")
                            .put(". Upgrade the database or ")
                            .put("remove file ")
                            .put(TableUtils.UPGRADE_FILE_NAME)
                            .put(" [storageVersion=").put(currentTableVersion)
                            .put(", databaseVersion=").put(latestTableVersion);
                }

                int currentMigrationVersion = ff.readNonNegativeInt(upgradeFd, 4);
                if (currentMigrationVersion <= 0 || configuration.getRepeatMigrationsFromVersion() == currentTableVersion) {
                    currentMigrationVersion = currentTableVersion;
                }

                if (currentMigrationVersion == latestMigrationVersion) {
                    LOG.info().$("upgraded to [migrationVersion=").$(currentMigrationVersion).I$();
                    ff.fsyncAndClose(upgradeFd);
                    return;
                }
            }

            try {
                LOG.info().$("upgrading database [version=").$(latestMigrationVersion).I$();
                upgradeTables(context, latestTableVersion, latestMigrationVersion);
                TableUtils.writeIntOrFail(
                        ff,
                        upgradeFd,
                        0,
                        latestTableVersion,
                        mem,
                        path
                );
                TableUtils.writeIntOrFail(
                        ff,
                        upgradeFd,
                        4,
                        latestMigrationVersion,
                        mem,
                        path
                );
            } finally {
                Vm.bestEffortClose(
                        ff,
                        LOG,
                        upgradeFd,
                        2 * Integer.BYTES
                );
            }
        } finally {
            Unsafe.free(mem, tempMemSize, MemoryTag.NATIVE_MIG);
        }
    }

    private static @Nullable MigrationAction getMigrationToVersion(int version) {
        return MIGRATIONS.get(version);
    }

    private static void upgradeTables(MigrationContext context, int latestTableVersion, int latestMigrationVersion) {
        final FilesFacade ff = context.getFf();
        final CharSequence root = context.getConfiguration().getDbRoot();
        long mem = context.getTempMemory(8);

        try (Path path = new Path(); Path copyPath = new Path()) {
            path.of(root);
            copyPath.of(root);
            final int rootLen = path.size();

            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type)) {
                    copyPath.trimTo(rootLen);
                    copyPath.concat(pUtf8NameZ);
                    final int tablePlen = path.size();

                    if (ff.exists(path.concat(TableUtils.META_FILE_NAME).$())) {
                        final long fdMeta = openFileRWOrFail(ff, path.$(), context.getConfiguration().getWriterFileOpenOpts());
                        try {
                            int currentTableVersion = TableUtils.readIntOrFail(ff, fdMeta, META_OFFSET_VERSION, mem, path);
                            if (currentTableVersion < latestMigrationVersion) {
                                LOG.info()
                                        .$("upgrading [path=").$(copyPath.$())
                                        .$(", fromVersion=").$(currentTableVersion)
                                        .$(", toVersion=").$(latestMigrationVersion)
                                        .I$();

                                copyPath.trimTo(tablePlen);

                                if (currentTableVersion < latestTableVersion) {
                                    // backup meta file
                                    LOG.info().$("backing up meta file [path=").$(path)
                                            .$(", toPath=").$(copyPath)
                                            .I$();
                                    backupFile(ff, path, copyPath, TableUtils.META_FILE_NAME, currentTableVersion);
                                }

                                path.trimTo(tablePlen);
                                context.of(path, copyPath, fdMeta);

                                for (int ver = currentTableVersion + 1; ver <= latestMigrationVersion; ver++) {
                                    final MigrationAction migration = getMigrationToVersion(ver);
                                    if (migration != null) {
                                        try {
                                            LOG.info().$("upgrading table [path=").$(path)
                                                    .$(", toVersion=").$(ver)
                                                    .I$();
                                            migration.migrate(context);
                                            path.trimTo(tablePlen);
                                            copyPath.trimTo(tablePlen);
                                        } catch (Throwable e) {
                                            LOG.error().$("failed to upgrade table [path=").$(path.trimTo(tablePlen))
                                                    .$(", e=").$(e)
                                                    .I$();
                                            throw e;
                                        }
                                    }

                                    if (ver <= latestTableVersion) {
                                        path.trimTo(tablePlen).concat(TableUtils.META_FILE_NAME).$();
                                        LOG.info().$("upgrading table _meta [path=").$(path).$(", toVersion=").$(ver).I$();
                                        // Upgrades between (latestTableVersion, latestMigrationVersion]
                                        // are backwards compatible and are not set in table _meta
                                        TableUtils.writeIntOrFail(ff, fdMeta, META_OFFSET_VERSION, ver, mem, path);
                                    }
                                    path.trimTo(tablePlen);
                                }
                            }
                        } finally {
                            ff.close(fdMeta);
                            path.trimTo(tablePlen);
                            copyPath.trimTo(tablePlen);
                        }
                    }
                }
            });
            LOG.info().$("upgraded tables to ").$(latestMigrationVersion).$();
        }
    }

    static void backupFile(FilesFacade ff, Path src, Path toTemp, String backupName, int version) {
        // make a copy
        int copyPathLen = toTemp.size();
        try {
            toTemp.concat(backupName).put(".v").put(version);
            int versionLen = toTemp.size();
            for (int i = 1; ff.exists(toTemp.$()); i++) {
                // if backup file already exists
                // add .<num> at the end until file name is unique
                LOG.info().$("backup dest exists [to=").$(toTemp).I$();
                toTemp.trimTo(versionLen).put('.').put(i);
            }

            LOG.info().$("backing up [file=").$(src).$(", to=").$(toTemp).I$();
            if (ff.copy(src.$(), toTemp.$()) < 0) {
                throw CairoException.critical(ff.errno()).put("Cannot backup transaction file [to=").put(toTemp).put(']');
            }
        } finally {
            toTemp.trimTo(copyPathLen);
        }
    }

    static {
        MIGRATIONS.put(417, Mig505::migrate);
        // there is no tagged version with _meta 418, this is something unreleased
        MIGRATIONS.put(418, Mig506::migrate);
        MIGRATIONS.put(419, Mig600::migrate);
        MIGRATIONS.put(420, Mig605::migrate);
        MIGRATIONS.put(422, Mig607::migrate);
        MIGRATIONS.put(423, Mig608::migrate);
        MIGRATIONS.put(424, Mig609::migrate);
        MIGRATIONS.put(425, Mig614::migrate);
        MIGRATIONS.put(426, Mig620::migrate);
//        MIGRATIONS.put(427, Mig702::migrate);
    }
}
