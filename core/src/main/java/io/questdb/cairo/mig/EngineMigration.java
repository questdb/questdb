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
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.META_OFFSET_VERSION;
import static io.questdb.cairo.TableUtils.openFileRWOrFail;

public class EngineMigration {

    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final IntObjHashMap<MigrationAction> MIGRATIONS = new IntObjHashMap<>();

    public static void migrateEngineTo(CairoEngine engine, int latestVersion, boolean force) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final CairoConfiguration configuration = engine.getConfiguration();
        int tempMemSize = 8;
        long mem = Unsafe.malloc(tempMemSize, MemoryTag.NATIVE_DEFAULT);

        try (
                MemoryARW virtualMem = Vm.getARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
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
                int currentVersion = TableUtils.readIntOrFail(
                        ff,
                        upgradeFd,
                        0,
                        mem,
                        path
                );

                if (currentVersion >= latestVersion) {
                    LOG.info().$("table structures are up to date").$();
                    ff.close(upgradeFd);
                    upgradeFd = -1;
                }
            }

            if (upgradeFd != -1) {
                try {
                    LOG.info().$("upgrading database [version=").$(latestVersion).I$();
                    if (upgradeTables(context, latestVersion)) {
                        TableUtils.writeIntOrFail(
                                ff,
                                upgradeFd,
                                0,
                                latestVersion,
                                mem,
                                path
                        );
                    }
                } finally {
                    Vm.bestEffortClose(
                            ff,
                            LOG,
                            upgradeFd,
                            true,
                            Integer.BYTES
                    );
                }
            }
        } finally {
            Unsafe.free(mem, tempMemSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static @Nullable MigrationAction getMigrationToVersion(int version) {
        return MIGRATIONS.get(version);
    }

    static void backupFile(FilesFacade ff, Path src, Path toTemp, String backupName, int version) {
        // make a copy
        int copyPathLen = toTemp.length();
        try {
            toTemp.concat(backupName).put(".v").put(version);
            for (int i = 1; ff.exists(toTemp.$()); i++) {
                // if backup file already exists
                // add .<num> at the end until file name is unique
                LOG.info().$("backup dest exists [to=").$(toTemp).I$();
                toTemp.trimTo(copyPathLen);
                toTemp.concat(backupName).put(".v").put(version).put(".").put(i);
            }

            LOG.info().$("backing up [file=").$(src).$(",to=").$(toTemp).I$();
            if (ff.copy(src.$(), toTemp.$()) < 0) {
                throw CairoException.instance(ff.errno()).put("Cannot backup transaction file [to=").put(toTemp).put(']');
            }
        } finally {
            toTemp.trimTo(copyPathLen);
        }
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

            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(rootLen);
                    path.concat(pUtf8NameZ);
                    copyPath.trimTo(rootLen);
                    copyPath.concat(pUtf8NameZ);
                    final int plen = path.length();
                    path.concat(TableUtils.META_FILE_NAME);

                    if (ff.exists(path.$())) {
                        final long fd = openFileRWOrFail(ff, path);
                        try {
                            int currentTableVersion = TableUtils.readIntOrFail(ff, fd, META_OFFSET_VERSION, mem, path);
                            if (currentTableVersion < latestVersion) {
                                LOG.info()
                                        .$("upgrading [path=").$(path)
                                        .$(",fromVersion=").$(currentTableVersion)
                                        .$(",toVersion=").$(latestVersion)
                                        .I$();

                                copyPath.trimTo(plen);
                                backupFile(ff, path, copyPath, TableUtils.META_FILE_NAME, currentTableVersion);

                                path.trimTo(plen);
                                context.of(path, copyPath, fd);

                                for (int ver = currentTableVersion + 1; ver <= latestVersion; ver++) {
                                    final MigrationAction migration = getMigrationToVersion(ver);
                                    if (migration != null) {
                                        try {
                                            LOG.info().$("upgrading table [path=").$(path).$(",toVersion=").$(ver).I$();
                                            migration.migrate(context);
                                            path.trimTo(plen);
                                        } catch (Throwable e) {
                                            LOG.error().$("failed to upgrade table path=")
                                                    .$(path.trimTo(plen))
                                                    .$(", exception: ")
                                                    .$(e).$();
                                            throw e;
                                        }
                                    }

                                    TableUtils.writeIntOrFail(
                                            ff,
                                            fd,
                                            META_OFFSET_VERSION,
                                            ver,
                                            mem,
                                            path.trimTo(plen)
                                    );
                                }
                            }
                        } finally {
                            ff.close(fd);
                            path.trimTo(plen);
                            copyPath.trimTo(plen);
                        }
                    }
                }
            });
            LOG.info().$("upgraded tables to ").$(latestVersion).$();
        }
        return updateSuccess.get();
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
    }
}
