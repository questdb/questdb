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

import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS;
import static io.questdb.cairo.TableUtils.META_OFFSET_O3_MAX_LAG;

final class Mig600 {
    static void migrate(MigrationContext migrationContext) {
        MigrationActions.LOG.info().$("configuring default o3MaxLag [table=").$(migrationContext.getTablePath()).I$();
        final Path path = migrationContext.getTablePath();
        final FilesFacade ff = migrationContext.getFf();
        final long tempMem = migrationContext.getTempMemory(8);
        final long fd = migrationContext.getMetadataFd();

        TableUtils.writeIntOrFail(
                ff,
                fd,
                META_OFFSET_MAX_UNCOMMITTED_ROWS,
                migrationContext.getConfiguration().getMaxUncommittedRows(),
                tempMem,
                path
        );

        TableUtils.writeLongOrFail(
                ff,
                fd,
                META_OFFSET_O3_MAX_LAG,
                migrationContext.getConfiguration().getO3MaxLag(),
                tempMem,
                path
        );
    }
}
