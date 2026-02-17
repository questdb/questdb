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

import static io.questdb.cairo.TableUtils.META_OFFSET_TABLE_ID;

final class Mig505 {
    static void migrate(MigrationContext migrationContext) {
        MigrationActions.LOG.info().$("assigning table ID [table=").$(migrationContext.getTablePath()).I$();
        final long mem = migrationContext.getTempMemory(8);
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final long fd = migrationContext.getMetadataFd();

        MigrationActions.LOG.info().$("setting table id in [path=").$(path).I$();
        TableUtils.writeIntOrFail(
                ff,
                fd,
                META_OFFSET_TABLE_ID,
                migrationContext.getNextTableId(),
                mem,
                path
        );
    }
}
