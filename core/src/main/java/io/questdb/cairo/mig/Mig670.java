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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

import static io.questdb.cairo.mig.MigrationActions.SEQ_DIR_670;

final class Mig670 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);

    static void migrate(MigrationContext migrationContext) {
        final Path path = migrationContext.getTablePath();
        path.concat(SEQ_DIR_670).$();

        final FilesFacade ff = migrationContext.getFf();
        if (ff.exists(path) && ff.rmdir(path) != 0) {
            LOG.error().$("failed to remove dir [path='").utf8(path).$(']').$();
        }
    }
}
