/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;

import java.io.Closeable;


// todo: potentially refactor some of the logic into static functions
// todo: lots of cleanup
public class HydrateMetadataJob extends SynchronizedJob implements Closeable {
    public static final Log LOG = LogFactory.getLog(HydrateMetadataJob.class);
    public static boolean completed = false;
    ColumnVersionReader columnVersionReader = new ColumnVersionReader();
    CairoConfiguration configuration;
    MemoryCMR metaMem;
    MemoryCMR offsetMem;
    Path path = new Path();
    int position;
    ObjHashSet<TableToken> tokens = new ObjHashSet<>();

    public HydrateMetadataJob(CairoEngine engine) {
        engine.getTableTokens(tokens, false);
        position = 0;
        this.configuration = engine.getConfiguration();
    }

    @Override
    public void close() {
        if (metaMem != null) {
            metaMem.close();
            metaMem = null;
        }
        if (path != null) {
            path.close();
            path = null;
        }
        if (offsetMem != null) {
            offsetMem.close();
            offsetMem = null;
        }
    }

    @Override
    protected boolean runSerially() {
        if (completed) {
            close();
            return true;
        }

        if (position == 0) {
            LOG.info().$("hydration started [all_tables=").$(tokens.size()).I$();
        }

        if (position >= tokens.size()) {
            completed = true;
            close();
            LOG.info().$("hydration completed [user_tables=").$(CairoMetadata.INSTANCE.getTablesCount()).I$();
            tokens.clear();
            position = -1;
            return true;
        }

        final TableToken token = tokens.get(position);

        // skip system tables
        if (!token.isSystem()) {
            CairoMetadata.INSTANCE.hydrateTable(token, configuration, LOG, false);
        }

        position++;
        return false;
    }
}
