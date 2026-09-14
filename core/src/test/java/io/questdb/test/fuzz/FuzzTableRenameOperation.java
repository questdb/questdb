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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;

/**
 * Renames the table the given WAL writer belongs to. Insert into a transaction
 * list via {@link FuzzTransactionGenerator#insertTableRename(io.questdb.std.ObjList, int, String)}
 * so that the rename is serialized against concurrent transactions and the WAL
 * writers are reopened afterwards; writer table tokens are stale after a rename.
 * <p>
 * Colliding renames are supported: when the target name is still in use, e.g.
 * table1 -&gt; table2 is requested while table2 -&gt; table3 has not been applied
 * yet, the rename is retried until the sibling rename frees the name or the
 * retry budget runs out.
 */
public class FuzzTableRenameOperation implements FuzzTransactionOperation {
    private static final Log LOG = LogFactory.getLog(FuzzTableRenameOperation.class);
    private static final long RENAME_RETRY_TIMEOUT_MS = 60_000;
    private final String newTableName;

    public FuzzTableRenameOperation(String newTableName) {
        this.newTableName = newTableName;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals) {
        try (
                Path fromPath = new Path();
                Path toPath = new Path();
                MemoryMARW mem = Vm.getCMARWInstance()
        ) {
            engine.releaseInactive();
            long deadline = System.currentTimeMillis() + RENAME_RETRY_TIMEOUT_MS;
            while (true) {
                // Resolve the current table name by dir name; the writer token can be stale
                // after a previous rename and table names can be reused across tables.
                TableToken token = engine.getTableTokenByDirName(tableWriter.getTableToken().getDirName());
                if (token == null) {
                    throw new IllegalStateException("table is missing before rename [table=" + tableWriter.getTableToken() + "]");
                }
                LOG.info().$("fuzz rename table [from=").$safe(token.getTableName()).$(", to=").$safe(newTableName).I$();
                try {
                    engine.rename(
                            AllowAllSecurityContext.INSTANCE,
                            fromPath,
                            mem,
                            token.getTableName(),
                            toPath,
                            newTableName
                    );
                    return true;
                } catch (CairoException ex) {
                    if (!Chars.contains(ex.getFlyweightMessage(), "cannot rename table, new name is already in use")
                            || System.currentTimeMillis() > deadline) {
                        throw ex;
                    }
                    // The target name is owned by a sibling table that is about to be renamed
                    // itself; wait for the colliding rename to free the name.
                    Os.sleep(10);
                }
            }
        }
    }
}
