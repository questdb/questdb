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

package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A {@code _txn} that names more symbol columns than {@code _meta} declares must fail with something that
 * says so, naming the table and both counts.
 * <p>
 * The two are views of the same list -- {@code _txn}'s symbol area is written FROM
 * {@code denseSymbolMapWriters}, and that list is rebuilt from {@code _meta}'s live symbol columns on every
 * open -- so they can only disagree if the two files were not captured from the same table shape. Recovery
 * restoring a durable epoch is the way that happened in practice.
 * <p>
 * Untended, it surfaced as a bare {@code "Array index out of range: N"} from {@code ObjList.get}, thrown from
 * inside the {@code TableWriter} CONSTRUCTOR (initLastPartition -> performRecovery -> rollbackSymbolTables),
 * naming neither the table nor the mismatch. Tracing one back to its cause took a long time, which is the
 * whole reason the guard exists.
 * <p>
 * The mismatch is built here by putting a genuine earlier {@code _meta} -- one that predates a symbol column
 * -- back over the live one, with its metadataVersion re-stamped so nothing rejects it for the wrong reason.
 * That is exactly the shape recovery produced.
 */
public class TableWriterSymbolCountMismatchTest extends AbstractCairoTest {

    @Test
    public void testTxnNamingMoreSymbolsThanMetaFailsWithBothCounts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day");
            execute("insert into t values ('2024-10-01T00:00:00.000000Z', 's1', 1)");

            final TableToken tt = engine.verifyTableName("t");
            final String staleMeta = stashLiveMeta(tt); // ONE symbol column

            // A second symbol column, so the live _txn's symbol area grows to 2.
            execute("alter table t add column sym2 symbol");
            execute("insert into t values ('2024-10-01T01:00:00.000000Z', 's2', 2, 'x')");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Re-stamp the stale _meta to the live metadataVersion so the mismatch under test is the ONLY
            // thing wrong with it, then put it back in place: _meta says 1 symbol column, _txn says 2.
            alignMetadataVersion(tt, staleMeta);
            restoreOver(tt, staleMeta);

            // performRecovery is armed by a leftover table lock file -- a writer that died without releasing
            // it. That is the only path on which rollbackSymbolTables runs at open, and it is exactly the
            // situation the crash-fuzz was in when this surfaced.
            armStaleLock(tt);

            Throwable failure = null;
            try {
                engine.getWriter(tt, "test").close();
            } catch (Throwable th) {
                failure = th;
            }
            Assert.assertNotNull("a _txn naming more symbol columns than _meta declares must not open "
                    + "silently: rollbackSymbolTables would index past denseSymbolMapWriters", failure);
            {
                final String message = String.valueOf(failure.getMessage());
                Assert.assertFalse(
                        "the failure is still a bare ArrayIndexOutOfBounds from ObjList.get, thrown inside "
                                + "the TableWriter constructor and naming neither the table nor the counts: "
                                + message,
                        failure instanceof ArrayIndexOutOfBoundsException
                                || message.contains("Array index out of range")
                );
                Assert.assertTrue(
                        "the failure must name the table and BOTH counts so it can be traced without a "
                                + "debugger, got: " + message,
                        message.contains("symbol column count")
                                && message.contains("txnSymbolColumns=")
                                && message.contains("metaSymbolWriters=")
                );
            }
        });
    }

    private void armStaleLock(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final long fd = ff.openRW(TableUtils.lockName(p), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("could not create the stale lock file", fd > -1);
            ff.close(fd);
        }
    }

    private void alignMetadataVersion(TableToken tt, String staleMeta) {
        final long liveVersion;
        try (io.questdb.cairo.sql.TableMetadata meta = engine.getTableMetadata(tt)) {
            liveVersion = meta.getMetadataVersion();
        }
        try (Path p = new Path(); MemoryMARW mem = Vm.getCMARWInstance()) {
            p.of(staleMeta);
            mem.smallFile(engine.getConfiguration().getFilesFacade(), p.$(), MemoryTag.MMAP_DEFAULT);
            TableUtils.resetMetadataVersion(mem, liveVersion);
        }
    }

    private void restoreOver(TableToken tt, String staleMeta) {
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(staleMeta);
            dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
            TableUtils.replaceFileContent(
                    engine.getConfiguration().getFilesFacade(),
                    src.$(),
                    dst.$(),
                    engine.getConfiguration().getWriterFileOpenOpts()
            );
        }
    }

    private String stashLiveMeta(TableToken tt) {
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
            dst.of(engine.getConfiguration().getDbRoot()).concat("stale_meta_one_symbol");
            Assert.assertTrue("could not stash the one-symbol _meta",
                    engine.getConfiguration().getFilesFacade().copy(src.$(), dst.$()) >= 0);
            return dst.toString();
        }
    }
}
