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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

// Validation-only compilation must not mutate server state. These tests cover the compile
// paths whose side effect runs inline during compilation (not via deferred operation
// execution): ALTER TABLE SET TYPE writes the WAL convert file, SUSPEND / RESUME WAL toggle
// the sequencer's suspended flag, and REINDEX rebuilds index files. Each test validates the
// statement (asserting no mutation), then runs it for real (asserting the mutation takes effect).
//
// Validation runs the full compile path (it no longer short-circuits at the ALTER/REINDEX
// dispatch), so it also resolves the target table. Validating a statement against a
// non-existent table therefore reports "table does not exist" rather than passing as
// syntactically valid; testValidateAlterMissingTableFails / testValidateReindexMissingTableFails
// pin that behavior.
public class ValidationOnlyAlterTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // SUSPEND WAL is gated on dev mode; enable it so validation reaches the suspend path.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testValidateAlterMissingTableFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                validate("ALTER TABLE no_such_table SUSPEND WAL");
                Assert.fail("validation must reject ALTER against a non-existent table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
            }
        });
    }

    @Test
    public void testValidateReindexDoesNotReindex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rix (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO rix VALUES ('2024-01-01T00:00:00','A',1.0),('2024-01-01T01:00:00','B',2.0)");
            engine.releaseAllWriters();

            final TableToken token = engine.verifyTableName("rix");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01");
                final int plen = path.size();

                final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE);
                Assert.assertTrue("setup: index key file must exist", ff.exists(keyFile));

                // Remove the index key file so a rebuild is detectable.
                Assert.assertTrue(ff.removeQuiet(keyFile));
                Assert.assertFalse(ff.exists(PostingIndexUtils.keyFileName(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE)));

                validate("REINDEX TABLE rix COLUMN sym LOCK EXCLUSIVE");
                Assert.assertFalse("validation must not rebuild the index", ff.exists(PostingIndexUtils.keyFileName(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE)));

                execute("REINDEX TABLE rix COLUMN sym LOCK EXCLUSIVE");
                Assert.assertTrue("real reindex must rebuild the index", ff.exists(PostingIndexUtils.keyFileName(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE)));
            }
        });
    }

    @Test
    public void testValidateReindexMissingTableFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                validate("REINDEX TABLE no_such_table COLUMN sym LOCK EXCLUSIVE");
                Assert.fail("validation must reject REINDEX against a non-existent table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
            }
        });
    }

    @Test
    public void testValidateResumeDoesNotResume() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            engine.getTableSequencerAPI().suspendTable(token, ErrorTag.NONE, "test suspension");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));

            validate("ALTER TABLE x RESUME WAL");
            Assert.assertTrue("validation must not resume the table", engine.getTableSequencerAPI().isSuspended(token));

            execute("ALTER TABLE x RESUME WAL");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    @Test
    public void testValidateSetTypeDoesNotCreateConvertFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            Assert.assertFalse(convertFileExists());

            validate("ALTER TABLE x SET TYPE BYPASS WAL");
            Assert.assertFalse("validation must not create the convert file", convertFileExists());

            execute("ALTER TABLE x SET TYPE BYPASS WAL");
            Assert.assertTrue(convertFileExists());
        });
    }

    @Test
    public void testValidateSuspendDoesNotSuspend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));

            validate("ALTER TABLE x SUSPEND WAL");
            Assert.assertFalse("validation must not suspend the table", engine.getTableSequencerAPI().isSuspended(token));

            execute("ALTER TABLE x SUSPEND WAL");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    private static boolean convertFileExists() {
        final TableToken token = engine.verifyTableName("x");
        final Path path = Path.PATH.get().of(configuration.getDbRoot()).concat(token).concat(WalUtils.CONVERT_FILE_NAME);
        return Files.exists(path.$());
    }

    private static void validate(String sql) throws Exception {
        final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
        ctx.setValidationOnly(true);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.compile(sql, ctx);
        } finally {
            ctx.setValidationOnly(false);
        }
    }
}
