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

package io.questdb.test.griffin.engine.functions.table.parquet;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.questdb.PropertyKey;

/**
 * Asserts the RemoteParquetFsProvider SPI dispatch fires for URIs the stub
 * provider claims, and stays out of the way for ordinary local paths. The
 * stub provider registers via META-INF/services on the test classpath.
 */
public class RemoteParquetFsProviderDispatchTest extends AbstractCairoTest {

    @Before
    public void setUpTest() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "false");
        super.setUp();
        inputRoot = root;
        StubRemoteParquetFsProvider.reset();
    }

    @Test
    public void testCanHandleConsultedForLocalPath() throws Exception {
        // Local paths must still consult the stub's canHandle (returns false)
        // and route to the local file path - no resolveLocal / expandGlob calls.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            writeParquet("rpd_local/a.parquet", "src");
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory ignored = compiler.compile(
                         "select count(*) from read_parquet('rpd_local/a.parquet')", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                // compile is enough; just need the SPI dispatch to be exercised.
            }
            Assert.assertTrue(
                    "canHandle should be consulted for the URI; called " + StubRemoteParquetFsProvider.canHandleCalls.get(),
                    StubRemoteParquetFsProvider.canHandleCalls.get() >= 1
            );
            Assert.assertEquals(
                    "local path must not dispatch to resolveLocal",
                    0, StubRemoteParquetFsProvider.resolveLocalCalls.get()
            );
            Assert.assertEquals(
                    "local path must not dispatch to expandAndResolveGlob",
                    0, StubRemoteParquetFsProvider.expandGlobCalls.get()
            );
        });
    }

    @Test
    public void testSingleStubUriDispatchesToResolveLocal() throws Exception {
        // For stub:// URIs without glob characters the provider's resolveLocal is
        // invoked and its returned local path is then opened by the OSS code. We
        // prime the stub to return a real local parquet so the open succeeds.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("rpd_single/data.parquet", "src");
            final String localAbs = root + java.io.File.separator + "rpd_single" + java.io.File.separator + "data.parquet";
            StubRemoteParquetFsProvider.localSingleResponse = new Utf8String(localAbs);

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory ignored = compiler.compile(
                         "select count(*) from read_parquet('stub://bucket/data.parquet')", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                // compile drives the SPI dispatch.
            }
            Assert.assertEquals(
                    "stub:// must route through resolveLocal",
                    1, StubRemoteParquetFsProvider.resolveLocalCalls.get()
            );
            Assert.assertEquals(
                    "single-file URI must NOT call expandAndResolveGlob",
                    0, StubRemoteParquetFsProvider.expandGlobCalls.get()
            );
        });
    }

    @Test
    public void testResolveLocalThrowsPropagatesAsSqlException() throws Exception {
        // Provider failure surfaces as the SqlException the user sees, with the
        // user-given URI in the position. Locks in the error-path behaviour the
        // RBAC denial test in Enterprise will later rely on.
        assertMemoryLeak(() -> {
            StubRemoteParquetFsProvider.throwOnResolve = SqlException.$(0, "stub denied");
            try {
                engine.execute("select count(*) from read_parquet('stub://bucket/blocked.parquet')", sqlExecutionContext);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                io.questdb.test.tools.TestUtils.assertContains(e.getFlyweightMessage(), "stub denied");
            }
            Assert.assertEquals(1, StubRemoteParquetFsProvider.resolveLocalCalls.get());
        });
    }

    /**
     * Mirrors the writeParquet helper in HivePartitionedReadParquetFunctionTest -
     * we don't extend that class, so we re-implement the minimum we need.
     */
    private static void writeParquet(String relativePath, String tableName) {
        try (io.questdb.std.str.Path path = new io.questdb.std.str.Path();
             io.questdb.std.str.Path dir = new io.questdb.std.str.Path();
             io.questdb.griffin.engine.table.parquet.PartitionDescriptor desc = new io.questdb.griffin.engine.table.parquet.PartitionDescriptor();
             io.questdb.cairo.TableReader reader = engine.getReader(tableName)) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, io.questdb.std.FilesFacadeImpl.INSTANCE.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            io.questdb.griffin.engine.table.parquet.PartitionEncoder.populateFromTableReader(reader, desc, 0);
            io.questdb.griffin.engine.table.parquet.PartitionEncoder.encode(desc, path);
            Assert.assertTrue(io.questdb.std.Files.exists(path.$()));
        }
    }
}
