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

package io.questdb.test.cutlass.pgwire;

import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;

@RunWith(Parameterized.class)
public class PGFunctionsTest extends BasePGTest {

    public PGFunctionsTest(LegacyMode legacyMode) {
        super(legacyMode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return legacyModeParams();
    }

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                st1.execute();
            }
            sink.clear();
            long openFilesBefore = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            try (PreparedStatement ps = connection.prepareStatement("select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "id[INTEGER],table_name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],o3MaxLag[BIGINT]\n" +
                                    "2,a,null,NONE,1000,300000000\n",
                            sink,
                            rs
                    );
                }
            }
            engine.releaseAllReaders();
            long openFilesAfter = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            Assert.assertEquals(openFilesBefore, openFilesAfter);
        });
    }
}
