/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.mp.WorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGFunctionsTest extends BasePGTest {

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server.getPort(), true, true)) {
                    try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                        st1.execute();
                    }
                    sink.clear();
                    long openFilesBefore = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
                    try (PreparedStatement ps = connection.prepareStatement("select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()")) {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertResultSet(
                                    "id[INTEGER],name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],o3MaxLag[BIGINT]\n" +
                                            "1,sys.text_import_log,ts,DAY,1000,300000000\n" +
                                            "2,a,null,NONE,1000,300000000\n",
                                    sink,
                                    rs
                            );
                        }
                    }
                    long openFilesAfter = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();

                    Assert.assertEquals(openFilesBefore, openFilesAfter);
                }
            }
        });
    }
}
