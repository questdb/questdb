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

package io.questdb.test.cutlass.http;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.*;

public class ScoreboardHandlingTest extends AbstractBootstrapTest {

    static String backupRoot;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        unchecked(() -> {
            backupRoot = temp.newFolder("backups").getAbsolutePath();
        });
    }

    @Test
    public void testVacuumRerunOnWindows() throws Exception {
        createDummyConfiguration();
        assertMemoryLeak(() -> {
            try (
                    ServerMain main = startWithEnvVariables(
                            PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true"

                    );
                    TestHttpClient httpClient = new TestHttpClient()
            ) {
                TestUtils.executeSQLViaPostgres(
                        main.getConfiguration().getPGWireConfiguration().getDefaultUsername(),
                        main.getConfiguration().getPGWireConfiguration().getDefaultPassword(),
                        main.getPgWireServerPort(),
                        "create table trades(ts timestamp, ticker symbol index, value double) timestamp(ts) partition by day wal"
                );

                exec(
                        httpClient,
                        main,
                        "{\"query\":\"select ticker from trades\",\"columns\":[{\"name\":\"ticker\",\"type\":\"SYMBOL\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}",
                        "select ticker from trades"
                );

                dml(httpClient, main, "insert into trades values ('2023-07-01T00:00:00.000000', 'ABC', 1.0)");
                dml(httpClient, main, "insert into trades values ('2023-07-02T00:00:00.000000', 'DEF', 2.0)");

                // this is a bad naming schema: "updated" is a txn, not the number of updated rows
                exec(
                        httpClient,
                        main,
                        "{\"dml\":\"OK\",\"updated\":3}",
                        "update trades set value = value + 1 where ticker = 'ABC'"
                );

                ddl(httpClient, main, "rename table trades to old_trades");
                ddl(httpClient, main, "alter table old_trades set param maxUncommittedRows =  100000");
                ddl(httpClient, main, "alter table old_trades set type wal");
                ddl(httpClient, main, "alter table old_trades set type bypass wal");
                ddl(httpClient, main, "alter table old_trades set type wal");
                ddl(httpClient, main, "alter table old_trades suspend wal");
                ddl(httpClient, main, "alter table old_trades resume wal");
                ddl(httpClient, main, "alter table old_trades detach partition list '2023-07-01'");

                Files.rename(
                        Path.getThreadLocal(root).concat("db").concat("old_trades").concat("2023-07-01.detached").$(),
                        Path.getThreadLocal2(root).concat("db").concat("old_trades").concat("2023-07-01.attachable").$()
                );

                ddl(httpClient, main, "alter table old_trades attach partition list '2023-07-01'");
                ddl(httpClient, main, "alter table old_trades drop partition list '2023-07-01'");

                exec(
                        httpClient,
                        main,
                        "{\"query\":\"select * from old_trades where ts in '2022'\",\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"ticker\",\"type\":\"SYMBOL\"},{\"name\":\"value\",\"type\":\"DOUBLE\"}],\"timestamp\":0,\"dataset\":[],\"count\":0}",
                        "select * from old_trades where ts in '2022'"
                );

                dml(httpClient, main, "insert into old_trades values ('2023-07-09T00:00:00.000000', 'ABC', 1.0)");
                exec(httpClient, main, "{\"dml\":\"OK\",\"updated\":10}", "update old_trades set value = value + 1 where ticker = 'ABC'");
                ddl(httpClient, main, "truncate table old_trades");
                ddl(httpClient, main, "vacuum table old_trades");
                ddl(httpClient, main, "alter table old_trades alter column ticker drop index");
                ddl(httpClient, main, "alter table old_trades alter column ticker add index");
                ddl(httpClient, main, "alter table old_trades alter column ticker nocache");
                ddl(httpClient, main, "alter table old_trades alter column ticker cache");
                ddl(httpClient, main, "alter table old_trades rename column ticker to sym");
                ddl(httpClient, main, "alter table old_trades drop column sym");
                ddl(httpClient, main, "alter table old_trades add column currency symbol");
                assertEventually(() -> ddl(httpClient, main, "rename table old_trades to older_trades"));
                ddl(httpClient, main, "drop table older_trades");
                TestUtils.executeSQLViaPostgres(
                        main.getConfiguration().getPGWireConfiguration().getDefaultUsername(),
                        main.getConfiguration().getPGWireConfiguration().getDefaultPassword(),
                        main.getPgWireServerPort(),
                        "create table trades (ticker symbol)"
                );
            }
        });
    }

    private static void ddl(TestHttpClient httpClient, ServerMain main, String sql) {
        httpClient.assertGet(
                "/exec",
                "{\"ddl\":\"OK\"}",
                sql,
                "localhost",
                main.getHttpServerPort(),
                null,
                null,
                null
        );
    }

    private static void dml(TestHttpClient httpClient, ServerMain main, String sql) {
        httpClient.assertGet(
                "/exec",
                "{\"dml\":\"OK\"}",
                sql,
                "localhost",
                main.getHttpServerPort(),
                null,
                null,
                null
        );
    }

    private static void exec(TestHttpClient httpClient, ServerMain main, String expectedResponse, String sql) {
        httpClient.assertGet(
                "/exec",
                expectedResponse,
                sql,
                "localhost",
                main.getHttpServerPort(),
                null,
                null,
                null
        );
    }
}
