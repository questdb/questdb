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

package io.questdb.test.griffin.engine.functions.activity;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ExportActivityFunctionFactoryTest extends AbstractCairoTest {
    private SqlExecutionContextImpl adminUserContext;
    private SqlExecutionContextImpl regularUserContext;

    @Override
    public void setUp() {
        super.setUp();
        regularUserContext = new SqlExecutionContextImpl(engine, 1).with(new UserContext());
        regularUserContext.with(new AtomicBooleanCircuitBreaker(engine));
        adminUserContext = new SqlExecutionContextImpl(engine, 1).with(new AdminContext());
        adminUserContext.with(new AtomicBooleanCircuitBreaker(engine));
    }

    @Test
    public void testAdminCanSeeAllExports() throws Exception {
        assertMemoryLeak(() -> {
            CopyExportContext copyExportContext = engine.getCopyExportContext();
            CopyExportContext.ExportTaskEntry entry = copyExportContext.assignExportEntry(
                    regularUserContext.getSecurityContext(),
                    "select * from test_table",
                    "test",
                    null,
                    CopyExportContext.CopyTrigger.SQL
            );
            entry.setStartTime(1000, 1);
            entry.setPopulatedRowCount(2);
            entry.setTotalRowCount(3);
            entry.setPhase(CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE);

            try {
                assertQuery(
                        "worker_id\tusername\tstart_time\tphase\texport_path\trequest_source\texport_sql\tmessage\n" +
                                "1\tbob\t1970-01-01T00:00:00.001000Z\tpopulating_data_to_temp_table\ttest\tcopy sql\tselect * from test_table\trows: 2 / 3\n",
                        "select worker_id, username, start_time, phase, export_path, request_source, export_sql, message from export_activity()",
                        null,
                        false,
                        false
                );
            } finally {
                copyExportContext.releaseEntry(entry);
            }
        });
    }

    @Test
    public void testEmptyExportActivityWhenNoExports() throws Exception {
        assertQuery(
                "export_id\tworker_id\tusername\tstart_time\tphase\trequest_source\texport_path\texport_sql\tmessage\n",
                "select * from export_activity()",
                null,
                false,
                false
        );
    }

    @Test
    public void testNonAdminCanOnlySeeManagedExports() throws Exception {
        assertMemoryLeak(() -> {
            CopyExportContext copyExportContext = engine.getCopyExportContext();
            CopyExportContext.ExportTaskEntry userEntry = copyExportContext.assignExportEntry(
                    regularUserContext.getSecurityContext(),
                    "copy test_table to 'user_export.parquet'",
                    "user_export.parquet",
                    null,
                    CopyExportContext.CopyTrigger.SQL
            );
            userEntry.setStartTime(2000, 1);
            CopyExportContext.ExportTaskEntry adminEntry = copyExportContext.assignExportEntry(
                    adminUserContext.getSecurityContext(),
                    "copy test_table to 'admin_export.parquet'",
                    "admin_export.parquet",
                    null,
                    CopyExportContext.CopyTrigger.HTTP
            );
            adminEntry.setStartTime(3000, 2);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertQueryNoLeakCheck(
                        compiler,
                        "worker_id\tusername\tstart_time\tphase\texport_path\trequest_source\trequest_source1\texport_sql\tmessage\n" +
                                "1\tbob\t1970-01-01T00:00:00.002000Z\twait_to_run\tuser_export.parquet\tcopy sql\tcopy sql\tcopy test_table to 'user_export.parquet'\t\n" +
                                "2\tadmin\t1970-01-01T00:00:00.003000Z\twait_to_run\tadmin_export.parquet\thttp export\thttp export\tcopy test_table to 'admin_export.parquet'\t\n",
                        "select worker_id, username, start_time, phase, export_path, request_source, request_source, export_sql, message from export_activity() order by start_time",
                        "start_time",
                        adminUserContext,
                        true,
                        false
                );

                assertQueryNoLeakCheck(
                        compiler,
                        "worker_id\tusername\tstart_time\tphase\texport_path\trequest_source\texport_sql\tmessage\n" +
                                "1\tbob\t1970-01-01T00:00:00.002000Z\twait_to_run\tuser_export.parquet\tcopy sql\tcopy test_table to 'user_export.parquet'\t\n",
                        "select worker_id, username, start_time, phase, export_path, request_source, export_sql, message from export_activity()",
                        null,
                        regularUserContext,
                        false,
                        false
                );
            } finally {
                copyExportContext.releaseEntry(userEntry);
                copyExportContext.releaseEntry(adminEntry);
            }
        });
    }

    private static class AdminContext extends AllowAllSecurityContext {
        @Override
        public String getPrincipal() {
            return "admin";
        }
    }

    private static class UserContext extends ReadOnlySecurityContext {
        @Override
        public void authorizeSqlEngineAdmin() {
            throw CairoException.authorization().put("Access denied for ").put(getPrincipal()).put(" [SQL ENGINE ADMIN]");
        }

        @Override
        public String getPrincipal() {
            return "bob";
        }
    }
}