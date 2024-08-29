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

package io.questdb.test;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;

public class JoinProfiling {

    private static final String ASOF_JOIN = "select * \n" +
            "from core_price \n" +
            "asof join fourotc_market_data_snapshot on core_price.event_sequence = fourotc_market_data_snapshot.snapshot_id\n" +
            "where send_ts in '2020-03-02'";
    private static final String HASHJOIN_SQL = "with md_cp_join as (\n" +
            "  select * from fourotc_market_data_snapshot\n" +
            "  inner join core_price  \n" +
            "  on core_price.event_sequence=fourotc_market_data_snapshot.snapshot_id\n" +
            "  where core_price.send_ts in '2020-03-02'\n" +
            "  and fourotc_market_data_snapshot.adapter_in_ts in '2020-03-02'\n" +
            ")\n" +
            "select snapshot_id, adapter_in_ts, event_sequence, send_ts from md_cp_join;";
    private static final boolean USE_ASOF_JOIN = true;
    private static final String EFFECTIVE_SQL = USE_ASOF_JOIN ? ASOF_JOIN : HASHJOIN_SQL;

    public static void main(String[] args) throws SqlException {
        String[] effectiveArgs = {
                "-d", "/opt/homebrew/var/questdb",
        };
        try (ServerMain serverMain = new ServerMain(effectiveArgs)) {
            serverMain.start();
            CairoEngine engine = serverMain.getEngine();
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                try (RecordCursorFactory factory = engine.select(EFFECTIVE_SQL, executionContext)) {

                    for (int i = 0; i < 10; i++) {
                        long c = 0;
                        long startNanos = System.nanoTime();
                        try (RecordCursor cursor = factory.getCursor(executionContext)) {
                            while (cursor.hasNext()) {
                                c++;
                            }
                            System.out.println("row count = " + c);
                            System.out.println("duration = " + (System.nanoTime() - startNanos) / 1000000 + "ms");
                        }
                    }
                }
            }
        }
    }
}
