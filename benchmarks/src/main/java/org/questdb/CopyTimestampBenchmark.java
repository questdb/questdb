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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;

public class CopyTimestampBenchmark {

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(".");

    public static void main(String[] args) throws SqlException {
        setup();
        testInsert();
    }

    public static void setup() throws SqlException {
        execute("drop table if exists tab");
        execute("create table tab (ts timestamp) timestamp(ts) partition by year");
        execute("drop table if exists tab1");
        execute("create table tab1 (ts timestamp_ns) timestamp(ts) partition by year");
        execute("insert into tab select timestamp_sequence(0, x) ts from long_sequence(100000000)");
    }

    public static void testInsert() throws SqlException {
        long start = System.nanoTime();
        execute("insert into tab1 select ts from tab");

        System.out.println("convert micro to nanos for 100_000_000 cost times: " + (System.nanoTime() - start) + " ns");
    }

    private static void execute(String ddl) throws SqlException {
        try (CairoEngine engine = new CairoEngine(CopyTimestampBenchmark.configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            CopyTimestampBenchmark.configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            engine.execute(ddl, sqlExecutionContext);
        }
    }
}
