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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.CopyDataProgressReporter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;

public class ExportProgressReporter implements CopyDataProgressReporter {
    private static final Log LOG = LogFactory.getLog(ExportProgressReporter.class);
    private final StringSink copyIdHex = new StringSink();
    private SqlExecutionCircuitBreaker circuitBreaker;
    private CopyExportContext.ExportTaskEntry entry;
    private CharSequence tableName;

    public void of(SqlExecutionCircuitBreaker circuitBreaker, CopyExportContext.ExportTaskEntry entry, long copyId, CharSequence name) {
        this.circuitBreaker = circuitBreaker;
        this.entry = entry;
        this.copyIdHex.clear();
        Numbers.appendHex(copyIdHex, copyId, true);
        this.tableName = name;
    }

    @Override
    public void onProgress(Stage stage, long rows) {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        switch (stage) {
            case Start:
                entry.setTotalRowCount(rows);
                break;
            case Inserting:
                entry.setPopulatedRowCount(rows);
                LOG.info().$("populating temporary table progress [id=")
                        .$(copyIdHex)
                        .$(", table=")
                        .$(tableName)
                        .$(", rows=")
                        .$(rows).$(']')
                        .$();
                break;
            case Finish:
                entry.setPopulatedRowCount(rows);
                break;
        }
    }
}
