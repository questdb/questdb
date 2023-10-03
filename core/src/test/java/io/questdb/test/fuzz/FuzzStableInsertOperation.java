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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.Rnd;

public class FuzzStableInsertOperation implements FuzzTransactionOperation {
    private final int commit;
    private final String symbol;
    private final long timestamp;
    private final boolean withSymbol;

    public FuzzStableInsertOperation(long timestamp, int commit) {
        this.timestamp = timestamp;
        this.commit = commit;
        this.symbol = null;
        this.withSymbol = false;
    }

    public FuzzStableInsertOperation(long timestamp, int commit, String symbol) {
        this.timestamp = timestamp;
        this.commit = commit;
        this.symbol = symbol;
        this.withSymbol = true;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex) {
        TableWriter.Row row = tableWriter.newRow(getTimestamp());
        if (virtualTimestampIndex != -1) {
            row.putTimestamp(virtualTimestampIndex, getTimestamp());
        }
        row.putInt(1, commit);
        if (withSymbol) {
            row.putSym(2, getSymbol());
        }
        row.append();
        return false;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
