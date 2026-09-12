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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;

/**
 * Slot-local view of a logical probe record and copied payload. It borrows both
 * sources. The probe source must provide independent symbol flyweights per slot;
 * build symbols remain available on misses, including an entirely empty build.
 */
public final class HashJoinGroupByRecord extends OuterJoinRecord implements SymbolTableSource {
    private final int probeColumnCount;
    private FrozenHashJoinBuild.Probe buildProbe;
    private SymbolTableSource probeSymbols;

    public HashJoinGroupByRecord(int probeColumnCount, ColumnTypes payloadTypes) {
        super(probeColumnCount, NullRecordFactory.getInstance(payloadTypes));
        this.probeColumnCount = probeColumnCount;
    }

    /** Drop execution-local backing only after all slot and output consumers finish. */
    public void clear() {
        super.of(null, null);
        hasSlave(false);
        probeSymbols = null;
        buildProbe = null;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return columnIndex < probeColumnCount
                ? probeSymbols.getSymbolTable(columnIndex)
                : buildProbe.getSymbolTable(columnIndex - probeColumnCount);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return columnIndex < probeColumnCount
                ? probeSymbols.newSymbolTable(columnIndex)
                : buildProbe.newSymbolTable(columnIndex - probeColumnCount);
    }

    public void of(Record probeRecord, SymbolTableSource probeSymbols, FrozenHashJoinBuild.Probe buildProbe) {
        this.probeSymbols = probeSymbols;
        this.buildProbe = buildProbe;
        super.of(probeRecord, buildProbe.getRecord());
        hasSlave(false);
    }

    /** Select a real payload after next()/recordAt(), or typed nulls for an ON miss. */
    public void setHasMatch(boolean hasMatch) {
        hasSlave(hasMatch);
    }

}
