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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private final CharSequence baseTableName;
    private final long sampleByFromEpochMicros;
    private final long sampleByPeriodMicros;
    private final char samplingIntervalUnit;
    private final TableToken tableToken;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;
    private final CharSequence viewSql;

    public MaterializedViewDefinition(
            TableToken matViewToken,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            long fromMicros,
            long toMicros,
            String timeZone,
            String timeZoneOffset
    ) {
        this.tableToken = matViewToken;
        this.viewSql = query;
        this.baseTableName = baseTableName;
        this.sampleByPeriodMicros = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.sampleByFromEpochMicros = fromMicros;
        this.toMicros = toMicros;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
    }

    public CharSequence getBaseTableName() {
        return baseTableName;
    }

    public long getFromMicros() {
        return sampleByFromEpochMicros;
    }

    public TableToken getMatViewToken() {
        return tableToken;
    }

    public CharSequence getQuery() {
        return viewSql;
    }

    public long getSampleByFromEpochMicros() {
        return sampleByFromEpochMicros;
    }

    public long getSampleByInterval() {
        return sampleByPeriodMicros;
    }

    public char getSamplingIntervalUnit() {
        return samplingIntervalUnit;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public long getToMicros() {
        return toMicros;
    }

    public CharSequence getViewSql() {
        return viewSql;
    }

//    public void setBaseTableName(CharSequence baseTableName) {
//        this.baseTableName = baseTableName;
//    }

//    public void setSampleByFromEpochMicros(long sampleByFromEpochMicros) {
//        this.sampleByFromEpochMicros = sampleByFromEpochMicros;
//    }
//
//    public void setSampleByPeriodMicros(long sampleByPeriodMicros) {
//        this.sampleByPeriodMicros = sampleByPeriodMicros;
//    }
//
//    public void setTableToken(TableToken tableToken) {
//        this.tableToken = tableToken;
//    }
//
//    public void setViewSql(CharSequence viewSql) {
//        this.viewSql = viewSql;
//    }
}
