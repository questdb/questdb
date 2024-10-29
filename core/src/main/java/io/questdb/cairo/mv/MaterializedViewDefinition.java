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
    private final TableToken baseTableToken;
    private final long fromMicros;
    private final CharSequence matViewSql;
    private final TableToken matViewToken;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;

    public MaterializedViewDefinition(
            TableToken matViewToken,
            String matViewSql,
            TableToken baseTableToken,
            long samplingInterval,
            char samplingIntervalUnit,
            long fromMicros,
            long toMicros,
            String timeZone,
            String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.matViewSql = matViewSql;
        this.baseTableToken = baseTableToken;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.fromMicros = fromMicros;
        this.toMicros = toMicros;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
    }

    public TableToken getBaseTableToken() {
        return baseTableToken;
    }

    public long getFromMicros() {
        return fromMicros;
    }

    public CharSequence getMatViewSql() {
        return matViewSql;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public char getSamplingIntervalUnit() {
        return samplingIntervalUnit;
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
}
