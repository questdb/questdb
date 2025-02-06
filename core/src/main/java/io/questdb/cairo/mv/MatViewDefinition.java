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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.meta.AppendableBlock;
import io.questdb.cairo.meta.MetaFileWriter;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public class MatViewDefinition {
    public static final String MAT_VIEW_DEFINITION_FILE_NAME = "_mv";
    public static final byte MAT_VIEW_DEFINITION_FORMAT_FLAGS = 0;
    public static final short MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;
    public static final byte MAT_VIEW_DEFINITION_FORMAT_MSG_VERSION = 0;

    private final String baseTableName;
    private final String matViewSql;
    private final TableToken matViewToken;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;

    // is not persisted, parsed from timeZoneOffset
    private long fixedOffset;
    // is not persisted, parsed from timeZone
    private @Nullable TimeZoneRules rules;

    public MatViewDefinition(
            TableToken matViewToken,
            String matViewSql,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.matViewSql = matViewSql;
        this.baseTableName = baseTableName;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;

        if (timeZone != null) {
            try {
                this.rules = Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, timeZone);
            } catch (NumericException e) {
                throw CairoException.critical(0).put("invalid timezone: ").put(timeZone);
            }
        }

        if (timeZoneOffset != null) {
            final long val = Timestamps.parseOffset(timeZoneOffset);
            if (val == Numbers.LONG_NULL) {
                throw CairoException.critical(0).put("invalid offset: ").put(timeZoneOffset);
            }
            this.fixedOffset = Numbers.decodeLowInt(val) * MINUTE_MICROS;
        }
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public String getMatViewSql() {
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

    public long getFixedOffset() {
        return fixedOffset;
    }

    public static void commitTo(final MetaFileWriter writer, final MatViewDefinition matViewDefinition) {
        final AppendableBlock mem = writer.append();
        writeTo(mem, matViewDefinition);
        mem.commit(
                MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE,
                MAT_VIEW_DEFINITION_FORMAT_MSG_VERSION,
                MAT_VIEW_DEFINITION_FORMAT_FLAGS
        );
        writer.commit();
    }

    public @Nullable TimeZoneRules getTzRules() {
        return rules;
    }

    public static void writeTo(final AppendableBlock mem, final MatViewDefinition matViewDefinition) {
        mem.putStr(matViewDefinition.getBaseTableName());
        mem.putLong(matViewDefinition.getSamplingInterval());
        mem.putChar(matViewDefinition.getSamplingIntervalUnit());
        mem.putStr(matViewDefinition.getTimeZone());
        mem.putStr(matViewDefinition.getTimeZoneOffset());
        mem.putStr(matViewDefinition.getMatViewSql());
    }
}
