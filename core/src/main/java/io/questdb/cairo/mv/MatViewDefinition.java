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
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public class MatViewDefinition {
    public static final String MAT_VIEW_DEFINITION_FILE_NAME = "_mv";
    public static final int MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;

    private final String baseTableName;
    // is not persisted, parsed from timeZoneOffset
    private final long fixedOffset;
    private final String matViewSql;
    private final TableToken matViewToken;
    // is not persisted, parsed from timeZone
    private final @Nullable TimeZoneRules rules;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final @Nullable String timeZone;
    private final @Nullable String timeZoneOffset;
    // is not persisted, parsed from samplingInterval and samplingIntervalUnit;
    // access must be synchronized as this object is not thread-safe
    private final TimestampSampler timestampSampler;

    public MatViewDefinition(
            @NotNull TableToken matViewToken,
            @NotNull String matViewSql,
            @NotNull String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.matViewSql = matViewSql;
        this.baseTableName = baseTableName;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;

        try {
            this.timestampSampler = TimestampSamplerFactory.getInstance(
                    samplingInterval,
                    samplingIntervalUnit,
                    0
            );
        } catch (SqlException e) {
            throw CairoException.critical(0).put("invalid sampling interval and/or unit: ").put(samplingInterval)
                    .put(", ").put(samplingIntervalUnit);
        }

        if (timeZone != null) {
            try {
                this.rules = Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, timeZone);
            } catch (NumericException e) {
                throw CairoException.critical(0).put("invalid timezone: ").put(timeZone);
            }
        } else {
            this.rules = null;
        }

        if (timeZoneOffset != null) {
            final long val = Timestamps.parseOffset(timeZoneOffset);
            if (val == Numbers.LONG_NULL) {
                throw CairoException.critical(0).put("invalid offset: ").put(timeZoneOffset);
            }
            this.fixedOffset = Numbers.decodeLowInt(val) * MINUTE_MICROS;
        } else {
            this.fixedOffset = 0;
        }
    }

    public static void append(@NotNull MatViewDefinition matViewDefinition, @NotNull AppendableBlock block) {
        block.putStr(matViewDefinition.getBaseTableName());
        block.putLong(matViewDefinition.getSamplingInterval());
        block.putChar(matViewDefinition.getSamplingIntervalUnit());
        block.putStr(matViewDefinition.getTimeZone());
        block.putStr(matViewDefinition.getTimeZoneOffset());
        block.putStr(matViewDefinition.getMatViewSql());
    }

    public static void append(@NotNull MatViewDefinition matViewDefinition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        append(matViewDefinition, block);
        block.commit(MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public static MatViewDefinition readFrom(@NotNull BlockFileReader reader, Path path, int rootLen, final TableToken matViewToken) {
        path.trimTo(rootLen).concat(matViewToken.getDirName()).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        // Iterate through the block until we find the one we recognize.
        while (cursor.hasNext()) {
            final MatViewDefinition matViewDefinition = loadMatViewDefinition(cursor.next(), matViewToken);
            if (matViewDefinition != null) {
                return matViewDefinition;
            }
        }
        throw CairoException.critical(0)
                .put("cannot read materialized view definition, block not found [path=").put(path)
                .put(']');
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public long getFixedOffset() {
        return fixedOffset;
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

    public @Nullable String getTimeZone() {
        return timeZone;
    }

    public @Nullable String getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public TimestampSampler getTimestampSampler() {
        return timestampSampler;
    }

    public @Nullable TimeZoneRules getTzRules() {
        return rules;
    }

    private static MatViewDefinition loadMatViewDefinition(final ReadableBlock block, final TableToken matViewToken) {
        if (block.type() != MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE) {
            // Unknown block.
            return null;
        }

        long offset = 0;
        final CharSequence baseTableName = block.getStr(offset);
        if (baseTableName == null || baseTableName.length() == 0) {
            throw CairoException.critical(0)
                    .put("base table name for materialized view is empty [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }
        offset += Vm.getStorageLength(baseTableName);
        final String baseTableNameStr = Chars.toString(baseTableName);

        final long samplingInterval = block.getLong(offset);
        offset += Long.BYTES;

        final char samplingIntervalUnit = block.getChar(offset);
        offset += Character.BYTES;

        final CharSequence timeZone = block.getStr(offset);
        offset += Vm.getStorageLength(timeZone);
        final String timeZoneStr = Chars.toString(timeZone);

        final CharSequence timeZoneOffset = block.getStr(offset);
        offset += Vm.getStorageLength(timeZoneOffset);
        final String timeZoneOffsetStr = Chars.toString(timeZoneOffset);

        final CharSequence matViewSql = block.getStr(offset);
        if (matViewSql == null || matViewSql.length() == 0) {
            throw CairoException.critical(0)
                    .put("materialized view SQL is empty [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }
        final String matViewSqlStr = Chars.toString(matViewSql);

        return new MatViewDefinition(
                matViewToken,
                matViewSqlStr,
                baseTableNameStr,
                samplingInterval,
                samplingIntervalUnit,
                timeZoneStr,
                timeZoneOffsetStr
        );
    }
}
