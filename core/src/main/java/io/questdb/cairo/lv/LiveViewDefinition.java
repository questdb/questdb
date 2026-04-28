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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LiveViewDefinition {
    public static final String LIVE_VIEW_DEFINITION_FILE_NAME = "_lv";
    public static final int LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;

    private final String baseTableName;
    private final TableToken baseTableToken;
    private final char lagUnit;
    private final long lagValue;
    private final GenericRecordMetadata metadata;
    private final char retentionUnit;
    private final long retentionValue;
    private final String viewName;
    private final String viewSql;

    public LiveViewDefinition(
            String viewName,
            String viewSql,
            String baseTableName,
            TableToken baseTableToken,
            long lagValue,
            char lagUnit,
            long retentionValue,
            char retentionUnit,
            GenericRecordMetadata metadata
    ) {
        this.viewName = viewName;
        this.viewSql = viewSql;
        this.baseTableName = baseTableName;
        this.baseTableToken = baseTableToken;
        this.lagValue = lagValue;
        this.lagUnit = lagUnit;
        this.retentionValue = retentionValue;
        this.retentionUnit = retentionUnit;
        this.metadata = metadata;
    }

    public static void append(@NotNull LiveViewDefinition definition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        block.putStr(definition.viewSql);
        block.putStr(definition.baseTableName);
        block.putLong(definition.lagValue);
        block.putChar(definition.lagUnit);
        block.putLong(definition.retentionValue);
        block.putChar(definition.retentionUnit);
        block.commit(LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public static long toMicros(long value, char unit) {
        if (value == 0) {
            return 0;
        }
        return switch (unit) {
            case 's' -> MicrosTimestampDriver.INSTANCE.fromSeconds(value);
            case 'm' -> MicrosTimestampDriver.INSTANCE.fromMinutes((int) value);
            case 'h' -> MicrosTimestampDriver.INSTANCE.fromHours((int) value);
            case 'd' -> MicrosTimestampDriver.INSTANCE.fromDays((int) value);
            default -> value;
        };
    }

    /**
     * Reads only the base table name from the {@code _lv} definition file.
     * Used during startup to resolve the base table token before constructing
     * the full {@link LiveViewDefinition}.
     */
    public static String readBaseTableName(
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken liveViewToken
    ) {
        path.trimTo(rootLen).concat(liveViewToken.getDirName()).concat(LIVE_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                long offset = 0;
                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                return Chars.toString(block.getStr(offset));
            }
        }
        throw CairoException.critical(0)
                .put("cannot read live view definition, block not found [path=").put(path).put(']');
    }

    public static LiveViewDefinition readFrom(
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken liveViewToken,
            @Nullable TableToken baseTableToken,
            @NotNull GenericRecordMetadata metadata
    ) {
        path.trimTo(rootLen).concat(liveViewToken.getDirName()).concat(LIVE_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                long offset = 0;
                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                String viewSql = Chars.toString(viewSqlCs);

                CharSequence baseTableNameCs = block.getStr(offset);
                offset += Vm.getStorageLength(baseTableNameCs);
                String baseTableName = Chars.toString(baseTableNameCs);

                long lagValue = block.getLong(offset);
                offset += Long.BYTES;
                char lagUnit = block.getChar(offset);
                offset += Character.BYTES;
                long retentionValue = block.getLong(offset);
                offset += Long.BYTES;
                char retentionUnit = block.getChar(offset);

                return new LiveViewDefinition(
                        liveViewToken.getTableName(),
                        viewSql,
                        baseTableName,
                        baseTableToken,
                        lagValue,
                        lagUnit,
                        retentionValue,
                        retentionUnit,
                        metadata
                );
            }
        }
        throw CairoException.critical(0)
                .put("cannot read live view definition, block not found [path=").put(path).put(']');
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public TableToken getBaseTableToken() {
        return baseTableToken;
    }

    public long getLagMicros() {
        return toMicros(lagValue, lagUnit);
    }

    public char getLagUnit() {
        return lagUnit;
    }

    public long getLagValue() {
        return lagValue;
    }

    public GenericRecordMetadata getMetadata() {
        return metadata;
    }

    public long getRetentionMicros() {
        return toMicros(retentionValue, retentionUnit);
    }

    public char getRetentionUnit() {
        return retentionUnit;
    }

    public long getRetentionValue() {
        return retentionValue;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewSql() {
        return viewSql;
    }
}
