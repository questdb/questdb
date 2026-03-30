/*******************************************************************************
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

package io.questdb.cairo.ev;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

public class ExpiringViewDefinition implements Mutable {
    public static final String EXPIRING_VIEW_DEFINITION_FILE_NAME = "_ev";
    public static final int EXPIRING_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;

    public static final int EXPIRY_TYPE_CUSTOM = 0;
    public static final int EXPIRY_TYPE_TIMESTAMP_COMPARE = 1;

    private String baseTableName;
    private long cleanupIntervalMicros;
    private int expiryColumnIndex = -1;
    private String expiryPredicateSql;
    private int expiryType = EXPIRY_TYPE_CUSTOM;
    private TableToken viewToken;

    public static void append(@NotNull ExpiringViewDefinition definition, @NotNull AppendableBlock block) {
        block.putStr(definition.baseTableName);
        block.putStr(definition.expiryPredicateSql);
        block.putInt(definition.expiryType);
        block.putInt(definition.expiryColumnIndex);
        block.putLong(definition.cleanupIntervalMicros);
    }

    public static void append(@NotNull ExpiringViewDefinition definition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        append(definition, block);
        block.commit(EXPIRING_VIEW_DEFINITION_FORMAT_MSG_TYPE);
        writer.commit();
    }

    @Override
    public void clear() {
        viewToken = null;
        baseTableName = null;
        expiryPredicateSql = null;
        expiryType = EXPIRY_TYPE_CUSTOM;
        expiryColumnIndex = -1;
        cleanupIntervalMicros = 0;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public long getCleanupIntervalMicros() {
        return cleanupIntervalMicros;
    }

    public int getExpiryColumnIndex() {
        return expiryColumnIndex;
    }

    public String getExpiryPredicateSql() {
        return expiryPredicateSql;
    }

    public int getExpiryType() {
        return expiryType;
    }

    public TableToken getViewToken() {
        return viewToken;
    }

    public void init(
            @NotNull TableToken viewToken,
            @NotNull String baseTableName,
            @NotNull String expiryPredicateSql,
            int expiryType,
            int expiryColumnIndex,
            long cleanupIntervalMicros
    ) {
        this.viewToken = viewToken;
        this.baseTableName = baseTableName;
        this.expiryPredicateSql = expiryPredicateSql;
        this.expiryType = expiryType;
        this.expiryColumnIndex = expiryColumnIndex;
        this.cleanupIntervalMicros = cleanupIntervalMicros;
    }

    public boolean isTimestampCompare() {
        return expiryType == EXPIRY_TYPE_TIMESTAMP_COMPARE;
    }

    public static void readDefinitionFromBlock(
            ExpiringViewDefinition destDefinition,
            ReadableBlock block,
            TableToken viewToken
    ) {
        assert block.type() == EXPIRING_VIEW_DEFINITION_FORMAT_MSG_TYPE;

        long offset = 0;

        final CharSequence baseTableNameCs = block.getStr(offset);
        if (baseTableNameCs == null || baseTableNameCs.isEmpty()) {
            throw CairoException.critical(0)
                    .put("base table name for expiring view is empty [view=")
                    .put(viewToken.getTableName())
                    .put(']');
        }
        // Convert to String immediately — getStr() returns a flyweight that the next call overwrites
        final String baseTableName = Chars.toString(baseTableNameCs);
        offset += Vm.getStorageLength(baseTableNameCs);

        final CharSequence expiryPredicateSqlCs = block.getStr(offset);
        if (expiryPredicateSqlCs == null || expiryPredicateSqlCs.isEmpty()) {
            throw CairoException.critical(0)
                    .put("expiry predicate SQL is empty [view=")
                    .put(viewToken.getTableName())
                    .put(']');
        }
        final String expiryPredicateSql = Chars.toString(expiryPredicateSqlCs);
        offset += Vm.getStorageLength(expiryPredicateSqlCs);

        final int expiryType = block.getInt(offset);
        offset += Integer.BYTES;

        final int expiryColumnIndex = block.getInt(offset);
        offset += Integer.BYTES;

        final long cleanupIntervalMicros = block.getLong(offset);

        destDefinition.init(
                viewToken,
                baseTableName,
                expiryPredicateSql,
                expiryType,
                expiryColumnIndex,
                cleanupIntervalMicros
        );
    }
}
