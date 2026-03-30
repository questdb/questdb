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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ev.ExpiringViewDefinition;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class CreateExpiringViewOperationBuilderImpl implements CreateExpiringViewOperationBuilder, Mutable {
    private String baseTableName;
    private int baseTableNamePosition;
    private long cleanupIntervalMicros;
    private int expiryColumnIndex = -1;
    private String expiryPredicateSql;
    private int expiryType = ExpiringViewDefinition.EXPIRY_TYPE_CUSTOM;
    private boolean ifNotExists;
    private String viewName;
    private int viewNamePosition;

    @Override
    public CreateExpiringViewOperation build(CharSequence sqlText) {
        return new CreateExpiringViewOperationImpl(
                Chars.toString(sqlText),
                viewName,
                viewNamePosition,
                baseTableName,
                baseTableNamePosition,
                expiryPredicateSql,
                expiryType,
                expiryColumnIndex,
                cleanupIntervalMicros,
                ifNotExists
        );
    }

    @Override
    public void clear() {
        viewName = null;
        viewNamePosition = 0;
        baseTableName = null;
        baseTableNamePosition = 0;
        expiryPredicateSql = null;
        expiryType = ExpiringViewDefinition.EXPIRY_TYPE_CUSTOM;
        expiryColumnIndex = -1;
        cleanupIntervalMicros = 0;
        ifNotExists = false;
    }

    @Override
    public CharSequence getTableName() {
        return viewName;
    }

    public void setBaseTableName(String baseTableName, int position) {
        this.baseTableName = baseTableName;
        this.baseTableNamePosition = position;
    }

    public void setCleanupIntervalMicros(long cleanupIntervalMicros) {
        this.cleanupIntervalMicros = cleanupIntervalMicros;
    }

    public void setExpiryColumnIndex(int expiryColumnIndex) {
        this.expiryColumnIndex = expiryColumnIndex;
    }

    public void setExpiryPredicateSql(String expiryPredicateSql) {
        this.expiryPredicateSql = expiryPredicateSql;
    }

    public void setExpiryType(int expiryType) {
        this.expiryType = expiryType;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setViewName(String viewName, int position) {
        this.viewName = viewName;
        this.viewNamePosition = position;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create expiring view ");
        sink.put(viewName);
        sink.putAscii(" on ");
        sink.put(baseTableName);
        sink.putAscii(" expire when ");
        sink.put(expiryPredicateSql);
    }
}
