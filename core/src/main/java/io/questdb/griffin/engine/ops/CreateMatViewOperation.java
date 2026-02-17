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

import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueryModel;

public interface CreateMatViewOperation extends TableStructure, Operation {

    static int matViewPeriodDelaySeconds(int delay, char delayUnit, int pos) throws SqlException {
        return switch (delayUnit) {
            case 's' -> delay;
            case 'm' -> delay * 60;
            case 'h' -> delay * 60 * 60;
            case 'd' -> delay * 24 * 60 * 60;
            default -> throw SqlException.position(pos).put("unsupported delay unit: ").put(delay).put(delayUnit)
                    .put(", supported units are 's', 'm', 'h', 'd'");
        };
    }

    static int matViewPeriodLengthSeconds(int length, char lengthUnit, int pos) throws SqlException {
        return switch (lengthUnit) {
            case 's' -> length;
            case 'm' -> length * 60;
            case 'h' -> length * 60 * 60;
            case 'd' -> length * 24 * 60 * 60;
            default -> throw SqlException.position(pos).put("unsupported length unit: ").put(length).put(lengthUnit)
                    .put(", supported units are 's', 'm', 'h', 'd'");
        };
    }

    static void validateMatViewPeriodLength(int length, char lengthUnit, int pos) throws SqlException {
        final int lengthSeconds = matViewPeriodLengthSeconds(length, lengthUnit, pos);
        if (lengthSeconds > 24 * 60 * 60) {
            throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(length).put(lengthUnit);
        }
    }

    CharSequence getBaseTableName();

    CreateTableOperation getCreateTableOperation();

    int getRefreshType();

    CharSequence getSqlText();

    int getTableNamePosition();

    CharSequence getVolumeAlias();

    int getVolumePosition();

    boolean ignoreIfExists();

    boolean isDeferred();

    void updateOperationFutureTableToken(TableToken tableToken);

    void validateAndUpdateMetadataFromModel(SqlExecutionContext sqlExecutionContext, FunctionFactoryCache functionFactoryCache, QueryModel queryModel) throws SqlException;

    void validateAndUpdateMetadataFromSelect(RecordMetadata selectMetadata, TableReaderMetadata baseTableMetadata, int scanDirection) throws SqlException;
}
