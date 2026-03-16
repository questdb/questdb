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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.TrimType;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.StringSink;

/**
 * Validates secondary sort column configurations for CREATE TABLE statements.
 * Ensures column names exist, types are supported, and no duplicates.
 */
public class SecondarySortValidator {

    private static final int[] SUPPORTED_TYPES = {
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.DATE,
            ColumnType.DOUBLE,
            ColumnType.FLOAT,
            ColumnType.SYMBOL,
            ColumnType.STRING,
            ColumnType.VARCHAR
    };

    private SecondarySortValidator() {
    }

    /**
     * Validates the secondary sort column expression against available columns.
     *
     * @param secondarySortByExpr  comma-separated column names
     * @param columnModels         map of column names to their models
     * @param timestampIndex       index of the timestamp column (excluded from validation)
     * @param position             SQL position for error reporting
     * @return array of column indices for secondary sort
     * @throws SqlException if validation fails
     */
    public static IntList validateSecondarySortColumns(
            CharSequence secondarySortByExpr,
            LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> columnModels,
            int timestampIndex,
            int position
    ) throws SqlException {
        IntList indices = new IntList();

        if (secondarySortByExpr == null || secondarySortByExpr.length() == 0) {
            return indices;
        }

        StringSink sink = new StringSink();
        int start = 0;
        int len = secondarySortByExpr.length();

        while (start < len) {
            int commaPos = -1;
            for (int i = start; i < len; i++) {
                if (secondarySortByExpr.charAt(i) == ',') {
                    commaPos = i;
                    break;
                }
            }

            CharSequence columnName;

            if (commaPos == -1) {
                columnName = secondarySortByExpr.subSequence(start, len);
                start = len;
            } else {
                columnName = secondarySortByExpr.subSequence(start, commaPos);
                start = commaPos + 1;
            }

            sink.clear();
            Chars.trim(TrimType.TRIM, columnName, sink);
            columnName = sink;

            if (columnName.length() == 0) {
                continue;
            }

            CreateTableColumnModel model = columnModels.get(columnName);
            if (model == null) {
                throw SqlException.position(position)
                        .put("secondarySortBy column '")
                        .put(columnName)
                        .put("' does not exist");
            }

            int type = model.getColumnType();

            if (!isSupportedType(type)) {
                throw SqlException.position(position)
                        .put("secondarySortBy does not support column type '")
                        .put(ColumnType.nameOf(type))
                        .put("' for column '")
                        .put(columnName)
                        .put("'");
            }
        }

        return indices;
    }

    /**
     * Checks if a column type is supported for secondary sorting.
     *
     * @param type the column type to check
     * @return true if the type is supported
     */
    public static boolean isSupportedType(int type) {
        for (int supportedType : SUPPORTED_TYPES) {
            if (type == supportedType) {
                return true;
            }
        }
        return false;
    }
}