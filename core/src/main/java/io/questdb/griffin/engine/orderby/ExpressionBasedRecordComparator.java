/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * A RecordComparator that can compare records based on compiled expression functions.
 * This allows ORDER BY clauses to work with complex expressions like array access.
 */
public class ExpressionBasedRecordComparator implements RecordComparator {
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;

    private final ObjList<Function> orderByFunctions;
    private final IntList directions; // 0 for ASC, 1 for DESC
    private Record leftRecord;

    public ExpressionBasedRecordComparator(ObjList<Function> orderByFunctions, IntList directions) {
        this.orderByFunctions = orderByFunctions;
        this.directions = directions;
    }

    @Override
    public int compare(Record rightRecord) {
        // Compare each ORDER BY expression
        for (int i = 0, n = orderByFunctions.size(); i < n; i++) {
            Function function = orderByFunctions.getQuick(i);
            int direction = directions.getQuick(i);

            double valueLeft = evaluateFunction(function, leftRecord);
            double valueRight = evaluateFunction(function, rightRecord);

            int cmp;
            if (Double.isNaN(valueLeft) && Double.isNaN(valueRight)) {
                cmp = 0;
            } else if (Double.isNaN(valueLeft)) {
                cmp = -1;
            } else if (Double.isNaN(valueRight)) {
                cmp = 1;
            } else {
                cmp = Double.compare(valueLeft, valueRight);
            }

            if (cmp != 0) {
                return direction == ORDER_DIRECTION_DESCENDING ? -cmp : cmp;
            }
        }
        return 0;
    }

    @Override
    public void setLeft(Record record) {
        this.leftRecord = record;
    }

    private double evaluateFunction(Function function, Record record) {
        // This is a simplified approach
        try {
            // Set the current record context and evaluate
            return function.getDouble(record);
        } catch (Exception e) {
            // If function doesn't support double, try other types
            try {
                return function.getInt(record);
            } catch (Exception e2) {
                try {
                    return function.getLong(record);
                } catch (Exception e3) {
                    try {
                        // Try string comparison for non-numeric types
                        CharSequence str = function.getStrA(record);
                        if (str != null) {
                            return str.hashCode(); // Use hashCode for ordering
                        }
                    } catch (Exception e4) {
                        // If all types fail, return NaN
                        return Double.NaN;
                    }
                    return Double.NaN;
                }
            }
        }
    }
}
