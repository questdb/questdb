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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;

/**
 * The RIGHT and FULL joins with join keys that a nested loop LEFT join without join keys runs after,
 * although the query writes them after it. For a right row without a match, such an outer join returns
 * one row with NULL in every column of the tables written before it, the columns of the LEFT join
 * included. Run first, the LEFT join never sees that row. Run after the outer join, it must return the
 * row once with NULL in its own columns instead of evaluating its ON clause, which may be true for NULL,
 * as <code>NULL &gt;= NULL</code> is.
 * <p>
 * The LEFT join consults the records of the outer joins for each master row. Every join between an outer
 * join and the LEFT join reads its master row by row, so the record of the outer join describes the row
 * that the LEFT join reads: SqlOptimiser lets only INNER, CROSS and LEFT joins and keyed RIGHT and FULL
 * joins be written after such a LEFT join, and none of them moves a master that is a join to the side it
 * hashes, as a join cannot return its rows by row id. A later outer join that NULL-extends its own left
 * side is in the list too.
 * <p>
 * A WHERE or INNER join filter that is a runtime constant gates the joins below it, see
 * RuntimeConstGateRecordCursorFactory. A false gate returns no row and opens none of the joins below it, so an
 * outer join below it has no cursor in that execution, or the one of an earlier execution. The LEFT join then
 * reads only the rows that a RIGHT or FULL join above the gate returns without a master row. Such an outer join
 * runs after the one below the gate, so the query writes it after that one, and after the LEFT join, which puts
 * it in the list. As it NULL-extends each of those rows, the check returns true for them whatever the outer
 * join below the gate holds, and leaves that join out while it has no record.
 */
public final class OuterJoinNullCheck {
    private final ObjList<Record> records = new ObjList<>();
    private final ObjList<OuterJoinRecordSource> sources = new ObjList<>();

    public void add(OuterJoinRecordSource source) {
        sources.add(source);
    }

    // Returns true when an outer join NULL-extended the tables written before it in the current master row.
    boolean isNullExtended() {
        for (int i = 0, n = records.size(); i < n; i++) {
            final Record record = records.getQuick(i);
            if (record instanceof RightOuterJoinRecord rightJoinRecord) {
                if (!rightJoinRecord.hasMaster()) {
                    return true;
                }
            } else if (!((FullOuterJoinRecord) record).hasMaster()) {
                return true;
            }
        }
        return false;
    }

    // Reads the records of the outer joins. The LEFT join calls this once its master cursor is open, which opens
    // the cursors of the outer joins, except those below a false gate: an outer join that never opened a cursor
    // has no record, and the check leaves it out, see the class comment.
    void of() {
        records.clear();
        for (int i = 0, n = sources.size(); i < n; i++) {
            final Record record = sources.getQuick(i).getOuterJoinRecord();
            if (record != null) {
                assert record instanceof RightOuterJoinRecord || record instanceof FullOuterJoinRecord;
                records.add(record);
            }
        }
    }
}
