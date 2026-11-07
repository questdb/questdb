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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.wal.MetadataService;

public interface TableMetadataChange {

    long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException;

    /**
     * Returns a column name that the change adds, removes or renames, see {@link #getChangedColumnNameCount()}. The
     * returned sequence may be a flyweight that the next call reuses.
     *
     * @param index from 0 to {@link #getChangedColumnNameCount()} - 1
     */
    default CharSequence getChangedColumnName(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns how many column names the change adds, removes or renames: the names an ADD COLUMN adds and a DROP
     * COLUMN removes, and both the old and the new name of a RENAME COLUMN. A base column change under such a name
     * can switch a bare select-list name of a dependent materialized view between a column and a call to a
     * zero-argument function, which the WAL apply job guards against before it applies the change.
     */
    default int getChangedColumnNameCount() {
        return 0;
    }

    /**
     * Returns a non-null reason when the operation on the base table may remove or retype a column that a dependent
     * live view reads. The WAL apply job then invalidates, with this reason, each live view on the base that reads
     * a column missing from the base's metadata after the change. No base operation invalidates a materialized view
     * when it applies: the view's next refresh that recomputes rows compiles the view's SQL and decides, apart from
     * the bare-name guard, see {@link #getChangedColumnNameCount()}.
     */
    default String liveViewInvalidationReason() {
        return null;
    }

    /**
     * Returns true if the operation should trigger the re-compilation of dependent views.
     */
    default boolean shouldCompileDependentViews() {
        return false;
    }
}
