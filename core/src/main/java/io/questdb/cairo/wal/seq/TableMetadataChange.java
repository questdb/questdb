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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.wal.MetadataService;

public interface TableMetadataChange {

    long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException;

    /**
     * Should return a non-null string in case when the operation on the base table leaves dependent mat views in invalid state.
     */
    default String matViewInvalidationReason() {
        return null;
    }

    /**
     * Returns true if the operation should trigger the re-compilation of dependent views.
     */
    default boolean shouldCompileDependentViews() {
        return false;
    }
}
