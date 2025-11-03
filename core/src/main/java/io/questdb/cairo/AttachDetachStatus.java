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

package io.questdb.cairo;

public enum AttachDetachStatus {
    OK(false),
    DETACH_ERR_ACTIVE(false),
    DETACH_ERR_MISSING_PARTITION(false),
    DETACH_ERR_MISSING_PARTITION_DIR,
    DETACH_ERR_COPY_META,
    DETACH_ERR_HARD_LINK,
    DETACH_ERR_COPY,
    DETACH_ERR_ALREADY_DETACHED(false),
    DETACH_ERR_MKDIR,
    ATTACH_ERR_PARTITION_EXISTS(false),
    ATTACH_ERR_RENAME,
    ATTACH_ERR_COPY,
    ATTACH_ERR_MISSING_PARTITION(false),
    ATTACH_ERR_DIR_EXISTS(false),
    ATTACH_ERR_EMPTY_PARTITION(false);

    private final boolean isCritical;

    AttachDetachStatus() {
        this(true);
    }

    AttachDetachStatus(boolean isCritical) {
        this.isCritical = isCritical;
    }

    public CairoException getException(
            int position,
            AttachDetachStatus status,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTs
    ) {
        assert status != OK;
        CairoException exception = isCritical ?
                CairoException.critical(CairoException.METADATA_VALIDATION) :
                CairoException.partitionManipulationRecoverable();
        String statusName = status.name();
        String operation = statusName.split("_")[0].toLowerCase();
        exception.put("could not ").put(operation)
                .put(" partition [table=").put(tableToken != null ? tableToken.getTableName() : "<null>")
                .put(", detachStatus=").put(statusName)
                .put(", partitionTimestamp=").ts(timestampType, partitionTs)
                .put(", partitionBy=").put(PartitionBy.toString(partitionBy))
                .put(']')
                .position(position);
        return exception;
    }
}
