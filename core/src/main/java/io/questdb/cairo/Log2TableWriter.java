/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.str.Path;

public class Log2TableWriter {
    public void applyWriteAheadLogData(
            TableWriter tableWriter,
            Path walPath,
            long rowLo,
            long rowHi,
            boolean inOrder,
            long timestampLo,
            long timestampHi
    ) {
        TableWriterMetadata metadata = tableWriter.getMetadata();
        if (metadata.getTimestampIndex() > -1L) {
            applyLogToDesignatedTimestampTable(metadata, metadata.getTimestampIndex(), tableWriter, walPath, rowLo, rowHi, inOrder, timestampLo, timestampHi);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void applyLogToDesignatedTimestampTable(
            TableWriterMetadata metadata,
            int timestampIndex,
            TableWriter tableWriter,
            Path walPath,
            long rowLo,
            long rowHi,
            boolean inOrder,
            long timestampLo,
            long timestampHi
    ) {
        int walLen = walPath.length();
        tableWriter.processWalCommit(
                walPath,
                0,
                timestampIndex,
                rowLo,
                rowHi,
                timestampLo,
                timestampHi
        );
        walPath.trimTo(walLen);
    }
}
