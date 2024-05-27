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

package io.questdb.cliutil;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.RebuildColumnBase;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Utf8String;

/**
 * Utility class for command-line operations related to QuestDB.
 */
public class CmdUtils {
    
    /**
     * Rebuilds the index for a specified column in a QuestDB table.
     *
     * @param params the parameters required for the column rebuild, including table path, partition, and column name
     * @param ri the RebuildColumnBase instance used to perform the rebuild operation
     * @throws IllegalArgumentException if any of the required parameters are invalid
     */
    static void runColumnRebuild(RebuildColumnCommandArgs params, RebuildColumnBase ri) {
        final Log log = LogFactory.getLog("recover-var-index");

        // Validate parameters
        if (params == null) {
            throw new IllegalArgumentException("Parameters cannot be null.");
        }
        if (params.tablePath == null || params.tablePath.isEmpty()) {
            throw new IllegalArgumentException("Table path cannot be null or empty.");
        }
        if (params.partition == null || params.partition.isEmpty()) {
            throw new IllegalArgumentException("Partition cannot be null or empty.");
        }
        if (params.column == null || params.column.isEmpty()) {
            throw new IllegalArgumentException("Column cannot be null or empty.");
        }

        // Initialize rebuild operation
        ri.of(new Utf8String(params.tablePath));
        
        try {
            log.info().$("Starting reindex for table: ").$(params.tablePath)
                      .$(", partition: ").$(params.partition)
                      .$(", column: ").$(params.column).$();
            ri.reindex(params.partition, params.column);
            log.info().$("Reindex completed successfully for table: ").$(params.tablePath)
                      .$(", partition: ").$(params.partition)
                      .$(", column: ").$(params.column).$();
        } catch (CairoException ex) {
            log.error().$("Error during reindex: ").$(ex.getFlyweightMessage()).$();
            // Optionally, rethrow the exception or handle it as needed
            // throw ex;
        }
    }
}

