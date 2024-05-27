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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CmdUtils {
    private static final Log log = LogFactory.getLog("recover-var-index");
    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    static void runColumnRebuild(RebuildColumnCommandArgs params, RebuildColumnBase ri) {
        if (!validateParams(params)) {
            log.error().$("Invalid parameters provided").$();
            return;
        }

        ri.of(new Utf8String(params.tablePath));
        
        Runnable rebuildTask = () -> {
            try {
                log.info().$("Starting rebuild for table: ").$(params.tablePath)
                        .$(", partition: ").$(params.partition)
                        .$(", column: ").$(params.column).$();
                
                ri.reindex(params.partition, params.column);
                
                log.info().$("Successfully rebuilt index for table: ").$(params.tablePath)
                        .$(", partition: ").$(params.partition)
                        .$(", column: ").$(params.column).$();
            } catch (CairoException ex) {
                log.error().$("Failed to rebuild index for table: ").$(params.tablePath)
                        .$(", partition: ").$(params.partition)
                        .$(", column: ").$(params.column).$(" - ")
                        .$(ex.getFlyweightMessage()).$();
            } catch (Exception e) {
                log.error().$("Unexpected error during rebuild: ").$(e.getMessage()).$();
            }
        };

        executor.submit(rebuildTask);
    }

    private static boolean validateParams(RebuildColumnCommandArgs params) {
        if (params == null || params.tablePath == null || params.partition == null || params.column == null) {
            return false;
        }
        if (!Files.exists(Paths.get(params.tablePath))) {
            log.error().$("Table path does not exist: ").$(params.tablePath).$();
            return false;
        }
        return true;
    }

    static void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.error().$("Executor did not terminate").$();
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
