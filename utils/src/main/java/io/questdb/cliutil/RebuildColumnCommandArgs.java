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

package io.questdb.cliutil;

import io.questdb.std.Files;

public class RebuildColumnCommandArgs {
    String column;
    String partition;
    String tablePath;

    private static void printUsage(String command) {
        System.out.println("usage: " + command + " <table_path> [-p <partition_name>] [-c <column_name>]");
    }

    static RebuildColumnCommandArgs parseCommandArgs(String[] args, String command) {

        if (args.length > 5 || args.length % 2 != 1) {
            printUsage(command);
            return null;
        }

        RebuildColumnCommandArgs res = new RebuildColumnCommandArgs();
        res.tablePath = args[0];
        for (int i = 1, n = args.length; i < n; i += 2) {
            if ("-p".equals(args[i])) {
                if (res.partition == null) {
                    res.partition = args[i + 1];
                } else {
                    System.err.println("-p parameter can be only used once");
                    printUsage(command);
                    return null;
                }
            }

            if ("-c".equals(args[i])) {
                if (res.column == null) {
                    res.column = args[i + 1];
                } else {
                    System.err.println("-c parameter can be only used once");
                    printUsage(command);
                    return null;
                }
            }
        }

        if (res.tablePath.endsWith(String.valueOf(Files.SEPARATOR))) {
            res.tablePath = "";
        }

        return res;
    }
}
