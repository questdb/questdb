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

package io.questdb.cliutil;

import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

public class RecoverVarIndex {
    public static void main(String[] args) {
        String[] params = parseCommandArgs(args);
        if (params == null) {
            // Invalid params, usage already printed
            return;
        }

        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        String tablePath = args[0];
        String columnName = args[1];

        Path path1 = new Path().of(tablePath)
                .concat(columnName)
                .concat(".d")
                .$();

        Path path2 = new Path().of(tablePath)
                .concat(columnName)
                .concat(".i")
                .$();

        long maxOffset = ff.length(path1);
        MemoryCMR roMem = new MemoryCMRImpl(
                ff,
                path1,
                maxOffset,
                MemoryTag.NATIVE_DEFAULT
        );

        MemoryCMARW rwMem = new MemoryCMARWImpl(
                ff,
                path2,
                8 * 1024 * 1024,
                0,
                MemoryTag.NATIVE_DEFAULT,
                0
        );

        // index
        long offset = 0;
        while (offset < maxOffset) {
            int len = roMem.getInt(offset);
            rwMem.putLong(offset);

            if (len > -1) {
                offset += 4 + len * 2L;
            } else {
                offset += 4;
            }
        }
        rwMem.putLong(offset);

        rwMem.close();
    }


    static String[] parseCommandArgs(String[] args) {
        if (args.length != 3 || !"-c".equals(args[1])) {
            printUsage();
            return null;
        }
        return args;
    }

    private static void printUsage() {
        System.out.println("usage: " + RecoverVarIndex.class.getName() + " <table_path> -c <column_name>");
    }
}