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

package io.questdb.cairo.mig;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;

class MigrationUtils {
    static MemoryCMARW openFileSafe(FilesFacade ff, LPSZ path, long readOffset) {
        long fileLen = ff.length(path);

        if (fileLen < 0) {
            throw CairoException.critical(ff.errno()).put("cannot read file length: ").put(path);
        }

        if (fileLen < readOffset + Long.BYTES) {
            throw CairoException.critical(0).put("File length ").put(fileLen).put(" is too small at ").put(path);
        }

        return Vm.getCMARWInstance(ff, path, Files.PAGE_SIZE, fileLen, MemoryTag.NATIVE_MIG_MMAP, CairoConfiguration.O_NONE);
    }
}
