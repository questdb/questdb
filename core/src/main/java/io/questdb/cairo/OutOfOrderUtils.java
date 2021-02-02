/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public final class OutOfOrderUtils {

    public static void appendTxnToPath(Path path, long txn) {
        path.put("-n-").put(txn);
    }

    static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        if (ff.mkdirs(path, mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
        }
    }

    static long getVarColumnLength(
            long srcLo,
            long srcHi,
            long srcFixAddr,
            long srcFixSize,
            long srcVarSize
    ) {
        final long lo = OutOfOrderUtils.findVarOffset(srcFixAddr, srcLo, srcHi, srcVarSize);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = OutOfOrderUtils.findVarOffset(srcFixAddr, srcHi + 1, srcFixSize / Long.BYTES, srcVarSize);
        }
        return hi - lo;
    }

    static long findVarOffset(long srcFixAddr, long srcLo, long srcHi, long srcVarSize) {
        long lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
        if (lo > -1) {
            return lo;
        }

        while (++srcLo < srcHi) {
            lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
            if (lo > -1) {
                return lo;
            }
        }

        return srcVarSize;
    }
}
