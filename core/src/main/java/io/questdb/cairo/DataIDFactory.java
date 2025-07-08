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

import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

public class DataIDFactory {
    public static final CharSequence DATA_ID_FILENAME = "_data_id";

    public static DataID open(CairoConfiguration configuration) {
        long dataFd = 0;
        long dataMem = 0;
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot());
            path.concat(DATA_ID_FILENAME);
            dataFd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            dataMem = TableUtils.mapRW(ff, dataFd, DataID.FILE_SIZE, MemoryTag.MMAP_DEFAULT);
        } catch (Throwable th) {
            if (dataFd != 0) {
                if (dataMem != 0) {
                    ff.munmap(dataMem, DataID.FILE_SIZE, MemoryTag.MMAP_DEFAULT);
                }
                ff.close(dataFd);
            }
            throw th;
        }

        return new DataID(configuration, dataFd, dataMem);
    }
}
