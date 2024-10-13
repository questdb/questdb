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

package io.questdb.std.filewatch;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;

public class WindowsAccessorImpl implements WindowsAccessor {
    public static final WindowsAccessorImpl INSTANCE = new WindowsAccessorImpl();
    private final Log LOG = LogFactory.getLog(WindowsAccessorImpl.class);

    @Override
    public void closeDirectory(long pWatch) {
        closeDirectory0(pWatch);
    }

    @Override
    public long getFileName(long pWatch) {
        return getFileName0(pWatch);
    }

    @Override
    public int getFileNameSize(long pWatch) {
        return getFileNameSize0(pWatch);
    }

    @Override
    public long openDirectory(LPSZ dirName) {
        return openDirectory0(dirName);
    }

    @Override
    public boolean readDirectoryChanges(long pWatch) {
        if (readDirectoryChanges0(pWatch)) {
            return true;
        }
        LOG.critical().$("could not read directory changes [errno=").$(Os.errno()).I$();
        return false;
    }

    @Override
    public void stopWatch(long pWatch) {
        stopWatch0(pWatch);
    }

    private static native long closeDirectory0(long pWatch);

    private static native long getFileName0(long pWatch);

    private static native int getFileNameSize0(long pWatch);

    private static native long openDirectory0(long pDirName);

    private static long openDirectory0(LPSZ dirName) {
        return openDirectory0(dirName.ptr());
    }

    private static native boolean readDirectoryChanges0(long pWatch);

    private static native long stopWatch0(long pWatch);
}
