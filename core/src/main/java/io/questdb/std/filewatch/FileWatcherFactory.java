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

import io.questdb.FileEventCallback;
import io.questdb.KqueueFileWatcher;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

// File watchers were added for the purpose of config file reload,
// but since they aren't reliable reload_config() SQL function
// was introduced.
//
// Still, file watchers may be handy in the future, so we kept them.
public class FileWatcherFactory {

    public static FileWatcher getFileWatcher(@NotNull Utf8Sequence filePath, @NotNull FileEventCallback callback) {
        if (filePath.size() == 0) {
            throw new IllegalArgumentException("file to watch cannot be empty");
        }
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueFileWatcher(filePath, callback);
        } else if (Os.isWindows()) {
            return new FileWatcherWindows(WindowsAccessorImpl.INSTANCE, filePath, callback);
        } else {
            return new LinuxFileWatcher(LinuxAccessorFacadeImpl.INSTANCE, EpollFacadeImpl.INSTANCE, filePath, callback);
        }
    }
}
