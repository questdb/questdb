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
import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Paths;

public class FileWatcherWindows extends FileWatcher {
    private final WindowsAccessor accessor;
    private final Path dirPath = new Path();
    private final DirectUtf8Sink fileName = new DirectUtf8Sink(64);
    private long pWatch;

    public FileWatcherWindows(
            WindowsAccessor accessor,
            @NotNull Utf8Sequence filePath,
            @NotNull FileEventCallback callback
    ) {
        super(callback);
        this.accessor = accessor;
        this.dirPath.of(filePath).parent();
        this.fileName.put(Paths.get(filePath.toString()).getFileName().toString());
        this.pWatch = accessor.openDirectory(dirPath.$());
        if (pWatch == -1) {
            Misc.free(dirPath);
            Misc.free(fileName);
            throw CairoException.critical(Os.errno()).put("could not create configuration watch [dirPath=").put(dirPath).put(']');
        }
    }

    @Override
    protected void _close() {
        if (pWatch != 0) {
            accessor.closeDirectory(pWatch);
            pWatch = 0;
        }
        Misc.free(dirPath);
        Misc.free(fileName);
    }

    @Override
    protected void releaseWait() {
        accessor.stopWatch(pWatch);
    }

    @Override
    protected void waitForChange() {
        if (accessor.readDirectoryChanges(pWatch)) {
            callback.onFileEvent();
        }
    }
}
