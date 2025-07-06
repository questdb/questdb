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

import io.questdb.std.Files;

public class LinuxAccessor {
    public static final int IN_CLOSE_WRITE;
    public static final int IN_CREATE;
    public static final int IN_MODIFY;
    public static final int IN_MOVED_TO;

    public static int inotifyAddWatch(long fd, long pathPtr, int flags) {
        return inotifyAddWatch(Files.toOsFd(fd), pathPtr, flags);
    }

    public static short inotifyRmWatch(long fd, int wd) {
        return inotifyRmWatch(Files.toOsFd(fd), wd);
    }

    public static int readEvent(long fd, long buf, int bufSize) {
        return readEvent(Files.toOsFd(fd), buf, bufSize);
    }

    public static int readPipe(long fd) {
        return readPipe(Files.toOsFd(fd));
    }

    public static int writePipe(long fd) {
        return writePipe(Files.toOsFd(fd));
    }

    private static native int inotifyAddWatch(int fd, long pathPtr, int flags);

    private static native short inotifyRmWatch(int fd, int wd);

    private static native int readEvent(int fd, long buf, int bufSize);

    private static native int readPipe(int fd);

    private static native int writePipe(int fd);

    static native short getEventFilenameOffset();

    static native short getEventFilenameSizeOffset();

    static native int getINCLOSEWRITE();

    static native int getINCREATE();

    static native int getINMODIFY();

    static native int getINMOVEDTO();

    static native short getSizeofEvent();

    static native int inotifyInit();

    static native long pipe();

    static {
        IN_CLOSE_WRITE = getINCLOSEWRITE();
        IN_CREATE = getINCREATE();
        IN_MODIFY = getINMODIFY();
        IN_MOVED_TO = getINMOVEDTO();
    }
}
