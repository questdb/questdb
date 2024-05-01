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

package io.questdb;

public class InotifyAccessor {
    public static final int IN_CLOSE_WRITE;
    public static final int IN_CREATE;
    public static final int IN_MODIFY;
    public static final int IN_MOVED_TO;

    static native short getEventFilenameOffset();

    static native short getEventFilenameSizeOffset();

    static native int getINCLOSEWRITE();

    static native int getINCREATE();

    static native int getINMODIFY();

    static native int getINMOVEDTO();

    static native short getSizeofEvent();

    static native int inotifyAddWatch(int fd, long pathPtr, int flags);

    static native int inotifyInit();

    static native short inotifyRmWatch(int fd, int wd);

    static native long pipe();

    static native int readEvent(int fd, long buf, int bufSize);

    static native int readPipe(int fd);

    static native int writePipe(int fd);

    static {
        IN_CLOSE_WRITE = getINCLOSEWRITE();
        IN_CREATE = getINCREATE();
        IN_MODIFY = getINMODIFY();
        IN_MOVED_TO = getINMOVEDTO();
    }
}
