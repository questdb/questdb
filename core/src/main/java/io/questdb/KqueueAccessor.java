/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class KqueueAccessor {
    public static final short EVFILT_READ;
    public static final short SIZEOF_KEVENT;
    public static final short EVFILT_VNODE;
    public static final short NOTE_WRITE;
    public static final short DATA_OFFSET;
    public static final short EVFILT_WRITE;
    public static final short EV_ADD;
    public static final short EV_DELETE;
    public static final short EV_ONESHOT;
    public static final short EV_CLEAR;
    public static final short FD_OFFSET;
    public static final short FILTER_OFFSET;
    public static final short FLAGS_OFFSET;

    public static native short getDataOffset();

    public static native short getEvAdd();

    public static native short getEvDelete();
    public static native short getEvClear();

    public static native short getEvOneshot();

    public static native short getEvfiltRead();

    public static native short getEvfiltWrite();
    public static native short getEvfiltVnode();
    public static native short getNoteWrite();

    public static native short getFdOffset();

    public static native short getFilterOffset();

    public static native short getFlagsOffset();

    public static native short getSizeofKevent();

    public static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout);
    public static native int keventRegister(int kq, long changeList, int nChanges);
    public static native int keventGetBlocking(int kq, long eventList, int nEvents);
    public static native long evSet(long ident, int filter, int flags, int fflags, long data);

    public static native int kqueue();

    public static native long pipe();

    public static native int readPipe(int fd);

    public static native int writePipe(int fd);

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        EVFILT_VNODE = getEvfiltVnode();
        NOTE_WRITE = getNoteWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
        EV_DELETE = getEvDelete();
        EV_CLEAR = getEvClear();
    }
}
