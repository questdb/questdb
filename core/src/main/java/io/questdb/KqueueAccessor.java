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

package io.questdb;

import static io.questdb.std.Files.toOsFd;

public class KqueueAccessor {
    public static final short DATA_OFFSET;
    public static final short EVFILT_READ;
    public static final short EVFILT_VNODE;
    public static final short EVFILT_WRITE;
    public static final short EV_ADD;
    public static final short EV_CLEAR;
    public static final short EV_DELETE;
    public static final short EV_ONESHOT;
    public static final short FD_OFFSET;
    public static final short FILTER_OFFSET;
    public static final short FLAGS_OFFSET;
    public static final short NOTE_ATTRIB;
    public static final short NOTE_DELETE;
    public static final short NOTE_EXTEND;
    public static final short NOTE_LINK;
    public static final short NOTE_RENAME;
    public static final short NOTE_REVOKE;
    public static final short NOTE_WRITE;
    public static final short SIZEOF_KEVENT;

    public static long evtAlloc(long ident, int filter, int flags, int fflags, long data) {
        return evtAlloc(toOsFd(ident), filter, flags, fflags, data);
    }

    public static native long evtFree(long event);

    public static int kevent(long kq, long changeList, int nChanges, long eventList, int nEvents, int timeout) {
        return kevent(toOsFd(kq), changeList, nChanges, eventList, nEvents, timeout);
    }

    public static int keventGetBlocking(long kq, long eventList, int nEvents) {
        return keventGetBlocking(toOsFd(kq), eventList, nEvents);
    }

    public static int keventRegister(long kq, long changeList, int nChanges) {
        return keventRegister(toOsFd(kq), changeList, nChanges);
    }

    public static native int kqueue();

    public static native long pipe();

    public static int readPipe(long fd) {
        return readPipe(toOsFd(fd));
    }

    public static int writePipe(long fd) {
        return writePipe(toOsFd(fd));
    }

    private static native long evtAlloc(int ident, int filter, int flags, int fflags, long data);

    private static native short getDataOffset();

    private static native short getEvAdd();

    private static native short getEvClear();

    private static native short getEvDelete();

    private static native short getEvOneshot();

    private static native short getEvfiltRead();

    private static native short getEvfiltVnode();

    private static native short getEvfiltWrite();

    private static native short getFdOffset();

    private static native short getFilterOffset();

    private static native short getFlagsOffset();

    private static native short getNoteAttrib();

    private static native short getNoteDelete();

    private static native short getNoteExtend();

    private static native short getNoteLink();

    private static native short getNoteRename();

    private static native short getNoteRevoke();

    private static native short getNoteWrite();

    private static native short getSizeofKevent();

    private static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout);

    private static native int keventGetBlocking(int kq, long eventList, int nEvents);

    private static native int keventRegister(int kq, long changeList, int nChanges);

    private static native int readPipe(int fd);

    private static native int writePipe(int fd);

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        EVFILT_VNODE = getEvfiltVnode();
        NOTE_DELETE = getNoteDelete();
        NOTE_WRITE = getNoteWrite();
        NOTE_EXTEND = getNoteExtend();
        NOTE_ATTRIB = getNoteAttrib();
        NOTE_LINK = getNoteLink();
        NOTE_RENAME = getNoteRename();
        NOTE_REVOKE = getNoteRevoke();
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
