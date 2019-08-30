/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.network;

public class KqueueAccessor {
    public static final short EVFILT_READ;
    public static final short SIZEOF_KEVENT;
    public static final int EV_EOF = -32751;
    static final short EV_ONESHOT;
    static final short EVFILT_WRITE;
    static final short FD_OFFSET;
    static final short FILTER_OFFSET;
    static final short FLAGS_OFFSET;
    static final short DATA_OFFSET;
    static final short EV_ADD;

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
    }

    static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents);

    static native int kqueue();

    static native short getEvfiltRead();

    static native short getEvfiltWrite();

    static native short getSizeofKevent();

    static native short getFdOffset();

    static native short getFilterOffset();

    static native short getEvAdd();

    static native short getEvOneshot();

    static native short getFlagsOffset();

    static native short getDataOffset();

}
