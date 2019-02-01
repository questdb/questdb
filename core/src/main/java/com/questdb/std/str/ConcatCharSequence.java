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

package com.questdb.std.str;

import com.questdb.std.ObjList;

public class ConcatCharSequence extends AbstractCharSequence {
    private final ObjList<CharSequence> delegates = new ObjList<>();
    private int len = -1;
    private CharSequence lastSeq;
    private int lastCharIndex = -1;
    private int lastSeqLen;
    private int lastSeqIndex;
    private int lastOffset;

    public void add(CharSequence cs) {
        if (cs == null) {
            return;
        }

        if (delegates.size() == 0) {
            lastSeq = cs;
            lastOffset = 0;
            lastSeqLen = cs.length();
            lastSeqIndex = 0;
        }

        delegates.add(cs);
    }

    public int computeLen() {
        int l = 0;
        for (int i = 0, n = delegates.size(); i < n; i++) {
            CharSequence d = delegates.getQuick(i);
            if (d == lastSeq) {
                lastSeqLen = d.length();
            }
            l += d.length();
        }
        return len = l;
    }

    @Override
    public int length() {
        if (len > -1) {
            return len;
        }
        return computeLen();
    }

    @Override
    public char charAt(int index) {
        if (index == lastCharIndex + 1) {
            lastCharIndex = index;
            if (index - lastOffset < lastSeqLen) {
                return lastSeq.charAt(index - lastOffset);
            } else {
                return next(index);
            }
        }
        return seek(index);
    }

    public void surroundWith(CharSequence left, CharSequence right) {
        // assume we have only one delegate
        if (delegates.size() > 0) {
            CharSequence delegate = delegates.get(0);
            delegates.setPos(3);
            delegates.extendAndSet(0, left);
            delegates.extendAndSet(1, delegate);
            delegates.extendAndSet(2, right);
        } else {
            delegates.setPos(2);
            delegates.extendAndSet(0, left);
            delegates.extendAndSet(1, right);
        }
        lastSeq = left;
        lastSeqLen = left.length();
    }

    private char next(int index) {
        lastOffset += lastSeqLen;
        do {
            lastSeqIndex++;
            lastSeq = delegates.getQuick(lastSeqIndex);
            lastSeqLen = lastSeq.length();
        } while (lastSeqLen == 0);
        return lastSeq.charAt(index - lastOffset);
    }

    private char seek(int index) {
        int l = 0;
        for (int i = 0, n = delegates.size(); i < n; i++) {
            CharSequence cs = delegates.getQuick(i);
            int len = cs.length();

            if (index < len + l) {
                lastSeq = cs;
                lastSeqLen = len;
                lastOffset = l;
                lastSeqIndex = i;
                lastCharIndex = index;
                return cs.charAt(index - l);
            }
            l += len;
        }
        throw new IndexOutOfBoundsException();
    }
}
