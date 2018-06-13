/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.ImmutableIterator;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static com.questdb.cairo.QMap.ENTRY_HEADER_SIZE;

public class QMapCursor implements ImmutableIterator<Record>, Iterable<Record> {

    private final VirtualMemory entries;
    private final EntryRecord record = new EntryRecord();
    private long offset;
    private long offsetHi;
    private long nextOffset;

    public QMapCursor(VirtualMemory entries) {
        this.entries = entries;
    }

    void of(long offsetHi) {
        this.nextOffset = ENTRY_HEADER_SIZE;
        this.offsetHi = offsetHi;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (nextOffset < offsetHi) {
            offset = nextOffset;
            return true;
        }
        return false;
    }

    @Override
    public Record next() {
        nextOffset = entries.getLong(offset - 8) + offset;
        return record;
    }

    private class EntryRecord implements Record {
        @Override
        public boolean getBool(int col) {
            return entries.getBool(offset + col * 8);
        }

        @Override
        public byte getByte(int col) {
            return (byte) getLong(col);
        }

        @Override
        public long getLong(int col) {
            return entries.getLong(offset + col * 8);
        }

        @Override
        public short getShort(int col) {
            return (short) getLong(col);
        }

        @Override
        public int getInt(int col) {
            return (int) getLong(col);
        }

        @Override
        public float getFloat(int col) {
            return (float) getDouble(col);
        }

        @Override
        public double getDouble(int col) {
            return entries.getDouble(offset + col * 8);
        }

        @Override
        public CharSequence getStr(int col) {
            long o = getLong(col);
            if (o == TableUtils.NULL_LEN) {
                return null;
            }
            return entries.getStr(offset + o - ENTRY_HEADER_SIZE);
        }

        @Override
        public CharSequence getStrB(int col) {
            long o = getLong(col);
            if (o == TableUtils.NULL_LEN) {
                return null;
            }
            return entries.getStr2(offset + o - ENTRY_HEADER_SIZE);
        }

        @Override
        public int getStrLen(int col) {
            long o = getLong(col);
            if (o == TableUtils.NULL_LEN) {
                return TableUtils.NULL_LEN;
            }
            return entries.getStrLen(offset + getLong(col) - ENTRY_HEADER_SIZE);
        }

        @Override
        public BinarySequence getBin(int col) {
            long o = getLong(col);
            // todo: check if type cast impacts performance
            if (o == TableUtils.NULL_LEN) {
                return null;
            }
            return entries.getBin(QMapCursor.this.offset + o - ENTRY_HEADER_SIZE);
        }

        @Override
        public long getBinLen(int col) {
            long o = getLong(col);
            if (o == TableUtils.NULL_LEN) {
                return TableUtils.NULL_LEN;
            }
            return entries.getBinLen(offset + o - ENTRY_HEADER_SIZE);
        }
    }
}
