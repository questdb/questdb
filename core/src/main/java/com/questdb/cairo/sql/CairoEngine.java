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

package com.questdb.cairo.sql;

import com.questdb.cairo.TableReader;
import com.questdb.cairo.TableWriter;
import com.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface CairoEngine extends Closeable {
    TableReader getReader(CharSequence tableName, long version);

    int getStatus(Path path, CharSequence tableName, int lo, int hi);

    default int getStatus(Path path, CharSequence tableName) {
        return getStatus(path, tableName, 0, tableName.length());
    }

    TableWriter getWriter(CharSequence tableName);

    boolean lock(CharSequence tableName);

    boolean releaseAllReaders();

    boolean releaseAllWriters();

    void unlock(CharSequence tableName, @Nullable TableWriter writer);

    void remove(Path path, CharSequence tableName);

    void rename(Path path, CharSequence tableName, Path otherPath, String newName);
}
