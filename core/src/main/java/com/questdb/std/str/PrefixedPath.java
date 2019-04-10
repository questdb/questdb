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

import com.questdb.std.Numbers;

public final class PrefixedPath extends Path {

    private final int prefixLen;

    public PrefixedPath(CharSequence prefix, int capacity) {
        super(capacity);
        super.of(prefix);
        ensureSeparator();
        this.prefixLen = length();
    }

    public PrefixedPath(CharSequence prefix) {
        this(prefix, Numbers.ceilPow2(prefix.length() + 32));
    }

    public PrefixedPath rewind() {
        trimTo(prefixLen);
        return this;
    }
}
