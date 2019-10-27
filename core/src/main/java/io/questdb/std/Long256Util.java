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

package io.questdb.std;

public class Long256Util {

    // this method is used by byte-code generator
    public static int compare(Long256 a, Long256 b) {

        if (a.getLong3() < b.getLong3()) {
            return -1;
        }

        if (a.getLong3() > b.getLong3()) {
            return 1;
        }

        if (a.getLong2() < b.getLong2()) {
            return -1;
        }

        if (a.getLong2() > b.getLong2()) {
            return 1;
        }

        if (a.getLong1() < b.getLong1()) {
            return -1;
        }

        if (a.getLong1() > b.getLong1()) {
            return 1;
        }

        return Long.compare(a.getLong0(), b.getLong0());
    }
}
