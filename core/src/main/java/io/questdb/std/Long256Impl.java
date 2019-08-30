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

public class Long256Impl implements Long256Sink, Long256 {
    private long l0;
    private long l1;
    private long l2;
    private long l3;

    @Override
    public long getLong0() {
        return l0;
    }

    @Override
    public void setLong0(long value) {
        this.l0 = value;
    }

    @Override
    public long getLong1() {
        return l1;
    }

    @Override
    public void setLong1(long value) {
        this.l1 = value;
    }

    @Override
    public long getLong2() {
        return l2;
    }

    @Override
    public void setLong2(long value) {
        this.l2 = value;
    }

    @Override
    public long getLong3() {
        return l3;
    }

    @Override
    public void setLong3(long value) {
        this.l3 = value;
    }
}
