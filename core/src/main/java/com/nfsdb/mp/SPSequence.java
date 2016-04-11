/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.mp;

public class SPSequence extends AbstractSSequence {
    private final int cycle;
    private volatile long index = -1;
    private volatile long cache = -1;

    private SPSequence(int cycle, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.cycle = cycle;
    }

    public SPSequence(int cycle) {
        this(cycle, null);
    }

    @Override
    public long availableIndex(long lo) {
        return index;
    }

    @Override
    public long current() {
        return index;
    }

    @Override
    public void done(long cursor) {
        index = cursor;
        barrier.signal();
    }

    @Override
    public long next() {
        long next = index + 1;
        long lo = next - cycle;
        return lo > cache && lo > (cache = barrier.availableIndex(lo)) ? -1 : next;
    }

    @Override
    public void reset() {

    }
}
