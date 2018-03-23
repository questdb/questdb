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

package com.questdb.griffin.engine.table;

import com.questdb.common.RowCursor;

class MergingRowCursor implements RowCursor {

    private RowCursor lhc;
    private RowCursor rhc;
    private long nxtl;
    private long nxtr;

    @Override
    public boolean hasNext() {
        return nxtl > -1 || lhc.hasNext() || nxtr > -1 || rhc.hasNext();
    }

    @Override
    public long next() {
        long result;

        if (nxtl == -1 && lhc.hasNext()) {
            nxtl = lhc.next();
        }

        if (nxtr == -1 && rhc.hasNext()) {
            nxtr = rhc.next();
        }

        if (nxtr == -1 || (nxtl > -1 && nxtl < nxtr)) {
            result = nxtl;
            nxtl = -1;
        } else {
            result = nxtr;
            nxtr = -1;
        }

        return result;
    }

    public void of(RowCursor lhc, RowCursor rhc) {
        this.lhc = lhc;
        this.rhc = rhc;
        nxtl = nxtr = -1;
    }
}
