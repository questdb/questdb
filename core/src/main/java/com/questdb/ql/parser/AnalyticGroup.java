/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.ql.parser;

import com.questdb.std.IntList;

public class AnalyticGroup {
    private final IntList orderBy;
    private int hashCode = -1;

    public AnalyticGroup(IntList orderBy) {
        this.orderBy = orderBy;
    }

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            computeHashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AnalyticGroup that = (AnalyticGroup) o;

        if (orderBy.size() != that.orderBy.size()) {
            return false;
        }

        for (int i = 0, n = orderBy.size(); i < n; i++) {
            if (orderBy.getQuick(i) != that.orderBy.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    private void computeHashCode() {
        hashCode = 0;
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            hashCode = hashCode * 31 + orderBy.getQuick(i);
        }
    }
}
