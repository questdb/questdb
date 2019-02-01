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

package com.questdb.griffin.model;

import com.questdb.std.*;

public class JoinContext implements Mutable {
    public static final ObjectFactory<JoinContext> FACTORY = JoinContext::new;
    private static final int TYPICAL_NUMBER_OF_JOIN_COLUMNS = 4;

    public final IntList aIndexes = new IntList();
    public final IntList bIndexes = new IntList();
    public final ObjList<CharSequence> aNames = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final ObjList<CharSequence> bNames = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final ObjList<ExpressionNode> aNodes = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final ObjList<ExpressionNode> bNodes = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    // indexes of parent join clauses
    public final IntHashSet parents = new IntHashSet(4);
    public int inCount;
    public int slaveIndex = -1;

    @Override
    public void clear() {
        aIndexes.clear();
        aNames.clear();
        aNodes.clear();

        bIndexes.clear();
        bNames.clear();
        bNodes.clear();

        slaveIndex = -1;
        parents.clear();
    }
}
