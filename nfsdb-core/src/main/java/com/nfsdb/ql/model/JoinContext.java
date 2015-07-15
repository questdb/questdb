/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.model;

import com.nfsdb.collections.*;

public class JoinContext implements Mutable {
    public static final ObjectPoolFactory<JoinContext> FACTORY = new ObjectPoolFactory<JoinContext>() {
        @Override
        public JoinContext newInstance() {
            return new JoinContext();
        }
    };

    public final IntList aIndexes = new IntList();
    public final IntList bIndexes = new IntList();
    public final ObjList<CharSequence> aNames = new ObjList<>();
    public final ObjList<CharSequence> bNames = new ObjList<>();
    public final ObjList<ExprNode> aNodes = new ObjList<>();
    public final ObjList<ExprNode> bNodes = new ObjList<>();
    // indexes of parent join clauses
    public final IntHashSet parents = new IntHashSet(4);
    public boolean trivial = true;
    public int slaveIndex = -1;

    @Override
    public void clear() {
        aIndexes.clear();
        aNames.clear();
        aNodes.clear();

        bIndexes.clear();
        bNames.clear();
        bNodes.clear();

        trivial = true;
        slaveIndex = -1;
        parents.clear();
    }
}
