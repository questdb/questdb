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

import com.nfsdb.collections.ObjectPoolFactory;

public class JoinModel extends QueryModel {
    public static final JoinModelFactory FACTORY = new JoinModelFactory();
    private ExprNode joinCriteria;
    private JoinType joinType;

    @Override
    public void clear() {
        super.clear();
        joinCriteria = null;
    }

    public ExprNode getJoinCriteria() {
        return joinCriteria;
    }

    public void setJoinCriteria(ExprNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public enum JoinType {
        INNER, OUTER, CROSS
    }

    public static final class JoinModelFactory implements ObjectPoolFactory<JoinModel> {
        @Override
        public JoinModel newInstance() {
            return new JoinModel();
        }
    }
}
