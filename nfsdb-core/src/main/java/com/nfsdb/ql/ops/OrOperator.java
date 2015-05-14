/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.ql.ops;

import com.nfsdb.storage.ColumnType;

public class OrOperator extends AbstractBinaryOperator {

    public OrOperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public boolean getBool() {
        return lhs.getBool() || rhs.getBool();
    }

    @Override
    public boolean isConstant() {
        if (rhs.isConstant() && rhs.getBool()) {
            lhs = new BooleanConstant(true);
        }
        return (lhs.isConstant() && lhs.getBool()) || (lhs.isConstant() && rhs.isConstant());
    }
}
