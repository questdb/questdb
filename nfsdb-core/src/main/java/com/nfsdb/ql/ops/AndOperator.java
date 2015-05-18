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

import com.nfsdb.ql.Record;
import com.nfsdb.storage.ColumnType;

public class AndOperator extends AbstractBinaryOperator {

    public AndOperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public boolean getBool(Record rec) {
        return lhs.getBool(rec) && rhs.getBool(rec);
    }

    @Override
    public boolean isConstant() {
        if (rhs.isConstant() && !rhs.getBool(null)) {
            lhs = new BooleanConstant(false);
            return true;
        }
        return (lhs.isConstant() && !lhs.getBool(null)) || (lhs.isConstant() && rhs.isConstant());
    }
}
