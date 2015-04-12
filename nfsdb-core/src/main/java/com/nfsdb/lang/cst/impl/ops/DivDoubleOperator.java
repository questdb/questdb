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

package com.nfsdb.lang.cst.impl.ops;

import com.nfsdb.lang.cst.impl.virt.AbstractBinaryOperator;
import com.nfsdb.storage.ColumnType;

public class DivDoubleOperator extends AbstractBinaryOperator {

    public DivDoubleOperator() {
        super(ColumnType.DOUBLE);
    }

    @Override
    public double getDouble() {
        return lhs.getDouble() / rhs.getDouble();
    }

    @Override
    public float getFloat() {
        return (float) (lhs.getDouble() / rhs.getDouble());
    }

    @Override
    public int getInt() {
        return (int) (lhs.getDouble() / rhs.getDouble());
    }
}
