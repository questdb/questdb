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

import com.nfsdb.ql.SymFacade;
import com.nfsdb.ql.parser.ParserException;
import com.nfsdb.storage.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractBinaryOperator extends AbstractVirtualColumn implements Function {
    protected VirtualColumn lhs;
    protected VirtualColumn rhs;

    public AbstractBinaryOperator(ColumnType type) {
        super(type);
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant() && rhs.isConstant();
    }

    @Override
    public void prepare(SymFacade facade) {
        lhs.prepare(facade);
        rhs.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                setLhs(arg);
                break;
            case 1:
                setRhs(arg);
                break;
            default:
                throw new ParserException(0, "Too many arguments");
        }
    }

    @SuppressFBWarnings({"ACEM_ABSTRACT_CLASS_EMPTY_METHODS"})
    @Override
    public void setArgCount(int count) {
    }

    public void setLhs(VirtualColumn lhs) {
        this.lhs = lhs;
    }

    public void setRhs(VirtualColumn rhs) {
        this.rhs = rhs;
    }
}
