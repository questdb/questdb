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

package com.nfsdb.ql.impl;

import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.RowSource;
import com.nfsdb.ql.SymFacade;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractRowSource implements RowSource, RowCursor {
    @SuppressFBWarnings({"ACEM_ABSTRACT_CLASS_EMPTY_METHODS"})
    @Override
    public void prepare(SymFacade symFacade) {
    }
}
