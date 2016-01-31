/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.factory.configuration.JournalStructure;

public class Statement {
    private final StatementType type;
    private JournalStructure structure;
    private QueryModel queryModel;

    public Statement(StatementType type, JournalStructure structure) {
        this.structure = structure;
        this.type = type;
    }

    public Statement(StatementType type, QueryModel queryModel) {
        this.type = type;
        this.queryModel = queryModel;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public JournalStructure getStructure() {
        return structure;
    }

    public StatementType getType() {
        return type;
    }
}
