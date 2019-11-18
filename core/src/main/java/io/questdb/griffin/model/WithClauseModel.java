/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class WithClauseModel implements Mutable {

    public static final ObjectFactory<WithClauseModel> FACTORY = WithClauseModel::new;
    private int position;
    private QueryModel model;

    private WithClauseModel() {
    }

    @Override
    public void clear() {
        this.position = 0;
        this.model = null;
    }

    public int getPosition() {
        return position;
    }

    public void of(int position, QueryModel model) {
        this.position = position;
        this.model = model;
    }

    public QueryModel popModel() {
        QueryModel m = this.model;
        this.model = null;
        return m;
    }
}
