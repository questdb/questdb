/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.Nullable;

public class WithClauseModel implements Mutable {
    public static final ObjectFactory<WithClauseModel> FACTORY = WithClauseModel::new;
    private IQueryModel model;
    private int position;
    // The CTEs visible at the definition, as they stood there, which every parse of the body
    // reads; see of().
    private LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses;

    private WithClauseModel() {
    }

    @Override
    public void clear() {
        position = 0;
        model = null;
        withClauses = null;
    }

    public int getPosition() {
        return position;
    }

    /**
     * @return the CTEs visible at the definition, or null when there were none
     */
    @Nullable
    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return withClauses;
    }

    /**
     * @param withClauses the CTEs visible at the definition, or null for none: a copy that nothing
     *                    changes while this model is in use. The map of the WITH that defines the
     *                    CTE keeps changing. A later CTE of the same WITH adds a name, and can reuse
     *                    the name of a CTE inherited from an enclosing query, which replaces the
     *                    CTE under that name. A copy keeps every parse of the body binding each
     *                    name as the definition's parse did, so a CTE that names itself reads the
     *                    CTE it shadows rather than itself.
     */
    public void of(int position, @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses, IQueryModel model) {
        this.position = position;
        this.model = model;
        this.withClauses = withClauses;
    }

    public IQueryModel popModel() {
        IQueryModel m = model;
        model = null;
        return m;
    }
}
