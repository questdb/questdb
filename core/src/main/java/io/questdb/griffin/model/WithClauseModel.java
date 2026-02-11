/*******************************************************************************
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
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.Nullable;

public class WithClauseModel implements Mutable {
    public static final ObjectFactory<WithClauseModel> FACTORY = WithClauseModel::new;
    private QueryModel model;
    private LowerCaseCharSequenceObjHashMap<WithClauseModel> originalWithClauses;
    // Size of withClauses at time of `of()` method call. We need to maintain the 'snapshot' because
    // map can grow and subsequent WITH clause can override table used by current one,
    // leading to stack overflow on re-evaluation.
    private int originalWithClausesSize = -1;
    private int position;
    private LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses;
    private boolean withClausesInitialized;

    private WithClauseModel() {
    }

    @Override
    public void clear() {
        position = 0;
        model = null;
        originalWithClauses = null;
        originalWithClausesSize = -1;
        withClausesInitialized = false;
        withClauses = null;
    }

    public int getPosition() {
        return position;
    }

    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        if (!withClausesInitialized) {
            withClauses = getSubMap();
            withClausesInitialized = true;
        }
        return withClauses;
    }

    public void of(int position, LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses, QueryModel model) {
        this.position = position;
        this.model = model;
        this.originalWithClauses = withClauses;
        this.originalWithClausesSize = withClauses.size();
    }

    public QueryModel popModel() {
        QueryModel m = model;
        model = null;
        return m;
    }

    @Nullable
    private LowerCaseCharSequenceObjHashMap<WithClauseModel> getSubMap() {
        if (originalWithClausesSize == 0) {
            return null;
        } else {
            LowerCaseCharSequenceObjHashMap<WithClauseModel> subMap = new LowerCaseCharSequenceObjHashMap<>();
            ObjList<CharSequence> keys = originalWithClauses.keys();
            for (int i = 0; i < originalWithClausesSize; i++) {
                CharSequence key = keys.get(i);
                WithClauseModel value = originalWithClauses.get(key);
                subMap.put(key, value);
            }
            return subMap;
        }
    }
}
