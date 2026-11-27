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


package io.questdb.legacy.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.legacy.griffin.model.IQueryModel;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

/**
 * Master's methods of {@link io.questdb.cairo.view.ViewGraph} that read the legacy query model.
 */
public final class LegacyViewGraph {

    private LegacyViewGraph() {
    }

    public static void validateNoCycle(TableToken viewToken, IQueryModel model) {
        final ObjList<ViewDefinition> referencedViews = model.getReferencedViews();

        for (int i = 0, n = referencedViews.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(referencedViews.get(i).getViewToken().getTableName(), viewToken.getTableName())) {
                if (i == 0) {
                    throw CairoException.critical(0)
                            .put("circular dependency detected: view '").put(viewToken.getTableName())
                            .put("' cannot reference itself");
                } else {
                    final CharSequence directDep = referencedViews.get(0).getViewToken().getTableName();
                    throw CairoException.critical(0)
                            .put("circular dependency detected: '").put(viewToken.getTableName())
                            .put("' cannot depend on '").put(directDep)
                            .put("' because '").put(directDep)
                            .put("' already depends on '").put(viewToken.getTableName()).put('\'');
                }
            }
        }
    }
}
