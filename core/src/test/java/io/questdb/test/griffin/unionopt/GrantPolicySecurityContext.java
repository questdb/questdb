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

package io.questdb.test.griffin.unionopt;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Allows everything except SELECT, which it decides from an explicit grant set. Records every
 * SELECT authorisation call in a normal form, so two queries can be compared by the checks they trigger.
 */
public final class GrantPolicySecurityContext extends AllowAllSecurityContext {
    public static final String DENIED = "permission denied";
    private final TreeSet<String> checks = new TreeSet<>();
    private final Map<String, Set<String>> columnGrants = new HashMap<>();
    private final Set<String> viewGrants = new HashSet<>();

    @Override
    public void authorizeSelect(ViewDefinition viewDefinition) {
        final String view = viewDefinition.getViewToken().getTableName().toLowerCase();
        checks.add("VIEW " + view);
        if (!viewGrants.contains(view)) {
            throw deny(view);
        }
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        final String table = tableToken.getTableName().toLowerCase();
        final List<String> cols = new ArrayList<>(columnNames.size());
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            cols.add(columnNames.getQuick(i).toString().toLowerCase());
        }
        Collections.sort(cols);
        checks.add("COLUMNS " + table + " " + cols);
        final Set<String> granted = columnGrants.get(table);
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (granted == null || (!granted.contains("*") && !granted.contains(cols.get(i)))) {
                throw deny(table + "." + cols.get(i));
            }
        }
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
        final String table = tableToken.getTableName().toLowerCase();
        checks.add("ANY_COLUMN " + table);
        final Set<String> granted = columnGrants.get(table);
        if (granted == null || granted.isEmpty()) {
            throw deny(table);
        }
    }

    public void clearChecks() {
        checks.clear();
    }

    public TreeSet<String> getChecks() {
        return new TreeSet<>(checks);
    }

    public GrantPolicySecurityContext grant(Grant grant) {
        if (grant instanceof Grant.View v) {
            viewGrants.add(v.view().toLowerCase());
        } else if (grant instanceof Grant.Columns c) {
            columnGrants.computeIfAbsent(c.table().toLowerCase(), k -> new HashSet<>()).add(c.column().toLowerCase());
        }
        return this;
    }

    public GrantPolicySecurityContext revokeTable(String table) {
        columnGrants.remove(table.toLowerCase());
        return this;
    }

    public GrantPolicySecurityContext revokeView(String view) {
        viewGrants.remove(view.toLowerCase());
        return this;
    }

    private static CairoException deny(String object) {
        return CairoException.nonCritical().put(DENIED).put(" [object=").put(object).put(']');
    }
}
