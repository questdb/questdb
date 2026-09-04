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

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * One read of an audited view, captured while the parser expands that view.
 * <p>
 * It names the view and pairs each of the view's {@code DECLARE OVERRIDABLE} parameters with the
 * expression that parameter resolved to at this reference site - the caller's override where the
 * caller supplied one, the view's own default otherwise. The expression is what the parser
 * substituted into the view body, so it is exactly what the read filters on.
 * <p>
 * The expression is deliberately kept unevaluated here. A caller may bind a parameter to a bind
 * variable, in which case the same compiled plan serves many executions with different values, and
 * only evaluating per execution recovers what each one actually scanned.
 * <p>
 * Parameters are sorted by name at capture, so that the JSON rendered from them is canonical and
 * two reads with the same arguments produce byte-identical audit rows.
 */
public class ViewAuditModel implements Mutable {
    public static final ObjectFactory<ViewAuditModel> FACTORY = ViewAuditModel::new;
    private final ObjList<CharSequence> paramNames = new ObjList<>();
    private final ObjList<ExpressionNode> paramValues = new ObjList<>();
    private int viewId;
    private CharSequence viewName;

    public void addParam(CharSequence name, ExpressionNode value) {
        paramNames.add(name);
        paramValues.add(value);
    }

    @Override
    public void clear() {
        viewId = 0;
        viewName = null;
        paramNames.clear();
        paramValues.clear();
    }

    public int getParamCount() {
        return paramNames.size();
    }

    public CharSequence getParamName(int index) {
        return paramNames.getQuick(index);
    }

    public ExpressionNode getParamValue(int index) {
        return paramValues.getQuick(index);
    }

    public CharSequence getViewName() {
        return viewName;
    }

    public int getViewId() {
        return viewId;
    }

    /**
     * @param viewId the view's table id, recorded beside the name because a view can be renamed:
     *               the name says what a read asked for, the id says which view actually served it,
     *               so a report can follow one view across a rename instead of guessing.
     */
    public void of(CharSequence viewName, int viewId) {
        clear();
        this.viewName = viewName;
        this.viewId = viewId;
    }

    /**
     * Orders parameters by name. Call once, after the last {@link #addParam(CharSequence, ExpressionNode)},
     * so that rendering walks them in a stable order regardless of declaration order.
     */
    public void sortParams() {
        // The two lists are parallel, so sorting one alone would decouple them. An insertion sort
        // moves both together; parameter counts are small enough that O(n^2) does not matter.
        for (int i = 1, n = paramNames.size(); i < n; i++) {
            final CharSequence name = paramNames.getQuick(i);
            final ExpressionNode value = paramValues.getQuick(i);
            int j = i - 1;
            while (j >= 0 && Chars.compare(paramNames.getQuick(j), name) > 0) {
                paramNames.setQuick(j + 1, paramNames.getQuick(j));
                paramValues.setQuick(j + 1, paramValues.getQuick(j));
                j--;
            }
            paramNames.setQuick(j + 1, name);
            paramValues.setQuick(j + 1, value);
        }
    }
}
