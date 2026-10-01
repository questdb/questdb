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

package io.questdb.griffin.plan.logical;

import io.questdb.std.ObjectFactory;

/**
 * Parameter identity and inferred type; the executable link has a separate owner.
 */
public final class BindVariableExpression extends BoundExpression {
    public static final ObjectFactory<BindVariableExpression> FACTORY = BindVariableExpression::new;
    private boolean isDirectReference = true;
    private boolean isPredefined;
    private CharSequence name;

    @Override
    public void clear() {
        super.clear();
        isDirectReference = true;
        isPredefined = false;
        name = null;
    }

    public CharSequence getName() {
        return name;
    }

    public boolean isDirectReference() {
        return isDirectReference;
    }

    /** The caller defined the variable before compilation rather than binding inferring it. */
    public boolean isPredefined() {
        return isPredefined;
    }

    public BindVariableExpression markPredefined() {
        isPredefined = true;
        return this;
    }

    public BindVariableExpression of(CharSequence name, int dataType, int functionFlags, int position) {
        return of(name, dataType, functionFlags, position, true);
    }

    public BindVariableExpression of(CharSequence name, int dataType, int functionFlags, int position, boolean isDirectReference) {
        configure(dataType, position, functionFlags);
        this.isDirectReference = isDirectReference;
        this.name = name;
        return this;
    }
}
