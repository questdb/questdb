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

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Arguments evaluated for each row of the preceding join inputs.
 */
public final class UnnestSpec implements Mutable {
    public static final ObjectFactory<UnnestSpec> FACTORY = UnnestSpec::new;
    private final ObjList<CharSequence> columnAliases = new ObjList<>();
    private final ObjList<BoundExpression> expressions = new ObjList<>();
    private final ObjList<ObjList<CharSequence>> jsonColumnNames = new ObjList<>();
    private final ObjList<IntList> jsonColumnTypes = new ObjList<>();
    private final OutputSchema output = new OutputSchema();
    private boolean hasOrdinality;
    private boolean isStandalone;

    @Override
    public void clear() {
        columnAliases.clear();
        expressions.clear();
        jsonColumnNames.clear();
        jsonColumnTypes.clear();
        output.clear();
        hasOrdinality = false;
        isStandalone = false;
    }

    public ObjList<CharSequence> getColumnAliases() {
        return columnAliases;
    }

    public ObjList<BoundExpression> getExpressions() {
        return expressions;
    }

    /**
     * Null entries denote array sources; other entries declare JSON fields.
     */
    public ObjList<ObjList<CharSequence>> getJsonColumnNames() {
        return jsonColumnNames;
    }

    public ObjList<IntList> getJsonColumnTypes() {
        return jsonColumnTypes;
    }

    public OutputSchema getOutput() {
        return output;
    }

    public boolean hasOrdinality() {
        return hasOrdinality;
    }

    public boolean isStandalone() {
        return isStandalone;
    }

    public UnnestSpec of(boolean isStandalone, boolean hasOrdinality) {
        this.isStandalone = isStandalone;
        this.hasOrdinality = hasOrdinality;
        return this;
    }
}
