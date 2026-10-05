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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The filter conjuncts that may drop the rows a RIGHT or FULL join in a decorrelated LATERAL body
 * NULL-extends on its master side, depending on the type of the master-side column they read. That
 * type is not known before code generation, for example for a computed sub-query column, so
 * LateralJoinRewriter leaves the decision to the code generator, which knows the type when it
 * generates the join. Each candidate holds:
 * <ul>
 *     <li>the column, as the filter reads it</li>
 *     <li>a probe, the conjunct with the column replaced, which the code generator evaluates on the
 *     NULL record of the type, or null when the conjunct drops those rows for every type with NULL</li>
 *     <li>a NULL check, "value = NULL", when the conjunct drops those rows only while a value that
 *     reads no column is not NULL, which the code generator then checks once per execution</li>
 * </ul>
 * When no candidate drops those rows, the code generator fails the query with the error. A join that
 * the rewriter checks more than once holds a chain of such checks, each of which must hold.
 */
public class LateralNullRejection implements Mutable {
    private final ObjList<CharSequence> columns = new ObjList<>();
    private final ObjList<ExpressionNode> nullChecks = new ObjList<>();
    private final ObjList<ExpressionNode> probes = new ObjList<>();
    private CharSequence error;
    private LateralNullRejection next;

    public void add(CharSequence column, ExpressionNode probe, ExpressionNode nullCheck) {
        columns.add(column);
        probes.add(probe);
        nullChecks.add(nullCheck);
    }

    @Override
    public void clear() {
        columns.clear();
        nullChecks.clear();
        probes.clear();
        error = null;
        next = null;
    }

    public CharSequence getColumn(int index) {
        return columns.getQuick(index);
    }

    public CharSequence getError() {
        return error;
    }

    // Returns another check of the same join, or null
    public LateralNullRejection getNext() {
        return next;
    }

    public ExpressionNode getNullCheck(int index) {
        return nullChecks.getQuick(index);
    }

    public ExpressionNode getProbe(int index) {
        return probes.getQuick(index);
    }

    public void setError(CharSequence error) {
        this.error = error;
    }

    public void setNext(LateralNullRejection next) {
        this.next = next;
    }

    public int size() {
        return columns.size();
    }
}
