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

import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

/**
 * Records the facts of a plan object, see {@link PlanFactSource#visitFacts}, and the expressions and column ids it
 * reads, see {@link PlanFactSource#visitReads}, as a sequence of entries that two recordings compare one by one; an
 * expression records each of its nodes. A recording of an original under a renaming, see {@link #rename} and
 * {@link #pair}, records each column id the renaming holds as its new id, an outer column it holds as a column of the
 * new id, and a reference to an object paired with a copy as the copy, so it equals the recording of a faithful copy.
 * The checks of {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE} use it: the copier compares a copy with its
 * source, and the plan node pools compare a cleared object with a new one.
 */
public final class PlanFactRecorder implements PlanExpressionVisitor, ExpressionVisitor {
    private static final String COLUMN = "column";
    private static final String OUTER_COLUMN = "outer column";
    private static final String READ = "read";
    private final ObjList<Object> copies = new ObjList<>();
    private final ObjList<String> names = new ObjList<>();
    private final ObjList<Object> objects = new ObjList<>();
    private final ObjList<Object> originals = new ObjList<>();
    private final LongList values = new LongList();
    private IntIntHashMap renamedIds;

    public void columnId(String fact, int columnId) {
        add(fact, renamedId(columnId), null);
    }

    public void columnIds(String fact, IntList columnIds) {
        add(fact, columnIds.size(), null);
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            columnId(fact, columnIds.getQuick(i));
        }
    }

    public void fact(String fact, boolean value) {
        add(fact, value ? 1 : 0, null);
    }

    public void fact(String fact, long value) {
        add(fact, value, null);
    }

    /**
     * A value compared by identity, or by content when it is text.
     */
    public void fact(String fact, Object value) {
        add(fact, 0, value);
    }

    public void facts(String fact, IntList values) {
        add(fact, values.size(), null);
        for (int i = 0, n = values.size(); i < n; i++) {
            add(fact, values.getQuick(i), null);
        }
    }

    public void facts(String fact, LongList values) {
        add(fact, values.size(), null);
        for (int i = 0, n = values.size(); i < n; i++) {
            add(fact, values.getQuick(i), null);
        }
    }

    /**
     * Values compared as {@link #fact(String, Object)} compares one.
     */
    public void facts(String fact, ObjList<?> values) {
        add(fact, values.size(), null);
        for (int i = 0, n = values.size(); i < n; i++) {
            add(fact, 0, values.getQuick(i));
        }
    }

    /**
     * The name of the first fact this recording and {@code other} differ in, or null when they agree.
     */
    public String firstDifference(PlanFactRecorder other) {
        final int n = names.size();
        for (int i = 0; i < n; i++) {
            if (i == other.names.size() || names.getQuick(i) != other.names.getQuick(i) || values.getQuick(i) != other.values.getQuick(i)
                    || !isSameObject(objects.getQuick(i), other.objects.getQuick(i))) {
                return names.getQuick(i);
            }
        }
        return n == other.names.size() ? null : other.names.getQuick(n);
    }

    public void output(String fact, OutputSchema output) {
        add(fact, output.getColumnCount(), null);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            columnId(fact, output.getColumnId(i));
            outputColumn(fact, output, i);
        }
        add(fact, output.getTimestampIndex(), null);
    }

    /**
     * Pairs an original plan object with its copy: the recording of a reference to the original records the copy.
     */
    public void pair(Object original, Object copy) {
        originals.add(original);
        copies.add(copy);
    }

    /**
     * Records the facts and the reads of {@code source}, under the renaming.
     */
    public void record(PlanFactSource source) {
        names.clear();
        objects.clear();
        values.clear();
        source.visitFacts(this);
        source.visitReads(this);
    }

    /**
     * Records the fields of a RECORD column, whose ids are their positions in the record rather than plan columns, so
     * that the renaming does not apply to them.
     */
    public void recordFields(String fact, OutputSchema fields) {
        add(fact, fields.getColumnCount(), null);
        for (int i = 0, n = fields.getColumnCount(); i < n; i++) {
            add(fact, fields.getColumnId(i), null);
            outputColumn(fact, fields, i);
        }
        add(fact, fields.getTimestampIndex(), null);
    }

    public void reference(String fact, Object planObject) {
        Object recorded = planObject;
        for (int i = 0, n = originals.size(); i < n; i++) {
            if (originals.getQuick(i) == planObject) {
                recorded = copies.getQuick(i);
                break;
            }
        }
        add(fact, 0, recorded);
    }

    /**
     * Records every column id {@code renamedIds} holds as the id it maps it to; null records every column id as it
     * is. Clears the pairs.
     */
    public void rename(IntIntHashMap renamedIds) {
        this.renamedIds = renamedIds;
        originals.clear();
        copies.clear();
    }

    @Override
    public int visit(BoundExpression expression) {
        switch (expression) {
            case ColumnExpression column -> {
                add(COLUMN, renamedId(column.getColumnId()), null);
                add(COLUMN, (column.isCast() ? 1 : 0) | (column.isDirectReference() ? 2 : 0), null);
            }
            case OuterColumnExpression outer -> {
                final int columnId = renamedId(outer.getColumnId());
                if (columnId != outer.getColumnId()) {
                    add(COLUMN, columnId, null);
                    add(COLUMN, 2, null);
                } else {
                    add(OUTER_COLUMN, columnId, null);
                }
            }
            case FunctionExpression call -> {
                add(READ, call.getFunctionFlags(), call.getOverload());
                add(READ, (call.isProjectedOffset() ? 1 : 0) | (call.isSetOperation() ? 2 : 0) | (call.isPolicyPredicate() ? 4 : 0), null);
                for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                    add(READ, call.getArgumentPosition(i), null);
                }
            }
            default -> add(READ, 0, expression);
        }
        add(READ, expression.getDataType(), null);
        add(READ, expression.getPosition(), null);
        return TreeWalk.CONTINUE;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        columnId(READ, columnId);
        return columnId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        expression.walk(this);
        return expression;
    }

    private static boolean isSameObject(Object object, Object other) {
        return object == other || object instanceof CharSequence text && other instanceof CharSequence otherText && Chars.equals(text, otherText);
    }

    private void add(String name, long value, Object object) {
        names.add(name);
        values.add(value);
        objects.add(object);
    }

    private void outputColumn(String fact, OutputSchema output, int index) {
        add(fact, output.getColumnType(index), output.getColumnName(index));
        add(fact, (output.isVisible(index) ? 1 : 0) | (output.isSymbolTableStatic(index) ? 2 : 0) | (output.isNameProtected(index) ? 4 : 0),
                output.getColumnQualifier(index));
        add(fact, 0, output.getMetadata(index));
    }

    private int renamedId(int columnId) {
        final int renamed = columnId < 0 || renamedIds == null ? -1 : renamedIds.get(columnId);
        return renamed < 0 ? columnId : renamed;
    }
}
