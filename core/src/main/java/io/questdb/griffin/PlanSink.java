/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

/**
 * Gathers important query execution plan details and prints them in a readable format.
 */
public class PlanSink {

    private final IntList eolIndexes;
    private final ObjStack<RecordCursorFactory> factoryStack;
    private final StringSink sink;
    private String attrIndent;
    private String childIndent;
    private int depth;
    private SqlExecutionContext executionContext;
    private boolean useBaseMetadata;

    public PlanSink() {
        this.sink = new EscapingStringSink();
        this.depth = 0;
        this.attrIndent = "  ";
        this.childIndent = "    ";
        this.eolIndexes = new IntList();
        this.eolIndexes.add(0);
        this.factoryStack = new ObjStack<>();
    }

    public PlanSink attr(CharSequence name) {
        newLine();
        sink.put(attrIndent);
        sink.put(name).put(':').put(' ');
        return this;
    }

    public PlanSink child(CharSequence outer, Plannable inner) {
        depth++;
        newLine();
        sink.put(outer);
        child(inner);
        depth--;

        return this;
    }

    public PlanSink child(Plannable p) {
        depth++;
        newLine();
        if (p instanceof RecordCursorFactory) {
            //metadataStack.push(((RecordCursorFactory) p).getMetadata());
            factoryStack.push((RecordCursorFactory) p);
        }
        p.toPlan(this);
        if (p instanceof RecordCursorFactory) {
            factoryStack.pop();
        }
        depth--;

        return this;
    }

    public void clear() {
        reset();
    }

    public void end() {
        newLine();
    }

    public CharSequence getLine(int idx) {
        return sink.subSequence(eolIndexes.getQuick(idx - 1), eolIndexes.getQuick(idx));
    }

    public int getLineCount() {
        return eolIndexes.size() - 1;
    }

    public StringSink getSink() {
        return sink;
    }

    public StringSink getText() {
        StringSink result = Misc.getThreadLocalBuilder();

        for (int i = 0, n = eolIndexes.size() - 1; i < n; i++) {
            result.put(sink, eolIndexes.getQuick(i), eolIndexes.getQuick(i + 1));
        }

        return result;
    }

    public PlanSink meta(CharSequence name) {
        sink.put(" ");
        sink.put(name).put(": ");
        return this;
    }

    public void of(RecordCursorFactory factory, SqlExecutionContext executionContext) {
        clear();
        this.executionContext = executionContext;
        if (factory != null) {
            factoryStack.push(factory);
            factory.toPlan(this);
        }
        end();
    }

    public PlanSink optAttr(CharSequence name, Sinkable value) {
        if (value != null) {
            if (value instanceof ConstantFunction && ((ConstantFunction) value).isNullConstant()) {
                return this;
            }
            attr(name).val(value);
        }
        return this;
    }

    public PlanSink optAttr(CharSequence name, Plannable value) {
        if (value != null) {
            if (value instanceof ConstantFunction && ((ConstantFunction) value).isNullConstant()) {
                return this;
            }
            attr(name).val(value);
        }
        return this;
    }

    public PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value, boolean useBaseMetadata) {
        this.useBaseMetadata = useBaseMetadata;
        optAttr(name, value);
        this.useBaseMetadata = false;
        return this;
    }

    public PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value) {
        if (value != null && value.size() > 0) {
            attr(name).val(value);
        }
        return this;
    }

    public PlanSink put(Sinkable s) {
        val(s);
        return this;
    }

    public PlanSink put(RecordCursorFactory factory) {
        child(factory);
        return this;
    }

    public PlanSink put(CharSequence cs) {
        sink.put(cs);
        return this;
    }

    public PlanSink put(boolean b) {
        sink.put(b);
        return this;
    }

    public PlanSink put(char c) {
        sink.put(c);
        return this;
    }

    public PlanSink put(int i) {
        sink.put(i);
        return this;
    }

    public PlanSink put(double d) {
        sink.put(d);
        return this;
    }

    public PlanSink put(long l) {
        sink.put(l);
        return this;
    }

    public PlanSink put(Function f) {
        return val(f);
    }

    public PlanSink putBaseColumnName(int no) {
        sink.put(factoryStack.peek().getBaseColumnName(no, executionContext));
        return this;
    }

    public PlanSink putBaseColumnNameNoRemap(int no) {
        sink.put(factoryStack.peek().getBaseColumnNameNoRemap(no, executionContext));
        return this;
    }

    public PlanSink putColumnName(int no) {
        if (useBaseMetadata) {
            putBaseColumnName(no);
        } else {
            sink.put(factoryStack.peek().getColumnName(no, executionContext));
        }
        return this;
    }

    public void reset() {
        this.sink.clear();
        this.depth = 0;
        this.attrIndent = "  ";
        this.childIndent = "    ";
        this.eolIndexes.clear();
        this.eolIndexes.add(0);
        this.factoryStack.clear();
        this.executionContext = null;
    }

    public void setUseBaseMetadata(boolean b) {
        this.useBaseMetadata = b;
    }

    public PlanSink type(CharSequence type) {
        sink.put(type);
        return this;
    }

    public PlanSink val(char c) {
        sink.put(c);
        return this;
    }

    public PlanSink val(int i) {
        sink.put(i);
        return this;
    }

    public PlanSink val(long l) {
        sink.put(l);
        return this;
    }

    public PlanSink val(float f) {
        sink.put(f);
        return this;
    }

    public PlanSink val(double d) {
        sink.put(d);
        return this;
    }

    public PlanSink val(boolean b) {
        sink.put(b);
        return this;
    }

    public PlanSink val(CharSequence cs) {
        sink.put(cs);
        return this;
    }

    public PlanSink val(ObjList<? extends Plannable> s) {
        if (s != null) {
            put('[');
            for (int i = 0, n = s.size(); i < n; i++) {
                if (i > 0) {
                    val(',');
                }
                val(s.get(i));
            }
            put(']');
        }
        return this;
    }

    public PlanSink val(Sinkable s) {
        if (s != null) {
            sink.put(s);
        }
        return this;
    }

    public PlanSink val(Plannable s) {
        if (s != null) {
            s.toPlan(this);
        } else {
            sink.put("null");
        }
        return this;
    }

    public PlanSink val(Function s) {
        if (s != null) {
            s.toPlan(this);
        } else {
            sink.put("null");
        }
        return this;
    }

    private void newLine() {
        eolIndexes.add(sink.length());
        for (int i = 0; i < depth; i++) {
            sink.put(childIndent);
        }
    }

    private static class EscapingStringSink extends StringSink {
        @Override
        public CharSink put(CharSequence cs) {
            for (int i = 0, n = cs.length(); i < n; i++) {
                escapeSpace(cs.charAt(i));
            }
            return this;
        }

        @Override
        public CharSink put(CharSequence cs, int lo, int hi) {
            for (int i = lo; i < hi; i++) {
                escapeSpace(cs.charAt(i));
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            escapeSpace(c);
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            for (int i = start; i < start + len; i++) {
                escapeSpace(chars[i]);
            }
            return this;
        }

        private void escapeSpace(char c) {
            if (c < 32) {
                switch (c) {
                    case '\b':
                        super.put("\\b");
                        break;
                    case '\f':
                        super.put("\\f");
                        break;
                    case '\n':
                        super.put("\\n");
                        break;
                    case '\r':
                        super.put("\\r");
                        break;
                    case '\t':
                        super.put("\\t");
                        break;
                    default:
                        super.put("\\u00");
                        super.put(c >> 4);
                        super.put(Numbers.hexDigits[c & 15]);
                        break;
                }
            } else {
                super.put(c);
            }
        }
    }
}
