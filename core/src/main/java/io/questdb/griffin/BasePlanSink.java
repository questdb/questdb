/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjStack;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public abstract class BasePlanSink implements PlanSink {

    protected final ObjStack<RecordCursorFactory> factoryStack;
    protected final HtmlEscapingStringSink htmlSink;
    protected final EscapingStringSink textSink;
    protected int depth;
    protected SqlExecutionContext executionContext;
    protected int order;
    protected EscapingStringSink sink;
    protected boolean useBaseMetadata;

    public BasePlanSink() {
        this.htmlSink = new HtmlEscapingStringSink();
        this.textSink = new EscapingStringSink();
        this.sink = textSink;
        this.depth = 0;
        this.factoryStack = new ObjStack<>();
        this.order = -1;
    }

    public PlanSink child(Plannable p, int order) {
        this.order = order;
        child(p);
        this.order = -1;

        return this;
    }

    public void clear() {
        this.sink.clear();
        this.depth = 0;
        this.factoryStack.clear();
        this.executionContext = null;
        this.order = -1;
    }

    @Override
    public SqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public int getOrder() {
        return order;
    }

    @TestOnly
    public StringSink getSink() {
        return sink;
    }

    public boolean getUseBaseMetadata() {
        return useBaseMetadata;
    }

    public PlanSink optAttr(CharSequence name, Sinkable value) {
        if (value != null) {
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

    public PlanSink optAttr(CharSequence name, Plannable value, boolean useBaseMetadata) {
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

    public PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value, boolean useBaseMetadata) {
        this.useBaseMetadata = useBaseMetadata;
        optAttr(name, value);
        this.useBaseMetadata = false;
        return this;
    }

    public PlanSink putBaseColumnName(int columnIdx) {
        return val(factoryStack.peek().getBaseColumnName(columnIdx));
    }

    public PlanSink putBaseColumnNameNoRemap(int columnIdx) {
        return val(factoryStack.peek().getBaseColumnNameNoRemap(columnIdx));
    }

    public PlanSink putColumnName(int columnIdx) {
        if (useBaseMetadata) {
            putBaseColumnName(columnIdx);
        } else {
            val(factoryStack.peek().getMetadata().getColumnName(columnIdx));
        }
        return this;
    }

    @Override
    public void useBaseMetadata(boolean useBaseMetdata) {
        this.useBaseMetadata = useBaseMetdata;
    }

    public PlanSink val(ObjList<?> list) {
        return val(list, 0, list.size());
    }

    public PlanSink val(ObjList<?> list, int from) {
        return val(list, from, list.size());
    }

    public PlanSink val(ObjList<?> list, int from, int to) {
        sink.put('[');
        for (int i = from; i < to; i++) {
            if (i > from) {
                sink.put(',');
            }
            Object obj = list.getQuick(i);
            if (obj instanceof Plannable) {
                ((Plannable) obj).toPlan(this);
            } else if (obj instanceof Sinkable) {
                sink.put((Sinkable) obj);
            } else if (obj == null) {
                sink.put("null");
            } else {
                sink.put(obj.toString());
            }
        }
        sink.put(']');

        return this;
    }

    @Override
    public PlanSink valISODate(long l) {
        sink.putISODate(l);
        return this;
    }

    static class EscapingStringSink extends StringSink {
        @Override
        public StringSink put(@Nullable CharSequence cs) {
            if (cs != null) {
                put(cs, 0, cs.length());
            }
            return this;
        }

        @Override
        public StringSink put(@NotNull CharSequence cs, int lo, int hi) {
            for (int i = lo; i < hi; i++) {
                escape(cs.charAt(i));
            }
            return this;
        }

        @Override
        public StringSink put(char c) {
            escape(c);
            return this;
        }

        @Override
        public StringSink put(char @NotNull [] chars, int start, int len) {
            for (int i = start; i < start + len; i++) {
                escape(chars[i]);
            }
            return this;
        }

        public StringSink putNoEsc(CharSequence cs) {
            super.put(cs);
            return this;
        }

        protected void escape(char c) {
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

    static class HtmlEscapingStringSink extends EscapingStringSink {
        protected void escape(char c) {
            if (c == '<') {
                super.put("&lt;");
            } else if (c == '>') {
                super.put("&gt;");
            } else {
                super.escape(c);
            }
        }
    }
}
