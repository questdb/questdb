/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;

/**
 * Gathers important query execution plan details and prints them in a readable format.
 */
public class TextPlanSink extends BasePlanSink {

    private final IntList eolIndexes;
    private String attrIndent;
    private String childIndent;
    private int depth;

    public TextPlanSink() {
        super();
        this.attrIndent = "  ";
        this.childIndent = "    ";
        this.eolIndexes = new IntList();
        this.eolIndexes.add(0);
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
            factoryStack.push((RecordCursorFactory) p);
            p.toPlan(this);
            factoryStack.pop();
        } else {
            p.toPlan(this);
        }
        depth--;

        return this;
    }

    public void clear() {
        super.clear();
        this.attrIndent = "  ";
        this.childIndent = "    ";
        this.eolIndexes.clear();
        this.eolIndexes.add(0);
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

    public PlanSink meta(CharSequence name) {
        sink.put(" ");
        sink.put(name).put(": ");
        return this;
    }

    public void of(RecordCursorFactory factory, SqlExecutionContext executionContext) {
        clear();
        this.executionContext = executionContext;
        if (executionContext.getBindVariableService() == null) {//web console
            this.childIndent = "&nbsp;&nbsp;&nbsp;&nbsp;";
            this.attrIndent = "&nbsp;&nbsp;";
            this.sink = htmlSink;
        } else { // pg wire
            this.childIndent = "    ";
            this.attrIndent = "  ";
            this.sink = textSink;
        }

        if (factory != null) {
            factoryStack.push(factory);
            factory.toPlan(this);
        }
        end();
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

    public PlanSink val(Utf8Sequence utf8) {
        sink.put(utf8);
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

    public PlanSink val(long hash, int geoHashBits) {
        GeoHashes.append(hash, geoHashBits, sink);
        return this;
    }

    @Override
    public PlanSink valIPv4(int ip) {
        if (ip == Numbers.IPv4_NULL) {
            sink.put("null");
        } else {
            Numbers.intToIPv4Sink(sink, ip);
        }
        return this;
    }


    public PlanSink valLong256(long long0, long long1, long long2, long long3) {
        Numbers.appendLong256(long0, long1, long2, long3, sink);
        return this;
    }

    public PlanSink valUuid(long lo, long hi) {
        if (Uuid.isNull(lo, hi)) {
            sink.put("null");
        } else {
            Numbers.appendUuid(lo, hi, sink);
        }
        return this;
    }

    private void newLine() {
        eolIndexes.add(sink.length());
        for (int i = 0; i < depth; i++) {
            sink.put(childIndent);
        }
    }
}
