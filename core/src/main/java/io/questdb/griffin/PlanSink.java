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

import io.questdb.std.Sinkable;
import io.questdb.std.str.StringSink;

/**
 * Gathers important query execution plan details and prints them in a readable format.
 */
public class PlanSink {

    private final StringSink sink;
    private int depth;
    private String childIndent;
    private String attrIndent;

    public PlanSink() {
        this.sink = new StringSink();
        this.depth = 0;
        this.attrIndent = "  ";
        this.childIndent = "    ";
    }

    public void reset() {
        this.sink.clear();
        this.depth = 0;
        this.attrIndent = "  ";
        this.childIndent = "    ";
    }

    public PlanSink type(CharSequence type) {
        sink.put(type);
        return this;
    }

    public PlanSink meta(CharSequence name) {
        sink.put(" ");
        sink.put(name).put('=');
        return this;
    }

    public PlanSink attr(CharSequence name) {
        newLine();
        sink.put(attrIndent);
        sink.put(name).put('=');
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

    public PlanSink val(Sinkable s) {
        if (s != null) {
            sink.put(s);
        }
        return this;
    }

    public PlanSink child(Plannable p) {
        depth++;
        newLine();
        p.toPlan(this);
        depth--;

        return this;
    }

    private void newLine() {
        sink.put("\n");
        for (int i = 0; i < depth; i++) {
            sink.put(childIndent);
        }
    }

    public CharSequence getText() {
        return sink;
    }
}
