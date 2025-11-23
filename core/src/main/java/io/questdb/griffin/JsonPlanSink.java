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
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;

public class JsonPlanSink extends BasePlanSink {
    final int NODE_ATTR = 2;
    final int NODE_CHILD = 4;
    final int NODE_META = 3;
    final int NODE_NONE = 0;
    final int NODE_TYPE = 1;
    final int NODE_VALUE = 5;
    final String childIndent = "    ";
    int lastNodeDepth = 0;
    int lastNodeType = 0;
    boolean quoteValue = false;
    int valueStartPos = 0;

    @Override
    public PlanSink attr(CharSequence name) {
        checkType(NODE_ATTR);
        sink.put(name);
        return this;
    }

    @Override
    public PlanSink child(CharSequence outer, Plannable inner) {
        checkType(NODE_CHILD);
        depth++;

        lastNodeType = NODE_NONE;
        type(outer);
        child(inner);
        depth--;
        return this;
    }

    @Override
    public PlanSink child(Plannable p) {
        checkType(NODE_CHILD);
        depth++;
        lastNodeType = NODE_NONE;
        if (p instanceof RecordCursorFactory) {
            factoryStack.push((RecordCursorFactory) p);
            p.toPlan(this);
            factoryStack.pop();
        } else {
            p.toPlan(this);
        }
        closeChild();
        lastNodeType = NODE_CHILD;
        lastNodeDepth = --depth;
        return this;
    }

    @Override
    public void clear() {
        super.clear();
        lastNodeDepth = 0;
        lastNodeType = NODE_NONE;
        quoteValue = false;
        valueStartPos = 0;
    }

    @Override
    public void end() {
        switch (lastNodeType) {
            case NODE_TYPE:
            case NODE_VALUE:
                if (quoteValue) {
                    sink.putNoEsc("\"");
                }
                sink.putNoEsc("\n");
                break;
            case NODE_META:
            case NODE_ATTR:
                sink.putNoEsc("\": ");
                break;
            case NODE_CHILD:
                indent();
                sink.putNoEsc("} ]\n");
        }
        depth = lastNodeDepth;
        while (depth-- > 2) {
            indent();
            sink.putNoEsc("} ]\n");
        }
        sink.putNoEsc("    }\n  }\n]");
    }

    @Override
    public CharSequence getLine(int idx) {
        return sink;
    }

    @Override
    public int getLineCount() {
        return 1;
    }

    @Override
    public PlanSink meta(CharSequence name) {
        checkType(NODE_META);
        sink.put(name);
        return this;
    }

    @Override
    public void of(RecordCursorFactory factory, SqlExecutionContext executionContext) {
        clear();
        start();
        this.executionContext = executionContext;
        if (factory != null) {
            factoryStack.push(factory);
            factory.toPlan(this);
        }
        end();
    }

    @Override
    public PlanSink type(CharSequence type) {
        checkType(NODE_TYPE);
        sink.put("Node Type\": \"");
        sink.put(type);
        return this;
    }

    @Override
    public PlanSink val(ObjList<?> list, int from, int to) {
        checkType(NODE_VALUE);
        return super.val(list, from, to);
    }

    @Override
    public PlanSink val(char c) {
        quoteValue = true;
        checkType(NODE_VALUE);
        sink.put(c);
        return this;
    }

    @Override
    public PlanSink val(int i) {
        quoteValue = false;
        checkType(NODE_VALUE);
        sink.put(i);
        return this;
    }

    @Override
    public PlanSink val(long l) {
        quoteValue = false;
        checkType(NODE_VALUE);
        sink.put(l);
        return this;
    }

    @Override
    public PlanSink val(float f) {
        quoteValue = false;
        checkType(NODE_VALUE);
        sink.put(f);
        return this;
    }

    @Override
    public PlanSink val(double d) {
        quoteValue = false;
        checkType(NODE_VALUE);
        sink.put(d);
        return this;
    }

    @Override
    public PlanSink val(boolean b) {
        quoteValue = false;
        checkType(NODE_VALUE);
        sink.put(b);
        return this;
    }

    @Override
    public PlanSink val(CharSequence cs) {
        quoteValue = true;
        checkType(NODE_VALUE);
        sink.put(cs);
        return this;
    }

    @Override
    public PlanSink val(Utf8Sequence utf8) {
        quoteValue = true;
        checkType(NODE_VALUE);
        sink.put(utf8);
        return this;
    }

    @Override
    public PlanSink val(Sinkable s) {
        quoteValue = true;
        checkType(NODE_VALUE);
        sink.put(s);
        return this;
    }

    @Override
    public PlanSink val(Plannable s) {
        quoteValue = true;
        checkType(NODE_VALUE);
        if (s != null) {
            s.toPlan(this);
        } else {
            sink.put("null");
        }
        return this;
    }

    @Override
    public PlanSink val(long hash, int geoHashBits) {
        quoteValue = true;
        checkType(NODE_VALUE);
        GeoHashes.append(hash, geoHashBits, sink);
        return this;
    }

    @Override
    public PlanSink valDecimal(long value, int precision, int scale) {
        quoteValue = true;
        checkType(NODE_VALUE);
        Decimals.append(value, precision, scale, sink);
        return this;
    }

    @Override
    public PlanSink valDecimal(long hi, long lo, int precision, int scale) {
        quoteValue = true;
        checkType(NODE_VALUE);
        Decimals.append(hi, lo, precision, scale, sink);
        return this;
    }

    @Override
    public PlanSink valDecimal(long hh, long hl, long lh, long ll, int precision, int scale) {
        quoteValue = true;
        checkType(NODE_VALUE);
        Decimals.append(hh, hl, lh, ll, precision, scale, sink);
        return this;
    }

    @Override
    public PlanSink valIPv4(int ip) {
        quoteValue = true;
        checkType(NODE_VALUE);
        if (ip == Numbers.IPv4_NULL) {
            sink.put("null");
        } else {
            Numbers.intToIPv4Sink(sink, ip);
        }
        return this;
    }

    @Override
    public PlanSink valLong256(long long0, long long1, long long2, long long3) {
        quoteValue = true;
        checkType(NODE_VALUE);
        Numbers.appendLong256(long0, long1, long2, long3, sink);
        return this;
    }

    @Override
    public PlanSink valUuid(long lo, long hi) {
        quoteValue = true;
        checkType(NODE_VALUE);
        if (Uuid.isNull(lo, hi)) {
            sink.put("null");
        } else {
            Numbers.appendUuid(lo, hi, sink);
        }
        return this;
    }

    private void checkType(int newNodeType) {
        if (lastNodeType == NODE_NONE) {//start of new node
            indent();
            sink.put('"');
            lastNodeType = newNodeType;
            return;
        }
        if (newNodeType == lastNodeType &&
                newNodeType != NODE_CHILD) {
            return;//continuation
        }

        switch (lastNodeType) {
            case NODE_TYPE:
                sink.putNoEsc("\",\n");
                indent();
                break;
            case NODE_META:
            case NODE_ATTR:
                sink.put("\": ");
                break;
            case NODE_VALUE:
                if (quoteValue) {
                    sink.put('"');
                    sink.setCharAt(valueStartPos, '"');
                }
                sink.putNoEsc(",\n");
                indent();
                break;
            case NODE_CHILD:
                indent();
                if (newNodeType == NODE_CHILD) {
                    sink.putNoEsc("},\n");
                } else {
                    sink.putNoEsc("} ]\n");
                }
        }

        if (newNodeType == NODE_CHILD) {
            if (lastNodeType != NODE_CHILD) {
                sink.putNoEsc("\"Plans\": [\n");
            }
            indent();
            sink.putNoEsc("{\n");
        } else {
            char c = '"';
            if (newNodeType == NODE_VALUE) {
                valueStartPos = sink.length();
                if (!quoteValue) {
                    c = ' ';
                }
            }
            sink.put(c);
        }

        lastNodeType = newNodeType;
        lastNodeDepth = depth;
    }

    private void closeChild() {
        switch (lastNodeType) {
            case NODE_CHILD:
                indent();
                sink.putNoEsc("} ]\n");
                break;
            case NODE_VALUE:
                if (quoteValue) {
                    sink.setCharAt(valueStartPos, '"');
                    sink.putNoEsc("\"\n");
                } else {
                    sink.putNoEsc("\n");
                }
                break;
            case NODE_TYPE:
            case NODE_META:
            case NODE_ATTR:
                sink.putNoEsc("\"\n");
        }
    }

    private void indent() {
        for (int i = 0; i < depth; i++) {
            sink.put(childIndent);
        }
    }

    private void start() {
        depth = 2;
        sink.putNoEsc("[\n  {\n    \"Plan\": {\n");
    }
}
