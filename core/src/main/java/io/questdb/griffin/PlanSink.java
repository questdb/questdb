package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

/**
 * Sink used to generate query execution plan.
 * Note: methods like attr(), meta(), child() etc. expect a complete value while
 * calls to val() can be chained to produce a single entry.
 */
public interface PlanSink {

    PlanSink attr(CharSequence name);

    PlanSink child(CharSequence outer, Plannable inner);

    PlanSink child(Plannable p);

    void clear();

    void end();

    CharSequence getLine(int idx);

    int getLineCount();

    @TestOnly
    StringSink getSink();

    @TestOnly
    StringSink getText();

    PlanSink meta(CharSequence name);

    void of(RecordCursorFactory factory, SqlExecutionContext executionContext);

    PlanSink optAttr(CharSequence name, Sinkable value);

    PlanSink optAttr(CharSequence name, Plannable value);

    PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value, boolean useBaseMetadata);

    PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value);

    PlanSink putBaseColumnName(int no);

    PlanSink putBaseColumnNameNoRemap(int no);

    PlanSink putColumnName(int no);

    PlanSink type(CharSequence type);

    PlanSink val(ObjList<?> list);

    PlanSink val(ObjList<?> list, int from);

    PlanSink val(ObjList<?> list, int from, int to);

    PlanSink val(char c);

    PlanSink val(int i);

    PlanSink val(long l);

    PlanSink val(float f);

    PlanSink val(double d);

    PlanSink val(boolean b);

    PlanSink val(CharSequence cs);

    PlanSink val(Sinkable s);

    PlanSink val(Plannable s);

    PlanSink val(long long0, long long1, long long2, long long3);

    PlanSink val(long hash, int geoHashBits);

}
