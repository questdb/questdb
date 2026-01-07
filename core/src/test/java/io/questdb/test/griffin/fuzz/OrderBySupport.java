package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.std.IntList;

final class OrderBySupport {
    private OrderBySupport() {
    }

    static RecordCursorFactory orderBy(
            FuzzBuildContext context,
            RecordCursorFactory base,
            IntList orderByColumns
    ) throws SqlException {
        ListColumnFilter sortFilter = new ListColumnFilter();
        sortFilter.addAll(orderByColumns);
        if (base.recordCursorSupportsRandomAccess()) {
            return new SortedLightRecordCursorFactory(
                    context.getConfiguration(),
                    base.getMetadata(),
                    base,
                    context.getComparatorCompiler().newInstance(base.getMetadata(), orderByColumns),
                    sortFilter
            );
        }
        EntityColumnFilter columnFilter = new EntityColumnFilter();
        columnFilter.of(base.getMetadata().getColumnCount());
        return new SortedRecordCursorFactory(
                context.getConfiguration(),
                base.getMetadata(),
                base,
                RecordSinkFactory.getInstance(context.getAsm(), base.getMetadata(), columnFilter, context.getConfiguration()),
                context.getComparatorCompiler().newInstance(base.getMetadata(), orderByColumns),
                sortFilter
        );
    }

    static RecordCursorFactory orderByTimestamp(
            FuzzBuildContext context,
            RecordCursorFactory base,
            ScanDirectionRequirement requirement
    ) throws SqlException {
        int tsIndex = base.getMetadata().getTimestampIndex();
        if (tsIndex == -1) {
            return base;
        }
        IntList orderByColumns = new IntList();
        int orderIndex = tsIndex + 1;
        if (requirement == ScanDirectionRequirement.BACKWARD) {
            orderIndex = -orderIndex;
        }
        orderByColumns.add(orderIndex);
        return orderBy(context, base, orderByColumns);
    }
}
