package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.ConfigProperty;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjObjHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class ShowServerConfCursorFactory extends AbstractRecordCursorFactory {

    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final ShowServerConfRecordCursor cursor = new ShowServerConfRecordCursor();

    public ShowServerConfCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext.getCairoEngine().getConfiguration().getAllPairs());
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_server_conf");
    }

    private static final class EmptyIterator implements Iterator<ObjObjHashMap.Entry<ConfigProperty, String>> {
        private static final EmptyIterator INSTANCE = new EmptyIterator();

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ObjObjHashMap.Entry<ConfigProperty, String> next() {
            return null;
        }
    }

    private static class ShowServerConfRecordCursor implements RecordCursor {
        private ObjObjHashMap<ConfigProperty, String> allPairs;
        private ObjObjHashMap.Entry<ConfigProperty, String> entry;
        private final Record record = new Record() {
            @Override
            public CharSequence getStr(int col) {
                switch (col) {
                    case 0:
                        return entry.key.getPropertyPath();
                    case 1:
                        return entry.value;
                    default:
                        return null;
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                CharSequence s = getStr(col);
                return s != null ? s.length() : -1;
            }
        };
        @NotNull private Iterator<ObjObjHashMap.Entry<ConfigProperty, String>> iterator = EmptyIterator.INSTANCE;

        @Override
        public void close() {
            iterator = EmptyIterator.INSTANCE;
            allPairs = null;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (iterator.hasNext()) {
                entry = iterator.next();
                return true;
            }
            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return -1L;
        }

        @Override
        public void toTop() {
            iterator = allPairs != null ? allPairs.iterator() : EmptyIterator.INSTANCE;
            entry = null;
        }

        private ShowServerConfRecordCursor of(ObjObjHashMap<ConfigProperty, String> allPairs) {
            this.allPairs = allPairs;
            toTop();
            return this;
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("name", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("value", ColumnType.STRING));
    }
}
