package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.PropServerConfiguration;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;

public class ShowServerConfCursorFactory extends AbstractRecordCursorFactory {

    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final ShowServerConfRecordCursor cursor = new ShowServerConfRecordCursor();

    public ShowServerConfCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(loadServerConf(executionContext.getCairoEngine().getConfiguration().getRoot()));
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_server_conf");
    }

    private static Properties loadServerConf(String root) {
        java.nio.file.Path configFile = Paths.get(root, "..", PropServerConfiguration.CONFIG_DIRECTORY, "/server.conf");
        Properties properties = new Properties();
        try (InputStream is = java.nio.file.Files.newInputStream(configFile)) {
            properties.load(is);
        } catch (IOException err) {
            throw CairoException.critical(-1).put("cannot find QuestDB's configuration: ").put(err.getMessage());
        }
        return properties;
    }

    private static class ShowServerConfRecordCursor implements RecordCursor {
        private Iterator<Object> keyIterator;
        private String lastKey;
        private Properties properties;
        private final Record record = new Record() {
            @Override
            public CharSequence getStr(int col) {
                if (col == 0) { // name
                    return lastKey;
                }
                if (col == 1) { // value
                    return properties.getProperty(lastKey);

                }
                return null;
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

        @Override
        public void close() {
            keyIterator = null;
            properties.clear();
            properties = null;
            lastKey = null;
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
            boolean hasNext = keyIterator.hasNext();
            if (hasNext) {
                lastKey = (String) keyIterator.next();
            }
            return hasNext;
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
            keyIterator = properties.keySet().iterator();
            lastKey = null;
        }

        private ShowServerConfRecordCursor of(Properties properties) {
            this.properties = properties;
            toTop();
            return this;
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("name", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("value", ColumnType.STRING));
    }
}
