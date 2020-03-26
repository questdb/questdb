package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;

public class BackupTest extends AbstractGriffinTest {

	@Test
	public void simpleTableTest() throws Exception {
		String tableName = "testTable";
		assertMemoryLeak(() -> {
			// @formatter:off
			compiler.compile("create table " + tableName + " as (select" + 
					" rnd_symbol(4,4,4,2) sym," + 
					" rnd_double(2) d," + 
					" timestamp_sequence(0, 1000000000) ts" + 
					" from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
			// @formatter:on
		});

		TableUtils.backupTable(tableName, configuration);

		String sourceSelectAll = selectAll(tableName);
		engine.close();
		configuration = new DefaultCairoConfiguration(configuration.getBackupRoot());
		engine = new CairoEngine(configuration);
		compiler = new SqlCompiler(engine);
		String backupSelectAll = selectAll(tableName);
		Assert.assertEquals(sourceSelectAll, backupSelectAll);
	}

	private String selectAll(String tableName) throws Exception {
		assertMemoryLeak(() -> {
			CompiledQuery compiledQuery = compiler.compile("select * from " + tableName, sqlExecutionContext);
			RecordCursorFactory factory = compiledQuery.getRecordCursorFactory();
			try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
				sink.clear();
				printer.print(cursor, factory.getMetadata(), true);
				cursor.toTop();
				Record record = cursor.getRecord();
				while (cursor.hasNext()) {
					printer.print(record, factory.getMetadata());
				}
			}
		});
		return sink.toString();
	}
}
