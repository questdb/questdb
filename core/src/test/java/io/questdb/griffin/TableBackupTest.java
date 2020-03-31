package io.questdb.griffin;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.Path;

public class TableBackupTest extends AbstractGriffinTest {
	@BeforeClass
	public static void setUp3() throws IOException {
		final CharSequence backupRoot = temp.newFolder("dbBackupRoot").getAbsolutePath();
		configuration = new DefaultCairoConfiguration(root) {
			@Override
			public CharSequence getBackupRoot() {
				return backupRoot;
			}
		};
		engine = new CairoEngine(configuration, messageBus);
		compiler = new SqlCompiler(engine);
	}

	@Test
	public void simpleTableTest1() throws Exception {
		String tableName = "testTable";
		Path path = new Path();
		AbstractGriffinTest.assertMemoryLeak(() -> {
			// @formatter:off
			compiler.compile("create table " + tableName + " as (select" + 
					" rnd_symbol(4,4,4,2) sym," + 
					" rnd_double(2) d," + 
					" timestamp_sequence(0, 1000000000) ts" + 
					" from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
			// @formatter:on
		});

		TableBackupManager tableBackupManager = new TableBackupManager(configuration, engine, compiler);
		AbstractGriffinTest.assertMemoryLeak(() -> {
			tableBackupManager.backupTable(sqlExecutionContext.getCairoSecurityContext(), tableName, path);
		});

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
