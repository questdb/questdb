package io.questdb.griffin;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.Path;

public class TableBackupTest extends AbstractGriffinTest {
	private CharSequence backupRoot;

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
		setup(backupRoot, null);
		String backupSelectAll = selectAll(tableName);
		Assert.assertEquals(sourceSelectAll, backupSelectAll);
	}

	@Test
	public void allTypesPartitionedTableTest1() throws Exception {
		String tableName = "testTable";
		Path path = new Path();
		AbstractGriffinTest.assertMemoryLeak(() -> {
			// @formatter:off
			compiler.compile("create table " + tableName + " as (" +
                        "select" +
                        " rnd_char() ch," +
                        " rnd_long256() ll," +
                        " rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m" +
                        " from long_sequence(1000)" +
                        ")  timestamp(k) partition by DAY", sqlExecutionContext);
			// @formatter:on
		});

		TableBackupManager tableBackupManager = new TableBackupManager(configuration, engine, compiler);
		AbstractGriffinTest.assertMemoryLeak(() -> {
			tableBackupManager.backupTable(sqlExecutionContext.getCairoSecurityContext(), tableName, path);
		});

		String sourceSelectAll = selectAll(tableName);
		setup(backupRoot, null);
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

	private void setup(CharSequence root, CharSequence backupRoot) {
		// Tear down previous setup
		engine.close();
		compiler.close();
		configuration = new DefaultCairoConfiguration(root) {
			@Override
			public CharSequence getBackupRoot() {
				return backupRoot;
			}
		};
		engine = new CairoEngine(configuration, messageBus);
		compiler = new SqlCompiler(engine);
		AbstractCairoTest.root = root;
		this.backupRoot = backupRoot;
	}

	private static int nBackupRoot = 0;

	@Before
	public void before() throws IOException {
		backupRoot = temp.newFolder("dbBackupRoot" + ++nBackupRoot).getAbsolutePath();
		setup(root, backupRoot);
	}
}
