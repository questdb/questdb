package io.questdb.griffin;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.RecordCursorPrinter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class TableBackupTest {
	private static final StringSink sink = new StringSink();
	private static final RecordCursorPrinter printer = new RecordCursorPrinter(sink);
	@ClassRule
	public static TemporaryFolder temp = new TemporaryFolder();

	private static Path path;

	private static CairoConfiguration mainConfiguration;
	private static CairoEngine mainEngine;
	private static SqlCompiler mainCompiler;
	private static SqlExecutionContext mainSqlExecutionContext;

	private static CairoEngine backupEngine;
	private static SqlCompiler backupCompiler;
	private static SqlExecutionContext backupSqlExecutionContext;

	@Test
	public void simpleTableTest1() throws Exception {
		assertMemoryLeak(() -> {
			String tableName = "testTable1";
			// @formatter:off
			mainCompiler.compile("create table " + tableName + " as (select" + 
					" rnd_symbol(4,4,4,2) sym," + 
					" rnd_double(2) d," + 
					" timestamp_sequence(0, 1000000000) ts" + 
					" from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
			// @formatter:on

			mainCompiler.backupTable(tableName, mainSqlExecutionContext);

			String sourceSelectAll = selectAll(mainEngine, mainCompiler, mainSqlExecutionContext, tableName);
			String backupSelectAll = selectAll(backupEngine, backupCompiler, backupSqlExecutionContext, tableName);
			Assert.assertEquals(sourceSelectAll, backupSelectAll);
		});
	}

	@Test
	public void allTypesPartitionedTableTest1() throws Exception {
		assertMemoryLeak(() -> {
			String tableName = "testTable2";
			// @formatter:off
			mainCompiler.compile("create table " + tableName + " as (" +
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
                        ")  timestamp(k) partition by DAY", mainSqlExecutionContext);
			// @formatter:on

			mainCompiler.backupTable(tableName, mainSqlExecutionContext);

			String sourceSelectAll = selectAll(mainEngine, mainCompiler, mainSqlExecutionContext, tableName);
			String backupSelectAll = selectAll(backupEngine, backupCompiler, backupSqlExecutionContext, tableName);
			Assert.assertEquals(sourceSelectAll, backupSelectAll);
		});
	}

	private String selectAll(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String tableName) throws Exception {
		CompiledQuery compiledQuery = compiler.compile("select * from " + tableName, sqlExecutionContext);
		try (RecordCursorFactory factory = compiledQuery.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
			sink.clear();
			printer.print(cursor, factory.getMetadata(), true);
			cursor.toTop();
			Record record = cursor.getRecord();
			while (cursor.hasNext()) {
				printer.print(record, factory.getMetadata());
			}
		}
		return sink.toString();
	}

	@BeforeClass
	public static void setup() throws IOException {
		path = new Path();
		CharSequence root = temp.newFolder("dbRoot").getAbsolutePath();
		CharSequence backupRoot = temp.newFolder("dbBackupRoot").getAbsolutePath();

		mainConfiguration = new DefaultCairoConfiguration(root) {
			@Override
			public CharSequence getBackupRoot() {
				return backupRoot;
			}
		};
		MessageBus mainMessageBus = new MessageBusImpl();
		mainEngine = new CairoEngine(mainConfiguration, mainMessageBus);
		mainCompiler = new SqlCompiler(mainEngine);
		mainSqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableService(), mainMessageBus);

		CairoConfiguration backupConfiguration = new DefaultCairoConfiguration(backupRoot);
		MessageBus backupMessageBus = new MessageBusImpl();
		backupEngine = new CairoEngine(backupConfiguration, backupMessageBus);
		backupCompiler = new SqlCompiler(backupEngine);
		backupSqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableService(), backupMessageBus);
	}

	@AfterClass
	public static void tearDown() {
		path.close();
	}

	private void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
		TestUtils.assertMemoryLeak(() -> {
			try {
				code.run();
				mainEngine.releaseInactive();
				Assert.assertEquals(0, mainEngine.getBusyWriterCount());
				Assert.assertEquals(0, mainEngine.getBusyReaderCount());
				backupEngine.releaseInactive();
				Assert.assertEquals(0, backupEngine.getBusyWriterCount());
				Assert.assertEquals(0, backupEngine.getBusyReaderCount());
			} finally {
				mainEngine.releaseAllReaders();
				mainEngine.releaseAllWriters();
				backupEngine.releaseAllReaders();
				backupEngine.releaseAllWriters();
			}
		});
	}
}
