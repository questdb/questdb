/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalCatalogueTest extends AbstractCairoTest {
    private static final String PARTITION_COLUMNS = "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\thasParquetGenerated\tisParquet\tparquetFileSize\tseqTxn\tisRemotelyServed";

    @Test
    public void testEmptyCataloguesPreserveAllColumnMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> names = new ObjList<>();
            names.add("pg_description");
            names.add("pg_enum");
            names.add("pg_index");
            names.add("pg_inherits");
            names.add("pg_locks");
            names.add("pg_range");
            names.add("pg_roles");
            names.add("pg_catalog.pg_shdescription");
            names.add("information_schema.key_column_usage");
            names.add("information_schema.referential_constraints");
            names.add("information_schema.table_constraints");
            names.add("pg_catalog.pg_description");
            names.add("pg_catalog.pg_index");
            names.add("pg_catalog.pg_inherits");
            names.add("pg_catalog.pg_locks");
            names.add("pg_catalog.pg_roles");
            {
                final int i = 0;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        objoid	classoid	objsubid	description
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        objoid	classoid	objsubid	description
                        """);
            }
            {
                final int i = 1;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        oid	enumtypid	enumsortorder	enumlabel
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        oid	enumtypid	enumsortorder	enumlabel
                        """);
            }
            {
                final int i = 2;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        indexrelid	indrelid	indnatts	indnkeyatts	indisunique	indnullsnotdistinct	indisprimary	indisexclusion	indimmediate	indisclustered	indisvalid	indcheckxmin	indisready	indislive	indisreplident	indkey	indcollation	indclass	indoption	indexprs	indpred
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        indexrelid	indrelid	indnatts	indnkeyatts	indisunique	indnullsnotdistinct	indisprimary	indisexclusion	indimmediate	indisclustered	indisvalid	indcheckxmin	indisready	indislive	indisreplident	indkey	indcollation	indclass	indoption	indexprs	indpred
                        """);
            }
            {
                final int i = 3;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        inhrelid	inhparent	inhseqno
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        inhrelid	inhparent	inhseqno
                        """);
            }
            {
                final int i = 4;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        locktype	database	relation	page	tuple	virtualxid	transactionid	classid	objid	objsubid	virtualtransaction	pid	mode	granted	fastpath	waitstart
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        locktype	database	relation	page	tuple	virtualxid	transactionid	classid	objid	objsubid	virtualtransaction	pid	mode	granted	fastpath	waitstart
                        """);
            }
            {
                final int i = 5;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        rngtypid	rngsubtype	rngcollation	rngsubopc	rngcanonical	rngsubdiff
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        rngtypid	rngsubtype	rngcollation	rngsubopc	rngcanonical	rngsubdiff
                        """);
            }
            {
                final int i = 6;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        rolname	rolsuper	rolinherit	rolcreaterole	rolcreatedb	rolcanlogin	rolreplication	rolconnlimit	rolpassword	rolvaliduntil	rolbypassrls	rolconfig	oid
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        rolname	rolsuper	rolinherit	rolcreaterole	rolcreatedb	rolcanlogin	rolreplication	rolconnlimit	rolpassword	rolvaliduntil	rolbypassrls	rolconfig	oid
                        """);
            }
            {
                final int i = 7;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        objoid	classoid	description
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        objoid	classoid	description
                        """);
            }
            {
                final int i = 8;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        constraint_catalog	constraint_schema	constraint_name	table_catalog	table_schema	table_name	column_name	ordinal_position	position_in_unique_constraint
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        constraint_catalog	constraint_schema	constraint_name	table_catalog	table_schema	table_name	column_name	ordinal_position	position_in_unique_constraint
                        """);
            }
            {
                final int i = 9;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        constraint_catalog	constraint_schema	constraint_name	unique_constraint_catalog	unique_constraint_schema	unique_constraint_name	match_option	update_rule	delete_rule
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        constraint_catalog	constraint_schema	constraint_name	unique_constraint_catalog	unique_constraint_schema	unique_constraint_name	match_option	update_rule	delete_rule
                        """);
            }
            {
                final int i = 10;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        constraint_catalog	constraint_schema	constraint_name	table_catalog	table_schema	table_name	constraint_type	is_deferrable	initially_deferred	enforced	nulls_distinct
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        constraint_catalog	constraint_schema	constraint_name	table_catalog	table_schema	table_name	constraint_type	is_deferrable	initially_deferred	enforced	nulls_distinct
                        """);
            }
            {
                final int i = 11;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        objoid	classoid	objsubid	description
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        objoid	classoid	objsubid	description
                        """);
            }
            {
                final int i = 12;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        indexrelid	indrelid	indnatts	indnkeyatts	indisunique	indnullsnotdistinct	indisprimary	indisexclusion	indimmediate	indisclustered	indisvalid	indcheckxmin	indisready	indislive	indisreplident	indkey	indcollation	indclass	indoption	indexprs	indpred
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        indexrelid	indrelid	indnatts	indnkeyatts	indisunique	indnullsnotdistinct	indisprimary	indisexclusion	indimmediate	indisclustered	indisvalid	indcheckxmin	indisready	indislive	indisreplident	indkey	indcollation	indclass	indoption	indexprs	indpred
                        """);
            }
            {
                final int i = 13;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        inhrelid	inhparent	inhseqno
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        inhrelid	inhparent	inhseqno
                        """);
            }
            {
                final int i = 14;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        locktype	database	relation	page	tuple	virtualxid	transactionid	classid	objid	objsubid	virtualtransaction	pid	mode	granted	fastpath	waitstart
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        locktype	database	relation	page	tuple	virtualxid	transactionid	classid	objid	objsubid	virtualtransaction	pid	mode	granted	fastpath	waitstart
                        """);
            }
            {
                final int i = 15;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "()", """
                        rolname	rolsuper	rolinherit	rolcreaterole	rolcreatedb	rolcanlogin	rolreplication	rolconnlimit	rolpassword	rolvaliduntil	rolbypassrls	rolconfig	oid
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i), """
                        rolname	rolsuper	rolinherit	rolcreaterole	rolcreatedb	rolcanlogin	rolreplication	rolconnlimit	rolpassword	rolvaliduntil	rolbypassrls	rolconfig	oid
                        """);
            }
            assertQueryRows("SELECT count() FROM pg_catalog.pg_locks()", """
                    count
                    0
                    """);
            assertQueryRows(
                    "SELECT 7 AS answer FROM information_schema.table_constraints()",
                    """
                            answer
                            """
            );
        });
    }

    @Test
    public void testEngineCataloguesJoinFilterAndProject() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW lp_catalogue_view AS (SELECT id FROM lp_catalogue_a)");
            drainWalAndViewQueues();
            assertQueryRows("SELECT table_name FROM all_tables() ORDER BY table_name", """
                    table_name
                    lp_catalogue_a
                    lp_catalogue_b
                    lp_catalogue_view
                    """);
            assertQueryRows(
                    "SELECT table_name,table_type,is_insertable_into FROM information_schema.tables ORDER BY table_name",
                    """
                            table_name	table_type	is_insertable_into
                            lp_catalogue_a	BASE TABLE	true
                            lp_catalogue_b	BASE TABLE	true
                            lp_catalogue_view	VIEW	false
                            """
            );
            assertQueryRows(
                    "SELECT table_name,column_name,data_type,ordinal_position FROM information_schema.columns() ORDER BY table_name,ordinal_position",
                    """
                            table_name	column_name	data_type	ordinal_position
                            lp_catalogue_a	id	integer	0
                            lp_catalogue_a	label	character varying	1
                            lp_catalogue_a	ts	timestamp without time zone	2
                            lp_catalogue_b	id	bigint	0
                            lp_catalogue_view	id	integer	0
                            """
            );
            assertQueryRows(
                    "SELECT table_name,column_name,data_type FROM information_schema.questdb_columns ORDER BY table_name,ordinal_position",
                    """
                            table_name	column_name	data_type
                            lp_catalogue_a	id	INT
                            lp_catalogue_a	label	STRING
                            lp_catalogue_a	ts	TIMESTAMP
                            lp_catalogue_b	id	LONG
                            lp_catalogue_view	id	INT
                            """
            );
            assertQueryRows(
                    "SELECT view_name,view_sql,view_status FROM views() ORDER BY view_name",
                    """
                            view_name	view_sql	view_status
                            lp_catalogue_view	SELECT id FROM lp_catalogue_a	valid
                            """
            );
            assertQueryRows(
                    "SELECT name,signature,runtime_constant,type FROM functions() WHERE name='abs' ORDER BY signature",
                    """
                            name	signature	runtime_constant	type
                            abs	abs(D)	false	STANDARD
                            abs	abs(E)	false	STANDARD
                            abs	abs(I)	false	STANDARD
                            abs	abs(L)	false	STANDARD
                            abs	abs(Ξ)	false	STANDARD
                            """
            );
            assertQueryRows(
                    "SELECT t.table_name,c.column_name,c.data_type FROM information_schema.tables t "
                    + "JOIN information_schema.questdb_columns c ON t.table_name=c.table_name "
                    + "WHERE t.is_insertable_into ORDER BY t.table_name,c.ordinal_position",
                    """
                            table_name	column_name	data_type
                            lp_catalogue_a	id	INT
                            lp_catalogue_a	label	STRING
                            lp_catalogue_a	ts	TIMESTAMP
                            lp_catalogue_b	id	LONG
                            """
            );
            assertQueryRows("SELECT count() FROM all_tables()", """
                    count
                    3
                    """);
            assertQueryRows("SELECT 7 AS answer FROM views()", """
                    answer
                    7
                    """);
        });
    }

    @Test
    public void testExplainRetainsExistingCatalogueFactories() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQueryRows("EXPLAIN SELECT * FROM pg_namespace()", """
                    QUERY PLAN
                    GenericRecord
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM pg_catalog.pg_database()", """
                    QUERY PLAN
                    pg_database()
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM pg_catalog.pg_locks()", """
                    QUERY PLAN
                    Empty table
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM information_schema.tables()", """
                    QUERY PLAN
                    information_schema.tables
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM information_schema.columns()", """
                    QUERY PLAN
                    information_schema.columns()
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM views()", """
                    QUERY PLAN
                    views()
                    """);
            assertQueryRows("EXPLAIN SELECT * FROM functions()", """
                    QUERY PLAN
                    functions()
                    """);
        });
    }

    @Test
    public void testFailedBindingClosesPreparationsAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_catalogue_dest (id INT)");
            final ObjList<String> queries = new ObjList<>();
            final IntList positions = new IntList();
            final ObjList<String> messages = new ObjList<>();
            queries.add("SELECT * FROM pg_database(1)");
            positions.add(14);
            messages.add("wrong number of arguments for function `pg_database`; expected: 0, provided: 1");
            queries.add("SELECT missing FROM pg_namespace()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT missing FROM pg_class()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT missing FROM pg_catalog.pg_attrdef()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT missing FROM pg_attribute()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT missing FROM pg_proc()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT * FROM pg_class(1)");
            positions.add(14);
            messages.add("wrong number of arguments for function `pg_class`; expected: 0, provided: 1");
            queries.add("SELECT table_name FROM all_tables() ORDER BY missing");
            positions.add(45);
            messages.add("Invalid column: missing");
            queries.add("INSERT INTO lp_catalogue_dest(id) SELECT * FROM pg_database()");
            positions.add(12);
            messages.add("column count mismatch");
            queries.add("SELECT * FROM table_partitions('lp_missing')");
            positions.add(31);
            messages.add("table does not exist [table=lp_missing]");
            queries.add("SELECT * FROM table_partitions(NULL)");
            positions.add(31);
            messages.add("table name cannot be NULL");
            queries.add("SELECT * FROM table_columns(NULL)");
            positions.add(28);
            messages.add("table name cannot be NULL");
            queries.add("SELECT * FROM wal_transactions(NULL)");
            positions.add(31);
            messages.add("table name cannot be NULL");
            queries.add("SELECT * FROM table_partitions()");
            positions.add(14);
            messages.add("function `table_partitions` requires arguments: table_partitions(STRING constant)");
            queries.add("SELECT * FROM table_storage(1)");
            positions.add(14);
            messages.add("wrong number of arguments for function `table_storage`; expected: 0, provided: 1");
            queries.add("SELECT missing FROM table_partitions('lp_catalogue_dest')");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("SELECT missing FROM table_storage()");
            positions.add(7);
            messages.add("Invalid column: missing");
            queries.add("INSERT INTO lp_catalogue_dest(id) SELECT * FROM table_partitions('lp_catalogue_dest')");
            positions.add(12);
            messages.add("column count mismatch");
            queries.add("INSERT INTO lp_catalogue_dest(id) SELECT * FROM table_storage()");
            positions.add(12);
            messages.add("column count mismatch");
            queries.add("SELECT * FROM read_parquet('example.parquet')");
            positions.add(27);
            messages.add("failed to read parquet file: example.parquet: [27] parquet files can only be read from sql.copy.input.root, please add sql.copy.input.root=<path> to your configuration");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0, n = queries.size(); i < n; i++) {
                    final String sql = queries.getQuick(i);
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail(sql);
                    } catch (SqlException e) {
                        Assert.assertEquals(sql, positions.getQuick(i), e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), messages.getQuick(i));
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT 7 AS answer FROM pg_database()", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, "answer\n7\n");
                    }
                }
            }
            assertQueryRows("SELECT * FROM information_schema._pg_expandarray(1)", """
                    x	n
                    """);
        });
    }

    @Test
    public void testPartitionSourceMetadataAndPruning() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionTable("lp_meta_micro", "TIMESTAMP", "000001", "000009");
            createPartitionTable("lp_meta_nano", "TIMESTAMP_NS", "000000001", "000000009");
            execute("CREATE TABLE lp_meta_plain(id INT)");
            execute("INSERT INTO lp_meta_plain VALUES(1)");
            assertPartitionMetadata("lp_meta_micro", ColumnType.TIMESTAMP_MICRO);
            assertPartitionMetadata("lp_meta_nano", ColumnType.TIMESTAMP_NANO);
            assertPartitionMetadata("lp_meta_plain", ColumnType.TIMESTAMP_NANO);
            assertSourceHeader("SELECT * FROM table_partitions('lp_meta_micro')", PARTITION_COLUMNS, 18);
            assertSourceHeader("SELECT * FROM table_partitions('lp_meta_nano')", PARTITION_COLUMNS, 18);
            assertSourceHeader("SELECT * FROM table_partitions('lp_meta_plain')", PARTITION_COLUMNS, 18);
            assertSource("SELECT maxTimestamp AS hi,name,minTimestamp AS lo FROM table_partitions('lp_meta_micro') "
                    + "WHERE active ORDER BY minTimestamp DESC", """
                    hi	name	lo
                    2020-01-02T00:00:00.000009Z	2020-01-02	2020-01-02T00:00:00.000009Z
                    """, 18);
            assertSource("SELECT maxTimestamp AS hi,name,minTimestamp AS lo FROM table_partitions('lp_meta_nano') "
                    + "WHERE active ORDER BY minTimestamp DESC", """
                    hi	name	lo
                    2020-01-02T00:00:00.000000009Z	2020-01-02	2020-01-02T00:00:00.000000009Z
                    """, 18);
            assertSource("SELECT numRows FROM table_partitions('lp_meta_nano') ORDER BY maxTimestamp DESC LIMIT 1", """
                    numRows
                    1
                    """, 2);
            assertSource("SELECT count() FROM table_partitions('lp_meta_micro')", """
                    count
                    2
                    """, 0);
            assertSource("SELECT 7 AS answer FROM table_partitions('lp_meta_micro')", """
                    answer
                    7
                    7
                    """, 18);
            assertQueryRows("EXPLAIN SELECT * FROM table_partitions('lp_meta_nano')", """
                    QUERY PLAN
                    show_partitions of: lp_meta_nano
                    """);
        });
    }

    @Test
    public void testPartitionSourceReadsConvertedParquetMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionTable("lp_meta_parquet", "TIMESTAMP_NS", "000000001", "000000009");
            execute("ALTER TABLE lp_meta_parquet CONVERT PARTITION TO PARQUET WHERE ts<'2020-01-02'");
            assertSource("SELECT name,minTimestamp,maxTimestamp,numRows,hasParquetGenerated,isParquet,parquetFileSize>0 AS has_file "
                    + "FROM table_partitions('lp_meta_parquet') ORDER BY name", """
                    name	minTimestamp	maxTimestamp	numRows	hasParquetGenerated	isParquet	has_file
                    2020-01-01	2020-01-01T00:00:00.000000001Z	2020-01-01T00:00:00.000000001Z	1	true	true	true
                    2020-01-02	2020-01-02T00:00:00.000000009Z	2020-01-02T00:00:00.000000009Z	1	false	false	false
                    """, 18);
            assertSource("SELECT name,minTimestamp FROM table_partitions('lp_meta_parquet') WHERE isParquet", """
                    name	minTimestamp
                    2020-01-01	2020-01-01T00:00:00.000000001Z
                    """, 18);
        });
    }

    @Test
    public void testPartitionSourceRetainsTableIdentityAfterRecreation() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionTable("lp_meta_identity", "TIMESTAMP", "000001", "000009");
            final String sql = "SELECT name,minTimestamp FROM table_partitions('lp_meta_identity') ORDER BY name";
            try (RecordCursorFactory logical = compileRetained(sql)) {
                assertResult(logical, """
                        name	minTimestamp
                        2020-01-01	2020-01-01T00:00:00.000001Z
                        2020-01-02	2020-01-02T00:00:00.000009Z
                        """);
                execute("DROP TABLE lp_meta_identity");
                TestUtils.assertContains(cursorError(logical, CairoException.class), "table does not exist [table=lp_meta_identity]");

                createPartitionTable("lp_meta_identity", "TIMESTAMP_NS", "000000001", "000000009");
                TestUtils.assertContains(cursorError(logical, TableReferenceOutOfDateException.class), "cached query plan cannot be used");
                Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, logical.getMetadata().getColumnType(1));
                assertPartitionMetadata("lp_meta_identity", ColumnType.TIMESTAMP_NANO);
                assertQueryRows(sql, """
                        name	minTimestamp
                        2020-01-01	2020-01-01T00:00:00.000000001Z
                        2020-01-02	2020-01-02T00:00:00.000000009Z
                        """);
            }
        });
    }

    @Test
    public void testPgRelationCataloguesProjectFilterJoinAndExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE lp_catalogue_a ADD COLUMN dropped STRING");
            execute("ALTER TABLE lp_catalogue_a DROP COLUMN dropped");
            {
                final String source = "pg_class";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            oid	relname	relnamespace	reltype	reloftype	relowner	relam	relfilenode	reltablespace	relpages	reltuples	relallvisible	reltoastrelid	relhasindex	relisshared	relpersistence	relkind	relnatts	relchecks	relhasrules	relhastriggers	relhassubclass	relrowsecurity	relforcerowsecurity	relispopulated	relreplident	relispartition	relrewrite	relfrozenxid	relminmxid	relacl	reloptions	relpartbound	relhasoids	xmin
                            1	lp_catalogue_a	2200	0	0	0	0	0	0	false	-1.0	0	0	false	false	p	r	0	0	false	false	false	false	false	true	d	false	0	0	0				false	0
                            2	lp_catalogue_b	2200	0	0	0	0	0	0	false	-1.0	0	0	false	false	p	r	0	0	false	false	false	false	false	true	d	false	0	0	0				false	0
                            1259	pg_class	11	0	0	0	0	0	0	false	-1.0	0	0	false	false	u	r	0	0	false	false	false	false	false	false	d	false	0	0	0				false	0
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [oid, relname]
                                pg_class
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        3
                        """);
            }
            {
                final String source = "pg_catalog.pg_class";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            oid	relname	relnamespace	reltype	reloftype	relowner	relam	relfilenode	reltablespace	relpages	reltuples	relallvisible	reltoastrelid	relhasindex	relisshared	relpersistence	relkind	relnatts	relchecks	relhasrules	relhastriggers	relhassubclass	relrowsecurity	relforcerowsecurity	relispopulated	relreplident	relispartition	relrewrite	relfrozenxid	relminmxid	relacl	reloptions	relpartbound	relhasoids	xmin
                            1	lp_catalogue_a	2200	0	0	0	0	0	0	false	-1.0	0	0	false	false	p	r	0	0	false	false	false	false	false	true	d	false	0	0	0				false	0
                            2	lp_catalogue_b	2200	0	0	0	0	0	0	false	-1.0	0	0	false	false	p	r	0	0	false	false	false	false	false	true	d	false	0	0	0				false	0
                            1259	pg_class	11	0	0	0	0	0	0	false	-1.0	0	0	false	false	u	r	0	0	false	false	false	false	false	false	d	false	0	0	0				false	0
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [oid, relname]
                                pg_class
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        3
                        """);
            }
            {
                final String source = "pg_attribute";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            attrelid	attname	attnum	atttypid	attnotnull	atttypmod	attlen	attidentity	attisdropped	atthasdef
                            1	id	1	23	false	-1	4		false	true
                            1	label	2	1043	false	-1	-1		false	true
                            1	ts	3	1114	false	-1	8		false	true
                            2	id	1	20	false	-1	8		false	true
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [attrelid, attname]
                                pg_attribute()
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        4
                        """);
            }
            {
                final String source = "pg_catalog.pg_attribute";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            attrelid	attname	attnum	atttypid	attnotnull	atttypmod	attlen	attidentity	attisdropped	atthasdef
                            1	id	1	23	false	-1	4		false	true
                            1	label	2	1043	false	-1	-1		false	true
                            1	ts	3	1114	false	-1	8		false	true
                            2	id	1	20	false	-1	8		false	true
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [attrelid, attname]
                                pg_attribute()
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        4
                        """);
            }
            {
                final String source = "pg_attrdef";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            adrelid	adnum	adbin
                            1	1\t
                            1	2\t
                            1	3\t
                            1	4\t
                            2	1\t
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [adrelid, adnum]
                                pg_attrdef()
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        5
                        """);
            }
            {
                final String source = "pg_catalog.pg_attrdef";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            adrelid	adnum	adbin
                            1	1\t
                            1	2\t
                            1	3\t
                            1	4\t
                            2	1\t
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [adrelid, adnum]
                                pg_attrdef()
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        5
                        """);
            }
            {
                final String source = "pg_proc";
                for (String suffix : new String[]{"", "()"}) {
                    final String sql = "SELECT * FROM " + source + suffix + " ORDER BY 1,2";
                    assertQueryRows(sql, """
                            oid	proname	pronamespace	proowner	prolang	procost	prorows	provariadic	prosupport	prokind	prosecdef	proleakproof	proisstrict	proretset	provolatile	proparallel	pronargs	pronargdefaults	prorettype	prosrc	probin
                            0	internal_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	2281	internalrecv\t
                            2400	array_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1022	array_recv\t
                            2400	array_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1015	array_recv\t
                            2404	int2_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	21	int2recv\t
                            2406	int4_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	23	int4recv\t
                            2408	int8_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	20	int8recv\t
                            2412	binary_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	17	binaryrecv\t
                            2418	oid_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	26	oidrecv\t
                            2424	float4_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	700	float4recv\t
                            2426	float8_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	701	float8recv\t
                            2432	varchar_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1043	varcharrecv\t
                            2434	bpchar_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1042	bpcharrecv\t
                            2436	bool_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	16	boolrecv\t
                            2474	timestamp_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1114	timestamprecv\t
                            2568	date_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1082	daterecv\t
                            2961	uuid_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	2950	uuidrecv\t
                            3823	numeric_recv	2200	0	0	0.0	0.0	0	0	f	false	false	true	false	i	s	1	0	1700	numericrecv\t
                            """);
                    assertQueryRows("EXPLAIN " + sql, """
                            QUERY PLAN
                            Encode sort
                              keys: [oid, proname]
                                GenericRecord
                            """);
                }
                assertQueryRows("SELECT count() FROM " + source + "()", """
                        count
                        17
                        """);
            }
            {
                final String sql = "SELECT c.relname,a.attname,a.attnum FROM pg_class c JOIN pg_attribute a ON c.oid=a.attrelid "
                            + "WHERE c.relname='lp_catalogue_a' ORDER BY a.attnum";
                assertQueryRows(sql, """
                        relname	attname	attnum
                        lp_catalogue_a	id	1
                        lp_catalogue_a	label	2
                        lp_catalogue_a	ts	3
                        """);
                assertQueryRows("EXPLAIN " + sql, """
                        QUERY PLAN
                        Encode sort
                          keys: [attnum]
                            SelectedRecord
                                Hash Join
                                  condition: a.attrelid=c.oid
                                    Filter filter: relname='lp_catalogue_a'
                                        pg_class
                                    Hash
                                        pg_attribute()
                        """);
            }
            {
                final String sql = "SELECT d.adnum,d.adbin FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_class c ON d.adrelid=c.oid "
                            + "WHERE c.relname='lp_catalogue_a' ORDER BY d.adnum";
                assertQueryRows(sql, """
                        adnum	adbin
                        1\t
                        2\t
                        3\t
                        4\t
                        """);
                assertQueryRows("EXPLAIN " + sql, """
                        QUERY PLAN
                        Encode sort
                          keys: [adnum]
                            SelectedRecord
                                Hash Join
                                  condition: c.oid=d.adrelid
                                    pg_attrdef()
                                    Hash
                                        Filter filter: relname='lp_catalogue_a'
                                            pg_class
                        """);
            }
            {
                final String sql = "SELECT proname,prosrc FROM pg_proc() WHERE proisstrict ORDER BY proname LIMIT 3";
                assertQueryRows(sql, """
                        proname	prosrc
                        array_recv	array_recv
                        array_recv	array_recv
                        binary_recv	binaryrecv
                        """);
                assertQueryRows("EXPLAIN " + sql, """
                        QUERY PLAN
                        Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                            Encode sort
                              keys: [proname]
                                SelectedRecord
                                    Filter filter: proisstrict
                                        GenericRecord
                        """);
            }
            {
                final String sql = "SELECT a.relname,b.relname FROM pg_class a JOIN pg_class b ON a.oid=b.oid "
                            + "WHERE a.relname='lp_catalogue_a'";
                assertQueryRows(sql, """
                        relname	relname1
                        lp_catalogue_a	lp_catalogue_a
                        """);
                assertQueryRows("EXPLAIN " + sql, """
                        QUERY PLAN
                        SelectedRecord
                            Hash Join
                              condition: b.oid=a.oid
                                Filter filter: relname='lp_catalogue_a'
                                    pg_class
                                Hash
                                    pg_class
                        """);
            }
            try (RecordCursorFactory factory = compileRetained(
                    "SELECT c.relname,a.attname FROM pg_class c JOIN pg_attribute a ON c.oid=a.attrelid "
                            + "WHERE c.relname='lp_catalogue_a' ORDER BY a.attnum")) {
                assertResult(factory, "relname\tattname\nlp_catalogue_a\tid\nlp_catalogue_a\tlabel\nlp_catalogue_a\tts\n");
            }
        });
    }

    @Test
    public void testRetainedPgRelationCataloguesRefreshSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_pg_refresh(id INT,label STRING)");
            final int tableId = engine.verifyTableName("lp_pg_refresh").getTableId();
            try (
                    RecordCursorFactory classes = compileRetained(
                            "SELECT relname FROM pg_class() WHERE relname='lp_pg_refresh' ORDER BY relname");
                    RecordCursorFactory attributes = compileRetained(
                            "SELECT attname FROM pg_attribute() WHERE attrelid=" + tableId + " ORDER BY attnum");
                    RecordCursorFactory defaults = compileRetained(
                            "SELECT adnum FROM pg_attrdef() WHERE adrelid=" + tableId + " ORDER BY adnum")
            ) {
                assertResult(classes, "relname\nlp_pg_refresh\n");
                assertResult(attributes, "attname\nid\nlabel\n");
                assertResult(defaults, "adnum\n1\n2\n");
                execute("ALTER TABLE lp_pg_refresh ADD COLUMN qty LONG");
                assertResult(attributes, "attname\nid\nlabel\nqty\n");
                assertResult(defaults, "adnum\n1\n2\n3\n");
                assertResult(attributes, "attname\nid\nlabel\nqty\n");
                execute("ALTER TABLE lp_pg_refresh DROP COLUMN qty");
                assertResult(attributes, "attname\nid\nlabel\n");
                assertResult(classes, "relname\nlp_pg_refresh\n");
            }
            execute("DROP TABLE lp_pg_refresh");
        });
    }

    @Test
    public void testRetainedStorageSourcesRefreshPartitionsAndTables() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionTable("lp_meta_refresh", "TIMESTAMP", "000001", "000009");
            final String partitionSql = "SELECT name,numRows,detached,active FROM table_partitions('lp_meta_refresh') ORDER BY name";
            final String storageSql = "SELECT tableName,partitionCount,rowCount FROM table_storage() ORDER BY tableName";
            try (
                    RecordCursorFactory partitions = compileRetained(partitionSql);
                    RecordCursorFactory storage = compileRetained(storageSql)
            ) {
                assertResult(partitions, "name\tnumRows\tdetached\tactive\n2020-01-01\t1\tfalse\tfalse\n2020-01-02\t1\tfalse\ttrue\n");
                assertResult(storage, "tableName\tpartitionCount\trowCount\nlp_meta_refresh\t2\t2\n");

                execute("INSERT INTO lp_meta_refresh VALUES(3,'2020-01-02T00:00:01Z'),(4,'2020-01-03T00:00:00Z')");
                execute("CREATE TABLE lp_meta_extra(id INT)");
                execute("INSERT INTO lp_meta_extra VALUES(1)");
                engine.releaseAllWriters();
                assertResult(partitions, "name\tnumRows\tdetached\tactive\n2020-01-01\t1\tfalse\tfalse\n2020-01-02\t2\tfalse\tfalse\n2020-01-03\t1\tfalse\ttrue\n");
                assertResult(storage, "tableName\tpartitionCount\trowCount\nlp_meta_extra\t1\t1\nlp_meta_refresh\t3\t4\n");

                execute("ALTER TABLE lp_meta_refresh DETACH PARTITION LIST '2020-01-01'");
                execute("ALTER TABLE lp_meta_refresh DROP PARTITION LIST '2020-01-02'");
                execute("DROP TABLE lp_meta_extra");
                engine.releaseAllWriters();
                assertResult(partitions, "name\tnumRows\tdetached\tactive\n2020-01-01.detached\t1\ttrue\tfalse\n2020-01-03\t1\tfalse\ttrue\n");
                assertResult(storage, "tableName\tpartitionCount\trowCount\nlp_meta_refresh\t1\t1\n");
            }
        });
    }

    @Test
    public void testStorageSourceMetadataAndProjection() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionTable("lp_storage_micro", "TIMESTAMP", "000001", "000009");
            execute("CREATE TABLE lp_storage_plain(id INT)");
            execute("INSERT INTO lp_storage_plain VALUES(1),(2),(3)");
            execute("CREATE TABLE lp_storage_wal(id INT,ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO lp_storage_wal VALUES(1,'2020-01-01T00:00:00.000000001Z')");
            drainWalQueue();
            engine.releaseAllWriters();
            assertSourceHeader("SELECT * FROM table_storage() ORDER BY tableName", "tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize", 6);
            assertSource("SELECT rowCount,tableName FROM table_storage WHERE walEnabled ORDER BY tableName", """
                    rowCount	tableName
                    1	lp_storage_wal
                    """, 6);
            assertSource("SELECT tableName FROM table_storage() ORDER BY rowCount DESC,tableName LIMIT 2", """
                    tableName
                    lp_storage_plain
                    lp_storage_micro
                    """, 2);
            assertSource("SELECT count() FROM table_storage()", """
                    count
                    3
                    """, 0);
            assertSource("SELECT 7 AS answer FROM table_storage()", """
                    answer
                    7
                    7
                    7
                    """, 6);
            assertQueryRows("EXPLAIN SELECT * FROM table_storage()", """
                    QUERY PLAN
                    table_storage()
                    """);
        });
    }

    @Test
    public void testRetainedCataloguesRefreshAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_catalogue_a (id INT)");
            try (
                    RecordCursorFactory tables = compileRetained(
                            "SELECT table_name FROM all_tables() ORDER BY table_name");
                    RecordCursorFactory columns = compileRetained(
                            "SELECT column_name,data_type FROM information_schema.questdb_columns() "
                                    + "WHERE table_name='lp_catalogue_a' ORDER BY ordinal_position");
                    RecordCursorFactory views = compileRetained(
                            "SELECT view_name FROM views() ORDER BY view_name")
            ) {
                assertResult(tables, "table_name\nlp_catalogue_a\n");
                assertResult(columns, "column_name\tdata_type\nid\tINT\n");
                assertResult(views, "view_name\n");

                execute("CREATE TABLE lp_catalogue_b (id LONG)");
                execute("ALTER TABLE lp_catalogue_a ADD COLUMN label STRING");
                execute("CREATE VIEW lp_catalogue_view AS (SELECT id FROM lp_catalogue_a)");
                drainWalAndViewQueues();
                assertResult(tables, "table_name\nlp_catalogue_a\nlp_catalogue_b\nlp_catalogue_view\n");
                assertResult(columns, "column_name\tdata_type\nid\tINT\nlabel\tSTRING\n");
                assertResult(views, "view_name\nlp_catalogue_view\n");

                execute("DROP VIEW lp_catalogue_view");
                drainWalAndViewQueues();
                execute("DROP TABLE lp_catalogue_b");
                execute("ALTER TABLE lp_catalogue_a DROP COLUMN label");
                assertResult(tables, "table_name\nlp_catalogue_a\n");
                assertResult(columns, "column_name\tdata_type\nid\tINT\n");
                assertResult(views, "view_name\n");
            }
            execute("DROP TABLE lp_catalogue_a");
        });
    }

    @Test
    public void testStaticCataloguesAndIndependentOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> names = new ObjList<>();
            names.add("pg_namespace");
            names.add("pg_catalog.pg_namespace");
            names.add("pg_database");
            names.add("pg_catalog.pg_database");
            names.add("pg_get_keywords");
            names.add("pg_catalog.pg_get_keywords");
            names.add("pg_extension");
            names.add("pg_catalog.pg_extension");
            names.add("keywords");
            names.add("information_schema.character_sets");
            {
                final int i = 0;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        nspname	oid	xmin	nspowner
                        pg_catalog	11	0	1
                        public	2200	0	1
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        nspname	oid	xmin	nspowner
                        pg_catalog	11	0	1
                        public	2200	0	1
                        """);
            }
            {
                final int i = 1;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        nspname	oid	xmin	nspowner
                        pg_catalog	11	0	1
                        public	2200	0	1
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        nspname	oid	xmin	nspowner
                        pg_catalog	11	0	1
                        public	2200	0	1
                        """);
            }
            {
                final int i = 2;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        oid	datname	datdba	encoding	datcollate	datctype	datistemplate	datallowconn	datconnlimit	datlastsysoid	datfrozenxid	datminmxid	dattablespace	datacl
                        1	qdb	2	0	en_US.UTF-8	en_US.UTF-8	false	true	-1	1	-1	0	3\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        oid	datname	datdba	encoding	datcollate	datctype	datistemplate	datallowconn	datconnlimit	datlastsysoid	datfrozenxid	datminmxid	dattablespace	datacl
                        1	qdb	2	0	en_US.UTF-8	en_US.UTF-8	false	true	-1	1	-1	0	3\t
                        """);
            }
            {
                final int i = 3;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        oid	datname	datdba	encoding	datcollate	datctype	datistemplate	datallowconn	datconnlimit	datlastsysoid	datfrozenxid	datminmxid	dattablespace	datacl
                        1	qdb	2	0	en_US.UTF-8	en_US.UTF-8	false	true	-1	1	-1	0	3\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        oid	datname	datdba	encoding	datcollate	datctype	datistemplate	datallowconn	datconnlimit	datlastsysoid	datfrozenxid	datminmxid	dattablespace	datacl
                        1	qdb	2	0	en_US.UTF-8	en_US.UTF-8	false	true	-1	1	-1	0	3\t
                        """);
            }
            {
                final int i = 4;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        word	catcode	barelabel	catdesc	baredesc
                        add		false	\t
                        all		false	\t
                        alter		false	\t
                        and		false	\t
                        as		false	\t
                        asc		false	\t
                        asof		false	\t
                        backup		false	\t
                        between		false	\t
                        by		false	\t
                        cache		false	\t
                        capacity		false	\t
                        case		false	\t
                        cast		false	\t
                        column		false	\t
                        columns		false	\t
                        copy		false	\t
                        create		false	\t
                        cross		false	\t
                        database		false	\t
                        default		false	\t
                        delete		false	\t
                        desc		false	\t
                        distinct		false	\t
                        drop		false	\t
                        else		false	\t
                        end		false	\t
                        except		false	\t
                        exists		false	\t
                        fill		false	\t
                        foreign		false	\t
                        from		false	\t
                        grant		false	\t
                        group		false	\t
                        header		false	\t
                        if		false	\t
                        in		false	\t
                        index		false	\t
                        inner		false	\t
                        insert		false	\t
                        intersect		false	\t
                        into		false	\t
                        isolation		false	\t
                        join		false	\t
                        key		false	\t
                        latest		false	\t
                        left		false	\t
                        level		false	\t
                        limit		false	\t
                        lock		false	\t
                        lt		false	\t
                        nan		false	\t
                        natural		false	\t
                        nocache		false	\t
                        none		false	\t
                        not		false	\t
                        null		false	\t
                        on		false	\t
                        only		false	\t
                        or		false	\t
                        order		false	\t
                        outer		false	\t
                        over		false	\t
                        partition		false	\t
                        primary		false	\t
                        references		false	\t
                        rename		false	\t
                        repair		false	\t
                        right		false	\t
                        sample		false	\t
                        select		false	\t
                        show		false	\t
                        splice		false	\t
                        system		false	\t
                        table		false	\t
                        tables		false	\t
                        then		false	\t
                        to		false	\t
                        transaction		false	\t
                        truncate		false	\t
                        type		false	\t
                        union		false	\t
                        unlock		false	\t
                        update		false	\t
                        values		false	\t
                        when		false	\t
                        where		false	\t
                        window		false	\t
                        with		false	\t
                        writer		false	\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        word	catcode	barelabel	catdesc	baredesc
                        add		false	\t
                        all		false	\t
                        alter		false	\t
                        and		false	\t
                        as		false	\t
                        asc		false	\t
                        asof		false	\t
                        backup		false	\t
                        between		false	\t
                        by		false	\t
                        cache		false	\t
                        capacity		false	\t
                        case		false	\t
                        cast		false	\t
                        column		false	\t
                        columns		false	\t
                        copy		false	\t
                        create		false	\t
                        cross		false	\t
                        database		false	\t
                        default		false	\t
                        delete		false	\t
                        desc		false	\t
                        distinct		false	\t
                        drop		false	\t
                        else		false	\t
                        end		false	\t
                        except		false	\t
                        exists		false	\t
                        fill		false	\t
                        foreign		false	\t
                        from		false	\t
                        grant		false	\t
                        group		false	\t
                        header		false	\t
                        if		false	\t
                        in		false	\t
                        index		false	\t
                        inner		false	\t
                        insert		false	\t
                        intersect		false	\t
                        into		false	\t
                        isolation		false	\t
                        join		false	\t
                        key		false	\t
                        latest		false	\t
                        left		false	\t
                        level		false	\t
                        limit		false	\t
                        lock		false	\t
                        lt		false	\t
                        nan		false	\t
                        natural		false	\t
                        nocache		false	\t
                        none		false	\t
                        not		false	\t
                        null		false	\t
                        on		false	\t
                        only		false	\t
                        or		false	\t
                        order		false	\t
                        outer		false	\t
                        over		false	\t
                        partition		false	\t
                        primary		false	\t
                        references		false	\t
                        rename		false	\t
                        repair		false	\t
                        right		false	\t
                        sample		false	\t
                        select		false	\t
                        show		false	\t
                        splice		false	\t
                        system		false	\t
                        table		false	\t
                        tables		false	\t
                        then		false	\t
                        to		false	\t
                        transaction		false	\t
                        truncate		false	\t
                        type		false	\t
                        union		false	\t
                        unlock		false	\t
                        update		false	\t
                        values		false	\t
                        when		false	\t
                        where		false	\t
                        window		false	\t
                        with		false	\t
                        writer		false	\t
                        """);
            }
            {
                final int i = 5;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        word	catcode	barelabel	catdesc	baredesc
                        add		false	\t
                        all		false	\t
                        alter		false	\t
                        and		false	\t
                        as		false	\t
                        asc		false	\t
                        asof		false	\t
                        backup		false	\t
                        between		false	\t
                        by		false	\t
                        cache		false	\t
                        capacity		false	\t
                        case		false	\t
                        cast		false	\t
                        column		false	\t
                        columns		false	\t
                        copy		false	\t
                        create		false	\t
                        cross		false	\t
                        database		false	\t
                        default		false	\t
                        delete		false	\t
                        desc		false	\t
                        distinct		false	\t
                        drop		false	\t
                        else		false	\t
                        end		false	\t
                        except		false	\t
                        exists		false	\t
                        fill		false	\t
                        foreign		false	\t
                        from		false	\t
                        grant		false	\t
                        group		false	\t
                        header		false	\t
                        if		false	\t
                        in		false	\t
                        index		false	\t
                        inner		false	\t
                        insert		false	\t
                        intersect		false	\t
                        into		false	\t
                        isolation		false	\t
                        join		false	\t
                        key		false	\t
                        latest		false	\t
                        left		false	\t
                        level		false	\t
                        limit		false	\t
                        lock		false	\t
                        lt		false	\t
                        nan		false	\t
                        natural		false	\t
                        nocache		false	\t
                        none		false	\t
                        not		false	\t
                        null		false	\t
                        on		false	\t
                        only		false	\t
                        or		false	\t
                        order		false	\t
                        outer		false	\t
                        over		false	\t
                        partition		false	\t
                        primary		false	\t
                        references		false	\t
                        rename		false	\t
                        repair		false	\t
                        right		false	\t
                        sample		false	\t
                        select		false	\t
                        show		false	\t
                        splice		false	\t
                        system		false	\t
                        table		false	\t
                        tables		false	\t
                        then		false	\t
                        to		false	\t
                        transaction		false	\t
                        truncate		false	\t
                        type		false	\t
                        union		false	\t
                        unlock		false	\t
                        update		false	\t
                        values		false	\t
                        when		false	\t
                        where		false	\t
                        window		false	\t
                        with		false	\t
                        writer		false	\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        word	catcode	barelabel	catdesc	baredesc
                        add		false	\t
                        all		false	\t
                        alter		false	\t
                        and		false	\t
                        as		false	\t
                        asc		false	\t
                        asof		false	\t
                        backup		false	\t
                        between		false	\t
                        by		false	\t
                        cache		false	\t
                        capacity		false	\t
                        case		false	\t
                        cast		false	\t
                        column		false	\t
                        columns		false	\t
                        copy		false	\t
                        create		false	\t
                        cross		false	\t
                        database		false	\t
                        default		false	\t
                        delete		false	\t
                        desc		false	\t
                        distinct		false	\t
                        drop		false	\t
                        else		false	\t
                        end		false	\t
                        except		false	\t
                        exists		false	\t
                        fill		false	\t
                        foreign		false	\t
                        from		false	\t
                        grant		false	\t
                        group		false	\t
                        header		false	\t
                        if		false	\t
                        in		false	\t
                        index		false	\t
                        inner		false	\t
                        insert		false	\t
                        intersect		false	\t
                        into		false	\t
                        isolation		false	\t
                        join		false	\t
                        key		false	\t
                        latest		false	\t
                        left		false	\t
                        level		false	\t
                        limit		false	\t
                        lock		false	\t
                        lt		false	\t
                        nan		false	\t
                        natural		false	\t
                        nocache		false	\t
                        none		false	\t
                        not		false	\t
                        null		false	\t
                        on		false	\t
                        only		false	\t
                        or		false	\t
                        order		false	\t
                        outer		false	\t
                        over		false	\t
                        partition		false	\t
                        primary		false	\t
                        references		false	\t
                        rename		false	\t
                        repair		false	\t
                        right		false	\t
                        sample		false	\t
                        select		false	\t
                        show		false	\t
                        splice		false	\t
                        system		false	\t
                        table		false	\t
                        tables		false	\t
                        then		false	\t
                        to		false	\t
                        transaction		false	\t
                        truncate		false	\t
                        type		false	\t
                        union		false	\t
                        unlock		false	\t
                        update		false	\t
                        values		false	\t
                        when		false	\t
                        where		false	\t
                        window		false	\t
                        with		false	\t
                        writer		false	\t
                        """);
            }
            {
                final int i = 6;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        oid	extname	extowner	extnamespace	extrelocatable	extversion	extconfig	extcondition
                        1	questdb	1	1	false	[DEVELOPMENT]	\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        oid	extname	extowner	extnamespace	extrelocatable	extversion	extconfig	extcondition
                        1	questdb	1	1	false	[DEVELOPMENT]	\t
                        """);
            }
            {
                final int i = 7;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        oid	extname	extowner	extnamespace	extrelocatable	extversion	extconfig	extcondition
                        1	questdb	1	1	false	[DEVELOPMENT]	\t
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        oid	extname	extowner	extnamespace	extrelocatable	extversion	extconfig	extcondition
                        1	questdb	1	1	false	[DEVELOPMENT]	\t
                        """);
            }
            {
                final int i = 8;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        keyword
                        add
                        all
                        alter
                        and
                        as
                        asc
                        asof
                        backup
                        between
                        by
                        cache
                        capacity
                        case
                        cast
                        column
                        columns
                        copy
                        create
                        cross
                        database
                        default
                        delete
                        desc
                        distinct
                        drop
                        else
                        end
                        except
                        exists
                        fill
                        foreign
                        from
                        grant
                        group
                        header
                        if
                        in
                        index
                        inner
                        insert
                        intersect
                        into
                        isolation
                        join
                        key
                        latest
                        left
                        level
                        limit
                        lock
                        lt
                        nan
                        natural
                        nocache
                        none
                        not
                        null
                        on
                        only
                        or
                        order
                        outer
                        over
                        partition
                        primary
                        references
                        rename
                        repair
                        right
                        sample
                        select
                        show
                        splice
                        system
                        table
                        tables
                        then
                        to
                        transaction
                        truncate
                        type
                        union
                        unlock
                        update
                        values
                        when
                        where
                        window
                        with
                        writer
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        keyword
                        add
                        all
                        alter
                        and
                        as
                        asc
                        asof
                        backup
                        between
                        by
                        cache
                        capacity
                        case
                        cast
                        column
                        columns
                        copy
                        create
                        cross
                        database
                        default
                        delete
                        desc
                        distinct
                        drop
                        else
                        end
                        except
                        exists
                        fill
                        foreign
                        from
                        grant
                        group
                        header
                        if
                        in
                        index
                        inner
                        insert
                        intersect
                        into
                        isolation
                        join
                        key
                        latest
                        left
                        level
                        limit
                        lock
                        lt
                        nan
                        natural
                        nocache
                        none
                        not
                        null
                        on
                        only
                        or
                        order
                        outer
                        over
                        partition
                        primary
                        references
                        rename
                        repair
                        right
                        sample
                        select
                        show
                        splice
                        system
                        table
                        tables
                        then
                        to
                        transaction
                        truncate
                        type
                        union
                        unlock
                        update
                        values
                        when
                        where
                        window
                        with
                        writer
                        """);
            }
            {
                final int i = 9;
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + "() ORDER BY 1", """
                        character_set_catalog	character_set_schema	character_set_name	character_repertoire	form_of_use	default_collate_catalog	default_collate_schema	default_collate_name\s
                        		UTF8	UCS	UTF8	public	public	en_US.utf8
                        """);
                assertQueryRows("SELECT * FROM " + names.getQuick(i) + " ORDER BY 1", """
                        character_set_catalog	character_set_schema	character_set_name	character_repertoire	form_of_use	default_collate_catalog	default_collate_schema	default_collate_name\s
                        		UTF8	UCS	UTF8	public	public	en_US.utf8
                        """);
            }
            assertQueryRows(
                    "SELECT p.nspname AS name,p.oid+1 AS next_id FROM pg_catalog.pg_namespace p WHERE p.oid>0 ORDER BY next_id DESC LIMIT 1",
                    """
                            name	next_id
                            public	2201
                            """
            );
            assertQueryRows(
                    "SELECT a.nspname AS a,b.nspname AS b FROM pg_namespace() a CROSS JOIN pg_namespace() b ORDER BY a,b",
                    """
                            a	b
                            pg_catalog	pg_catalog
                            pg_catalog	public
                            public	pg_catalog
                            public	public
                            """
            );
            assertQueryRows("SELECT count() FROM pg_database()", """
                    count
                    1
                    """);
            assertQueryRows("SELECT 7 AS answer FROM pg_database()", """
                    answer
                    7
                    """);
        });
    }

    @Test
    public void testTableNamesStillTakePrecedence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pg_database (id INT)");
            execute("INSERT INTO pg_database VALUES (42)");
            assertQueryRows("SELECT id FROM pg_database", """
                    id
                    42
                    """);
            assertQueryRows("SELECT datname FROM pg_database()", """
                    datname
                    qdb
                    """);
            assertQueryRows("SELECT datname FROM pg_catalog.pg_database", """
                    datname
                    qdb
                    """);
        });
    }

    private void assertSource(String sql, String expected, int sourceColumnCount) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            assertSourceColumnCount(sql, compiler, sourceColumnCount);
            assertResult(factory, expected);
        }
    }

    private void assertSourceColumnCount(String sql, SqlCompilerImpl compiler, int sourceColumnCount) {
        if (sourceColumnCount >= 0) {
            LogicalPlan source = compiler.getLogicalPlanForTesting();
            while (source.inputCount() > 0) {
                source = source.inputAt(0);
            }
            Assert.assertTrue(sql, source instanceof FunctionSourcePlan);
            Assert.assertEquals(sql, sourceColumnCount, source.getOutput().getColumnCount());
        }
    }

    private void assertSourceHeader(String sql, String expectedColumns, int sourceColumnCount) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            assertSourceColumnCount(sql, compiler, sourceColumnCount);
            final RecordMetadata metadata = factory.getMetadata();
            final StringSink columns = new StringSink();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    columns.put('\t');
                }
                columns.put(metadata.getColumnName(i));
            }
            TestUtils.assertEquals(expectedColumns, columns);
            Assert.assertEquals(-1, metadata.getTimestampIndex());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
            }
        }
    }

    private void assertPartitionMetadata(String table, int timestampType) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile("SELECT * FROM table_partitions('" + table + "')", sqlExecutionContext).getRecordCursorFactory()) {
                final RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(18, metadata.getColumnCount());
                Assert.assertEquals(-1, metadata.getTimestampIndex());
                Assert.assertEquals(timestampType, metadata.getColumnType(3));
                Assert.assertEquals(timestampType, metadata.getColumnType(4));
            }
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private RecordCursorFactory compileRetained(String sql) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                try (RecordCursorFactory ignored = compiler.compile("SELECT missing FROM pg_namespace()", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("expected invalid column");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
                }
                try (RecordCursorFactory other = compiler.compile("SELECT 7 AS answer FROM pg_database()", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(other, "answer\n7\n");
                }
                compiler.clear();
                return factory;
            } catch (Throwable th) {
                Misc.free(factory, th);
                throw th;
            }
        }
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE lp_catalogue_a (id INT,label STRING,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_catalogue_b (id LONG)");
    }

    private void createPartitionTable(String table, String timestampType, String firstFraction, String secondFraction) throws Exception {
        execute("CREATE TABLE " + table + "(id INT,ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " VALUES(1,'2020-01-01T00:00:00." + firstFraction
                + "Z'),(2,'2020-01-02T00:00:00." + secondFraction + "Z')");
        engine.releaseAllWriters();
    }

    private String cursorError(RecordCursorFactory factory, Class<? extends RuntimeException> type) throws Exception {
        try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
            throw new AssertionError("expected " + type.getSimpleName());
        } catch (RuntimeException e) {
            Assert.assertEquals(type, e.getClass());
            return ((FlyweightMessageContainer) e).getFlyweightMessage().toString();
        }
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
