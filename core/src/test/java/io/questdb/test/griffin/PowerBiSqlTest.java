/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PowerBiSqlTest extends AbstractCairoTest {

    @Test
    public void testCharSet() throws Exception {
        assertQuery(
                "character_set_name\n" +
                        "UTF8\n",
                "select character_set_name from INFORMATION_SCHEMA.character_sets",
                null,
                false,
                true
        );
    }

    @Test
    public void testCheckConstraints1() throws SqlException {
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertSql("PK_COLUMN_NAME\tFK_TABLE_SCHEMA\tFK_TABLE_NAME\tFK_COLUMN_NAME\tORDINAL\tFK_NAME\n",
                "select\n" +
                        "    pkcol.COLUMN_NAME as PK_COLUMN_NAME,\n" +
                        "    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,\n" +
                        "    fkcol.TABLE_NAME AS FK_TABLE_NAME,\n" +
                        "    fkcol.COLUMN_NAME as FK_COLUMN_NAME,\n" +
                        "    fkcol.ORDINAL_POSITION as ORDINAL,\n" +
                        "    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'trades' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME\n" +
                        "from\n" +
                        "    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name\n" +
                        "        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon\n" +
                        "        inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol\n" +
                        "        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA\n" +
                        "        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME\n" +
                        "        inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol\n" +
                        "        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA\n" +
                        "        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME\n" +
                        "where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'trades'\n" +
                        "        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION\n" +
                        "order by FK_NAME, fkcol.ORDINAL_POSITION"
        );
    }

    @Test
    public void testCheckConstraints2() throws SqlException {
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertSql(
                "INDEX_NAME\tCOLUMN_NAME\tORDINAL_POSITION\tPRIMARY_KEY\n",
                "select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY\n" +
                        "from INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME\n" +
                        "where i.TABLE_SCHEMA = 'public' and i.TABLE_NAME = 'trades'\n" +
                        "and i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')\n" +
                        "order by i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME, ii.TABLE_SCHEMA, ii.TABLE_NAME, ii.ORDINAL_POSITION"
        );
    }

    @Test
    public void testEnum() throws SqlException {
        assertSql(
                "oid\tenumlabel\n",
                "/*** Load enum fields ***/\n" +
                        "SELECT pg_type.oid, enumlabel\n" +
                        "FROM pg_enum\n" +
                        "JOIN pg_type ON pg_type.oid=enumtypid\n" +
                        "ORDER BY oid, enumsortorder"
        );
    }

    @Test
    public void testParanoidTableSelect() throws SqlException {
        execute("create table trades as (select rnd_int() a, rnd_double() b, 0::timestamp t from long_sequence(10)) timestamp(t) partition by hour");
        assertSql(
                "a\tb\tt\n" +
                        "-1148479920\t0.8043224099968393\t1970-01-01T00:00:00.000000Z\n" +
                        "-727724771\t0.08486964232560668\t1970-01-01T00:00:00.000000Z\n" +
                        "1326447242\t0.0843832076262595\t1970-01-01T00:00:00.000000Z\n" +
                        "-847531048\t0.6508594025855301\t1970-01-01T00:00:00.000000Z\n" +
                        "-1436881714\t0.7905675319675964\t1970-01-01T00:00:00.000000Z\n" +
                        "1545253512\t0.22452340856088226\t1970-01-01T00:00:00.000000Z\n" +
                        "-409854405\t0.3491070363730514\t1970-01-01T00:00:00.000000Z\n" +
                        "1904508147\t0.7611029514995744\t1970-01-01T00:00:00.000000Z\n" +
                        "1125579207\t0.4217768841969397\t1970-01-01T00:00:00.000000Z\n" +
                        "426455968\t0.0367581207471136\t1970-01-01T00:00:00.000000Z\n",
                "select \"$Table\".\"a\" as \"a\",\n" +
                        "    \"$Table\".\"b\" as \"b\",\n" +
                        "    \"$Table\".\"t\" as \"t\",\n" +
                        "from \"public\".\"trades\" \"$Table\""
        );
    }

    @Test
    public void testSelectParanoidEnvelope() throws SqlException {
        execute("create table trades as (select rnd_int() a, rnd_double() b, 0::timestamp t from long_sequence(10)) timestamp(t) partition by hour");
        assertSql(
                "a\tb\tt\n" +
                        "-1148479920\t0.8043224099968393\t1970-01-01T00:00:00.000000Z\n" +
                        "-727724771\t0.08486964232560668\t1970-01-01T00:00:00.000000Z\n" +
                        "1326447242\t0.0843832076262595\t1970-01-01T00:00:00.000000Z\n" +
                        "-847531048\t0.6508594025855301\t1970-01-01T00:00:00.000000Z\n" +
                        "-1436881714\t0.7905675319675964\t1970-01-01T00:00:00.000000Z\n" +
                        "1545253512\t0.22452340856088226\t1970-01-01T00:00:00.000000Z\n" +
                        "-409854405\t0.3491070363730514\t1970-01-01T00:00:00.000000Z\n" +
                        "1904508147\t0.7611029514995744\t1970-01-01T00:00:00.000000Z\n" +
                        "1125579207\t0.4217768841969397\t1970-01-01T00:00:00.000000Z\n" +
                        "426455968\t0.0367581207471136\t1970-01-01T00:00:00.000000Z\n",
                "select \"$Table\".\"a\" as \"a\",\n" +
                        "    \"$Table\".\"b\" as \"b\",\n" +
                        "    \"$Table\".\"t\" as \"t\",\n" +
                        "from \n" +
                        "(\n" +
                        "    select * from trades\n" +
                        ") \"$Table\"\n" +
                        "limit 1000"
        );
    }

    @Test
    public void testTableColumns() throws SqlException {
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertSql(
                "COLUMN_NAME\tORDINAL_POSITION\tIS_NULLABLE\tDATA_TYPE\n" +
                        "a\t0\tyes\tinteger\n" +
                        "b\t1\tyes\tdouble precision\n" +
                        "t\t2\tyes\ttimestamp without time zone\n",
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE\n" +
                        "from INFORMATION_SCHEMA.columns\n" +
                        "where TABLE_SCHEMA = 'public' and TABLE_NAME = 'trades'\n" +
                        "order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
        );
    }

    @Test
    public void testTableListing() throws SqlException {
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertSql(
                "TABLE_SCHEMA\tTABLE_NAME\tTABLE_TYPE\n" +
                        "public\ttrades\tBASE TABLE\n",
                "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                        "from INFORMATION_SCHEMA.tables\n" +
                        "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                        "order by TABLE_SCHEMA, TABLE_NAME"
        );
    }
}
