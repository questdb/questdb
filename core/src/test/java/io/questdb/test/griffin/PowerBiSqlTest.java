/*+*****************************************************************************
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PowerBiSqlTest extends AbstractCairoTest {

    @Test
    public void testCharSet() throws Exception {
        assertQuery(
                """
                        character_set_name
                        UTF8
                        """,
                "select character_set_name from INFORMATION_SCHEMA.character_sets",
                null,
                false,
                true
        );
    }

    @Test
    public void testCheckConstraints1() throws Exception{
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertQuery("""
                select
                    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
                    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
                    fkcol.TABLE_NAME AS FK_TABLE_NAME,
                    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
                    fkcol.ORDINAL_POSITION as ORDINAL,
                    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'trades' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
                from
                    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
                        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
                        inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
                        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
                        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
                        inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
                        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
                        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
                where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'trades'
                        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
                order by FK_NAME, fkcol.ORDINAL_POSITION""")
                .noLeakCheck()
                .returns("PK_COLUMN_NAME\tFK_TABLE_SCHEMA\tFK_TABLE_NAME\tFK_COLUMN_NAME\tORDINAL\tFK_NAME\n");
    }

    @Test
    public void testCheckConstraints2() throws Exception{
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertQuery("""
                select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY
                from INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME
                where i.TABLE_SCHEMA = 'public' and i.TABLE_NAME = 'trades'
                and i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
                order by i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME, ii.TABLE_SCHEMA, ii.TABLE_NAME, ii.ORDINAL_POSITION""")
                .noLeakCheck()
                .returns("INDEX_NAME\tCOLUMN_NAME\tORDINAL_POSITION\tPRIMARY_KEY\n");
    }

    @Test
    public void testEnum() throws Exception{
        assertQuery("""
                /*** Load enum fields ***/
                SELECT pg_type.oid, enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid=enumtypid
                ORDER BY oid, enumsortorder""")
                .noLeakCheck()
                .returns("oid\tenumlabel\n");
    }

    @Test
    public void testParanoidTableSelect() throws Exception{
        execute("create table trades as (select rnd_int() a, rnd_double() b, 0::timestamp t from long_sequence(10)) timestamp(t) partition by hour");
        assertQuery("""
                select "$Table"."a" as "a",
                    "$Table"."b" as "b",
                    "$Table"."t" as "t",
                from "public"."trades" "$Table\"""")
                .noLeakCheck()
                .expectSize()
                .timestamp("t")
                .returns("""
                        a\tb\tt
                        -1148479920\t0.8043224099968393\t1970-01-01T00:00:00.000000Z
                        -727724771\t0.08486964232560668\t1970-01-01T00:00:00.000000Z
                        1326447242\t0.0843832076262595\t1970-01-01T00:00:00.000000Z
                        -847531048\t0.6508594025855301\t1970-01-01T00:00:00.000000Z
                        -1436881714\t0.7905675319675964\t1970-01-01T00:00:00.000000Z
                        1545253512\t0.22452340856088226\t1970-01-01T00:00:00.000000Z
                        -409854405\t0.3491070363730514\t1970-01-01T00:00:00.000000Z
                        1904508147\t0.7611029514995744\t1970-01-01T00:00:00.000000Z
                        1125579207\t0.4217768841969397\t1970-01-01T00:00:00.000000Z
                        426455968\t0.0367581207471136\t1970-01-01T00:00:00.000000Z
                        """);
    }

    @Test
    public void testSelectParanoidEnvelope() throws Exception{
        execute("create table trades as (select rnd_int() a, rnd_double() b, 0::timestamp t from long_sequence(10)) timestamp(t) partition by hour");
        assertQuery("""
                select "$Table"."a" as "a",
                    "$Table"."b" as "b",
                    "$Table"."t" as "t",
                from\s
                (
                    select * from trades
                ) "$Table"
                limit 1000""")
                .noLeakCheck()
                .expectSize()
                .timestamp("t")
                .returns("""
                        a\tb\tt
                        -1148479920\t0.8043224099968393\t1970-01-01T00:00:00.000000Z
                        -727724771\t0.08486964232560668\t1970-01-01T00:00:00.000000Z
                        1326447242\t0.0843832076262595\t1970-01-01T00:00:00.000000Z
                        -847531048\t0.6508594025855301\t1970-01-01T00:00:00.000000Z
                        -1436881714\t0.7905675319675964\t1970-01-01T00:00:00.000000Z
                        1545253512\t0.22452340856088226\t1970-01-01T00:00:00.000000Z
                        -409854405\t0.3491070363730514\t1970-01-01T00:00:00.000000Z
                        1904508147\t0.7611029514995744\t1970-01-01T00:00:00.000000Z
                        1125579207\t0.4217768841969397\t1970-01-01T00:00:00.000000Z
                        426455968\t0.0367581207471136\t1970-01-01T00:00:00.000000Z
                        """);
    }

    @Test
    public void testTableColumns() throws Exception{
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertQuery("""
                select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
                from INFORMATION_SCHEMA.columns
                where TABLE_SCHEMA = 'public' and TABLE_NAME = 'trades'
                order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION""")
                .noLeakCheck()
                .returns("""
                        COLUMN_NAME\tORDINAL_POSITION\tIS_NULLABLE\tDATA_TYPE
                        a\t0\tyes\tinteger
                        b\t1\tyes\tdouble precision
                        t\t2\tyes\ttimestamp without time zone
                        """);
    }

    @Test
    public void testTableListing() throws Exception{
        execute("create table trades(a int, b double, t timestamp) timestamp(t) partition by hour");
        assertQuery("""
                select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                from INFORMATION_SCHEMA.tables
                where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
                order by TABLE_SCHEMA, TABLE_NAME""")
                .noLeakCheck()
                .returns("""
                        TABLE_SCHEMA\tTABLE_NAME\tTABLE_TYPE
                        public\ttrades\tBASE TABLE
                        """);
    }
}
