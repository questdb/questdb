/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Ignore;
import org.junit.Test;

public class AttributeCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testKafkaMetadataQuery() throws Exception {

        String query = "\n" +
                "SELECT\n" +
                "    result.TABLE_CAT,        \n" +
                "    result.TABLE_SCHEM,        \n" +
                "    result.TABLE_NAME,        \n" +
                "    result.COLUMN_NAME,        \n" +
                "    result.KEY_SEQ,        \n" +
                "    result.PK_NAME,\n" +
                "    result.KEYS,\n" +
                "    result.A_ATTNUM,\n" +
                "    RAW \n" +
                "FROM\n" +
                "    (SELECT \n" +
                "        NULL AS TABLE_CAT, \n" +
                "        n.nspname AS TABLE_SCHEM,   \n" +
                "        ct.relname AS TABLE_NAME, \n" +
                "        a.attname AS COLUMN_NAME,   \n" +
                "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                "        ci.relname AS PK_NAME,   \n" +
                "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                "        a.attnum AS A_ATTNUM,\n" +
                "        i.indkey AS RAW \n" +
                "    FROM pg_catalog.pg_class ct\n" +
                "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                "    WHERE \n" +
                "        true  \n" +
                "        AND ct.relname = E'po_items' \n" +
                "        AND i.indisprimary  \n" +
                "    ) result; \n";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                query,
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity1() throws Exception {
        String query = "SELECT\n" +
                "    result.TABLE_CAT,        \n" +
                "    result.TABLE_SCHEM,        \n" +
                "    result.TABLE_NAME,        \n" +
                "    result.COLUMN_NAME,        \n" +
                "    result.KEY_SEQ,        \n" +
                "    result.PK_NAME\n" +
                "FROM\n" +
                "    (SELECT \n" +
                "        NULL AS TABLE_CAT, \n" +
                "        n.nspname AS TABLE_SCHEM,   \n" +
                "        ct.relname AS TABLE_NAME, \n" +
                "        a.attname AS COLUMN_NAME,   \n" +
                "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                "        ci.relname AS PK_NAME,   \n" +
                "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                "        a.attnum AS A_ATTNUM,\n" +
                "        i.indkey AS RAW \n" +
                "    FROM pg_catalog.pg_class ct\n" +
                "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                "    WHERE \n" +
                "        true  \n" +
                "        AND ct.relname = E'po_items' \n" +
                "        AND i.indisprimary  \n" +
                "    ) result \n" +
                "WHERE A_ATTNUM = (result.KEYS).x  \n" +
                "ORDER BY result.table_name, result.PK_NAME, result.KEY_SEQ;";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\n",
                query,
                "create table x(a int)",
                null,
                true,
                false
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity2() throws Exception {
        String query = "SELECT\n" +
                "    result.TABLE_CAT,        \n" +
                "    result.TABLE_SCHEM,        \n" +
                "    result.TABLE_NAME,        \n" +
                "    result.COLUMN_NAME,        \n" +
                "    result.KEY_SEQ,        \n" +
                "    result.PK_NAME,\n" +
                "    result.KEYS,\n" +
                "    result.A_ATTNUM,\n" +
                "    RAW \n" +
                "FROM\n" +
                "    (SELECT \n" +
                "        NULL AS TABLE_CAT, \n" +
                "        n.nspname AS TABLE_SCHEM,   \n" +
                "        ct.relname AS TABLE_NAME, \n" +
                "        a.attname AS COLUMN_NAME,   \n" +
                "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                "        ci.relname AS PK_NAME,   \n" +
                "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                "        a.attnum AS A_ATTNUM,\n" +
                "        i.indkey AS RAW \n" +
                "    FROM pg_catalog.pg_class ct\n" +
                "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                "    WHERE \n" +
                "        true  \n" +
                "        AND ct.relname = E'po_items' \n" +
                "        AND i.indisprimary  \n" +
                "    ) result \n" +
                "WHERE A_ATTNUM = (result.KEYS).x  \n" +
                "ORDER BY result.TABLE_NAME, result.pk_name, result.KEY_SEQ;";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                query,
                "create table x(a int)",
                null,
                true,
                false,
                false
        );
    }

    @Test
    @Ignore
    public void testKafkaQuery3() throws Exception {
        assertQuery(
                "",
                "SELECT * FROM (\n" +
                        "    SELECT \n" +
                        "        n.nspname,\n" +
                        "        c.relname,\n" +
                        "        a.attname,\n" +
                        "        a.atttypid,\n" +
                        "        a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,\n" +
                        "        a.atttypmod,\n" +
                        "        a.attlen,\n" +
                        "        t.typtypmod,\n" +
                        "        row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, \n" +
                        "        nullif(a.attidentity, '') as attidentity,\n" +
                        "        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,\n" +
                        "        dsc.description,\n" +
                        "        t.typbasetype,\n" +
                        "        t.typtype  \n" +
                        "    FROM pg_catalog.pg_namespace n  \n" +
                        "        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  \n" +
                        "        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  \n" +
                        "        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  \n" +
                        "        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  \n" +
                        "        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  \n" +
                        "        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  \n" +
                        "        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  \n" +
                        "    WHERE \n" +
                        "        c.relkind in ('r','p','v','f','m') \n" +
                        "        and a.attnum > 0 \n" +
                        "        AND NOT a.attisdropped  \n" +
                        "        AND c.relname LIKE E'x'\n" +
                        "    ) c \n" +
                        "WHERE true  \n" +
                        "ORDER BY \n" +
                        "    nspname,\n" +
                        "    c.relname,\n" +
                        "    attnum",
                "create table x(a int)",
                null,
                true,
                false,
                false
        );
    }

    @Test
    public void testPgAttributeFunc() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\n" +
                        "1\ta\t1\n",
                "pg_catalog.pg_attribute;",
                "create table x(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testPgAttributeFuncNoTables() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\n",
                "pg_catalog.pg_attribute;",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testPgAttributeFuncWith2Tables() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\n" +
                        "1\ta\t1\n",
                "pg_catalog.pg_attribute order by 1;",
                "create table x(a int)",
                null,
                "create table y(a double, b string)",
                "attrelid\tattname\tattnum\n" +
                        "1\ta\t1\n" +
                        "2\ta\t1\n" +
                        "2\tb\t2\n",
                true,
                false,
                false
        );
    }

    @Test
    public void testPgAttributeFuncWith2TablesLimit1() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\n" +
                        "1\ta\t1\n",
                "pg_catalog.pg_attribute order by 1 limit 1;",
                "create table x(a int)",
                null,
                "create table y(a double, b string)",
                "attrelid\tattname\tattnum\n" +
                        "1\ta\t1\n",
                true,
                false,
                true
        );
    }
}