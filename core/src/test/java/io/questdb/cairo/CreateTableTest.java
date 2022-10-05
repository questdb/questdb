package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test interactions between cast and index clauses in CREATE TABLE and CREATE TABLE AS SELECT statements .
 */
public class CreateTableTest extends AbstractGriffinTest {

    @Test
    public void testCreateTableWithNoIndex() throws Exception {
        assertQuery("s\n", "select * from tab", "create table tab (s symbol) ", null);
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        assertQuery("s\n", "select * from tab", "create table tab (s symbol), index(s)", null);

        assertColumnsIndexed("tab", "s");
    }

    @Test
    public void testCreateTableWithMultipleIndexes() throws Exception {
        assertQuery("s1\ts2\ts3\n", "select * from tab", "create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)", null);

        assertColumnsIndexed("tab", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableAsSelectWithNoIndex() throws Exception {
        assertCompile("create table old(s1 symbol)");
        assertQuery("s1\n", "select * from new", "create table new as (select * from old)", null);
    }

    @Test
    public void testCreateTableAsSelectWithOneIndex() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), index(s1)", null);

        assertColumnsIndexed("new", "s1");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleIndexs() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), index(s1), index(s2), index(s3)", null);

        assertColumnsIndexed("new", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableAsSelectWithOneCast() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), cast(s1 as string)", null);
    }

    @Test
    public void testCreateTableAsSelectWithMultipleCasts() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as string), cast(l as long), cast(ts as date)", null);
    }

    @Test
    public void testCreateTableAsSelectWithCastAndSeparateIndex() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(l as int), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndex_v2() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(l as int)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumn() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV2() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV3() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithCastSymbolToStringAndIndexOnIt() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as string)", null);
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithIndexOnSymbolCastedToString() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as string), index(s)", null);
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexes() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV2() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV3() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectInheritsColumnIndex() throws Exception {
        assertCompile("create table old(s string,sym symbol index, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableFromLikeTableWithNoIndex() throws Exception {
        assertCompile("create table y (s1 symbol)");
        assertQuery("s1\n","select * from tab", "create table tab (like y)", null);
    }

    @Test
    public void testCreateTableFromLikeTableWithIndex() throws Exception {
        assertCompile("create table tab (s symbol), index(s)");
        assertQuery("s\n", "select * from x", "create table x (like tab)", null);
        assertColumnsIndexed("x", "s");
    }

    @Test
    public void testCreateTableFromLikeTableWithMultipleIndexes() throws Exception {
        assertCompile("create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)");
        assertQuery("s1\ts2\ts3\n", "select * from x", "create table x(like tab)",null);
        assertColumnsIndexed("x", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableFromLikeTableCreatedAsSelectWithNoIndex() throws Exception {
        assertCompile("create table old(s1 symbol)");
        assertCompile("create table new as (select * from old)");
        assertQuery("s1\n", "select * from x", "create table x(like new)",null);
    }

    @Test
    public void testCreateTableFromLikeTableWithPartition() throws Exception {
        assertCompile("create table x (" +
                "a INT," +
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\tt\n", "select * from tab", "create table tab (like x)","t");
        assertPartitionIndex("tab", PartitionBy.MONTH, 1);
    }

    @Test
    public void testCreateTableLikeTableWithSymbolCapacityNotSame() throws Exception {
        assertCompile("create table x (" +
                "a INT," +
                "y SYMBOL capacity 100 cache,"+
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\ty\tt\n", "select * from tab", "create table tab ( like x)", "t");
        assertSymbolCapacityNotSame("tab",1,100);
    }

    private void assertSymbolCapacityNotSame(String tableName, int colIndex, int symbolCapacity) throws Exception {
        assertMemoryLeak(() -> {
            try(TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                Assert.assertNotEquals(symbolCapacity, reader.getSymbolMapReader(colIndex).getSymbolCapacity());
            }
        });
    }

    private void assertPartitionIndex(String tableName, int partitionIndex, int timestampIndex) throws Exception {
        assertMemoryLeak(() -> {
            try(TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                Assert.assertEquals(partitionIndex, reader.getPartitionedBy());
                Assert.assertEquals(timestampIndex, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    private void assertColumnsIndexed(String tableName, String... columnNames) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader r = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TableReaderMetadata metadata = r.getMetadata();
                IntList indexed = new IntList();
                indexed.setPos(metadata.getColumnCount());

                for (String columnName : columnNames) {
                    int i = metadata.getColumnIndex(columnName);
                    indexed.setQuick(i, 1);

                    Assert.assertTrue("Column " + columnName + " should be indexed!", metadata.isColumnIndexed(i));
                }

                for (int i = 0, len = indexed.size(); i < len; i++) {
                    if (indexed.getQuick(i) == 0) {
                        String columnName = metadata.getColumnName(i);
                        Assert.assertFalse("Column " + columnName + " shouldn't be indexed!", metadata.isColumnIndexed(i));
                    }
                }
            }
        });
    }
}

