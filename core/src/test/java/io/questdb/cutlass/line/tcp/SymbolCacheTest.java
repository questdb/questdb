package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SymbolCacheTest extends AbstractCairoTest {
    @Test
    public void test() throws Exception {
        String tableName = "tb1";
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                         .col("symCol1", ColumnType.SYMBOL)
                         .col("symCol2", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache()
            ) {
                CairoTestUtils.create(model);
                try (
                        TableWriter writer = new TableWriter(configuration, tableName);
                        MemoryMR txMem = Vm.getMRInstance()
                ) {
                    int symColIndex1 = writer.getColumnIndex("symCol1");
                    int symColIndex2 = writer.getColumnIndex("symCol2");
                    long symCountOffset = TableUtils.getSymbolWriterIndexOffset(symColIndex2);
                    long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex2);
                    path.of(configuration.getRoot()).concat(tableName);
                    txMem.partialFile(configuration.getFilesFacade(), path.concat(TableUtils.TXN_FILE_NAME).$(),
                            transientSymCountOffset + Integer.BYTES, MemoryTag.MMAP_DEFAULT);
                    cache.of(configuration, path.of(configuration.getRoot()).concat(tableName), "symCol2", symColIndex2);

                    TableWriter.Row r = writer.newRow();
                    r.putSym(symColIndex1, "sym11");
                    r.putSym(symColIndex2, "sym21");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(1, txMem.getInt(symCountOffset));
                    Assert.assertEquals(1, txMem.getInt(transientSymCountOffset));
                    int rc = cache.getSymIndex("missing");
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(0, cache.getNCached());
                    rc = cache.getSymIndex("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getNCached());

                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym21");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(1, txMem.getInt(symCountOffset));
                    Assert.assertEquals(1, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("missing");
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(1, cache.getNCached());
                    rc = cache.getSymIndex("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getNCached());

                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym22");
                    r.append();
                    Assert.assertEquals(1, txMem.getInt(symCountOffset));
                    Assert.assertEquals(2, txMem.getInt(transientSymCountOffset));
                    writer.commit();
                    Assert.assertEquals(2, txMem.getInt(symCountOffset));
                    Assert.assertEquals(2, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getNCached());
                    rc = cache.getSymIndex("sym22");
                    Assert.assertEquals(1, rc);
                    Assert.assertEquals(2, cache.getNCached());

                    // Test cached uncommitted symbols
                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym23");
                    r.append();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym24");
                    r.append();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym25");
                    r.append();
                    Assert.assertEquals(2, txMem.getInt(symCountOffset));
                    Assert.assertEquals(5, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("sym22");
                    Assert.assertEquals(1, rc);
                    Assert.assertEquals(2, cache.getNCached());
                    rc = cache.getSymIndex("sym24");
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(3, cache.getNCached());
                    writer.commit();
                    Assert.assertEquals(5, txMem.getInt(symCountOffset));
                    Assert.assertEquals(5, txMem.getInt(transientSymCountOffset));

                    // Test deleting a symbol column
                    writer.removeColumn("symCol1");
                    cache.close();
                    txMem.close();
                    symColIndex2 = writer.getColumnIndex("symCol2");
                    symCountOffset = TableUtils.getSymbolWriterIndexOffset(symColIndex2);
                    transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex2);
                    path.of(configuration.getRoot()).concat(tableName);
                    txMem.partialFile(configuration.getFilesFacade(), path.concat(TableUtils.TXN_FILE_NAME).$(),
                            transientSymCountOffset + Integer.BYTES, MemoryTag.MMAP_DEFAULT);
                    cache.of(configuration, path.of(configuration.getRoot()).concat(tableName), "symCol2", symColIndex2);

                    Assert.assertEquals(5, txMem.getInt(symCountOffset));
                    Assert.assertEquals(5, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("sym24");
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(1, cache.getNCached());

                    r = writer.newRow();
                    r.putSym(symColIndex2, "sym26");
                    r.append();
                    Assert.assertEquals(5, txMem.getInt(symCountOffset));
                    Assert.assertEquals(6, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("sym26");
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getNCached());
                    writer.commit();
                    Assert.assertEquals(6, txMem.getInt(symCountOffset));
                    Assert.assertEquals(6, txMem.getInt(transientSymCountOffset));
                    rc = cache.getSymIndex("sym26");
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getNCached());
                }
            }
        });
    }
}
