package com.questdb.cairo;

import com.questdb.common.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class TableColumnMetadataTest {
    @Test
    public void testLackOfIndexSupport() throws Exception {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT);
        Assert.assertNull(metadata.getSymbolTable());
        Assert.assertEquals(0, metadata.getBucketCount());
        Assert.assertFalse(metadata.isIndexed());
    }
}