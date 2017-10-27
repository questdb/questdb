package com.questdb.parser.typeprobe;

import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;
import org.junit.Assert;
import org.junit.Test;

public class TypeProbeCollectionTest {
    @Test
    public void testTypeProbeCollectionInstantiation() throws Exception {
        TypeProbeCollection typeProbeCollection = new TypeProbeCollection(this.getClass().getResource("/date_test.formats").getFile(),
                new DateFormatFactory(), DateLocaleFactory.INSTANCE);

        Assert.assertEquals(7, typeProbeCollection.getProbeCount());
        Assert.assertTrue(typeProbeCollection.getProbe(0) instanceof IntProbe);
        Assert.assertTrue(typeProbeCollection.getProbe(1) instanceof LongProbe);
        Assert.assertTrue(typeProbeCollection.getProbe(2) instanceof DoubleProbe);
        Assert.assertTrue(typeProbeCollection.getProbe(3) instanceof BooleanProbe);

        Assert.assertTrue(typeProbeCollection.getProbe(4) instanceof DateProbe);
        Assert.assertTrue(typeProbeCollection.getProbe(5) instanceof DateProbe);
        Assert.assertTrue(typeProbeCollection.getProbe(6) instanceof DateProbe);

        DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();

        Assert.assertEquals("dd/MM/y", typeProbeCollection.getProbe(4).getFormat());
        Assert.assertEquals(defaultLocale.getId(), typeProbeCollection.getProbe(4).getDateLocale().getId());

        Assert.assertEquals("yyyy-MM-dd HH:mm:ss", typeProbeCollection.getProbe(5).getFormat());
        Assert.assertEquals("es-PA", typeProbeCollection.getProbe(5).getDateLocale().getId());

        Assert.assertEquals("MM/dd/y", typeProbeCollection.getProbe(6).getFormat());
        Assert.assertEquals(defaultLocale.getId(), typeProbeCollection.getProbe(6).getDateLocale().getId());
    }
}