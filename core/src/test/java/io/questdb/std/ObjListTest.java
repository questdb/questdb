package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ObjListTest {

    @Test
    public void testEquals() {
        Assert.assertEquals(list("a"), list("a"));
        Assert.assertEquals(list("a", null, "b"), list("a", null, "b"));
        Assert.assertEquals(list("a", "b", "c"), list("a", "b", "c"));

        Assert.assertEquals(list(), list());

        Assert.assertNotEquals(list("a"), list("b"));
        Assert.assertNotEquals(list("a"), list("a", "b"));
        Assert.assertNotEquals(list("a", null), list("a", "b"));
        Assert.assertNotEquals(list("a"), list());
    }

    private ObjList<String> list(String... values) {
        ObjList<String> result = new ObjList<>();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }

    @Test
    public void testContains() {
        Assert.assertTrue(list("a", "b", "c").contains("a"));
        Assert.assertTrue(list("a", "b", "c").contains("b"));
        Assert.assertTrue(list("a", "b", "c").contains("c"));
        Assert.assertTrue(list("a", "b", "c", null).contains(null));
        Assert.assertTrue(list("a", "b", "c", "").contains(""));

        Assert.assertFalse(list().contains("a"));
        Assert.assertFalse(list().contains(null));
        Assert.assertFalse(list().contains(""));
        Assert.assertFalse(list("a", "b", "c").contains("d"));
        Assert.assertFalse(list("a", "b", "c").contains(""));
        Assert.assertFalse(list("a", "b", "c").contains(null));
    }
}
