package io.questdb.test;

import io.questdb.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class PropertyKeyTest {

    @Test
    public void testSensitiveFlag() {
        for (PropertyKey propKey : PropertyKey.values()) {
            Assert.assertEquals(propKey.getPropertyPath().contains(".password"), propKey.isSensitive());
        }
    }
}
