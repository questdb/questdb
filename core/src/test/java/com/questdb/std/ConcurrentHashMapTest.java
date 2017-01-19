package com.questdb.std;

import com.questdb.misc.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class ConcurrentHashMapTest {

    @Test
    public void testPutAndIterate() throws Exception {
        HashSet<Integer> expected = new HashSet<>();

        ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();

        Rnd rnd = new Rnd();
        int n = 10000;

        for (int i = 0; i < n; i++) {
            int k = rnd.nextInt();

            expected.add(k);
            map.put(k, k);
        }

        Assert.assertEquals(n, map.size());
        Assert.assertEquals(n, expected.size());

        for (Integer i : map.values()) {
            Assert.assertTrue(expected.remove(i));
        }

        Assert.assertEquals(0, expected.size());
    }
}