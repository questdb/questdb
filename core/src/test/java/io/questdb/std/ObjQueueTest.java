package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class ObjQueueTest {
    @Test
    public void test1ElementInOut() {
        ObjQueue<Integer> q = new ObjQueue<>();
        q.push(1);
        int i = q.pop();

        Assert.assertEquals(1, i);
        Assert.assertNull(q.pop());
    }

    @Test
    public void test10ElementsInOut() {
        ObjQueue<Integer> q = new ObjQueue<>();
        for(int i = 0; i < 10; i++){
            q.push(i);
        }

        for(int i = 0; i < 10; i++){
            Integer r = q.pop();
            Assert.assertEquals((Integer)i, r);
        }
        Assert.assertNull(q.pop());
    }

    @Test
    public void test120ElementsInOut() {
        ObjQueue<Integer> q = new ObjQueue<>();
        for(int i = 0; i < 20; i++){
            q.push(i);
        }

        for(int i = 0; i < 20; i++){
            Integer r = q.pop();
            Assert.assertEquals((Integer)i, r);
        }
        Assert.assertNull(q.pop());
    }

    @Test
    public void test2In1Out() {
        ObjQueue<Integer> q = new ObjQueue<>();
        for(int i = 0; i < 1000; i++){
            q.push(2*i);
            q.push(2*i + 1);

            Integer r = q.pop();
            Assert.assertEquals((Integer)i, r);
            Assert.assertEquals(i + 1, q.size());
        }
    }

    @Test
    public void clearTest() {
        ObjQueue<Integer> q = new ObjQueue<>();
        for(int i = 0; i < 10; i++){
            q.push(2*i);
        }

        Assert.assertEquals(10, q.size());
        q.clear();
        Assert.assertEquals(0, q.size());
        Assert.assertNull(q.pop());

        for(int i = 0; i < 1000; i++){
            q.push(2*i);
        }
        Assert.assertEquals(1000, q.size());
    }

    @Test
    public void constCapacityTest() {
        ObjQueue<Integer> q = new ObjQueue<>(3);
        for(int i = 0; i < 10; i++){
            q.push(2*i);
        }
        Assert.assertEquals(10, q.size());
    }
}
