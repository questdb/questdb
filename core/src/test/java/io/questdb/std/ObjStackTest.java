package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class ObjStackTest {
    @Test
    public void test1ElementInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        s.push(1);
        int i = s.pop();

        Assert.assertEquals(1, i);
        Assert.assertNull(s.pop());
    }

    @Test
    public void test10ElementsInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 10; i++){
            s.push(i);
        }

        for(int i = 9; i >= 0; i--){
            Integer r = s.pop();
            Assert.assertEquals((Integer)i, r);
        }
        Assert.assertNull(s.pop());
    }

    @Test
    public void test120ElementsInOut() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 20; i++){
            s.push(i);
        }

        for(int i = 19; i >= 0; i--){
            Integer r = s.pop();
            Assert.assertEquals((Integer)i, r);
        }
        Assert.assertNull(s.pop());
    }

    @Test
    public void test2In1Out() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 1000; i++){
            s.push(2*i);
            s.push(2*i + 1);

            Integer r = s.pop();
            Assert.assertEquals((Integer)(2*i + 1), r);
            Assert.assertEquals(i + 1, s.size());
        }
    }

    @Test
    public void test2In2Out() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 1000; i++){
            s.push(2*i);
            s.push(2*i + 1);

            Integer r = s.pop();
            Assert.assertEquals((Integer)(2*i + 1), r);

            Integer r2 = s.pop();
            Assert.assertEquals((Integer)(2*i), r2);

            Assert.assertEquals(0, s.size());
        }
    }

    @Test
    public void peekTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 10; i++){
            s.push(i);
        }

        Assert.assertEquals(s.size() - 1, (int)s.peek());
        for(int i = 0; i < 10; i++){
            Assert.assertEquals(s.size() - i - 1, (int)s.peek(i));
        }
    }

    @Test
    public void clearTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 10; i++){
            s.push(i);
        }

        Assert.assertEquals(10, s.size());
        s.clear();
        Assert.assertEquals(0, s.size());

        for(int i = 0; i < 10; i++){
            Assert.assertNull(s.peek(i));
        }
    }

    @Test
    public void updateTest() {
        ObjStack<Integer> s = new ObjStack<>();
        for(int i = 0; i < 10; i++){
            s.push(i);
        }

        Assert.assertEquals(s.size() - 1, (int)s.peek());
        s.update(20);
        Assert.assertEquals(20, (int)s.peek());
        Assert.assertEquals(20, (int)s.pop());
    }

    @Test
    public void notEmptyTest() {
        ObjStack<Integer> s = new ObjStack<>();
        Assert.assertFalse(s.notEmpty());
        s.push(1);
        Assert.assertTrue(s.notEmpty());
        s.pop();
        Assert.assertFalse(s.notEmpty());
    }

    @Test
    public void constCapacityTest() {
        ObjStack<Integer> s = new ObjStack<>(3);
        for(int i = 0; i < 10; i++){
            s.push(2*i);
        }
        Assert.assertEquals(10, s.size());
    }
}
