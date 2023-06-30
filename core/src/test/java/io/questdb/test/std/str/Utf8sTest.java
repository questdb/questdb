package io.questdb.test.std.str;

import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.junit.Assert;
import org.junit.Test;

public class Utf8sTest {
    @Test
    public void testEquals() {
        final DirectUtf8Sequence nullDirect = null;
        final DirectUtf8Sequence str1a = new GcUtf8String("test1");
        final DirectUtf8Sequence str1b = new GcUtf8String("test1");
        final DirectUtf8Sequence str2 = new GcUtf8String("test2");
        final DirectUtf8Sequence str3 = new GcUtf8String("a_longer_string");
        Assert.assertNotEquals(str1a.ptr(), str1b.ptr());

        Assert.assertTrue(Utf8s.equals(nullDirect, nullDirect));
        Assert.assertFalse(Utf8s.equals(nullDirect, str1a));
        Assert.assertFalse(Utf8s.equals(str1a, null));
        Assert.assertTrue(Utf8s.equals(str1a, str1a));
        Assert.assertTrue(Utf8s.equals(str1a, str1b));
        Assert.assertFalse(Utf8s.equals(str1a, str2));
        Assert.assertFalse(Utf8s.equals(str2, str3));

        final Utf8Sequence nullSequence = null;
        final Utf8Sequence seq1a = str1a;
        final Utf8Sequence seq1b = str1b;
        final Utf8Sequence seq2 = str2;
        final Utf8Sequence seq3 = str3;

        Assert.assertTrue(Utf8s.equals(nullSequence, nullSequence));
        Assert.assertFalse(Utf8s.equals(nullSequence, seq1a));
        Assert.assertFalse(Utf8s.equals(seq1a, nullSequence));
        Assert.assertTrue(Utf8s.equals(seq1a, seq1a));
        Assert.assertTrue(Utf8s.equals(seq1a, seq1b));
        Assert.assertFalse(Utf8s.equals(seq1a, seq2));
        Assert.assertFalse(Utf8s.equals(seq2, seq3));
    }
}
