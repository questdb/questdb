/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.JournalWriter;
import com.nfsdb.collections.IntStack;
import com.nfsdb.collections.ObjList;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;

public class IntrinsicExtractorTest extends AbstractTest {

    private final RpnBuilder rpn = new RpnBuilder();
    private final ExprParser p = new ExprParser();
    private final AstBuilder ast = new AstBuilder();
    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final IntStack indexStack = new IntStack();
    private JournalWriter<Quote> w;

    @Before
    public void setUp() throws Exception {
        w = factory.writer(Quote.class);
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and bid > 100");
        Assert.assertNotNull(m.interval);
        assertFilter(m, "bid100>");
    }

    @Test
    public void testBadEndDate() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-02T12:30:00.000Z\", \"2014-01Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown date format"));
        }
    }

    @Test
    public void testBadStartDate() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown date format"));
        }
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        assertFilter(m, "bid100>");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("sym in (\"a\", \"b\", \"c\") and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        ObjList<String> set = m.inSets.get("sym");
        Assert.assertNotNull(set);
        Assert.assertEquals(3, set.size());
        Assert.assertEquals("[a,b,c]", set.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("sym in ('a') and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        ObjList<String> set = m.inSets.get("sym");
        Assert.assertNotNull(set);
        Assert.assertEquals(1, set.size());
        Assert.assertEquals("a", set.get(0));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOrInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("bid > 100 or timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNull(m.interval);
        assertFilter(m, "bid100>\"2014-01-02T12:30:00.000Z\"\"2014-01-01T12:30:00.000Z\"timestampinor");
    }

    @Test
    public void testIndexedFieldTooFewArgs2() throws Exception {
        assertFilter(modelOf("sym in (x)"), "symxin");
    }

    @Test
    public void testIndexedFieldTooFewArgs3() throws Exception {
        try {
            modelOf("sym in ()");
            Assert.fail("exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arguments"));
        }
    }

    @Test
    public void testIntervalTooFewArgs() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arg"));
        }
    }

    @Test
    public void testIntervalTooFewArgs2() throws Exception {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arg"));
        }
    }

    @Test
    public void testIntervalTooManyArgs() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\", \"2014-01-03T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too many arg"));
        }
    }

    @Test
    public void testLiteralInInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", c)");
        Assert.assertNull(m.interval);
        assertFilter(m, "c\"2014-01-01T12:30:00.000Z\"timestampin");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("sym in (\"a\", z) and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        Assert.assertEquals(0, m.inSets.size());
        assertFilter(m, "z\"a\"symin");
    }

    @Test
    public void testMultipleAnds() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("a > 10 and b > 20 and (c > 100 and d < 20 and e = 30)");
        assertFilter(m, "a10>b20>andc100>d20<ande30=andand");
    }

    @Test
    public void testNestedFunctionTest() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("substr(parse(x, 1, 3), 2, 4)");
        Assert.assertNull(m.interval);
        assertFilter(m, "4231xparsesubstr");
    }

    @Test
    public void testNoIntrinsics() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("a > 10 or b > 20");
        Assert.assertNull(m.interval);
        Assert.assertEquals(0, m.inSets.size());
        assertFilter(m, "a10>b20>or");
    }

    @Test
    public void testNonLiteralColumn() throws Exception {
        try {
            modelOf("10 in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Column name expected"));
        }
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        Assert.assertTrue(m.interval.getLo() < m.interval.getHi());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertNotNull(m.interval);
        Assert.assertTrue(m.interval.getLo() < m.interval.getHi());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicExtractor.IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "bid100>ask110<and");
        ObjList<String> set1 = m.inSets.get("sym");
        ObjList<String> set2 = m.inSets.get("ex");
        Assert.assertEquals("[a,b]", set1.toString());
        Assert.assertEquals("[c]", set2.toString());
        Assert.assertEquals("Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}", m.interval.toString());
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicExtractor.IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and timestamp in (\"2014-01-01T16:30:00.000Z\", \"2014-01-05T12:30:00.000Z\")");
        Assert.assertNotNull(m.interval);
        Assert.assertEquals("Interval{lo=2014-01-01T16:30:00.000Z, hi=2014-01-02T12:30:00.000Z}", m.interval.toString());
    }

    private void assertFilter(IntrinsicExtractor.IntrinsicModel m, CharSequence expected) {
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(expected, toRpn(m.filter));
    }

    private CharSequence toRpn(ExprNode node) {
        rpn.reset();
        stack.clear();
        indexStack.clear();
        ExprNode lastVisited = null;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                stack.addFirst(node);
                indexStack.push(0);
                node = node.lhs;
            } else {
                ExprNode peek = stack.peek();
                if (peek.paramCount < 3) {
                    if (peek.rhs != null && lastVisited != peek.rhs) {
                        node = peek.rhs;
                    } else {
                        rpn.onNode(peek);
                        lastVisited = stack.pollFirst();
                        indexStack.pop();
                    }
                } else {
                    int index = indexStack.peek();
                    if (index < peek.paramCount) {
                        node = peek.args.get(index);
                        indexStack.update(index + 1);
                    } else {
                        rpn.onNode(peek);
                        lastVisited = stack.pollFirst();
                        indexStack.pop();
                    }
                }
            }
        }
        return rpn.rpn();
    }

    private IntrinsicExtractor.IntrinsicModel modelOf(CharSequence seq) throws ParserException {
        IntrinsicExtractor e = new IntrinsicExtractor();
        p.parseExpr(seq, ast);
        return e.extract(ast.root(), w);
    }
}
