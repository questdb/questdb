package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import org.hamcrest.MatcherAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Test RBTree removal cases asserting final tree structure .
 */
public class LimitedSizeLongTreeChainTest extends AbstractGriffinTest {

    static class TestRecord implements Record {
        long value;
        long position;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return position;
        }
    }

    static class TestRecordCursor implements RecordCursor {
        Record left = new TestRecord();
        Record right = new TestRecord();
        LongList values = new LongList();
        int position = -1;

        TestRecordCursor(long... newValues) {
            for (int i = 0; i < newValues.length; i++) {
                this.values.add(newValues[i]);
            }
        }

        @Override
        public void close() {
            //nothing to do here
        }

        @Override
        public Record getRecord() {
            return left;
        }

        @Override
        public boolean hasNext() {
            if (position < values.size() - 1) {
                position++;
                recordAt(left, position);
                return true;
            }

            return false;
        }

        @Override
        public Record getRecordB() {
            return right;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((TestRecord) record).value = values.get((int) atRowId);
            ((TestRecord) record).position = atRowId;
        }

        public void recordAtValue(Record record, long value) {
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) == value) {
                    recordAt(record, i);
                    return;
                }
            }

            throw new RuntimeException("Can't find value " + value + " in " + values);
        }

        @Override
        public void toTop() {
            position = 0;
        }

        @Override
        public long size() {
            return values.size();
        }
    }

    static class TestRecordComparator implements RecordComparator {
        Record left;

        @Override
        public int compare(Record record) {
            return (int) (left.getLong(0) - record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            left = record;
        }
    }

    //used in all tests to hide api complexity 
    LimitedSizeLongTreeChain chain;
    TestRecordCursor cursor;
    TestRecord left;
    TestRecord placeholder;
    RecordComparator comparator;

    @Before
    public void before() {
        chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages(),
                true,
                20
        );
    }

    @After
    public void after() {
        chain.close();
    }

    @Test
    public void test_create_ordered_tree() {
        createTree(1, 2, 3, 4, 5);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,2]\n" +
                        " L-[Black,1]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,3]\n" +
                        "   R-[Red,5]\n"));
    }

    @Test
    public void test_create_ordered_tree_with_input_in_descending_order() {
        createTree(5, 4, 3, 2, 1);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,4]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        "   R-[Red,3]\n" +
                        " R-[Black,5]\n"));
    }

    @Test
    public void test_create_ordered_tree_with_input_in_no_order() {
        createTree(3, 2, 5, 1, 4);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,3]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        " R-[Black,5]\n" +
                        "   L-[Red,4]\n"));
    }

    @Test
    public void test_create_ordered_tree_with_duplicates() {
        createTree(1, 2, 3, 4, 5, 1, 4, 3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,2]\n" +
                        " L-[Black,1(2)]\n" +
                        " R-[Black,4(2)]\n" +
                        "   L-[Red,3(2)]\n" +
                        "   R-[Red,5]\n"));
    }

    @Test
    public void test_remove_black_node_with_no_children() {
        createTree(0, 1, 2);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Red,0]\n" +
                        " R-[Red,2]\n"));

        removeRowWithValue(2);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Red,0]\n"));
    }

    @Test
    public void test_remove_black_node_with_right_child_only() {
        createTree(0, 1, 2, 3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n"));

        removeRowWithValue(2);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n"));
    }

    @Test
    public void test_remove_black_node_with_left_child_only() {
        createTree(0, 1, 2, 3, -1);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        "   L-[Red,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n"));

        removeRowWithValue(0);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n"));
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_successor() {
        createTree(0, 1, 2, 3, 4);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n" +
                        "   L-[Red,2]\n" +
                        "   R-[Red,4]\n"));

        removeRowWithValue(3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,2]\n"));
    }

    @Test
    public void test_remove_red_node_with_both_children_and_right_is_black_successor() {
        createTree(0, 1, 2, 3, 4, 5, 6, 7);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n"));

        removeRowWithValue(5);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,7]\n"));
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor_but_doesnt_require_rotation() {
        createTree(0, 1, 2, 3, 4, 5, 6, 7);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n"));

        removeRowWithValue(3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,4]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,5]\n" +
                        "   R-[Black,7]\n"));
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor() {
        createTree(0, 1, 2, 3, 5, 4);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n"));

        removeRowWithValue(3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"));
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor_requires_rotation_todo() {
        createTree(0, 1, 2, 3, 5, 4);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n"));

        removeRowWithValue(3);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"));
    }

    //new test cases 
    //current node is double black and not the root; sibling is black and at least one of its children is red 
    //right right case
    @Test
    public void test_remove_black_node_with_black_sibling_with_red_right_child() {
        createTree(30, 20, 40, 35, 50);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n" +
                        "   R-[Red,50]\n"));

        removeRowWithValue(20L);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,40]\n" +
                        " L-[Black,30]\n" +
                        "   R-[Red,35]\n" +
                        " R-[Black,50]\n"));
    }

    //right left case 
    @Test
    public void test_remove_black_node_with_black_sibling_with_red_left_child() {
        createTree(30, 20, 40, 35);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n"));

        removeRowWithValue(20L);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"));
    }

    //sibling is black and its both children are black (?)
    @Test
    public void test_remove_black_node_with_black_sibling_with_both_children_black() {
        createTree(30, 20, 40, 35);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n"));

        removeRowWithValue(20L);

        MatcherAssert.assertThat(toString(cursor, placeholder), equalTo(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"));
    }

    @NotNull
    private String toString(TestRecordCursor cursor, TestRecord right) {
        StringSink sink = new StringSink();
        chain.print(sink, rowid -> {
            cursor.recordAt(right, rowid);
            return String.valueOf(right.getLong(0));
        });
        return sink.toString();
    }

    private void removeRowWithValue(long value) {
        cursor.recordAtValue(left, value);
        long node = chain.find(left, cursor, placeholder, comparator);
        chain.removeAndCache(node);
    }

    private void createTree(long... values) {
        cursor = new TestRecordCursor(values);
        left = (TestRecord) cursor.getRecord();
        placeholder = (TestRecord) cursor.getRecordB();
        comparator = new TestRecordComparator();
        comparator.setLeft(left);
        while (cursor.hasNext()) {
            chain.put(left, cursor, placeholder, comparator);
        }
    }
}
