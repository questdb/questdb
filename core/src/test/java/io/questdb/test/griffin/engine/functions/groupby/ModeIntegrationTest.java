/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ModeIntegrationTest extends AbstractCairoTest {

    @Test
    public void testModePerformanceWithManyDistinctValues() throws Exception {
        assertQuery(
                """
                        mode
                        frequent_value
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'frequent_value' as f from long_sequence(100) " +
                        "union all " +
                        "select 'value_' || x as f from long_sequence(1000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithCTE() throws Exception {
        assertQuery(
                """
                        region\tmost_frequent_category
                        EAST\tElectronics
                        WEST\tClothing
                        """,
                "with regional_sales as (" +
                        "select region, category, count(*) as sale_count " +
                        "from sales " +
                        "group by region, category" +
                        ") " +
                        "select s.region, mode(s.category) as most_frequent_category " +
                        "from sales s " +
                        "group by s.region " +
                        "order by s.region",
                "create table sales as (" +
                        "select 'EAST' as region, 'Electronics' as category, 100.0 as amount from long_sequence(3) " +
                        "union all " +
                        "select 'EAST' as region, 'Clothing' as category, 80.0 as amount from long_sequence(1) " +
                        "union all " +
                        "select 'WEST' as region, 'Clothing' as category, 120.0 as amount from long_sequence(3) " +
                        "union all " +
                        "select 'WEST' as region, 'Electronics' as category, 160.0 as amount from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithCaseWhen() throws Exception {
        assertQuery(
                """
                        grade_category\tmode_subject
                        HIGH\tMath
                        LOW\tEnglish
                        """,
                "select " +
                        "case when grade >= 85 then 'HIGH' else 'LOW' end as grade_category, " +
                        "mode(subject) as mode_subject " +
                        "from student_grades " +
                        "group by case when grade >= 85 then 'HIGH' else 'LOW' end " +
                        "order by grade_category",
                "create table student_grades as (" +
                        "select 'Alice' as student, 'Math' as subject, 90L as grade from long_sequence(1) " +
                        "union all " +
                        "select 'Alice' as student, 'Math' as subject, 88L as grade from long_sequence(1) " +
                        "union all " +
                        "select 'Bob' as student, 'Science' as subject, 92L as grade from long_sequence(1) " +
                        "union all " +
                        "select 'Charlie' as student, 'English' as subject, 75L as grade from long_sequence(1) " +
                        "union all " +
                        "select 'Charlie' as student, 'English' as subject, 78L as grade from long_sequence(1) " +
                        "union all " +
                        "select 'Dave' as student, 'History' as subject, 82L as grade from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithComplexSubquery() throws Exception {
        assertQuery(
                """
                        department\tavg_mode_performance
                        Engineering\t8.5
                        Sales\t7.0
                        """,
                "select department, avg(mode_performance) as avg_mode_performance " +
                        "from (" +
                        "  select department, mode(performance_rating) as mode_performance " +
                        "  from employee_reviews " +
                        "  where review_date >= '2023-01-01' " +
                        "  group by department, quarter " +
                        ") " +
                        "group by department " +
                        "order by department",
                "create table employee_reviews as (" +
                        "select 'Engineering' as department, 'Q1' as quarter, 9L as performance_rating, '2023-03-15'::date as review_date from long_sequence(2) " +
                        "union all " +
                        "select 'Engineering' as department, 'Q1' as quarter, 8L as performance_rating, '2023-03-25'::date as review_date from long_sequence(1) " +
                        "union all " +
                        "select 'Engineering' as department, 'Q2' as quarter, 8L as performance_rating, '2023-06-15'::date as review_date from long_sequence(2) " +
                        "union all " +
                        "select 'Engineering' as department, 'Q2' as quarter, 7L as performance_rating, '2023-06-25'::date as review_date from long_sequence(1) " +
                        "union all " +
                        "select 'Sales' as department, 'Q1' as quarter, 7L as performance_rating, '2023-03-15'::date as review_date from long_sequence(2) " +
                        "union all " +
                        "select 'Sales' as department, 'Q1' as quarter, 6L as performance_rating, '2023-03-25'::date as review_date from long_sequence(1) " +
                        "union all " +
                        "select 'Sales' as department, 'Q2' as quarter, 7L as performance_rating, '2023-06-15'::date as review_date from long_sequence(2) " +
                        "union all " +
                        "select 'Sales' as department, 'Q2' as quarter, 8L as performance_rating, '2023-06-25'::date as review_date from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithLongStringsAndUnicode() throws Exception {
        assertQuery(
                """
                        mode
                        Very long string that contains many words and characters to test the mode function with extended text content and various symbols 🚀
                        """,
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 'Very long string that contains many words and characters to test the mode function with extended text content and various symbols 🚀' as f from long_sequence(3) " +
                        "union all " +
                        "select 'Short text' as f from long_sequence(1) " +
                        "union all " +
                        "select '普通话测试' as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithMultipleAggregates() throws Exception {
        assertQuery(
                """
                        category\tmode_brand\tavg_price\tmax_price\tcount
                        Electronics\tApple\t400.0\t500.0\t3
                        Fashion\tNike\t83.33\t120.0\t3
                        """,
                "select category, mode(brand) as mode_brand, " +
                        "round(avg(price), 2) as avg_price, " +
                        "max(price) as max_price, " +
                        "count(*) as count " +
                        "from products " +
                        "group by category " +
                        "order by category",
                "create table products as (" +
                        "select 'Electronics' as category, 'Apple' as brand, 300.0 as price from long_sequence(1) " +
                        "union all " +
                        "select 'Electronics' as category, 'Apple' as brand, 400.0 as price from long_sequence(1) " +
                        "union all " +
                        "select 'Electronics' as category, 'Samsung' as brand, 500.0 as price from long_sequence(1) " +
                        "union all " +
                        "select 'Fashion' as category, 'Nike' as brand, 80.0 as price from long_sequence(1) " +
                        "union all " +
                        "select 'Fashion' as category, 'Nike' as brand, 120.0 as price from long_sequence(1) " +
                        "union all " +
                        "select 'Fashion' as category, 'Adidas' as brand, 50.0 as price from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithTimestampSampleByAndFill() throws Exception {
        assertQuery(
                """
                        ts\tmode_status
                        1970-01-01T00:00:00.000000Z\tACTIVE
                        1970-01-01T01:00:00.000000Z\tINACTIVE
                        1970-01-01T02:00:00.000000Z\tACTIVE
                        """,
                "select ts, mode(status) as mode_status " +
                        "from system_logs " +
                        "sample by 1h fill(prev) " +
                        "order by ts",
                "create table system_logs as (" +
                        "select 'ACTIVE' as status, cast(0 as timestamp) as ts from long_sequence(1) " +
                        "union all " +
                        "select 'ACTIVE' as status, cast(1800000000 as timestamp) as ts from long_sequence(1) " +
                        "union all " +
                        "select 'INACTIVE' as status, cast(3600000000 as timestamp) as ts from long_sequence(1) " +
                        "union all " +
                        "select 'INACTIVE' as status, cast(5400000000 as timestamp) as ts from long_sequence(1) " +
                        "union all " +
                        "select 'ACTIVE' as status, cast(7200000000 as timestamp) as ts from long_sequence(1) " +
                        "union all " +
                        "select 'ACTIVE' as status, cast(9000000000 as timestamp) as ts from long_sequence(1)" +
                        ") timestamp(ts)",
                "ts",
                false,
                false
        );
    }

    @Test
    public void testModeWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table orders as (" +
                    "select 'BUY' as side from long_sequence(3) " +
                    "union all " +
                    "select 'SELL' as side from long_sequence(1)" +
                    ");");
            execute("create table trades as (" +
                    "select 'SELL' as side from long_sequence(3) " +
                    "union all " +
                    "select 'BUY' as side from long_sequence(1)" +
                    ")");
            assertQueryNoLeakCheck(
                    """
                            source\tmode_value
                            orders\tBUY
                            trades\tSELL
                            """,
                    "select source, mode(side) as mode_value from (" +
                            "select 'orders' as source, side from orders " +
                            "union all " +
                            "select 'trades' as source, side from trades" +
                            ") group by source order by source",
                    null,
                    null,
                    true,
                    true
            );
        });
    }
}