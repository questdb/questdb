#      ___                  _   ____  ____
#     / _ \ _   _  ___  ___| |_|  _ \| __ )
#    | | | | | | |/ _ \/ __| __| | | |  _ \
#    | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#     \__\_\\__,_|\___||___/\__|____/|____/
#
#   Copyright (c) 2014-2019 Appsicle
#   Copyright (c) 2019-2026 QuestDB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""
Parquet compatibility tests for QuestDB.

Tests that Parquet files exported from QuestDB can be read by:
- PyArrow (versions 22 and 23)
- DuckDB
- Pandas
- Polars
- PySpark

This addresses the bug reported in https://github.com/questdb/questdb/issues/6692
where SYMBOL columns with multiple row groups caused:
- "Invalid number of indices: 0" (RLE encoding bug)
- "Column cannot have more than one dictionary" (multiple dict pages bug)
"""

import argparse
import os
import requests
import sys
import tempfile

# Detect which library to use (priority order)
READER = None
READER_VERSION = None

try:
    import polars as pl
    READER = "polars"
    READER_VERSION = pl.__version__
except ImportError:
    pass

if READER is None:
    try:
        import pandas as pd
        import fastparquet
        READER = "pandas"
        READER_VERSION = pd.__version__
    except ImportError:
        pass

if READER is None:
    try:
        import pyarrow.parquet as pq
        import pyarrow
        READER = "pyarrow"
        READER_VERSION = pyarrow.__version__
    except ImportError:
        pass

if READER is None:
    try:
        import duckdb
        READER = "duckdb"
        READER_VERSION = duckdb.__version__
    except ImportError:
        pass

if READER is None:
    try:
        from pyspark.sql import SparkSession
        import pyspark
        READER = "pyspark"
        READER_VERSION = pyspark.__version__
    except ImportError:
        pass

if READER is None:
    print("ERROR: No parquet reader installed (pyarrow, duckdb, pandas, polars, or pyspark)")
    sys.exit(1)


def execute_sql(host: str, port: int, sql: str) -> dict:
    """Execute SQL query via QuestDB HTTP API."""
    url = f"http://{host}:{port}/exec"
    params = {"query": sql}
    response = requests.get(url, params=params, timeout=120)
    if not response.ok:
        raise RuntimeError(f"SQL failed: {response.text}")
    return response.json()


def export_parquet(host: str, port: int, query: str, output_path: str) -> None:
    """Export query results to Parquet file via QuestDB HTTP API."""
    url = f"http://{host}:{port}/exp"
    params = {"query": query, "fmt": "parquet"}
    response = requests.get(url, params=params, timeout=300, stream=True)
    if not response.ok:
        raise RuntimeError(f"Export failed: {response.text}")

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise RuntimeError(f"Parquet file was not created or is empty: {output_path}")


def read_parquet_pyarrow(file_path: str) -> tuple:
    """Read Parquet file using PyArrow and return (row_count, num_row_groups)."""
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata

    num_row_groups = metadata.num_row_groups
    total_rows = 0

    for i in range(num_row_groups):
        rg = metadata.row_group(i)
        total_rows += rg.num_rows
        # Read the row group to validate it's not corrupt
        # This exercises the dictionary and RLE decoding
        parquet_file.read_row_group(i)

    # Also read the entire table to ensure all data is readable
    table = pq.read_table(file_path)
    assert len(table) == total_rows, f"Table row count mismatch: {len(table)} vs {total_rows}"

    return total_rows, num_row_groups


def read_parquet_duckdb(file_path: str) -> tuple:
    """Read Parquet file using DuckDB and return (row_count, num_row_groups)."""
    conn = duckdb.connect()

    # Escape single quotes in file path
    escaped_path = file_path.replace("'", "''")

    # Read the parquet file
    result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped_path}')").fetchone()
    total_rows = result[0]

    # Get row group count (parquet_metadata returns one row per column per row group)
    metadata = conn.execute(
        f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{escaped_path}')"
    ).fetchone()
    num_row_groups = metadata[0]

    # Read all data to validate it's not corrupt
    data = conn.execute(f"SELECT * FROM read_parquet('{escaped_path}')").fetchall()
    assert len(data) == total_rows, f"Data row count mismatch: {len(data)} vs {total_rows}"

    conn.close()
    return total_rows, num_row_groups


def read_parquet_pandas(file_path: str) -> tuple:
    """Read Parquet file using Pandas and return (row_count, num_row_groups)."""
    df = pd.read_parquet(file_path, engine='fastparquet')
    total_rows = len(df)
    return total_rows, None


def read_parquet_polars(file_path: str) -> tuple:
    """Read Parquet file using Polars and return (row_count, num_row_groups)."""
    df = pl.read_parquet(file_path)
    total_rows = len(df)
    return total_rows, None


def read_parquet_pyspark(file_path: str) -> tuple:
    """Read Parquet file using PySpark and return (row_count, num_row_groups)."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("ParquetCompatTest") \
        .master("local[1]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(file_path)
    total_rows = df.count()

    spark.stop()
    return total_rows, None


def read_parquet(file_path: str) -> tuple:
    if READER == "pyarrow":
        return read_parquet_pyarrow(file_path)
    elif READER == "duckdb":
        return read_parquet_duckdb(file_path)
    elif READER == "pandas":
        return read_parquet_pandas(file_path)
    elif READER == "polars":
        return read_parquet_polars(file_path)
    elif READER == "pyspark":
        return read_parquet_pyspark(file_path)
    else:
        raise RuntimeError(f"Unknown reader: {READER}")


class ParquetCompatTest:
    """Base class for Parquet compatibility tests."""

    def __init__(self, host: str, port: int, temp_dir: str):
        self.host = host
        self.port = port
        self.temp_dir = temp_dir

    def setup(self):
        """Create test table and insert data."""
        raise NotImplementedError

    def teardown(self):
        """Drop test table."""
        raise NotImplementedError

    @property
    def table_name(self) -> str:
        raise NotImplementedError

    @property
    def expected_row_count(self) -> int:
        raise NotImplementedError

    @property
    def expected_min_row_groups(self) -> int:
        return 2

    def run(self) -> bool:
        """Run the test. Returns True if passed, False if failed."""
        test_name = self.__class__.__name__

        try:
            self.setup()

            result = execute_sql(self.host, self.port, f"SELECT count() FROM {self.table_name}")
            actual_count = result['dataset'][0][0]
            assert actual_count == self.expected_row_count, \
                f"Row count mismatch: expected {self.expected_row_count}, got {actual_count}"

            parquet_path = os.path.join(self.temp_dir, f"{self.table_name}.parquet")
            export_parquet(self.host, self.port, f"SELECT * FROM {self.table_name}", parquet_path)

            total_rows, num_row_groups = read_parquet(parquet_path)

            assert total_rows == self.expected_row_count, \
                f"Parquet row count mismatch: expected {self.expected_row_count}, got {total_rows}"
            if num_row_groups is not None:
                assert num_row_groups >= self.expected_min_row_groups, \
                    f"Expected at least {self.expected_min_row_groups} row groups, got {num_row_groups}"

            print(f"Test '{test_name}' passed.")
            return True

        except Exception as e:
            print(f"Test '{test_name}' failed: {e}")
            return False

        finally:
            try:
                self.teardown()
            except Exception:
                pass


class TestSymbolMultipleRowGroups(ParquetCompatTest):
    @property
    def table_name(self) -> str:
        return "parquet_symbol_test"

    @property
    def expected_row_count(self) -> int:
        return 250001

    @property
    def expected_min_row_groups(self) -> int:
        return 3

    def setup(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")
        execute_sql(self.host, self.port, f"""
            CREATE TABLE {self.table_name} (
                sym_8 SYMBOL,
                sym_3 SYMBOL,
                sym_2 SYMBOL,
                value DOUBLE,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
        """)
        execute_sql(self.host, self.port, f"""
            INSERT INTO {self.table_name}
            SELECT
                rnd_symbol('A','B','C','D','E','F','G','H') as sym_8,
                rnd_symbol('X','Y','Z') as sym_3,
                rnd_symbol('ONE','TWO') as sym_2,
                rnd_double() as value,
                timestamp_sequence('2024-01-01', 1000000) as ts
            FROM long_sequence({self.expected_row_count})
        """)

    def teardown(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")


class TestSymbolHighCardinality(ParquetCompatTest):
    """
    Tests symbol column with high cardinality (many unique values).
    Uses 120001 rows which creates 2 row groups.
    """

    @property
    def table_name(self) -> str:
        return "parquet_symbol_high_card_test"

    @property
    def expected_row_count(self) -> int:
        return 120001

    @property
    def expected_min_row_groups(self) -> int:
        return 2

    def setup(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")
        execute_sql(self.host, self.port, f"""
            CREATE TABLE {self.table_name} (
                sym SYMBOL,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
        """)
        execute_sql(self.host, self.port, f"""
            INSERT INTO {self.table_name}
            SELECT
                'SYM_' || (x % 1000) as sym,
                timestamp_sequence('2024-01-01', 1000000) as ts
            FROM long_sequence({self.expected_row_count})
        """)

    def teardown(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")


class TestSymbolWithNulls(ParquetCompatTest):
    """
    Tests symbol columns with nulls across multiple row groups.
    Uses 150001 rows which creates 2 row groups.
    """

    @property
    def table_name(self) -> str:
        return "parquet_symbol_nulls_test"

    @property
    def expected_row_count(self) -> int:
        return 150001

    @property
    def expected_min_row_groups(self) -> int:
        return 2

    @property
    def expected_null_count(self) -> int:
        return 15000

    def setup(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")
        execute_sql(self.host, self.port, f"""
            CREATE TABLE {self.table_name} (
                sym SYMBOL,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
        """)
        execute_sql(self.host, self.port, f"""
            INSERT INTO {self.table_name}
            SELECT
                CASE WHEN x % 10 = 0 THEN NULL ELSE rnd_symbol('A','B','C') END as sym,
                timestamp_sequence('2024-01-01', 1000000) as ts
            FROM long_sequence({self.expected_row_count})
        """)

    def run(self) -> bool:
        """Override run to verify symbol column null count statistics."""
        test_name = self.__class__.__name__

        try:
            self.setup()

            result = execute_sql(self.host, self.port, f"SELECT count() FROM {self.table_name}")
            actual_count = result['dataset'][0][0]
            assert actual_count == self.expected_row_count, \
                f"Row count mismatch: expected {self.expected_row_count}, got {actual_count}"

            parquet_path = os.path.join(self.temp_dir, f"{self.table_name}.parquet")
            export_parquet(self.host, self.port, f"SELECT * FROM {self.table_name}", parquet_path)

            total_rows, num_row_groups = read_parquet(parquet_path)

            assert total_rows == self.expected_row_count, \
                f"Parquet row count mismatch: expected {self.expected_row_count}, got {total_rows}"
            if num_row_groups is not None:
                assert num_row_groups >= self.expected_min_row_groups, \
                    f"Expected at least {self.expected_min_row_groups} row groups, got {num_row_groups}"

            if READER == "pyarrow":
                parquet_file = pq.ParquetFile(parquet_path)
                metadata = parquet_file.metadata
                schema = parquet_file.schema_arrow

                sym_idx = None
                for i, name in enumerate(schema.names):
                    if name == "sym":
                        sym_idx = i
                        break

                assert sym_idx is not None, "sym column not found in schema"

                total_null_count = 0
                for rg_idx in range(metadata.num_row_groups):
                    col_meta = metadata.row_group(rg_idx).column(sym_idx)
                    if col_meta.statistics and col_meta.statistics.null_count is not None:
                        total_null_count += col_meta.statistics.null_count

                assert total_null_count == self.expected_null_count, \
                    f"Symbol null count mismatch: expected {self.expected_null_count}, got {total_null_count}"

            print(f"Test '{test_name}' passed.")
            return True

        except Exception as e:
            print(f"Test '{test_name}' failed: {e}")
            return False

        finally:
            try:
                self.teardown()
            except Exception:
                pass

    def teardown(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")


class TestSymbolColumnTop(ParquetCompatTest):
    @property
    def table_name(self) -> str:
        return "parquet_symbol_coltop_test"

    @property
    def expected_row_count(self) -> int:
        return 200000

    @property
    def expected_min_row_groups(self) -> int:
        return 2

    @property
    def expected_null_count(self) -> int:
        return 100000

    def setup(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")
        execute_sql(self.host, self.port, f"""
            CREATE TABLE {self.table_name} (
                id LONG,
                value DOUBLE,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
        """)
        execute_sql(self.host, self.port, f"""
            INSERT INTO {self.table_name}
            SELECT
                x as id,
                rnd_double() as value,
                timestamp_sequence('2024-01-01', 1000000) as ts
            FROM long_sequence(100000)
        """)
        execute_sql(self.host, self.port,
            f"ALTER TABLE {self.table_name} ADD COLUMN sym SYMBOL")
        execute_sql(self.host, self.port, f"""
            INSERT INTO {self.table_name}
            SELECT
                100000 + x as id,
                rnd_double() as value,
                timestamp_sequence('2024-01-01T00:01:40', 1000000) as ts,
                rnd_symbol('A','B','C','D','E') as sym
            FROM long_sequence(100000)
        """)

    def run(self) -> bool:
        """Override run to verify symbol column null count statistics."""
        test_name = self.__class__.__name__

        try:
            self.setup()

            result = execute_sql(self.host, self.port, f"SELECT count() FROM {self.table_name}")
            actual_count = result['dataset'][0][0]
            assert actual_count == self.expected_row_count, \
                f"Row count mismatch: expected {self.expected_row_count}, got {actual_count}"

            parquet_path = os.path.join(self.temp_dir, f"{self.table_name}.parquet")
            export_parquet(self.host, self.port, f"SELECT * FROM {self.table_name}", parquet_path)

            total_rows, num_row_groups = read_parquet(parquet_path)

            assert total_rows == self.expected_row_count, \
                f"Parquet row count mismatch: expected {self.expected_row_count}, got {total_rows}"
            if num_row_groups is not None:
                assert num_row_groups >= self.expected_min_row_groups, \
                    f"Expected at least {self.expected_min_row_groups} row groups, got {num_row_groups}"

            # Verify symbol column null count in statistics (pyarrow only)
            if READER == "pyarrow":
                parquet_file = pq.ParquetFile(parquet_path)
                metadata = parquet_file.metadata
                schema = parquet_file.schema_arrow

                sym_idx = None
                for i, name in enumerate(schema.names):
                    if name == "sym":
                        sym_idx = i
                        break

                assert sym_idx is not None, "sym column not found in schema"

                total_null_count = 0
                for rg_idx in range(metadata.num_row_groups):
                    col_meta = metadata.row_group(rg_idx).column(sym_idx)
                    if col_meta.statistics and col_meta.statistics.null_count is not None:
                        total_null_count += col_meta.statistics.null_count

                assert total_null_count == self.expected_null_count, \
                    f"Symbol null count mismatch: expected {self.expected_null_count}, got {total_null_count}"

            print(f"Test '{test_name}' passed.")
            return True

        except Exception as e:
            print(f"Test '{test_name}' failed: {e}")
            return False

        finally:
            try:
                self.teardown()
            except Exception:
                pass

    def teardown(self):
        execute_sql(self.host, self.port, f"DROP TABLE IF EXISTS {self.table_name}")


def main():
    parser = argparse.ArgumentParser(description='Parquet compatibility tests for QuestDB')
    parser.add_argument('--host', default='localhost', help='QuestDB host')
    parser.add_argument('--port', type=int, default=9000, help='QuestDB HTTP port')
    args = parser.parse_args()

    port = int(os.getenv('QUESTDB_HTTP_PORT', args.port))

    tests = [
        TestSymbolMultipleRowGroups,
        TestSymbolHighCardinality,
        TestSymbolWithNulls,
        TestSymbolColumnTop,
    ]

    failed = False
    with tempfile.TemporaryDirectory() as temp_dir:
        for test_class in tests:
            test = test_class(args.host, port, temp_dir)
            if not test.run():
                failed = True

    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
