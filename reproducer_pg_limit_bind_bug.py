
---

🗂 File: reproducer_pg_limit_bind_bug.py

# reproducer_pg_limit_bind_bug.py
# Reproducer for a bug in QuestDB PG wire protocol (e.g., LIMIT $1 or IN ($1, $2))
# Tested using psycopg2 (Python) against QuestDB running on localhost

import psycopg2

# Connect to QuestDB via PostgreSQL wire protocol (port 8812)
conn = psycopg2.connect(
    dbname='qdb',
    user='admin',
    password='quest',
    host='localhost',
    port=8812
)

cur = conn.cursor()

# Create a test table
cur.execute("""
    CREATE TABLE IF NOT EXISTS test_events (
        id INT,
        name TEXT,
        facility TEXT
    )
""")

# Insert sample data
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (1, 'alpha', 'core')")
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (2, 'beta', 'edge')")
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (3, 'gamma', 'core')")
conn.commit()

# Try a query with a bind parameter in LIMIT clause (this used to fail)
try:
    cur.execute("SELECT * FROM test_events LIMIT %s", (2,))
    rows = cur.fetchall()
    print("LIMIT bind test passed. Rows returned:")
    for row in rows:
        print(row)
except Exception as e:
    print("LIMIT bind test failed with error:", e)

# Try a query using IN clause with multiple bind variables
try:
    cur.execute("SELECT * FROM test_events WHERE name IN (%s, %s)", ('alpha', 'gamma'))
    rows = cur.fetchall()
    print("\nIN clause bind test passed. Rows returned:")
    for row in rows:
        print(row)
except Exception as e:
    print("IN clause bind test failed with error:", e)

cur.close()
conn.close()


---

📝 Optional: README.md

# QuestDB Bug Reproducer – PG Wire Bind Parameter Issues

This script reproduces bugs in QuestDB related to bind parameters via PostgreSQL wire protocol,
specifically when using `LIMIT $n` or `IN ($1, $2)` clauses.

## Prerequisites

- QuestDB running locally with PG wire support (port 8812)
- Python 3
- `psycopg2` library (`pip install psycopg2`)

## How to Run

```bash
python reproducer_pg_limit_bind_bug.py

Expected Output

Should print:

Rows from LIMIT %s query (2 rows)

Rows from IN (%s, %s) query


If there’s a bug, you’ll see errors like:

undefined bind variable

Related Issue

See: https://community.questdb.com/t/some-prepared-statements-originating-from-ruby-pg-gem-result-in-errors-on-questdb-8-2-0/254

---


