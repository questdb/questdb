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

import asyncio
import asyncpg
import random
from datetime import datetime


async def create_table(pool):
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_data (
                name SYMBOL,
                email VARCHAR,
                age LONG,
                created_at TIMESTAMP
            ) timestamp(created_at) partition by hour;
        ''')


def generate_test_data(num_records):
    data = []
    names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'example.com']

    for i in range(num_records):
        name = random.choice(names)
        email = f"{name.lower()}{i}@{random.choice(domains)}"
        age = random.randint(18, 80)
        created_at = datetime.now()
        data.append((name, email, age, created_at))

    return data


async def batch_insert(pool, records, batch_size=1000):
    async with pool.acquire() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            # Execute batch insert
            await conn.executemany(
                'INSERT INTO user_data(name, email, age, created_at) VALUES ($1, $2, $3, $4)',
                batch
            )

            if i % 100_000 == 0:
                print(f"Inserted {i} records")


async def main():
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'admin',
        'password': 'quest',
        'database': 'qdb'
    }

    pool = await asyncpg.create_pool(**db_config, min_size=20, max_size=20)

    try:
        await create_table(pool)

        print("Generating test data...")
        records = generate_test_data(1_000_000)

        start_time = datetime.now()

        print("Starting batch insert...")
        await batch_insert(pool, records)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"Successfully inserted 1,000,000 records in {duration:.2f} seconds")
        print(f"Average insertion rate: {1_000_000 / duration:.2f} records/second")

    finally:
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main())
