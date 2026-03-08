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
import os

import asyncpg
import re
import sys
from asyncpg import Connection

from common import *


def adjust_placeholder_syntax(query):
    # Replace $[n] with $n
    return re.sub(r'\$\[(\d+)\]', r'$\1', query)


async def execute_query(connection: Connection, query, parameters):
    query_type = query.strip().split()[0].lower()
    if query_type == 'select':
        return await connection.fetch(query, *parameters)

    status = await connection.execute(query, *parameters)
    # parse status string to update count (if any) as a result
    status_parts = status.split()
    if status_parts[0] == 'INSERT' or status_parts[0] == 'UPDATE':
        row_count = int(status_parts[-1])
        return [{'count': row_count}]

    return None


async def execute_steps(steps, variables, connection):
    for step in steps:
        if 'loop' in step:
            await execute_loop(step['loop'], variables, connection)
        else:
            await execute_step(step, variables, connection)


async def execute_loop(loop_def, variables, connection):
    loop_var_name = loop_def['as']
    loop_variables = variables.copy()

    if 'over' in loop_def:
        iterable = loop_def['over']
    elif 'range' in loop_def:
        start = loop_def['range']['start']
        end = loop_def['range']['end']
        iterable = range(start, end + 1)
    else:
        raise ValueError("Loop must have 'over' or 'range' defined.")

    for item in iterable:
        loop_variables[loop_var_name] = item
        await execute_steps(loop_def['steps'], loop_variables, connection)


async def execute_step(step, variables, connection):
    action = step.get('action')
    query_template = step.get('query')
    types_parameters = step.get('parameters', [])
    expect = step.get('expect', {})

    with_vars_substituted = substitute_variables(query_template, variables)
    query = adjust_placeholder_syntax(with_vars_substituted)

    resolved_parameters = resolve_parameters(types_parameters, variables)
    result = await execute_query(connection, query, resolved_parameters)

    # Assert result
    if expect:
        assert_result(expect, result)


async def run_test(test, global_variables):
    variables = global_variables.copy()
    variables.update(test.get('variables', {}))

    # port from env. variable of default to 8812
    port = int(os.getenv('PGPORT', 8812))
    connection = await asyncpg.connect(
        host='localhost',
        port=port,
        user='admin',
        password='quest',
        database='qdb'
    )

    # hard-code array types. this is a workaround for asyncpg using
    # introspection to determine array types. the introspection query uses recursive CTEs which are not supported
    # by QuestDB :-(
    # See: https://github.com/MagicStack/asyncpg/discussions/1015
    float8_array = {
        'oid': 1022,
        'elemtype': 701,
        'kind': 'b',
        'name': '_float8',
        'elemtype_name': 'float8',
        'ns': 'pg_catalog',
        'elemdelim': ',',
        'depth': 0,
        'range_subtype': None,
        'attrtypoids': None,
        'basetype': None
    }
    varchar_array = {
        'oid': 1015,
        'elemtype': 1043,
        'kind': 'b',
        'name': '_varchar',
        'elemtype_name': 'varchar',
        'ns': 'pg_catalog',
        'elemdelim': ',',
        'depth': 0,
        'range_subtype': None,
        'attrtypoids': None,
        'basetype': None
    }
    connection._protocol.get_settings().register_data_types([float8_array, varchar_array])

    test_failed = False
    try:
        # Prepare phase
        prepare_steps = test.get('prepare', [])
        await execute_steps(prepare_steps, variables, connection)

        # Test steps
        test_steps = test.get('steps', [])
        await execute_steps(test_steps, variables, connection)

        print(f"Test '{test['name']}' passed.")

        test_failed = False

    except Exception as e:
        print(f"Test '{test['name']}' failed: {str(e)}")
        test_failed = True

    finally:
        # Teardown phase should run regardless of test outcome
        teardown_steps = test.get('teardown', [])
        try:
            await execute_steps(teardown_steps, variables, connection)
        except Exception as teardown_exception:
            print(f"Teardown for test '{test['name']}' failed: {str(teardown_exception)}")
        await connection.close()
        if test_failed:
            sys.exit(1)


async def main(yaml_file):
    data = load_yaml(yaml_file)
    global_variables = data.get('variables', {})
    tests = data.get('tests', [])

    for test in tests:
        iterations = test.get('iterations', 50)
        exclusions = test.get('exclude', [])
        if 'asyncpg' in exclusions:
            print(f"Skipping test '{test['name']}' because it is excluded for asyncpg.")
            continue
        for _ in range(iterations):
            print(f"Running test '{test['name']}' (iteration {_ + 1})")
            await run_test(test, global_variables)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python runner.py <test_file.yaml>")
        sys.exit(1)
    yaml_file = sys.argv[1]
    asyncio.run(main(yaml_file))
