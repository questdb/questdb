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
import os
import psycopg
import re
import sys
from psycopg import Connection, Cursor
from psycopg.adapt import Dumper
from psycopg.pq import Format

from common import *


class VarcharArray(list):
    pass


# Text format dumper for varchar[] (OID 1015)
class VarcharArrayTextDumper(Dumper):
    oid = 1015  # varchar[]
    format = Format.TEXT

    def dump(self, obj):
        def escape_elem(e):
            if e is None:
                return 'NULL'
            s = str(e)
            s = s.replace('\\', '\\\\').replace('"', '\\"')
            if ',' in s or '"' in s or '{' in s or '}' in s or ' ' in s or s == '':
                return f'"{s}"'
            return s

        elements = ','.join(escape_elem(e) for e in obj)
        return f'{{{elements}}}'.encode('utf-8')


class VarcharArrayBinaryDumper(Dumper):
    oid = 1015  # varchar[]
    format = Format.BINARY

    def dump(self, obj):
        import struct
        #  binary array format:
        # - 4 bytes: number of dimensions (1 for 1D array)
        # - 4 bytes: has null flag (1 if any element is null)
        # - 4 bytes: element type OID (1043 for varchar)
        # - 4 bytes: dimension size
        # - 4 bytes: lower bound (1)
        # For each element:
        # - 4 bytes: element length (-1 for null, otherwise byte length)
        # - N bytes: element data (if not null)

        has_null = 1 if any(e is None for e in obj) else 0
        elem_oid = 1043  # varchar OID
        header = struct.pack('>iiiii', 1, has_null, elem_oid, len(obj), 1)

        elements = []
        for e in obj:
            if e is None:
                elements.append(struct.pack('>i', -1))
            else:
                data = str(e).encode('utf-8')
                elements.append(struct.pack('>i', len(data)) + data)

        return header + b''.join(elements)

def register_varchar_array_type(connection, binary):
    """Register varchar[] type dumper with the connection based on mode."""
    if binary:
        connection.adapters.register_dumper(VarcharArray, VarcharArrayBinaryDumper)
    else:
        connection.adapters.register_dumper(VarcharArray, VarcharArrayTextDumper)


def adjust_placeholder_syntax(query):
    # Replace $[n] with %s
    return re.sub(r'\$\[\d+\]', '%s', query)


def execute_query(cursor: Cursor, query, parameters):
    if parameters:
        cursor.execute(query, parameters)
    else:
        cursor.execute(query)
    try:
        if cursor.description:
            return cursor.fetchall()
        else:
            if cursor.rowcount == -1:
                return None
            return [(cursor.rowcount,)]
    except psycopg.errors.ProgrammingError:
        return cursor.statusmessage


def execute_steps(steps, variables, cursor: Cursor, connection: Connection):
    for step in steps:
        if 'loop' in step:
            execute_loop(step['loop'], variables, cursor, connection)
        else:
            execute_step(step, variables, cursor, connection)


def execute_loop(loop_def, variables, cursor: Cursor, connection: Connection):
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
        execute_steps(loop_def['steps'], loop_variables, cursor, connection)


def wrap_varchar_arrays(parameters, param_types):
    wrapped = []
    for i, param in enumerate(parameters):
        if i < len(param_types) and param_types[i].get('type', '').lower() == 'array_varchar':
            wrapped.append(VarcharArray(param) if isinstance(param, list) else param)
        else:
            wrapped.append(param)
    return wrapped


def execute_step(step, variables, cursor: Cursor, connection: Connection):
    action = step['action']
    query_template = step.get('query')
    parameters = step.get('parameters', [])
    expect = step.get('expect', {})

    # Substitute variables in query
    query_with_vars = substitute_variables(query_template, variables)

    # Replace parameter placeholders in query
    query = adjust_placeholder_syntax(query_with_vars)

    resolved_parameters = resolve_parameters(parameters, variables)

    # Wrap varchar arrays for proper type encoding
    resolved_parameters = wrap_varchar_arrays(resolved_parameters, parameters)
    result = execute_query(cursor, query, resolved_parameters)
    connection.commit()

    # Assert result
    if expect:
        assert_result(expect, result)


def run_test(test, global_variables, connection, binary):
    variables = global_variables.copy()
    variables.update(test.get('variables', {}))

    cursor = connection.cursor(binary=binary)

    test_failed = False
    try:
        # Prepare phase
        prepare_steps = test.get('prepare', [])
        execute_steps(prepare_steps, variables, cursor, connection)

        # Test steps
        test_steps = test.get('steps', [])
        execute_steps(test_steps, variables, cursor, connection)

        print(f"Test '{test['name']}' passed.")

        test_failed = False

    except Exception as e:
        print(f"Test '{test['name']}' failed: {str(e)}")
        test_failed = True

    finally:
        # Teardown phase should run regardless of test outcome
        teardown_steps = test.get('teardown', [])
        try:
            execute_steps(teardown_steps, variables, cursor, connection)
        except Exception as teardown_exception:
            print(f"Teardown for test '{test['name']}' failed: {str(teardown_exception)}")
        cursor.close()
        if test_failed:
            sys.exit(1)


def main(yaml_file):
    data = load_yaml(yaml_file)
    global_variables = data.get('variables', {})
    tests = data.get('tests', [])

    port = int(os.getenv('PGPORT', 8812))
    for binary in [True, False]:
        binary_mode = "binary" if binary else "text"
        print(f"\n=== Running tests with {binary_mode} mode ===\n")
        for test in tests:
            iterations = test.get('iterations', 50)
            exclusions = test.get('exclude', [])
            if 'psycopg3' in exclusions:
                print(f"Skipping test '{test['name']}' because it is excluded for psycopg3.")
                continue
            for i in range(iterations):
                print(f"Running test '{test['name']}' [{binary_mode}] (iteration {i + 1})")
                connection = psycopg.connect(
                    host='localhost',
                    port=port,
                    user='admin',
                    password='quest',
                    dbname='qdb',
                    autocommit=True
                )
                register_varchar_array_type(connection, binary)
                run_test(test, global_variables, connection, binary)
                connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python runner.py <test_file.yaml>")
        sys.exit(1)
    yaml_file = sys.argv[1]
    main(yaml_file)
