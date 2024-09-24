#      ___                  _   ____  ____
#     / _ \ _   _  ___  ___| |_|  _ \| __ )
#    | | | | | | |/ _ \/ __| __| | | |  _ \
#    | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#     \__\_\\__,_|\___||___/\__|____/|____/
#
#   Copyright (c) 2014-2019 Appsicle
#   Copyright (c) 2019-2024 QuestDB
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

import psycopg2
import re
import sys
import yaml
from string import Template


def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def substitute_variables(text, variables):
    if text is None:
        return None
    template = Template(str(text))
    return template.safe_substitute(variables)


def replace_param_placeholders(query):
    # Replace $[n] with %s for psycopg2
    # Matches $[1], $[2], etc.
    return re.sub(r'\$\[\d+\]', '%s', query)


def extract_parameters(parameters, variables):
    resolved_parameters = []
    for param in parameters:
        if isinstance(param, str):
            resolved_param = substitute_variables(param, variables)
            # Convert to appropriate type if necessary
            if resolved_param.isdigit():
                resolved_parameters.append(int(resolved_param))
            else:
                try:
                    resolved_parameters.append(float(resolved_param))
                except ValueError:
                    resolved_parameters.append(resolved_param)
        else:
            resolved_parameters.append(param)
    return resolved_parameters


def execute_query(cursor, query, parameters):
    if parameters:
        cursor.execute(query, parameters)
    else:
        cursor.execute(query)
    try:
        return cursor.fetchall()
    except psycopg2.ProgrammingError:
        # No results to fetch (e.g., for INSERT, UPDATE)
        return cursor.statusmessage


def assert_result(expect, actual):
    if 'result' in expect:
        expected_result = expect['result']
        if isinstance(expected_result, list):
            # Convert tuples in actual to lists
            actual_converted = [list(row) for row in actual]
            assert actual_converted == expected_result, f"Expected result {expected_result}, got {actual_converted}"
        else:
            # For non-list expected results, compare as strings
            assert str(actual) == str(expected_result), f"Expected result '{expected_result}', got '{actual}'"
    elif 'result_contains' in expect:
        # Convert tuples in actual to lists
        actual_converted = [list(row) for row in actual]
        for expected_row in expect['result_contains']:
            assert expected_row in actual_converted, f"Expected row {expected_row} not found in actual results."


def execute_steps(steps, variables, cursor, connection):
    for step in steps:
        if 'loop' in step:
            execute_loop(step['loop'], variables, cursor, connection)
        else:
            execute_step(step, variables, cursor, connection)


def execute_loop(loop_def, variables, cursor, connection):
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


def execute_step(step, variables, cursor, connection):
    action = step['action']
    query_template = step.get('query')
    parameters = step.get('parameters', [])
    expect = step.get('expect', {})

    # Substitute variables in query
    query_with_vars = substitute_variables(query_template, variables)

    # Replace parameter placeholders in query
    query = replace_param_placeholders(query_with_vars)

    resolved_parameters = extract_parameters(parameters, variables)
    result = execute_query(cursor, query, resolved_parameters)
    connection.commit()

    # Assert result
    if expect:
        assert_result(expect, result)


def run_test(test, global_variables, connection):
    variables = global_variables.copy()
    variables.update(test.get('variables', {}))

    cursor = connection.cursor()

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
            # Optionally handle teardown exceptions (e.g., logging)
        cursor.close()
        if test_failed:
            sys.exit(1)


def main(yaml_file):
    data = load_yaml(yaml_file)
    global_variables = data.get('variables', {})
    tests = data.get('tests', [])

    for test in tests:
        connection = psycopg2.connect(
            host='localhost',
            port=5432,
            user='admin',
            password='quest',
            database='qdb'
        )
        run_test(test, global_variables, connection)
        connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python runner.py <test_file.yaml>")
        sys.exit(1)
    yaml_file = sys.argv[1]
    main(yaml_file)
