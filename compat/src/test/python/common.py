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

import datetime
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


def resolve_parameters(typed_parameters, variables):
    resolved_parameters = []
    for typed_param in typed_parameters:
        type_ = typed_param.get('type').lower()
        value = typed_param.get('value')

        if isinstance(value, str):
            resolved_str_value = substitute_variables(value, variables)
            convert_and_append_parameters(resolved_str_value, type_, resolved_parameters)
        else:
            convert_and_append_parameters(value, type_, resolved_parameters)
    return resolved_parameters


def convert_and_append_parameters(value, type, resolved_parameters):
    if type == 'int4' or type == 'int8':
        resolved_parameters.append(int(value))
    elif type == 'float4' or type == 'float8':
        resolved_parameters.append(float(value))
    elif type == 'boolean':
        value = value.lower().strip()
        if value == 'true':
            resolved_parameters.append(True)
        elif value == 'false':
            resolved_parameters.append(False)
        else:
            raise ValueError(f"Invalid boolean value: {value}")
    elif type == 'varchar':
        resolved_parameters.append(str(value))
    elif type == 'timestamp':
        parsed_value = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
        resolved_parameters.append(parsed_value)
    elif type == 'date':
        parsed_value = datetime.date.fromisoformat(value)
        resolved_parameters.append(parsed_value)
    elif type == 'char':
        str_val = str(value)
        resolved_parameters.append(str_val)
    else:
        resolved_parameters.append(value)


def convert_query_result(result):
    first_item = next(iter(result), None)
    if first_item is None:
        return result

    if isinstance(first_item, dict):
        result_converted = [list(record.values()) for record in result]
    else:
        result_converted = [list(record) for record in result]

    for row in result_converted:
        for i, value in enumerate(row):
            # convert timestamps to strings for comparison, format: '2021-09-01T12:34:56.123456Z'
            if isinstance(value, datetime.datetime):
                row[i] = value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            # convert bytes to char. why? asyncpg client uses binary codec for char type
            # This means char columns are returned as bytes, but we want to compare them as chars
            elif isinstance(value, bytes):
                row[i] = value.decode()


    for row in result_converted:
        for i, value in enumerate(row):
            if isinstance(value, bytes):
                row[i] = value.decode()

    return result_converted


def assert_result(expect, actual):
    if 'result' in expect:
        expected_result = expect['result']
        if isinstance(expected_result, list):
            if isinstance(actual, str):
                # If actual is a status string, cannot compare to expected list
                raise AssertionError(f"Expected result {expected_result}, got status '{actual}'")
            actual_converted = convert_query_result(actual)
            assert actual_converted == expected_result, f"Expected result {expected_result}, got {actual_converted}"
        else:
            # For non-list expected results, compare as strings
            assert str(actual) == str(expected_result), f"Expected result '{expected_result}', got '{actual}'"
    elif 'result_contains' in expect:
        if isinstance(actual, str):
            # If actual is a status string, cannot compare to expected results
            raise AssertionError(f"Expected result containing {expect['result_contains']}, got status '{actual}'")
        actual_converted = convert_query_result(actual)
        for expected_row in expect['result_contains']:
            assert expected_row in actual_converted, f"Expected row {expected_row} not found in actual results."
