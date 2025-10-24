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
from decimal import Decimal

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
    elif type == 'array_float8':
        # Handle floating point arrays like {-1, 2, 3, 4, 5.42}
        if isinstance(value, str):
             # Strip curly braces and split by comma
            value = value.strip('{}')
            # Convert each element to float, handling null values
            float_array = []
            for item in value.split(','):
                item = item.strip()
                if not item:
                    continue
                if item.lower() == 'null':
                    float_array.append(None)
                else:
                    float_array.append(float(item))
            resolved_parameters.append(float_array)
        # If already a list, ensure all elements are floats or None
        elif isinstance(value, list):
            float_array = []
            for item in value:
                if item is None:
                    float_array.append(None)
                else:
                    float_array.append(float(item))
            resolved_parameters.append(float_array)
        else:
            raise ValueError(f"Invalid array_float8 value: {value}")
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
    elif type == 'numeric':
        resolved_parameters.append(Decimal(value))
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
            row[i] = convert_value(value)

    return result_converted


def convert_value(value):
    # Convert timestamps to strings for comparison, format: '2021-09-01T12:34:56.123456Z'
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Convert bytes to char
    elif isinstance(value, bytes):
        return value.decode()

    # Recursively handle lists
    elif isinstance(value, list):
        # Handle nested lists
        if value and isinstance(value[0], list):
            nested_items = [convert_value(item) for item in value]
            # If nested_items are already in PostgreSQL array format (start with '{')
            if all(isinstance(item, str) and item.startswith('{') for item in nested_items):
                return '{' + ','.join(nested_items) + '}'
            else:
                # Otherwise, wrap each nested list in braces
                return '{' + ''.join('{' + str(item)[1:-1] + '}' for item in nested_items) + '}'

        # Process flat lists of numbers or None values for PostgreSQL array formatting
        elif all(isinstance(item, (int, float, type(None))) for item in value):
            float_strs = []
            for item in value:
                if item is None:
                    float_strs.append("NULL")
                else:
                    float_val = float(item)
                    # Check if it's an integer value and add .0 if needed
                    if float_val.is_integer():
                        float_strs.append(f"{float_val:.1f}")
                    else:
                        float_strs.append(str(float_val))
            return '{' + ','.join(float_strs) + '}'

        # For other types of lists, recursively convert each item
        else:
            return [convert_value(item) for item in value]

    # Handle dictionaries by recursively converting their values
    elif isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}
    
    # Handle decimal to print them as string
    elif isinstance(value, Decimal):
        return value.normalize().__str__()

    # Return other types unchanged
    else:
        return value


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
