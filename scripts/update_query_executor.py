import random
import requests

from datetime import datetime, timedelta

# Variables
START_TIME = [
    '2024-01-01T00:00:00Z',
    '2024-01-01T01:00:00Z',
    '2024-01-01T02:00:00Z',
    '2024-01-01T03:00:00Z',
    '2024-01-01T04:00:00Z',
    '2024-01-01T05:00:00Z',
    '2024-01-01T06:00:00Z',
    '2024-01-01T07:00:00Z',
    '2024-01-01T08:00:00Z',
    '2024-01-01T09:00:00Z'
]

QUERY_TEMPLATE = "UPDATE table{} SET value = value + {} WHERE ts = '{}';"

# Function to generate queries
def generate_queries(start_times, duration_seconds=5, random_min=1, random_max=10):
    queries = []
    for table_id, start_time in enumerate(start_times, start=1):
        start_dt = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        for second in range(1, duration_seconds + 1):
            update_time = start_dt + timedelta(seconds=second)
            update_time_str = update_time.strftime('%Y-%m-%dT%H:%M:%S.000000Z')
            random_value = random.randint(random_min, random_max)  # Generate random integer
            queries.append(QUERY_TEMPLATE.format(table_id, random_value, update_time_str))
    return queries

def execute_queries(queries, url='http://localhost:9000/exec'):
    for query in queries:
        try:
            response = requests.get(url, params={'query': query})
            if response.status_code == 200:
                print(f"Query executed successfully: {query}")
            else:
                print(f"Failed to execute query: {query}")
                print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Exception: {e}")


# Generate queries
queries = generate_queries(START_TIME)
# Run queries
execute_queries(queries)
