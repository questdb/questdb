#!/usr/bin/env python3
"""Simple script to execute SQL queries against QuestDB REST API."""

import argparse
import requests
import sys

def execute_query(query, host="localhost", port=9000):
    """Execute a SQL query against QuestDB and return the result."""
    url = f"http://{host}:{port}/exec"
    response = requests.get(url, params={"query": query, "timings": "true"})
    response.raise_for_status()
    return response.json()

def main():
    parser = argparse.ArgumentParser(description="Execute SQL queries against QuestDB REST API")
    parser.add_argument("query", help="SQL query to execute")
    parser.add_argument("-n", "--iterations", type=int, default=1, help="Number of times to run the query")
    parser.add_argument("--host", default="localhost", help="QuestDB host (default: localhost)")
    parser.add_argument("--port", type=int, default=9000, help="QuestDB port (default: 9000)")
    parser.add_argument("--debug", action="store_true", help="Print raw response for debugging")

    args = parser.parse_args()

    try:
        for i in range(args.iterations):
            result = execute_query(args.query, args.host, args.port)

            if args.debug:
                print(result)
                continue

            num_rows = result.get("count", len(result.get("dataset", [])))
            timings = result.get("timings", {})
            exec_time = timings.get("execute", "N/A")

            exec_time_fmt = f"{exec_time:,}" if isinstance(exec_time, int) else exec_time

            if args.iterations > 1:
                print(f"[{i + 1}/{args.iterations}] rows: {num_rows:,}, exec time: {exec_time_fmt} ns")
            else:
                print(f"rows: {num_rows:,}, exec time: {exec_time_fmt} ns")

    except requests.RequestException as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
