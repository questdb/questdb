#!/usr/bin/env python3

import argparse
import http.client
import json
import signal
import time
import urllib.parse


def parse_args():
    parser = argparse.ArgumentParser()
    query = parser.add_mutually_exclusive_group(required=True)
    query.add_argument("--query")
    query.add_argument("--query-file")
    parser.add_argument("--output", required=True)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--max-requests", type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.query_file:
        with open(args.query_file, encoding="utf-8") as query_file:
            sql = query_file.read().strip().rstrip(";")
    else:
        sql = args.query

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    path = "/exec?" + urllib.parse.urlencode({"query": sql})
    connection = http.client.HTTPConnection("127.0.0.1", 9000, timeout=600)
    expected_dataset = None
    requests = 0
    result_changes = 0

    def execute():
        nonlocal expected_dataset, result_changes
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {body!r}")
        payload = json.loads(body)
        if "error" in payload:
            raise RuntimeError(payload["error"])
        dataset = payload.get("dataset")
        if expected_dataset is None:
            expected_dataset = dataset
        elif dataset != expected_dataset:
            result_changes += 1
            raise RuntimeError("query result changed during recording")

    for _ in range(args.warmups):
        execute()

    start = time.monotonic()
    while not stopping and (args.max_requests == 0 or requests < args.max_requests):
        execute()
        requests += 1
    elapsed = time.monotonic() - start
    connection.close()

    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(
            {
                "query": sql,
                "warmups": args.warmups,
                "recorded_requests": requests,
                "recorded_seconds": elapsed,
                "result": expected_dataset,
                "result_changes": result_changes,
            },
            output,
            indent=2,
        )
        output.write("\n")


if __name__ == "__main__":
    main()
