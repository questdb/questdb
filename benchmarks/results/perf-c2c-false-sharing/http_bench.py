#!/usr/bin/env python3

import argparse
import csv
import http.client
import time
import urllib.parse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--warmups", type=int, default=100)
    return parser.parse_args()


def main():
    args = parse_args()
    query = "SELECT sum(px) FROM ref WHERE sym = 'sel50'"
    path = "/exec?" + urllib.parse.urlencode({"query": query})
    connection = http.client.HTTPConnection("127.0.0.1", 9000, timeout=30)

    def execute():
        start = time.perf_counter_ns()
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read()
        elapsed = time.perf_counter_ns() - start
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {body!r}")
        if b'4.986108278E9' not in body:
            raise RuntimeError(f"unexpected query response: {body!r}")
        return elapsed, response.status, len(body), response.will_close

    for _ in range(args.warmups):
        execute()

    with open(args.output, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(("request", "elapsed_ns", "status", "response_bytes", "connection_will_close"))
        for request in range(1, args.requests + 1):
            elapsed, status, response_bytes, will_close = execute()
            writer.writerow((request, elapsed, status, response_bytes, int(will_close)))

    connection.close()


if __name__ == "__main__":
    main()
