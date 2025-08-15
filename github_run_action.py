#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import subprocess
import json
import argparse


VALID_ACTIONS = [
    "rebuild_rust",
    "rebuild_native_libs",
    "rebuild_rust_test"
]


def check_gh_authentication():
    """Check if GitHub CLI is authenticated."""
    result = subprocess.run(
        ["gh", "auth", "status"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print("GitHub CLI is not authenticated. Please run `gh auth login` to authenticate.")
        sys.exit(1)


def get_current_branch():
    """Get the current git branch."""
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def trigger_github_action(branch, action):
    """Trigger GitHub Action on the specified branch and get the run ID."""
    result = subprocess.run(
        ["gh", "workflow", "run", f"{action}.yml", "--ref", branch],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Could not trigger GitHub Action for branch: {branch}")
        print(result.stderr)
        sys.exit(1)
    print(f"To view the status, run: `gh run list` and `gh run view <run-id>`")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("actions", metavar="ACTION", choices=VALID_ACTIONS, nargs="*")
    return parser.parse_args()


def main():
    args = parse_args()
    branch = get_current_branch()
    check_gh_authentication()
    for action in args.actions:
        trigger_github_action(branch, action)


if __name__ == "__main__":
    main()
