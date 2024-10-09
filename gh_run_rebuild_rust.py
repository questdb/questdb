#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import subprocess
import json


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


def trigger_github_action(branch):
    """Trigger GitHub Action on the specified branch and get the run ID."""
    result = subprocess.run(
        ["gh", "workflow", "run", "rebuild_rust.yml", "--ref", branch],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Could not trigger GitHub Action for branch: {branch}")
        print(result.stderr)
        sys.exit(1)
    print(f"To view the status, run: `gh run list` and `gh run view <run-id>`")


if __name__ == "__main__":
    check_gh_authentication()
    branch = get_current_branch()
    trigger_github_action(branch)
