#!/usr/bin/env python3

import argparse
import base64
import os
import subprocess


TOKEN_VARIABLE = "GIT_GITHUB_TOKEN"
GITHUB_AUTH_KEY = "http.https://github.com/.extraheader"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--recursive", action="store_true")
    arguments = parser.parse_args()

    command = ["git", "submodule", "update", "--init"]
    if arguments.recursive:
        command.append("--recursive")
    command.append("java-questdb-client")

    environment = os.environ.copy()
    token = environment.pop(TOKEN_VARIABLE, "")
    if token and not (token.startswith("$(") and token.endswith(")")):
        encoded_credentials = base64.b64encode(
            f"x-access-token:{token}".encode("utf-8")
        ).decode("ascii")
        config_index = int(environment.get("GIT_CONFIG_COUNT", "0"))
        environment["GIT_CONFIG_COUNT"] = str(config_index + 1)
        environment[f"GIT_CONFIG_KEY_{config_index}"] = GITHUB_AUTH_KEY
        environment[f"GIT_CONFIG_VALUE_{config_index}"] = (
            f"AUTHORIZATION: basic {encoded_credentials}"
        )
        print("Updating Java client submodules with GitHub authentication")
    else:
        print("Updating Java client submodules without authentication")

    return subprocess.run(command, env=environment, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
