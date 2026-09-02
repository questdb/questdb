#!/usr/bin/env python3

import base64
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


CI_DIRECTORY = Path(__file__).parent
REPOSITORY_ROOT = CI_DIRECTORY.parent
SCRIPT_PATH = CI_DIRECTORY / "update_java_client_submodules.py"
TOKEN_VARIABLE = "GIT_GITHUB_TOKEN"

FAKE_GIT = """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

count = int(os.environ.get("GIT_CONFIG_COUNT", "0"))
result = {
    "arguments": sys.argv[1:],
    "config": [
        [
            os.environ.get(f"GIT_CONFIG_KEY_{index}"),
            os.environ.get(f"GIT_CONFIG_VALUE_{index}"),
        ]
        for index in range(count)
    ],
    "token": os.environ.get("GIT_GITHUB_TOKEN"),
}
Path(os.environ["FAKE_GIT_RESULT"]).write_text(json.dumps(result), encoding="utf-8")
sys.exit(int(os.environ.get("FAKE_GIT_EXIT_CODE", "0")))
"""


class UpdateJavaClientSubmodulesTest(unittest.TestCase):
    def run_script(self, token=None, arguments=(), extra_environment=None):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            fake_git = fake_bin / "git"
            fake_git.write_text(FAKE_GIT, encoding="utf-8")
            fake_git.chmod(fake_git.stat().st_mode | stat.S_IXUSR)
            result_path = root / "result.json"

            environment = os.environ.copy()
            environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
            environment["FAKE_GIT_RESULT"] = str(result_path)
            if token is None:
                environment.pop(TOKEN_VARIABLE, None)
            else:
                environment[TOKEN_VARIABLE] = token
            if extra_environment:
                environment.update(extra_environment)

            process = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), *arguments],
                cwd=SCRIPT_PATH.parent.parent,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            git_result = None
            if result_path.exists():
                git_result = json.loads(result_path.read_text(encoding="utf-8"))
            return process, git_result

    def test_authenticates_recursive_fetch_without_exposing_token_in_arguments(self):
        token = "github-token-for-test"

        process, git_result = self.run_script(token, ["--recursive"])

        self.assertEqual(0, process.returncode, process.stderr)
        self.assertEqual(
            [
                "submodule",
                "update",
                "--init",
                "--recursive",
                "java-questdb-client",
            ],
            git_result["arguments"],
        )
        self.assertNotIn(token, " ".join(git_result["arguments"]))
        self.assertIsNone(git_result["token"])
        self.assertEqual(
            "http.https://github.com/.extraheader", git_result["config"][0][0]
        )
        header = git_result["config"][0][1]
        prefix = "AUTHORIZATION: basic "
        self.assertTrue(header.startswith(prefix))
        self.assertEqual(
            f"x-access-token:{token}",
            base64.b64decode(header.removeprefix(prefix)).decode("utf-8"),
        )

    def test_uses_existing_anonymous_behavior_when_token_is_unavailable(self):
        process, git_result = self.run_script("$(GIT_GITHUB_TOKEN)")

        self.assertEqual(0, process.returncode, process.stderr)
        self.assertEqual([], git_result["config"])
        self.assertIn("without authentication", process.stdout)

    def test_preserves_existing_command_scoped_git_configuration(self):
        existing_config = {
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.lowSpeedLimit",
            "GIT_CONFIG_VALUE_0": "102400",
        }

        process, git_result = self.run_script(
            "github-token-for-test", extra_environment=existing_config
        )

        self.assertEqual(0, process.returncode, process.stderr)
        self.assertEqual(
            [
                ["http.lowSpeedLimit", "102400"],
                ["http.https://github.com/.extraheader", git_result["config"][1][1]],
            ],
            git_result["config"],
        )

    def test_returns_git_failure_status(self):
        process, _ = self.run_script(
            "github-token-for-test",
            extra_environment={"FAKE_GIT_EXIT_CODE": "23"},
        )

        self.assertEqual(23, process.returncode)


class PipelineWiringTest(unittest.TestCase):
    def test_shared_step_fetches_client_recursively_with_secret_environment(self):
        detect_template = (
            CI_DIRECTORY / "templates/detect-local-client.yml"
        ).read_text(encoding="utf-8")
        native_template = (
            CI_DIRECTORY / "templates/build-client-native.yml"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "update_java_client_submodules.py --recursive", detect_template
        )
        self.assertIn(
            "GIT_GITHUB_TOKEN: $(GIT_GITHUB_TOKEN)", detect_template
        )
        self.assertNotIn("git submodule update", native_template)

    def test_test_pipelines_import_public_github_credential_group(self):
        for relative_path in (
            "ci/test-pipeline.yml",
            "ci/test-fuzz.yml",
            "ci/test-hosted-pipeline.yml",
        ):
            with self.subTest(pipeline=relative_path):
                pipeline = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("- group: github-public-read", pipeline)


if __name__ == "__main__":
    unittest.main()
