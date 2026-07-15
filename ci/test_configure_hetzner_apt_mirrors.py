#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT_PATH = Path(__file__).with_name("configure_hetzner_apt_mirrors.py")
SPEC = importlib.util.spec_from_file_location("configure_hetzner_apt_mirrors", SCRIPT_PATH)
MIRRORS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIRRORS)


class ConfigureHetznerAptMirrorsTest(unittest.TestCase):
    def test_rewrites_amd64_one_line_sources(self):
        source = """\
deb http://archive.ubuntu.com/ubuntu noble main universe
deb https://security.ubuntu.com/ubuntu/ noble-security main universe
deb https://de.archive.ubuntu.com/ubuntu noble-updates main
deb https://apt.releases.hashicorp.com noble main
# deb http://archive.ubuntu.com/ubuntu noble-backports main
"""

        actual = MIRRORS.rewrite_one_line_sources(source)

        self.assertEqual(
            """\
deb https://mirror.hetzner.com/ubuntu/packages/ noble main universe
deb https://mirror.hetzner.com/ubuntu/security/ noble-security main universe
deb https://mirror.hetzner.com/ubuntu/packages/ noble-updates main
deb https://apt.releases.hashicorp.com noble main
# deb http://archive.ubuntu.com/ubuntu noble-backports main
""",
            actual,
        )

    def test_rewrites_arm64_deb822_sources(self):
        source = """\
Types: deb
URIs: http://ports.ubuntu.com/ubuntu-ports/
Suites: noble noble-updates noble-backports
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: http://ports.ubuntu.com/ubuntu-ports/
Suites: noble-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
"""

        actual = MIRRORS.rewrite_deb822_sources(source)

        self.assertEqual(
            """\
Types: deb
URIs: https://mirror.hetzner.com/ubuntu-ports/packages/
Suites: noble noble-updates noble-backports
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: https://mirror.hetzner.com/ubuntu-ports/security/
Suites: noble-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
""",
            actual,
        )

    def test_rewrites_lowercase_deb822_continuation_fields(self):
        source = """\
types: deb
uris:
 http://ports.ubuntu.com/ubuntu-ports/
suites:
 noble-security
components: main universe
"""

        actual = MIRRORS.rewrite_deb822_sources(source)

        self.assertEqual(
            """\
types: deb
uris:
 https://mirror.hetzner.com/ubuntu-ports/security/
suites:
 noble-security
components: main universe
""",
            actual,
        )

    def test_does_not_rewrite_comments_or_similar_paths(self):
        source = """\
deb https://ports.ubuntu.com/ubuntu-ports-custom noble main
# http://ports.ubuntu.com/ubuntu-ports/
Types: deb
# URIs: http://ports.ubuntu.com/ubuntu-ports/
URIs: https://example.com/ubuntu
Suites: noble
"""

        one_line_actual = MIRRORS.rewrite_one_line_sources(source)
        deb822_actual = MIRRORS.rewrite_deb822_sources(source)

        self.assertEqual(source, one_line_actual)
        self.assertEqual(source, deb822_actual)

    def test_preserves_source_architecture_family(self):
        source = """\
deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports noble main
deb [arch=amd64] http://archive.ubuntu.com/ubuntu noble main
"""

        actual = MIRRORS.rewrite_one_line_sources(source)

        self.assertEqual(
            """\
deb [arch=arm64] https://mirror.hetzner.com/ubuntu-ports/packages/ noble main
deb [arch=amd64] https://mirror.hetzner.com/ubuntu/packages/ noble main
""",
            actual,
        )

    def test_configure_is_idempotent_and_skips_unsupported_systems(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_directory = root / "etc/apt/sources.list.d"
            source_directory.mkdir(parents=True)
            os_release = root / "etc/os-release"
            os_release.write_text("ID=ubuntu\n", encoding="utf-8")
            source_file = source_directory / "ubuntu.sources"
            source_file.write_text(
                "Types: deb\n"
                "URIs: http://ports.ubuntu.com/ubuntu-ports/\n"
                "Suites: noble\n",
                encoding="utf-8",
            )

            self.assertEqual(1, MIRRORS.configure(root, "arm64"))
            first_result = source_file.read_text(encoding="utf-8")
            self.assertEqual(0, MIRRORS.configure(root, "arm64"))
            self.assertEqual(first_result, source_file.read_text(encoding="utf-8"))
            self.assertEqual(0, MIRRORS.configure(root, "riscv64"))

            os_release.write_text("ID=debian\n", encoding="utf-8")
            self.assertEqual(0, MIRRORS.configure(root, "amd64"))

    def test_configure_reports_source_read_failures(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_directory = root / "etc/apt/sources.list.d"
            source_directory.mkdir(parents=True)
            os_release = root / "etc/os-release"
            os_release.write_text("ID=ubuntu\n", encoding="utf-8")
            source_file = source_directory / "ubuntu.sources"
            source_file.write_text("Types: deb\n", encoding="utf-8")
            original_read_text = Path.read_text

            def read_text(path, *args, **kwargs):
                if path == source_file:
                    raise PermissionError("permission denied")
                return original_read_text(path, *args, **kwargs)

            with mock.patch.object(Path, "read_text", read_text):
                with self.assertRaisesRegex(
                    RuntimeError, r"cannot read APT source file .*ubuntu\.sources"
                ):
                    MIRRORS.configure(root, "arm64")

    def test_mixed_deb822_suite_uses_package_mirror(self):
        source = """\
Types: deb
URIs: http://ports.ubuntu.com/ubuntu-ports/
Suites: noble
 noble-security
"""

        actual = MIRRORS.rewrite_deb822_sources(source)

        self.assertIn("/ubuntu-ports/packages/", actual)
        self.assertNotIn("/ubuntu-ports/security/", actual)


if __name__ == "__main__":
    unittest.main()
