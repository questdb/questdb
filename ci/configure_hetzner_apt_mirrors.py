#!/usr/bin/env python3

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess


DEB822_FIELD = re.compile(r"^\s*([A-Za-z0-9-]+):\s*(.*)$")
MIRROR_LIST_DIRECTORY = Path("/etc/apt/mirrors")
MIRROR_LISTS = {
    ("ubuntu", "packages"): (
        "questdb-ubuntu-packages.list",
        (
            "https://mirror.hetzner.com/ubuntu/packages",
            "https://archive.ubuntu.com/ubuntu",
        ),
    ),
    ("ubuntu", "security"): (
        "questdb-ubuntu-security.list",
        (
            "https://mirror.hetzner.com/ubuntu/security",
            "https://security.ubuntu.com/ubuntu",
        ),
    ),
    ("ubuntu-ports", "packages"): (
        "questdb-ubuntu-ports-packages.list",
        (
            "https://mirror.hetzner.com/ubuntu-ports/packages",
            "https://ports.ubuntu.com/ubuntu-ports",
        ),
    ),
    ("ubuntu-ports", "security"): (
        "questdb-ubuntu-ports-security.list",
        (
            "https://mirror.hetzner.com/ubuntu-ports/security",
            "https://ports.ubuntu.com/ubuntu-ports",
        ),
    ),
}
ONE_LINE_SOURCE = re.compile(r"^\s*deb(?:-src)?\s")
SECURITY_SUITE = re.compile(r"(?:^|\s)\S+-security(?:\s|$)")
SOURCE_URL = re.compile(
    r"https?://(?:"
    r"(?:[A-Za-z0-9-]+\.)*archive\.ubuntu\.com/ubuntu"
    r"|security\.ubuntu\.com/ubuntu"
    r"|ports\.ubuntu\.com/ubuntu-ports"
    r"|mirror\.hetzner\.com/(?:ubuntu|ubuntu-ports)/(?:packages|security)"
    r")/?(?=\s|$)"
)
SUPPORTED_ARCHITECTURES = {"amd64", "arm64"}


def collect_deb822_fields(lines):
    fields = {}
    current_name = None
    for line_index, line in enumerate(lines):
        content = line.rstrip("\r\n")
        if not content or content.lstrip().startswith("#"):
            continue
        if content[0].isspace():
            if current_name is not None:
                fields[current_name].append((line_index, content.strip()))
            continue

        field_match = DEB822_FIELD.match(content)
        if field_match:
            current_name = field_match.group(1).lower()
            fields.setdefault(current_name, []).append(
                (line_index, field_match.group(2))
            )
        else:
            current_name = None
    return fields


def configure(root, architecture):
    os_release = root / "etc/os-release"
    if not os_release.is_file():
        print(f"Skipping Hetzner APT mirror configuration: {os_release} is unavailable")
        return 0

    os_release_values = parse_os_release(os_release.read_text(encoding="utf-8"))
    os_id = os_release_values.get("ID", "unknown")
    if os_id != "ubuntu":
        print(f"Skipping Hetzner APT mirror configuration on {os_id}")
        return 0

    if architecture not in SUPPORTED_ARCHITECTURES:
        print(
            "Skipping Hetzner APT mirror configuration on unsupported "
            f"architecture: {architecture}"
        )
        return 0

    write_mirror_lists(root)

    source_files = []
    source_list = root / "etc/apt/sources.list"
    if source_list.is_file():
        source_files.append(source_list)
    source_directory = root / "etc/apt/sources.list.d"
    if source_directory.is_dir():
        source_files.extend(sorted(source_directory.glob("*.list")))
        source_files.extend(sorted(source_directory.glob("*.sources")))

    updated_count = 0
    for source_file in source_files:
        try:
            original = source_file.read_text(encoding="utf-8")
        except OSError as error:
            raise RuntimeError(f"cannot read APT source file {source_file}: {error}") from error

        if source_file.suffix == ".sources":
            updated = rewrite_deb822_sources(original)
        else:
            updated = rewrite_one_line_sources(original)

        if updated != original:
            write_file(source_file, updated, root)
            updated_count += 1

    if updated_count == 0:
        print("No Ubuntu APT sources needed replacement")
    else:
        print(
            f"Configured prioritized Ubuntu mirrors for {architecture} in "
            f"{updated_count} source file(s)"
        )
    return updated_count


def create_directory(path, root):
    if root != Path("/") or os.geteuid() == 0:
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o755)
        return
    subprocess.run(
        ["sudo", "install", "-d", "-m", "0755", str(path)],
        check=True,
    )
    subprocess.run(["sudo", "chmod", "0755", str(path)], check=True)


def mirror_list_uri_for_match(match, is_security_only):
    source_url = match.group(0)
    origin_family = "ubuntu-ports" if "ubuntu-ports" in source_url else "ubuntu"
    mirror_kind = (
        "security"
        if "security.ubuntu.com" in source_url
        or "/security" in source_url
        or is_security_only
        else "packages"
    )
    filename, _ = MIRROR_LISTS[(origin_family, mirror_kind)]
    return f"mirror+file:{MIRROR_LIST_DIRECTORY / filename}"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Configure prioritized Ubuntu mirrors for Hetzner CI runners"
    )
    parser.add_argument(
        "--architecture",
        help="APT architecture; defaults to dpkg --print-architecture",
    )
    parser.add_argument(
        "--root",
        default="/",
        type=Path,
        help="filesystem root containing etc/apt; defaults to /",
    )
    return parser.parse_args()


def parse_os_release(contents):
    values = {}
    for line in contents.splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed = shlex.split(value, comments=True)
        values[key] = parsed[0] if parsed else ""
    return values


def replace_urls(line, is_security_only, count=0):
    return SOURCE_URL.sub(
        lambda match: mirror_list_uri_for_match(match, is_security_only),
        line,
        count=count,
    )


def rewrite_deb822_sources(contents):
    parts = re.split(r"(\r?\n[ \t]*\r?\n)", contents)
    for part_index in range(0, len(parts), 2):
        lines = parts[part_index].splitlines(keepends=True)
        fields = collect_deb822_fields(lines)
        suite_values = [
            suite
            for _, value in fields.get("suites", [])
            for suite in value.split()
        ]
        is_security_only = bool(suite_values) and all(
            suite.endswith("-security") for suite in suite_values
        )

        for line_index, _ in fields.get("uris", []):
            lines[line_index] = replace_urls(lines[line_index], is_security_only)
        parts[part_index] = "".join(lines)
    return "".join(parts)


def rewrite_one_line_sources(contents):
    rewritten_lines = []
    for line in contents.splitlines(keepends=True):
        if not ONE_LINE_SOURCE.match(line):
            rewritten_lines.append(line)
            continue

        active_line, comment_marker, comment = line.partition("#")
        is_security_only = bool(SECURITY_SUITE.search(active_line))
        active_line = replace_urls(active_line, is_security_only, count=1)
        rewritten_lines.append(active_line + comment_marker + comment)
    return "".join(rewritten_lines)


def set_file_mode(path, root, mode):
    if root != Path("/") or os.geteuid() == 0:
        path.chmod(mode)
        return
    subprocess.run(["sudo", "chmod", f"{mode:04o}", str(path)], check=True)


def write_file(path, contents, root, mode=None):
    if root != Path("/") or os.geteuid() == 0:
        path.write_text(contents, encoding="utf-8")
    else:
        subprocess.run(
            ["sudo", "tee", str(path)],
            check=True,
            input=contents,
            stdout=subprocess.DEVNULL,
            text=True,
        )
    if mode is not None:
        set_file_mode(path, root, mode)


def write_mirror_lists(root):
    mirror_list_directory = root / MIRROR_LIST_DIRECTORY.relative_to("/")
    create_directory(mirror_list_directory, root)
    for filename, mirrors in MIRROR_LISTS.values():
        path = mirror_list_directory / filename
        contents = "".join(
            f"{mirror}\tpriority:{priority}\n"
            for priority, mirror in enumerate(mirrors, start=1)
        )
        write_file(path, contents, root, mode=0o644)


def main():
    args = parse_args()
    architecture = args.architecture
    if not architecture:
        architecture = subprocess.check_output(
            ["dpkg", "--print-architecture"], text=True
        ).strip()
    configure(args.root, architecture)


if __name__ == "__main__":
    main()
