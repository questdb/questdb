#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import textwrap
import shutil
import shlex
import urllib.request
import urllib
import pathlib
import subprocess
import os
import argparse
import threading
import re
from collections import deque


def log_command(args):
    sys.stderr.write(f'>>> {shlex.join(args)}\n')
    sys.stderr.flush()
    return args


def download_file(url, dest):
    req = urllib.request.Request(url, method='GET')
    resp = urllib.request.urlopen(req, timeout=120)
    data = resp.read()
    with open(dest, 'wb') as dest_file:
        dest_file.write(data)


def default_cargo_path():
    return pathlib.Path.home() / '.cargo'


def export_cargo_bin_path(cargo_bin_path):
    """Add cargo's bin directory to PATH."""
    os.environ['PATH'] = str(cargo_bin_path) + os.pathsep + os.environ['PATH']
    print(f'##vso[task.prependpath]{cargo_bin_path}')


def may_export_cargo_home(cargo_home):
    """Export CARGO_HOME env variable."""
    if not os.environ.get('CARGO_HOME'):
        os.environ['CARGO_HOME'] = str(cargo_home)
        print(f'##vso[task.setvariable variable=CARGO_HOME]{cargo_home}')


def install_rust(nightly):
    platform_key = {'linux': 'unix', 'darwin': 'unix',
                    'win32': 'windows'}.get(sys.platform)
    if not platform_key:
        raise NotImplementedError(f'Unsupported platform: {sys.platform}')
    download_url, file_name, install_cmd = {
        'unix': (
            'https://sh.rustup.rs',
            'rustup-init.sh',
            ['sh', 'rustup-init.sh', '-y', '--profile', 'minimal'],
        ),
        'windows': (
            'https://win.rustup.rs/x86_64',
            'rustup-init.exe',
            ['rustup-init.exe', '-y', '--profile', 'minimal'],
        ),
    }[platform_key]
    if nightly:
        install_cmd.extend(['--default-toolchain', 'nightly'])
    try:
        download_file(download_url, file_name)
        subprocess.check_call(
            log_command(install_cmd),
            stderr=subprocess.STDOUT)
    finally:
        os.remove(file_name)
    export_cargo_bin_path(default_cargo_path() / 'bin')
    may_export_cargo_home(default_cargo_path())


def parse_rustc_info(output):
    attrs = dict(
        [component.strip() for component in line.split(':', 1)]
        for line in output.splitlines()
        if ':' in line)
    return attrs['host'], attrs['release']


def install_components(components):
    if components:
        subprocess.check_call(
            log_command(['rustup', 'component', 'add'] + list(set(components))),
            stderr=subprocess.STDOUT)


def linux_glibc_version():
    if sys.platform != 'linux':
        return ''
    output = subprocess.check_output(
        log_command(['ldd', '--version']),
        stderr=subprocess.STDOUT).decode('utf-8')
    for line in output.splitlines():
        if line.startswith('ldd ('):
            return line.split()[-1]
    raise RuntimeError('Failed to parse glibc version')


def call_rustup_install(args):
    # We need watch the output for Azure CI setup issues and rectify them.
    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        bufsize=1)  # Line buffered.
    
    broken_tools = deque()

    warn_pat = re.compile(
        r'warn: tool `([^`]+)` is already installed, ' +
        r'remove it from `([^`]+)`, then run `rustup update` ' +
        'to have rustup manage this tool.')

    def monitor_output(in_stream, out_stream):
        for line in in_stream:
            out_stream.write(line)
            match = warn_pat.match(line)
            if match:
                tool, path = match.groups()
                broken_tools.append((tool, pathlib.Path(path)))

    stdout_thread = threading.Thread(
        target=monitor_output, args=(proc.stdout, sys.stdout), daemon=True)
    stdout_thread.start()
    stderr_thread = threading.Thread(
        target=monitor_output, args=(proc.stderr, sys.stderr), daemon=True)
    stderr_thread.start()

    return_code = proc.wait()

    stdout_thread.join()
    stderr_thread.join()

    if return_code:
        raise subprocess.CalledProcessError(return_code, args)
    
    tool2component = {
        'rust-analyzer': 'rust-analyzer',
        'rustfmt': 'rustfmt',
        'cargo-fmt': 'rustfmt',
    }

    components = []
    for tool, path in broken_tools:
        components.append(tool2component[tool])
        tool_filename = f'{tool}.exe' \
            if sys.platform == 'win32' else tool
        tool_path = path / tool_filename
        sys.stderr.write(f'removing broken tool: {tool_path}\n')
        tool_path.unlink()

    return components


def ensure_rust_version(rustup_bin, version, components):
    """Ensure the specified version of Rust is installed and defaulted."""
    if subprocess.call(log_command([
        rustup_bin, 'self', 'update'])) != 0:
        print('Failed to update rustup. Ignoring and hoping for the best.')
    components = components + call_rustup_install(log_command([
        rustup_bin, 'toolchain', 'install', '--allow-downgrade', version]))
    subprocess.check_call(log_command([
        rustup_bin, 'default', version]))
    if components:
        subprocess.check_call(log_command([
            rustup_bin, 'update']))
        install_components(components)


def ensure_rust(version, components):
    rustup_bin = shutil.which('rustup')
    cargo_bin = shutil.which('cargo')
    if rustup_bin and cargo_bin:
        cargo_path = pathlib.Path(cargo_bin).parent.parent
        print(f'Rustup and cargo are already installed. `cargo` path: {cargo_path}')
        ensure_rust_version(rustup_bin, version, components)
        may_export_cargo_home(cargo_path)
    else:
        install_rust(version)
        install_components(components)

    output = subprocess.check_output(
        log_command(['rustc', '--version', '--verbose']),
        stderr=subprocess.STDOUT).decode('utf-8')
    host_triple, release = parse_rustc_info(output)
    print('\nRustc info:')
    print(textwrap.indent(output, '    '))

    # Export keying info we can use for build caching.
    print(f'##vso[task.setvariable variable=RUSTC_HOST_TRIPLE]{host_triple}')
    print(f'##vso[task.setvariable variable=RUSTC_RELEASE]{release}')
    print(f'##vso[task.setvariable variable=LINUX_GLIBC_VERSION]{linux_glibc_version()}')


def export_cargo_install_env():
    cargo_install_path = pathlib.Path.home() / 'cargo-install'
    print(f'##vso[task.setvariable variable=CARGO_INSTALL_PATH]{cargo_install_path}')
    print(f'##vso[task.prependpath]{cargo_install_path / "bin"}')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--export-cargo-install-env', action='store_true')
    parser.add_argument('--components', nargs='*', default=[])
    parser.add_argument(
        '--version', type=str, default='stable', 
        help='Specify the version (e.g., "stable", "beta", ' +
        '"nightly-2025-01-07"). Default is "stable".')    
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    ensure_rust(args.version, args.components)
    if args.export_cargo_install_env:
        export_cargo_install_env()
