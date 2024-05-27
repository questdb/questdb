#!/usr/bin/env python3

import sys
import textwrap
import shutil
import urllib.request
import pathlib
import subprocess
import os
import argparse
import logging

def setup_logging(verbose):
    logging_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file(url, dest):
    logging.info(f'Downloading {url} to {dest}')
    try:
        req = urllib.request.Request(url, method='GET')
        resp = urllib.request.urlopen(req, timeout=120)
        data = resp.read()
        with open(dest, 'wb') as dest_file:
            dest_file.write(data)
        logging.info(f'Successfully downloaded {url}')
    except Exception as e:
        logging.error(f'Failed to download {url}: {e}')
        raise

def default_cargo_path():
    return pathlib.Path.home() / '.cargo'

def export_cargo_bin_path(cargo_bin_path):
    """Add cargo's bin directory to PATH."""
    os.environ['PATH'] = str(cargo_bin_path) + os.pathsep + os.environ['PATH']
    logging.info(f'Added {cargo_bin_path} to PATH')
    print(f'##vso[task.prependpath]{cargo_bin_path}')

def may_export_cargo_home(cargo_home):
    """Export CARGO_HOME env variable."""
    if not os.environ.get('CARGO_HOME'):
        os.environ['CARGO_HOME'] = str(cargo_home)
        logging.info(f'Set CARGO_HOME to {cargo_home}')
        print(f'##vso[task.setvariable variable=CARGO_HOME]{cargo_home}')

def install_rust():
    platform_key = {'linux': 'unix', 'darwin': 'unix', 'win32': 'windows'}.get(sys.platform)
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
    try:
        download_file(download_url, file_name)
        logging.info('Running rustup installation script')
        subprocess.check_call(install_cmd, stderr=subprocess.STDOUT)
        logging.info('Rust installation completed successfully')
    except subprocess.CalledProcessError as e:
        logging.error(f'Rust installation failed: {e}')
        raise
    finally:
        os.remove(file_name)
        logging.info(f'Removed installation file {file_name}')
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
        logging.info(f'Installing components: {components}')
        try:
            subprocess.check_call(
                ['rustup', 'component', 'add'] + components,
                stderr=subprocess.STDOUT)
            logging.info(f'Successfully installed components: {components}')
        except subprocess.CalledProcessError as e:
            logging.error(f'Failed to install components {components}: {e}')
            raise

def linux_glibc_version():
    if sys.platform != 'linux':
        return ''
    try:
        output = subprocess.check_output(
            ['ldd', '--version'],
            stderr=subprocess.STDOUT).decode('utf-8')
        for line in output.splitlines():
            if line.startswith('ldd ('):
                version = line.split()[-1]
                logging.info(f'Detected GLIBC version: {version}')
                return version
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to determine GLIBC version: {e}')
        raise
    raise RuntimeError('Failed to parse glibc version')

def ensure_rust(components):
    cargo_bin = shutil.which('cargo')
    if cargo_bin:
        cargo_path = pathlib.Path(cargo_bin).parent.parent
        logging.info(f'Rust is already installed. Cargo path: {cargo_path}')
        may_export_cargo_home(cargo_path)
    else:
        install_rust()

    install_components(components)

    try:
        output = subprocess.check_output(
            ['rustc', '--version', '--verbose'],
            stderr=subprocess.STDOUT).decode('utf-8')
        host_triple, release = parse_rustc_info(output)
        logging.info('Rustc info retrieved successfully')
        print('\nRustc info:')
        print(textwrap.indent(output, '    '))

        # Export keying info we can use for build caching.
        print(f'##vso[task.setvariable variable=RUSTC_HOST_TRIPLE]{host_triple}')
        print(f'##vso[task.setvariable variable=RUSTC_RELEASE]{release}')
        print(f'##vso[task.setvariable variable=LINUX_GLIBC_VERSION]{linux_glibc_version()}')
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to retrieve rustc info: {e}')
        raise

def export_cargo_install_env():
    cargo_install_path = pathlib.Path.home() / 'cargo-install'
    logging.info(f'Setting CARGO_INSTALL_PATH to {cargo_install_path}')
    print(f'##vso[task.setvariable variable=CARGO_INSTALL_PATH]{cargo_install_path}')
    print(f'##vso[task.prependpath]{cargo_install_path / "bin"}')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--export-cargo-install-env', action='store_true')
    parser.add_argument('--components', nargs='*', default=[])
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    setup_logging(args.verbose)
    logging.info('Starting Rust installation process')
    try:
        ensure_rust(args.components)
        if args.export_cargo_install_env:
            export_cargo_install_env()
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        sys.exit(1)
    logging.info('Rust installation process completed')
