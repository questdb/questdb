#!/bin/bash
set -e

# Ensure rustup-managed toolchain is used, not Homebrew's rustc/cargo
export PATH="$HOME/.cargo/bin:$PATH"

# Work relative to this script's own location (the repo root) so build.sh can be
# invoked from any working directory, not just the repo root.
cd "$(dirname "${BASH_SOURCE[0]}")/core/rust/qdbr"

PLATFORM_DIR="io/questdb/bin/darwin-aarch64"
DYLIB="target/release/libquestdbr.dylib"

cargo build --release

# Overlay the freshly built dylib onto the Maven resources, and onto the compiled
# classes tree when it exists, so an already-built test classpath picks it up.
cp "$DYLIB" "../../src/main/resources/$PLATFORM_DIR/libquestdbr.dylib"
if [ -d "../../target/classes/$PLATFORM_DIR" ]; then
    cp "$DYLIB" "../../target/classes/$PLATFORM_DIR/libquestdbr.dylib"
fi
