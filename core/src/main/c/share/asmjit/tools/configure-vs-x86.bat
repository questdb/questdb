@echo off

set CURRENT_DIR=%CD%
set BUILD_DIR="build_vs_x86"

mkdir ..\%BUILD_DIR%
cd ..\%BUILD_DIR%
cmake .. -G"Visual Studio 16" -A Win32 -DASMJIT_TEST=1
cd %CURRENT_DIR%
