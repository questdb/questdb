@echo off
echo Attempting to run specific test validation...

REM Check if our classes compile individually
echo Testing compilation of modified files...

REM Test the Windows HTTP client class
echo Checking HttpClientWindows.java...
javac -cp "target\classes;target\test-classes" src\main\java\io\questdb\cutlass\http\client\HttpClientWindows.java 2>nul
if %errorlevel% equ 0 (
    echo ✓ HttpClientWindows.java compiles successfully
) else (
    echo ✗ HttpClientWindows.java compilation failed
)

REM Test the mock server test class
echo Checking LineHttpSenderMockServerTest.java...
javac -cp "target\classes;target\test-classes" src\test\java\io\questdb\test\cutlass\http\line\LineHttpSenderMockServerTest.java 2>nul
if %errorlevel% equ 0 (
    echo ✓ LineHttpSenderMockServerTest.java compiles successfully
) else (
    echo ✗ LineHttpSenderMockServerTest.java compilation failed
)

REM Test GROUP BY cursor classes
echo Checking GROUP BY cursor classes...
javac -cp "target\classes;target\test-classes" src\main\java\io\questdb\griffin\engine\groupby\GroupByRecordCursorFactory.java 2>nul
if %errorlevel% equ 0 (
    echo ✓ GroupByRecordCursorFactory.java compiles successfully
) else (
    echo ✗ GroupByRecordCursorFactory.java compilation failed
)

echo.
echo Test validation completed.
echo For full testing, Maven is required: mvn clean test
echo.
pause
