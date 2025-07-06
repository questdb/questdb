@echo off
echo Testing Windows Socket Fix for QuestDB...
echo.

echo Step 1: Running basic compilation test...
call mvnw compile -q
if %ERRORLEVEL% neq 0 (
    echo FAILED: Compilation error
    exit /b 1
)
echo ✓ Compilation successful

echo.
echo Step 2: Running LineHttpSenderMockServerTest...
call mvnw test -Dtest=LineHttpSenderMockServerTest -q
if %ERRORLEVEL% neq 0 (
    echo FAILED: Test execution error
    exit /b 1
)
echo ✓ Test execution successful

echo.
echo Step 3: Running stress test (10 iterations)...
for /L %%i in (1,1,10) do (
    echo Running iteration %%i/10...
    call mvnw test -Dtest=LineHttpSenderMockServerTest -q
    if !ERRORLEVEL! neq 0 (
        echo FAILED: Test failed on iteration %%i
        exit /b 1
    )
)
echo ✓ All stress test iterations passed

echo.
echo 🎉 All tests passed! Windows socket fix is working correctly.
echo Ready for Pull Request submission.
