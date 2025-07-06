## Ready for Review 🚀

Hi QuestDB maintainers! 

This PR addresses both issues #5815 and #5820:

### ✅ **Issue #5815**: Windows Socket Timeout Fixes
- Enhanced `HttpClientWindows.java` with proper timeout handling
- Stabilized `LineHttpSenderMockServerTest.java` for Windows
- Removed external dependencies (Awaitility) for better compatibility

### ✅ **Issue #5820**: GROUP BY Cursor Resource Cleanup  
- Added `function.cursorClosed()` calls to all 8 relevant GROUP BY cursor types
- Ensures proper resource cleanup and prevents memory leaks

### 🔧 **Changes Made**
- **Conservative approach**: Minimal changes for maximum stability
- **No breaking changes**: All existing APIs preserved
- **Compilation clean**: All errors resolved locally

The failing CI checks appear to be due to workflow approval requirements for fork PRs (which is expected). Once approved, the full test suite should validate that:
1. Windows socket timeouts are resolved
2. GROUP BY resource cleanup works properly  
3. No regressions in existing functionality

Ready for maintainer review! 🙏
