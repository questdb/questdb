#include <windows.h>
#include "common.h"
#include "io.h"
#include <time.h>

#pragma comment(lib, "advapi32.lib")

SERVICE_STATUS gSvcStatus;
SERVICE_STATUS_HANDLE gSvcStatusHandle;
HANDLE ghSvcStopEvent = NULL;
CONFIG *gConfig;

VOID WINAPI SvcCtrlHandler(DWORD);

VOID ReportSvcStatus(DWORD, DWORD, DWORD);

VOID WINAPI qdbService(DWORD argc, LPSTR *argv);

void log_event(WORD logType, char *serviceName, const char *message) {
    HANDLE hEventSource;
    LPCTSTR lpszStrings[1];

    hEventSource = RegisterEventSource(NULL, serviceName);

    if (NULL != hEventSource) {
        lpszStrings[0] = message;

        ReportEvent(hEventSource,        // event log handle
                    logType, // event type
                    0,                   // event category
                    0b11000000000000000000000000000000,                   // event identifier
                    NULL,                // no security identifier
                    1,                   // size of lpszStrings array
                    0,                   // no binary data
                    lpszStrings,         // array of strings
                    NULL);               // no binary data

        DeregisterEventSource(hEventSource);
    }
}

void qdbDispatchService(CONFIG *config) {

    gConfig = config;

    SERVICE_TABLE_ENTRY DispatchTable[] =
            {
                    {config->serviceName, (LPSERVICE_MAIN_FUNCTION) qdbService},
                    {NULL, NULL}
            };

    if (!StartServiceCtrlDispatcher(DispatchTable)) {
        log_event(EVENTLOG_ERROR_TYPE, config->serviceName, "StartServiceCtrlDispatcher");
    }
}

HANDLE openLogFile(CONFIG *config) {
    // create log dir
    char log[MAX_PATH];
    int len = snprintf(log, MAX_PATH, "%s\\log", config->dir);

    if (len < 0 || len >= MAX_PATH) {
        return INVALID_HANDLE_VALUE;
    }

    if (!makeDir(log)) {
        return INVALID_HANDLE_VALUE;
    }

    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    if (t == NULL) {
        return INVALID_HANDLE_VALUE;
    }

    len = snprintf(log, MAX_PATH, "%s\\log\\service-", config->dir);
    if (len >= 0 && len < MAX_PATH) {
        size_t written = strftime(log + len, MAX_PATH - len, "%Y-%m-%dT%H-%M-%S.txt", t);
        if (written == 0) {
            return INVALID_HANDLE_VALUE;
        }
    } else {
        return INVALID_HANDLE_VALUE;
    }

    FILE *stream;
    if ((stream = fopen(log, "w")) == NULL) {
        return INVALID_HANDLE_VALUE;
    }

    return (HANDLE)_get_osfhandle(_fileno(stream));
}

VOID WINAPI qdbService(DWORD argc, LPSTR *argv) {

    // Get service name from first command line arg

    if (argc > 0) {
        if (gConfig->serviceName != NULL) {
            free(gConfig->serviceName);
        }

        char *svcName = malloc(strlen(argv[0]) + 2);
        strcpy(svcName, argv[0]);
        gConfig->serviceName = svcName;
    }

    // Register the handler function for the service

    gSvcStatusHandle = RegisterServiceCtrlHandler(gConfig->serviceName, SvcCtrlHandler);

    if (!gSvcStatusHandle) {
        log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "RegisterServiceCtrlHandler failed");
        return;
    }

    // These SERVICE_STATUS members remain as set here

    gSvcStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    gSvcStatus.dwServiceSpecificExitCode = 0;

    // Report initial status to the SCM

    ReportSvcStatus(SERVICE_START_PENDING, NO_ERROR, 3000);

    // Perform service-specific initialization and work.

    ghSvcStopEvent = CreateEvent(
            NULL,    // default security attributes
            TRUE,    // manual reset event
            FALSE,   // not signaled
            NULL);   // no name

    if (ghSvcStopEvent == NULL) {
        log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Could not create stop event");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    if (!makeDir(gConfig->dir)) {
        log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName,
                  "Could not create root directory. Make sure it exists before starting service");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    HANDLE log = openLogFile(gConfig);
    if (log == INVALID_HANDLE_VALUE) {
        log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Could not open service log file.");
        return;
    }

    PROCESS_INFORMATION pi;
    ZeroMemory(&pi, sizeof(pi));

    STARTUPINFO si;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    si.hStdError = log;
    si.hStdOutput = log;
    si.dwFlags |= STARTF_USESTDHANDLES;

    char buf[2048];
    snprintf(buf, sizeof(buf), "Starting %s %s", gConfig->javaExec, gConfig->javaArgs);
    log_event(EVENTLOG_INFORMATION_TYPE, gConfig->serviceName, buf);

    if (!CreateProcess(gConfig->javaExec, gConfig->javaArgs, NULL, NULL, TRUE/*handles are inherited to redirect stdout/err*/, 0, NULL, NULL, &si, &pi)) {
        log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Could not start java");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    snprintf(buf, sizeof(buf), "Started %s %s", gConfig->javaExec, gConfig->javaArgs);
    log_event(EVENTLOG_INFORMATION_TYPE, gConfig->serviceName, buf);

    // Report running status when initialization is complete.
    ReportSvcStatus(SERVICE_RUNNING, NO_ERROR, 0);

    HANDLE lpHandles[2] = { ghSvcStopEvent, pi.hProcess };
    DWORD dwEvent = WaitForMultipleObjects(2, lpHandles, FALSE /* return if state of any object is signalled*/, INFINITE );

    switch (dwEvent) {
        // service stop event was signaled
        case WAIT_FAILED:
        case WAIT_TIMEOUT:
        case WAIT_OBJECT_0 + 0:
            if (WAIT_FAILED == dwEvent) {
                log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Java process or service wait failed.");
            }

            if (!TerminateProcess(pi.hProcess, 0)) {
                log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Failed to terminate java process");
            }

            log_event(EVENTLOG_INFORMATION_TYPE, gConfig->serviceName, "Shutdown Java process");
            break;
        // java process exit was signalled
        case WAIT_OBJECT_0 + 1:
            log_event(EVENTLOG_ERROR_TYPE, gConfig->serviceName, "Java process was abnormally terminated.");
            break;
    }

    // Close process and thread handles.
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    CloseHandle(log);

    ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);

    log_event(EVENTLOG_INFORMATION_TYPE, gConfig->serviceName, "QuestDB is shutdown");
}

VOID ReportSvcStatus(DWORD dwCurrentState, DWORD dwWin32ExitCode, DWORD dwWaitHint) {
    static DWORD dwCheckPoint = 1;

    // Fill in the SERVICE_STATUS structure.

    gSvcStatus.dwCurrentState = dwCurrentState;
    gSvcStatus.dwWin32ExitCode = dwWin32ExitCode;
    gSvcStatus.dwWaitHint = dwWaitHint;

    if (dwCurrentState == SERVICE_START_PENDING) {
        gSvcStatus.dwControlsAccepted = 0;
    } else {
        gSvcStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    }

    if ((dwCurrentState == SERVICE_RUNNING) ||
        (dwCurrentState == SERVICE_STOPPED)) {
        gSvcStatus.dwCheckPoint = 0;
    } else {
        gSvcStatus.dwCheckPoint = dwCheckPoint++;
    }

    // Report the status of the service to the SCM.
    SetServiceStatus(gSvcStatusHandle, &gSvcStatus);
}

VOID WINAPI SvcCtrlHandler(DWORD dwCtrl) {
    // Handle the requested control code.

    switch (dwCtrl) {
        case SERVICE_CONTROL_STOP:
            ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, 0);

            // Signal the service to stop.

            SetEvent(ghSvcStopEvent);
            ReportSvcStatus(gSvcStatus.dwCurrentState, NO_ERROR, 0);

            return;

        case SERVICE_CONTROL_INTERROGATE:
            break;

        default:
            break;
    }

}


