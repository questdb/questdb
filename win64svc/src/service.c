#include <windows.h>
#include "common.h"

#pragma comment(lib, "advapi32.lib")

SERVICE_STATUS gSvcStatus;
SERVICE_STATUS_HANDLE gSvcStatusHandle;
HANDLE ghSvcStopEvent = NULL;
CONFIG *gConfig;

VOID WINAPI SvcCtrlHandler(DWORD);

VOID ReportSvcStatus(DWORD, DWORD, DWORD);

VOID WINAPI qdbService();

void logEvent(CONFIG *config, char *message) {
    HANDLE hEventSource;
    LPCTSTR lpszStrings[1];

    hEventSource = RegisterEventSource(NULL, config->serviceName);

    if (NULL != hEventSource) {
        lpszStrings[0] = message;

        ReportEvent(hEventSource,        // event log handle
                    EVENTLOG_ERROR_TYPE, // event type
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

    logEvent(config, "About to call dispatcher");

    if (!StartServiceCtrlDispatcher(DispatchTable)) {
        logEvent(config, "StartServiceCtrlDispatcher");
    }
}

VOID WINAPI qdbService() {

    // Register the handler function for the service

    gSvcStatusHandle = RegisterServiceCtrlHandler(gConfig->serviceName, SvcCtrlHandler);

    if (!gSvcStatusHandle) {
        logEvent(gConfig, "RegisterServiceCtrlHandler failed");
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
        logEvent(gConfig, "Could not create stop event");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    if (!makeDir(gConfig->dir)) {
        logEvent(gConfig, "Could not create root directory. Make sure it exists before starting service");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    FILE *stream = redirectStdout(gConfig);
    if (stream == NULL) {
        logEvent(gConfig, "Could not setup STDOUT redirect");
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
    }

    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    ZeroMemory(&pi, sizeof(pi));

    // Start the child process.
    if (!CreateProcess(gConfig->javaExec, gConfig->javaArgs, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)) {
        logEvent(gConfig, "Could not start java");
        fclose(stream);
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    char buf[2048];
    sprintf(buf, "Started %s %s", gConfig->javaExec, gConfig->javaArgs);
    logEvent(gConfig, buf);


    // Report running status when initialization is complete.

    ReportSvcStatus(SERVICE_RUNNING, NO_ERROR, 0);

    WaitForSingleObject(ghSvcStopEvent, INFINITE);

    if (!TerminateProcess(pi.hProcess, 0)) {
        logEvent(gConfig, "Failed to terminate java process");
    }

    // Close process and thread handles.
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);

    ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
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


