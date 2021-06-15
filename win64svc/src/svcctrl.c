#include <stddef.h>
#include <stdio.h>
#include <rpc.h>
#include "common.h"

int svcInstall(CONFIG *config) {
    SC_HANDLE hSCM;
    SC_HANDLE hService;

    // Get a handle to the SCM database.

    hSCM = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == hSCM) {
        DWORD err = GetLastError();
        if (err != ERROR_ACCESS_DENIED) {
            eprintf("OpenSCManager failed (%lu)\n", err);
            return E_SERVICE_MANAGER;
        }
        return E_ACCESS_DENIED;
    }

    // put together service name
    char szPath[MAX_PATH];
    strcpy(szPath, "\"");
    strcat(szPath, config->exeName);
    strcat(szPath, "\"");
    strcat(szPath, " service -d ");
    strcat(szPath, config->dir);
    if (config->forceCopy) {
        strcat(szPath, " -f");
    }
    strcat(szPath, " -j ");
    strcat(szPath, "\"");
    strcat(szPath, config->javaExec);
    strcat(szPath, "\"");

    // Create the service

    hService = CreateService(
            hSCM,                  // SCM database
            config->serviceName,           // name of service
            config->serviceDisplayName,    // service name to display
            SERVICE_ALL_ACCESS,            // desired access
            SERVICE_WIN32_OWN_PROCESS,     // service type
            SERVICE_AUTO_START,            // start type
            SERVICE_ERROR_NORMAL,          // error control type
            szPath,                        // path to service's binary
            NULL,                          // no load ordering group
            NULL,                          // no tag identifier
            NULL,                          // no dependencies
            NULL,                          // LocalSystem account
            NULL);                         // no password

    if (hService == NULL) {
        DWORD err = GetLastError();
        if (err == ERROR_SERVICE_EXISTS) {
            eprintf("Service already exists: %s\n", config->serviceName);
        } else {
            eprintf("Failed to create service %s (%lu)\n", config->serviceName, GetLastError());
        }
        CloseServiceHandle(hSCM);
        return E_CREATE_SERVICE;
    }

    SERVICE_DESCRIPTION description;
    description.lpDescription = "High performance time series database (www.questdb.io)";
    ChangeServiceConfig2(hService, SERVICE_CONFIG_DESCRIPTION, &description);

    eprintf("Service installed: %s\n", config->serviceName);
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);

    return 0;
}

int svcRemove(CONFIG *config) {

    SC_HANDLE hSCM;
    SC_HANDLE hService;

    // Get a handle to the SCM database.

    hSCM = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == hSCM) {
        DWORD err = GetLastError();
        if (err != ERROR_ACCESS_DENIED) {
            eprintf("OpenSCManager failed (%lu)\n", err);
            return E_SERVICE_MANAGER;
        }
        return E_ACCESS_DENIED;
    }

    // Get a handle to the service.

    hService = OpenService(
            hSCM,             // SCM database
            config->serviceName,      // name of service
            DELETE);                  // need delete access

    if (hService == NULL) {
        DWORD err = GetLastError();
        if (err == ERROR_SERVICE_DOES_NOT_EXIST) {
            eprintf("Service does not exist: %s", config->serviceName);
        } else {
            eprintf("Failed to open service %s (%lu)\n", config->serviceName, err);
        }
        CloseServiceHandle(hSCM);
        return E_OPEN_SERVICE;
    }

    // Delete the service.

    int rtn = 0;
    if (DeleteService(hService)) {
        eprintf("Service removed: %s\n", config->serviceName);
    } else {
        eprintf("Failed to remove service %s (%lu)\n", config->serviceName, GetLastError());
        rtn = E_DELETE_SERVICE;
    }

    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);

    return rtn;
}


int svcStatus(CONFIG *config) {

    SC_HANDLE hSCM;
    SC_HANDLE hService;

    // Get a handle to the SCM database.

    hSCM = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == hSCM) {
        DWORD err = GetLastError();
        if (err != ERROR_ACCESS_DENIED) {
            eprintf("OpenSCManager failed (%lu)\n", err);
            return E_SERVICE_MANAGER;
        }
        return E_ACCESS_DENIED;
    }

    // Get a handle to the service.

    hService = OpenService(hSCM, config->serviceName, SERVICE_INTERROGATE);

    if (hService == NULL) {
        DWORD err = GetLastError();
        if (err == ERROR_SERVICE_DOES_NOT_EXIST) {
            eprintf("Service does not exist: %s", config->serviceName);
        } else {
            eprintf("Failed to open service %s (%lu)\n", config->serviceName, err);
        }
        CloseServiceHandle(hSCM);
        return E_OPEN_SERVICE;
    }

    SERVICE_STATUS service_status;
    if (!ControlService(hService, SERVICE_CONTROL_INTERROGATE, &service_status)) {

        DWORD err = GetLastError();

        if (err == ERROR_SERVICE_NOT_ACTIVE) {
            eprintf("Service %s is INACTIVE", config->serviceName);
        } else {
            eprintf("Failed call to ControlService (%lu)", GetLastError());
        }
        CloseServiceHandle(hService);
        CloseServiceHandle(hSCM);
        return E_CONTROL_SERVICE;
    }

    const char *text;
    switch (service_status.dwCurrentState) {
        case SERVICE_CONTINUE_PENDING:
            text = "CONTINUE PENDING";
            break;

        case SERVICE_PAUSE_PENDING:
            text = "PAUSE PENDING";
            break;

        case SERVICE_PAUSED:
            text = "PAUSED";
            break;

        case SERVICE_RUNNING:
            text = "RUNNING";
            break;

        case SERVICE_START_PENDING:
            text = "START PENDING";
            break;

        case SERVICE_STOP_PENDING:
            text = "STOP PENDING";
            break;

        case SERVICE_STOPPED:
            text = "STOPPED";
            break;

        default:
            text = "UNKNOWN";
    }

    eprintf("Service %s is %s", config->serviceName, text);

    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);

    return 0;
}

int svcStop(CONFIG *config) {

    SC_HANDLE hSCM;
    SC_HANDLE hService;

    // Get a handle to the SCM database.

    hSCM = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == hSCM) {
        DWORD err = GetLastError();
        if (err != ERROR_ACCESS_DENIED) {
            eprintf("OpenSCManager failed (%lu)\n", err);
            return E_SERVICE_MANAGER;
        }
        return E_ACCESS_DENIED;
    }

    // Get a handle to the service.

    hService = OpenService(hSCM, config->serviceName, SERVICE_STOP | SERVICE_QUERY_STATUS);

    if (hService == NULL) {
        DWORD err = GetLastError();

        if (err == ERROR_SERVICE_DOES_NOT_EXIST) {
            eprintf("Service does not exist: %s", config->serviceName);
        } else {
            eprintf("Failed to open service %s (%lu)\n", config->serviceName, err);
        }

        CloseServiceHandle(hSCM);
        return E_OPEN_SERVICE;
    }


    SERVICE_STATUS_PROCESS ssp;
    if (!ControlService(hService, SERVICE_CONTROL_STOP, (LPSERVICE_STATUS) &ssp)) {
        DWORD err = GetLastError();
        if (err == ERROR_SERVICE_NOT_ACTIVE) {
            eprintf("Service is already INACTIVE: %s", config->serviceName);
        } else {
            eprintf("Failed to stop service %s (%lu)\n", config->serviceName, err);
        }
    } else {
        DWORD dwStartTime = GetTickCount();
        DWORD dwTimeout = 30000;
        DWORD dwBytesNeeded;
        while (ssp.dwCurrentState != SERVICE_STOPPED) {
            Sleep(ssp.dwWaitHint);
            if (!QueryServiceStatusEx(hService, SC_STATUS_PROCESS_INFO, (LPBYTE) &ssp, sizeof(SERVICE_STATUS_PROCESS),
                                      &dwBytesNeeded)) {
                eprintf("QueryServiceStatusEx failed (%lu)\n", GetLastError());
                goto stop_cleanup;
            }

            if (ssp.dwCurrentState == SERVICE_STOPPED)
                break;

            if (GetTickCount() - dwStartTime > dwTimeout) {
                eprintf("Wait timed out\n");
                goto stop_cleanup;
            }
        }
        eprintf("Service stopped: %s\n", config->serviceName);
    }

    stop_cleanup:

    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);
    return 0;
}

int svcStart(CONFIG *config) {

    SC_HANDLE hSCM;
    SC_HANDLE hService;

    // Get a handle to the SCM database.

    hSCM = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == hSCM) {
        DWORD err = GetLastError();
        if (err != ERROR_ACCESS_DENIED) {
            eprintf("OpenSCManager failed (%lu)\n", err);
            return E_SERVICE_MANAGER;
        }
        return E_ACCESS_DENIED;
    }

    // Get a handle to the service.

    hService = OpenService(hSCM, config->serviceName, SERVICE_START);

    if (hService == NULL) {
        DWORD err = GetLastError();

        if (err == ERROR_SERVICE_DOES_NOT_EXIST) {
            eprintf("Service does not exist: %s", config->serviceName);
        } else {
            eprintf("Failed to open service %s (%lu)\n", config->serviceName, err);
        }

        CloseServiceHandle(hSCM);
        return E_OPEN_SERVICE;
    }

    if (!StartService(hService, 0, NULL)) {
        DWORD err = GetLastError();
        if (err == ERROR_SERVICE_ALREADY_RUNNING) {
            eprintf("Service is already running: %s", config->serviceName);
        } else {
            eprintf("Failed to start service %s (%lu)\n", config->serviceName, err);
        }
    } else {
        eprintf("Service started: %s\n", config->serviceName);
    }

    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);

    return 0;
}
