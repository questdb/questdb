#include <stddef.h>
#include <stdio.h>
#include <rpc.h>
#include "common.h"

int svcInstall(CONFIG *config) {
    SC_HANDLE schSCManager;
    SC_HANDLE schService;

    // Get a handle to the SCM database.

    schSCManager = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == schSCManager) {
        long err = GetLastError();
        eprintf("OpenSCManager failed (%lu)\n", err);
        return err == ERROR_ACCESS_DENIED ? E_ACCESS_DENIED : E_SERVICE_MANAGER;
    }

    // put together service name

    LPCSTR serviceNamePrefix = SVC_NAME_PREFIX;
    size_t tagSize = config->tag == NULL ? 0 : strlen(config->tag);

    char lpServiceName[strlen(serviceNamePrefix) + tagSize + 1];
    strcpy(lpServiceName, serviceNamePrefix);
    if (config->tag != NULL) {
        strcat(lpServiceName, ":");
        strcat(lpServiceName, config->tag);
    }

    size_t size = strlen(config->exeName);
    if (config->forceCopy) {
        size += 3;
    }
    size += strlen(config->dir) + 1;
    char szPath[size];
    strcpy(szPath, config->exeName);
    strcat(szPath, " ");
    strcat(szPath, config->dir);
    if (config->forceCopy) {
        strcat(szPath, " -f");
    }

    // Create the service

    schService = CreateService(
            schSCManager,              // SCM database
            lpServiceName,             // name of service
            SVC_DISPLAY_NAME,          // service name to display
            SERVICE_ALL_ACCESS,        // desired access
            SERVICE_WIN32_OWN_PROCESS, // service type
            SERVICE_AUTO_START,        // start type
            SERVICE_ERROR_NORMAL,      // error control type
            szPath,                    // path to service's binary
            NULL,                      // no load ordering group
            NULL,                      // no tag identifier
            NULL,                      // no dependencies
            NULL,                      // LocalSystem account
            NULL);                     // no password

    if (schService == NULL) {
        eprintf("Failed to create service %s (%lu)\n", lpServiceName, GetLastError());
        CloseServiceHandle(schSCManager);
        return E_CREATE_SERVICE;
    }
    else
        eprintf("Service installed: %s\n", lpServiceName);

    CloseServiceHandle(schService);
    CloseServiceHandle(schSCManager);

    return 0;
}

int svcRemove(CONFIG *config) {

    SC_HANDLE schSCManager;
    SC_HANDLE schService;

    // put together service name

    LPCSTR serviceNamePrefix = "QuestDB";
    size_t tagSize = config->tag == NULL ? 0 : strlen(config->tag);

    char lpServiceName[strlen(serviceNamePrefix) + tagSize + 1];
    strcpy(lpServiceName, serviceNamePrefix);
    if (config->tag != NULL) {
        strcat(lpServiceName, ":");
        strcat(lpServiceName, config->tag);
    }


    // Get a handle to the SCM database.

    schSCManager = OpenSCManager(
            NULL,                    // local computer
            NULL,                    // ServicesActive database
            SC_MANAGER_ALL_ACCESS);  // full access rights

    if (NULL == schSCManager) {
        long err = GetLastError();
        eprintf("OpenSCManager failed (%lu)\n", err);
        return err == ERROR_ACCESS_DENIED ? E_ACCESS_DENIED : E_SERVICE_MANAGER;
    }

    // Get a handle to the service.

    schService = OpenService(
            schSCManager,       // SCM database
            lpServiceName,      // name of service
            DELETE);            // need delete access

    if (schService == NULL) {
        eprintf("Failed to open service %s (%lu)\n", lpServiceName, GetLastError());
        CloseServiceHandle(schSCManager);
        return E_OPEN_SERVICE;
    }

    // Delete the service.

    int rtn = 0;
    if (DeleteService(schService)) {
        eprintf("Service removed: %s\n", lpServiceName);
    } else {
        eprintf("Failed to remove service %s (%lu)\n", lpServiceName, GetLastError());
        rtn = E_DELETE_SERVICE;
    }

    CloseServiceHandle(schService);
    CloseServiceHandle(schSCManager);

    return rtn;
}

