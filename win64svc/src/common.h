//
// Created by Vlad Ilyushchenko on 06/08/2016.
//

#include <ntdef.h>
#include <stdio.h>
#include <windows.h>

#ifndef WIN64SVC_COMMON_H
#define WIN64SVC_COMMON_H

#endif //WIN64SVC_COMMON_H

#define E_CREATE_SERVICE 101
#define E_ACCESS_DENIED  102
#define E_SERVICE_MANAGER 103
#define E_OPEN_SERVICE 104
#define E_DELETE_SERVICE 105

#define eprintf(format, ...) fprintf(stderr, format, ##__VA_ARGS__)
#define SVC_NAME_PREFIX "QuestDB"
#define SVC_DISPLAY_NAME "QuestDB Server"

#define MODE_SERVICE 1
#define MODE_CMDLINE 2

// configuration error codes

#define ECONFIG_OK                0
#define ECONFIG_UNKNOWN_OPTION    1
#define ECONFIG_JAVA_HOME         2
#define ECONFIG_UNKNOWN_COMMAND   3
#define ECONFIG_TOO_MANY_COMMANDS 4


typedef struct {
    int command;
    BOOL forceCopy;
    LPSTR dir;
    int errorCode;
    LPSTR javaExec;
    LPSTR javaArgs;
    LPSTR exeName;
    LPSTR serviceName;
} CONFIG;

int qdbRun(int mode, int argc, char **argv);

void qdbDispatchService(CONFIG *config);

void initAndParseConfig(int argc, char **argv, CONFIG *config);

int makeDir(const char *dir);

FILE *redirectStdout(CONFIG *config);

BOOL svcInstall(CONFIG *config);

int svcRemove(CONFIG *config);

void logEvent(CONFIG *config, char *message);