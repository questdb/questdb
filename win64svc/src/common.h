#ifndef WIN64SVC_COMMON_H
#define WIN64SVC_COMMON_H

#include <windows.h>
#include <stdio.h>

#define E_CREATE_SERVICE 101
#define E_ACCESS_DENIED  102
#define E_SERVICE_MANAGER 103
#define E_OPEN_SERVICE 104
#define E_DELETE_SERVICE 105
#define E_CONTROL_SERVICE 106
#define E_PATH_TOO_LONG 107

#define eprintf(format, ...) fprintf(stderr, format, ##__VA_ARGS__)
#define SVC_NAME_PREFIX "QuestDB"
#define SVC_DISPLAY_NAME "QuestDB Server"

// configuration error codes

#define ECONFIG_OK                0
#define ECONFIG_UNKNOWN_OPTION    1
#define ECONFIG_JAVA_HOME         2
#define ECONFIG_UNKNOWN_COMMAND   3
#define ECONFIG_TOO_MANY_COMMANDS 4
#define ECONFIG_PATH_ERROR        5


typedef struct {
    int command;
    BOOL forceCopy;
    BOOL localRuntime;
    LPSTR dir;
    int errorCode;
    LPSTR javaExec;
    LPSTR javaArgs;
    LPSTR exeName;
    LPSTR serviceName;
    LPSTR serviceDisplayName;
} CONFIG;

int qdbRun(int argc, char **argv);

void qdbDispatchService(CONFIG *config);

void initAndParseConfig(int argc, char **argv, CONFIG *config);

int makeDir(const char *dir);

FILE *createStdoutLog(CONFIG *config);

BOOL svcInstall(CONFIG *config);

int svcRemove(CONFIG *config);

int svcStatus(CONFIG *config);

int svcStart(CONFIG *config);

int svcStop(CONFIG *config);

void log_event(WORD logType, char* serviceName, const char *message);

#ifndef QUESTDB_MAIN_CLASS
#define QUESTDB_MAIN_CLASS @QUESTDB_MAIN_CLASS@
#endif

#ifndef QUESTDB_BANNER
#define QUESTDB_BANNER @QUESTDB_BANNER@
#endif

#endif //WIN64SVC_COMMON_H
