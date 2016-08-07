//
// Created by Vlad Ilyushchenko on 06/08/2016.
//

#include <ntdef.h>

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

typedef struct {
    int command;
    BOOL forceCopy;
    LPSTR dir;
    BOOL valid;
    LPSTR javaExec;
    LPSTR javaArgs;
    LPSTR exeName;
    LPSTR tag;
} CONFIG;

int qdbRun(int mode, int argc, char **argv);

BOOL svcInstall(CONFIG *config);

int svcRemove(CONFIG *config);
