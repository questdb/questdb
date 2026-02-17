#include <windows.h>
#include <stdio.h>
#include <sys/stat.h>
#include <direct.h>
#include <string.h>
#include <time.h>
#include "common.h"

#ifdef _MSC_VER
#include "getopt.h"
#else

#include <handleapi.h>
#include <synchapi.h>
#include <processthreadsapi.h>
#include <errhandlingapi.h>
#include <getopt.h>

#endif // _MSC_VER

#define CMD_START   1
#define CMD_STOP    2
#define CMD_INSTALL 3
#define CMD_REMOVE  4
#define CMD_STATUS  5
#define CMD_SERVICE 6
#define CMD_CONSOLE (-1)

void buildJavaExec(CONFIG *config, const char *javaExecOpt);

void freeConfig(CONFIG *config) {
    if (config->exeName != NULL) {
        free(config->exeName);
    }

    if (config->javaExec != NULL) {
        free(config->javaExec);
    }

    if (config->javaArgs != NULL) {
        free(config->javaArgs);
    }

    if (config->serviceName != NULL) {
        free(config->serviceName);
    }
}

int pathCopy(char *dest, size_t destSize, const char *file) {
    if (dest == NULL || file == NULL || destSize == 0) {
        return -1;
    }

    char *next;
    char *slash = (char *) file;

    while ((next = strpbrk(slash + 1, "\\/"))) slash = next;
    size_t len = slash - file;

    if (len >= destSize) {
        eprintf("Path too long for buffer (need %zu, have %zu)\n", len + 1, destSize);
        return -1;
    }

    strncpy(dest, file, len);
    dest[len] = '\0';
    return 0;
}

int makeDir(const char *dir) {
    struct stat st = {0};
    if (stat(dir, &st) == -1) {
        if (_mkdir(dir) == -1) {
            eprintf("Cannot create directory: %s\n", dir);
            return 0;
        }
    }
    return 1;
}

int fileExists(char *file) {
    WIN32_FIND_DATA FindFileData;
    HANDLE handle = FindFirstFile(file, &FindFileData);
    int found = handle != INVALID_HANDLE_VALUE;
    if (found) {
        FindClose(handle);
    }
    return found;
}

void buildJavaArgs(CONFIG *config) {
    // main class
    LPCSTR mainClass = QUESTDB_MAIN_CLASS;

    // put together static java opts
    LPCSTR javaOpts = "-XX:+UnlockExperimentalVMOptions"
                      " -XX:+AlwaysPreTouch"
                      " -XX:+UseParallelGC";

    // put together classpath

    char classpath[MAX_PATH + 64];
    memset(classpath, 0, sizeof(classpath));

    if (!config->localRuntime) {
        if (pathCopy(classpath, sizeof(classpath), config->exeName) != 0) {
            eprintf("Failed to extract classpath from executable path\n");
            config->errorCode = ECONFIG_PATH_ERROR;
            return;
        }
        strcat(classpath, "\\questdb.jar");
    }

    // put together command line, dir is x2 because we're including the path to `hs_err_pid`
    // 512 is extra for the constant strings
    char *args = malloc(
            (strlen(javaOpts) + strlen(classpath) + strlen(mainClass) + strlen(config->dir) * 2 + 512) * sizeof(char));
    strcpy(args, javaOpts);
    // quote the directory in case it contains spaces
    strcat(args, " -XX:ErrorFile=\"");
    strcat(args, config->dir);
    strcat(args, "\\db\\");
    strcat(args, "hs_err_pid+%p.log\""); // crash file name
    if (!config->localRuntime) {
        strcat(args, " -p \"");
        strcat(args, classpath);
        strcat(args, "\"");
    }
    strcat(args, " -Dcontainerized=false");
    strcat(args, " -m ");
    strcat(args, mainClass);
    strcat(args, " -d \"");
    strcat(args, config->dir);
    strcat(args, "\"");

    if (config->forceCopy) {
        strcat(args, " -f");
    }

    config->javaArgs = args;
}

void initAndParseConfig(int argc, char **argv, CONFIG *config) {
    int c;
    config->command = CMD_CONSOLE;
    config->dir = "qdbroot";
    config->forceCopy = FALSE;
    config->javaArgs = NULL;
    config->errorCode = ECONFIG_OK;
    DWORD n = GetFullPathName(argv[0], 0, NULL, NULL);
    char *exe = malloc(n * sizeof(TCHAR));
    GetFullPathName(argv[0], n, exe, NULL);
    config->exeName = exe;

    char *tag = NULL;
    char *javaExecOpt = NULL;

    BOOL parsing = TRUE;
    while (parsing) {

        c = getopt(argc, argv, "d:fj:t:");

        switch (c) {
            case -1:
                if (optind < argc) {
                    if (config->command != CMD_CONSOLE) {
                        fprintf(stderr, "Unexpected command: %s\n", argv[optind]);
                        config->errorCode = ECONFIG_TOO_MANY_COMMANDS;
                        parsing = FALSE;
                        break;
                    }

                    char *cmd = argv[optind];
                    if (strcmp("start", cmd) == 0) {
                        config->command = CMD_START;
                    } else if (strcmp("stop", cmd) == 0) {
                        config->command = CMD_STOP;
                    } else if (strcmp("install", cmd) == 0) {
                        config->command = CMD_INSTALL;
                    } else if (strcmp("status", cmd) == 0) {
                        config->command = CMD_STATUS;
                    } else if (strcmp("remove", cmd) == 0) {
                        config->command = CMD_REMOVE;
                    } else if (strcmp("service", cmd) == 0) {
                        config->command = CMD_SERVICE;
                    } else {
                        eprintf("Unknown command: %s\n", cmd);
                        config->errorCode = ECONFIG_UNKNOWN_COMMAND;
                        parsing = FALSE;
                        break;
                    }
                    optind++;
                } else {
                    parsing = FALSE;
                }
                break;

            case 'f':
                config->forceCopy = TRUE;
                break;

            case 'd':
                config->dir = optarg;
                break;

            case 'j':
                javaExecOpt = optarg;
                break;

            case 't':
                tag = optarg;
                break;

            default:
                config->errorCode = ECONFIG_UNKNOWN_OPTION;
                break;
        }
    }

    buildJavaExec(config, javaExecOpt);

    size_t tagSize = tag == NULL ? 0 : strlen(tag);

    // Service name

    LPCSTR serviceNamePrefix = SVC_NAME_PREFIX;

    char *lpServiceName = malloc(strlen(serviceNamePrefix) + tagSize + 1);
    strcpy(lpServiceName, serviceNamePrefix);
    if (tag != NULL) {
        strcat(lpServiceName, ":");
        strcat(lpServiceName, tag);
    }

    config->serviceName = lpServiceName;

    char buf[2048];
    snprintf(buf, sizeof(buf), "JAVA_HOME %s ", config->javaExec);
    log_event(EVENTLOG_INFORMATION_TYPE, config->serviceName, buf);

    // Service display name

    LPCSTR serviceDisplayNamePrefix = SVC_DISPLAY_NAME;

    char *lpServiceDisplayName = malloc(strlen(serviceDisplayNamePrefix) + tagSize + 6);
    strcpy(lpServiceDisplayName, serviceDisplayNamePrefix);

    if (tag != NULL) {
        strcat(lpServiceDisplayName, " [");
        strcat(lpServiceDisplayName, tag);
        strcat(lpServiceDisplayName, "]");
    }

    config->serviceDisplayName = lpServiceDisplayName;

    buildJavaArgs(config);
}

void buildJavaExec(CONFIG *config, const char *javaExecOpt) {
    config->javaExec = malloc(MAX_PATH);
    memset(config->javaExec, 0, MAX_PATH);

    if (javaExecOpt) {
        strncpy(config->javaExec, javaExecOpt, MAX_PATH - 1);
        config->javaExec[MAX_PATH - 1] = '\0';
        config->localRuntime = FALSE;
        return;
    } else {
        // check if we are being executed from runtime location
        if (pathCopy(config->javaExec, MAX_PATH, config->exeName) == 0) {
            size_t len = strlen(config->javaExec);
            if (len + strlen("\\java.exe") < MAX_PATH) {
                strcat(config->javaExec, "\\java.exe");
                if (fileExists(config->javaExec)) {
                    config->localRuntime = TRUE;
                    return;
                }
            }
        }

        // fallback to JAVA_HOME
        char *javaHome = getenv("JAVA_HOME");
        if (javaHome) {
            strncpy(config->javaExec, javaHome, MAX_PATH - 1);
            config->javaExec[MAX_PATH - 1] = '\0';
            size_t len = strlen(config->javaExec);
            if (len + strlen("\\bin\\java.exe") < MAX_PATH) {
                strcat(config->javaExec, "\\bin\\java.exe");
                if (fileExists(config->javaExec)) {
                    config->localRuntime = FALSE;
                    return;
                }
            }
        }
    }
    free(config->javaExec);
    config->javaExec = NULL;
    eprintf("\r\nJAVA_HOME is not defined\r\n");
    config->errorCode = ECONFIG_JAVA_HOME;
}


FILE *createStdoutLog(CONFIG *config) {
    // create log dir
    char log[MAX_PATH];
    int len = snprintf(log, MAX_PATH, "%s\\log", config->dir);

    if (len < 0 || len >= MAX_PATH) {
        eprintf("Log directory path too long\n");
        return NULL;
    }

    if (!makeDir(log)) {
        return NULL;
    }

    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    len = snprintf(log, MAX_PATH, "%s\\log\\stdout-stderr-", config->dir);
    if (len > 0 && len < MAX_PATH) {
        strftime(log + len, MAX_PATH - len, "%Y-%m-%dT%H-%M-%S.txt", t);
    } else {
        eprintf("Log file path too long\n");
        return NULL;
    }

    SECURITY_ATTRIBUTES sa;
    ZeroMemory(&sa, sizeof(sa));
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;

    HANDLE stream = CreateFile(
            log,
            FILE_APPEND_DATA,
            FILE_SHARE_WRITE | FILE_SHARE_READ,
            &sa,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL);

    if (stream == NULL) {
        eprintf("Cannot create file: %s (%i)\n", log, errno);
    }

    return stream;
}

int qdbConsole(CONFIG *config) {

    if (!makeDir(config->dir)) {
        return 55;
    }

    FILE *stream = createStdoutLog(config);
    if (stream == NULL) {
        return 55;
    }

    STARTUPINFO si;
    ZeroMemory(&si, sizeof(si));
    PROCESS_INFORMATION pi;
    ZeroMemory(&pi, sizeof(pi));

    si.cb = sizeof(si);
    si.dwFlags |= STARTF_USESTDHANDLES;
    si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    si.hStdOutput = stream;
    si.hStdError = stream;

    printf("%s %s\n\n", config->javaExec, config->javaArgs);

    // Start the child process.
    if (!CreateProcess(
            config->javaExec,
            config->javaArgs,
            NULL,
            NULL,
            TRUE,
            0,
            NULL,
            NULL,
            &si,
            &pi
    )) {
        eprintf("CreateProcess failed [%s](%lu).\n", config->javaExec, GetLastError());
        return 1;
    }

    char hello_txt[MAX_PATH];
    snprintf(hello_txt, MAX_PATH, "%s\\hello.txt", config->dir);
    for (int i = 0; i < 80; i++) {
        DWORD exit_code = 0;
        if (GetExitCodeProcess(pi.hProcess, &exit_code) && exit_code != STILL_ACTIVE) {
            break;
        }
        FILE *hello_in;
        fopen_s(&hello_in, hello_txt, "r");
        if (hello_in != NULL) {
            char buf[BUFSIZ];
            while (TRUE) {
                size_t read_count = fread(&buf, sizeof(char), BUFSIZ, hello_in);
                fwrite(&buf, sizeof(char), read_count, stdout);
                if (read_count < BUFSIZ) {
                    break;
                }
            }
            fclose(hello_in);
            _unlink(hello_txt);
            break;
        }
        Sleep(250);
    }

    // Wait until child process exits.
    WaitForSingleObject(pi.hProcess, INFINITE);

    // Close process and thread handles.
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    fclose(stream);
    return 0;
}

void logConfigError(CONFIG *config) {
    const char *text;

    switch (config->errorCode) {
        case ECONFIG_JAVA_HOME:
            text = "JAVA_HOME is not defined";
            break;

        case ECONFIG_UNKNOWN_COMMAND:
            text = "Unknown command.";
            break;

        case ECONFIG_UNKNOWN_OPTION:
            text = "Unknown option";
            break;

        case ECONFIG_TOO_MANY_COMMANDS:
            text = "Too many commands. Only one command is allowed.";
            break;

        case ECONFIG_PATH_ERROR:
            text = "Failed to extract path from executable.";
            break;

        default:
            text = NULL;
    }

    if (text != NULL) {
        char buf[128];
        snprintf(buf, sizeof(buf), "Failed to start service: %s\n", text);
        log_event(EVENTLOG_ERROR_TYPE, SVC_NAME_PREFIX, buf);
    }
}

int qdbRun(int argc, char **argv) {

    CONFIG config;
    initAndParseConfig(argc, argv, &config);

    int rtn = 55;

    if (config.errorCode == ECONFIG_OK) {
        switch (config.command) {
            case CMD_START:
                rtn = svcStart(&config);
                break;

            case CMD_STOP:
                rtn = svcStop(&config);
                break;

            case CMD_STATUS:
                rtn = svcStatus(&config);
                break;

            case CMD_INSTALL:
                rtn = svcInstall(&config);
                break;

            case CMD_REMOVE:
                rtn = svcRemove(&config);
                break;

            case CMD_SERVICE:
                qdbDispatchService(&config);
                rtn = 0;
                break;

            default:
                rtn = qdbConsole(&config);
                break;
        }
    } else {
        logConfigError(&config);
        eprintf("Usage: %s [start|stop|status|install|remove] [-d dir] [-f] [-j JAVA_HOME] [-t tag]", argv[0]);
    }

    freeConfig(&config);

    if (rtn == E_ACCESS_DENIED) {
        eprintf("ACCESS DENIED\n\nPlease try again as Administrator.");
    }

    return rtn;
}
