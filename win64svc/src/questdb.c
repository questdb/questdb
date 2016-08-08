#include <handleapi.h>
#include <synchapi.h>
#include <errhandlingapi.h>
#include <stdio.h>
#include <getopt.h>
#include <sys/stat.h>
#include <processthreadsapi.h>
#include <rpc.h>
#include "common.h"

#define CMD_START   1
#define CMD_STOP    2
#define CMD_INSTALL 3
#define CMD_REMOVE  4
#define CMD_STATUS  5
#define CMD_SERVICE 6

void freeConfig(CONFIG *config) {
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

void pathCopy(char *dest, const char *file) {
    char *next;
    char *slash = (char *) file;

    while ((next = strpbrk(slash + 1, "\\/"))) slash = next;
    strncpy(dest, file, slash - file);
}

int makeDir(const char *dir) {
    struct stat st = {0};
    if (stat(dir, &st) == -1) {
        if (mkdir(dir) == -1) {
            eprintf("Cannot create directory: %s\n", dir);
            return 0;
        }
    }
    return 1;
}

void buildJavaArgs(CONFIG *config) {
    // main class
    LPCSTR mainClass = "com.questdb.BootstrapMain";

    // put together static java opts
    LPCSTR javaOpts = "-da" \
    " -XX:+PrintGCApplicationStoppedTime" \
    " -XX:+PrintSafepointStatistics" \
    " -XX:PrintSafepointStatisticsCount=1" \
    " -XX:+UseParNewGC" \
    " -XX:+UseConcMarkSweepGC" \
    " -XX:+PrintGCDetails" \
    " -XX:+PrintGCTimeStamps" \
    " -XX:+PrintGCDateStamps" \
    " -XX:+UnlockDiagnosticVMOptions" \
    " -XX:GuaranteedSafepointInterval=90000000" \
    " -XX:-UseBiasedLocking" \
    " -XX:BiasedLockingStartupDelay=0";

    // put together classpath
    char classpath[strlen(config->exeName) + 64];
    pathCopy(classpath, config->exeName);
    strcat(classpath, "\\questdb.jar");


    // put together command line

    char *args = malloc((strlen(javaOpts) + strlen(classpath) + strlen(mainClass) + strlen(config->dir) + 256) *
                        sizeof(char));
    strcpy(args, javaOpts);
    strcat(args, " -cp \"");
    strcat(args, classpath);
    strcat(args, "\" ");
    strcat(args, mainClass);
    strcat(args, " \"");
    strcat(args, config->dir);
    strcat(args, "\"");

    if (config->forceCopy) {
        strcat(args, " -f");
    }

    config->javaArgs = args;
}

void initAndParseConfig(int argc, char **argv, CONFIG *config) {
    int c;
    config->command = -1;
    config->dir = "qdbroot";
    config->forceCopy = FALSE;
    config->exeName = argv[0];
    config->javaArgs = NULL;
    config->errorCode = ECONFIG_OK;

    char *tag = NULL;
    char *javaHome = NULL;

    BOOL parsing = TRUE;
    while (parsing) {

        c = getopt(argc, argv, "d:fj:t:");


        switch (c) {
            case -1:
                if (optind < argc) {
                    if (config->command != -1) {
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
                javaHome = optarg;
                break;

            case 't':
                tag = optarg;
                break;

            default:
                config->errorCode = ECONFIG_UNKNOWN_OPTION;
                break;
        }
    }

    if (javaHome == NULL) {
        // check our environment
        javaHome = getenv("JAVA_HOME");
        if (javaHome == NULL) {
            eprintf("JAVA_HOME is not defined");
            config->errorCode = ECONFIG_JAVA_HOME;
        }
    }

    if (javaHome != NULL) {
        char *javaExec = malloc((strlen(javaHome) + 64) * sizeof(char));
        strcpy(javaExec, javaHome);
        strcat(javaExec, "\\bin\\java.exe");
        config->javaExec = javaExec;
    }

    if (config->command == -1) {
        config->command = CMD_START;
    }

    LPCSTR serviceNamePrefix = SVC_NAME_PREFIX;
    size_t tagSize = tag == NULL ? 0 : strlen(tag);

    char *lpServiceName = malloc(strlen(serviceNamePrefix) + tagSize + 1);
    strcpy(lpServiceName, serviceNamePrefix);
    if (tag != NULL) {
        strcat(lpServiceName, ":");
        strcat(lpServiceName, tag);
    }

    config->serviceName = lpServiceName;

    buildJavaArgs(config);
}


FILE *redirectStdout(CONFIG *config) {
    // create log dir
    char log[strlen(config->dir) + 64];
    strcpy(log, config->dir);
    strcat(log, "\\log");

    if (!makeDir(log)) {
        return NULL;
    }
    strcat(log, "\\stdout.txt");

    FILE *stream;
    if ((stream = freopen(log, "w", stdout)) == NULL) {
        eprintf("Cannot open file for write: %s (%i)\n", log, errno);
    }

    return stream;
}

int qdbStart(CONFIG *config) {
    if (!makeDir(config->dir)) {
        return 55;
    }

    FILE *stream = redirectStdout(config);
    if (stream == NULL) {
        return 55;
    }

    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    ZeroMemory(&pi, sizeof(pi));

    // Start the child process.
    if (!CreateProcess(config->javaExec, config->javaArgs, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)) {
        eprintf("CreateProcess failed (%lu).\n", GetLastError());
        return 1;
    }

    // Wait until child process exits.
    WaitForSingleObject(pi.hProcess, INFINITE);

    // Close process and thread handles.
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    fclose(stream);
    return 0;
}

int qdbRun(int mode, int argc, char **argv) {

    eprintf("\n");
    eprintf("  ___                  _   ____  ____\n");
    eprintf(" / _ \\ _   _  ___  ___| |_|  _ \\| __ )\n");
    eprintf("| | | | | | |/ _ \\/ __| __| | | |  _ \\\n");
    eprintf("| |_| | |_| |  __/\\__ \\ |_| |_| | |_) |\n");
    eprintf(" \\__\\_\\\\__,_|\\___||___/\\__|____/|____/\n");
    eprintf("                       www.questdb.org\n\n");

    CONFIG config;
    initAndParseConfig(argc, argv, &config);

    int rtn = 55;

    if (config.errorCode == ECONFIG_OK) {
        if (mode == MODE_SERVICE || config.command == CMD_START) {
            rtn = qdbStart(&config);
        } else {
            switch (config.command) {
                case CMD_STOP:
                    eprintf("do stop");
                    break;

                case CMD_STATUS:
                    eprintf("do status");
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
                    rtn = qdbStart(&config);
            }
        }
    } else {
        switch (mode) {
            case MODE_SERVICE:
                eprintf("Usage: %s  [-d dir] [-f] [-j JAVA_HOME]", argv[0]);
                break;

            default:
                eprintf("Usage: %s [start|stop|status|install|remove] [-d dir] [-f]", argv[0]);
                break;
        }
    }

    freeConfig(&config);
    return rtn;
}