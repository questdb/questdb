#include <stdio.h>
#include <handleapi.h>
#include <rpc.h>
#include <io.h>
#include <sys/stat.h>
#include <getopt.h>

#define CMD_START   1;
#define CMD_STOP    2;
#define CMD_INSTALL 3;
#define CMD_REMOVE  4;
#define CMD_STATUS  5;
#define CMD_UNKNOWN (-1);

typedef struct {
    int command;
    boolean forceCopy;
    char* dir;
    boolean valid;
} CONFIG;

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
            fprintf(stderr, "Cannot create directory: %s\n", dir);
            return 0;
        }
    }
    return 1;
}

void parseArgs(int argc, char **argv, CONFIG* config) {
    int c;
    config->command = CMD_UNKNOWN;
    config->dir = NULL;
    config->valid = TRUE;
    config->forceCopy = FALSE;

    while (1) {

        c = getopt(argc, argv, "d:f");

        switch (c) {
            case -1:
                if (optind < argc) {
                    if (config->command != -1) {
                        fprintf(stderr, "Unexpected command: %s\n", argv[optind]);
                        config->valid = FALSE;
                        return;
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
                    } else {
                        fprintf(stderr, "Unknown command: %s\n", cmd);
                        config->valid = FALSE;
                        return;
                    }
                    optind++;
                } else {
                    return;
                }
                break;

            case 'f':
                config->forceCopy = TRUE;
                break;

            case 'd':
                config->dir = optarg;
                break;

            default:
                config->valid = FALSE;
                break;
        }
    }
}

int main(int argc, char **argv) {

    CONFIG config;
    parseArgs(argc, argv, &config);

    if (!config.valid) {
        fprintf(stderr, "Usage: %s [start|stop|status|install|remove] [-d dir] [-f]", argv[0]);
        return 55;
    }

    // check our environment
    const char *javaHome = getenv("JAVA_HOME");
    if (javaHome == NULL) {
        fprintf(stderr, "JAVA_HOME is not defined");
        return 55;
    }

    // put together path to java.exe
    char javaExec[strlen(javaHome) + 64];
    strcpy(javaExec, javaHome);
    strcat(javaExec, "\\bin\\java.exe");

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
    char classpath[strlen(argv[0]) + 64];
    pathCopy(classpath, argv[0]);
    strcat(classpath, "\\questdb.jar");

    // check if directory exists and create if necessary
    char *qdbroot = config.dir == NULL ? "qdbroot" : config.dir;
    if (!makeDir(qdbroot)) {
        return 55;
    }

    // create log dir
    char log[strlen(qdbroot) + 64];
    strcpy(log, qdbroot);
    strcat(log, "\\log");

    if (!makeDir(log)) {
        return 55;
    }
    strcat(log, "\\stdout.txt");

    FILE *stream;
    if ((stream = freopen(log, "w", stdout)) == NULL) {
        fprintf(stderr, "Cannot open file for write: %s (%i)\n", log, errno);
    }

    // put together command line
    char args[strlen(javaOpts) + strlen(classpath) + strlen(mainClass) + strlen(qdbroot) + 256];
    strcpy(args, javaOpts);
    strcat(args, " -cp \"");
    strcat(args, classpath);
    strcat(args, "\" ");
    strcat(args, mainClass);
    strcat(args, " \"");
    strcat(args, qdbroot);
    strcat(args, "\"");

    if (config.forceCopy) {
        strcat(args, " -f");
    }

    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    ZeroMemory(&pi, sizeof(pi));

    // Start the child process.
    if (!CreateProcess(javaExec, args, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)) {
        fprintf(stderr, "CreateProcess failed (%lu).\n", GetLastError());
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