#include <stdio.h>
#include <handleapi.h>
#include <rpc.h>
#include <io.h>
#include <sys/stat.h>


void pathcpy(char *dest, const char *file) {
    char *next;
    char *slash = (char *) file;

    while ((next = strpbrk(slash + 1, "\\/"))) slash = next;
    strncpy(dest, file, slash - file);
}

int main(int argc, char **argv) {

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
    pathcpy(classpath, argv[0]);
    strcat(classpath, "\\questdb.jar");

    char* qdbroot = "qdbroot";
    // check if directory exists and create if necessary
    struct stat st = {0};

    if (stat(qdbroot, &st) == -1) {
        mkdir(qdbroot);
    }

    if (stat(qdbroot, &st) == -1) {
        fprintf(stderr, "Cannot create directory: %s\n", qdbroot);
        return 55;
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
    return 0;
}