#include <sys/event.h>
#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct file_watcher
{
    char *filePathPtr;
    int fd;
    int kq;
    bool changed;
    time_t lastChangedSec;
    long lastChangedNsec;
};

void *
runLoop(void *vargs)
{
    struct kevent event;
    int ret;
    int *kq = (int *)vargs;

    for (;;)
    {
        /* Sleep until something happens. */
        ret = kevent(*kq, NULL, 0, &event, 1, NULL);
        printf("Something was written to the watched dir");
        char *p = (char *)event.udata;
        printf("udata %s\n", p);
    }
}

static uintptr_t
setup(const char *filepath)
{
    /* Allocate memory for fw and user data */
    struct file_watcher *fw = (file_watcher *)malloc(sizeof(file_watcher));
    fw->filePathPtr = (char *)malloc(sizeof(char) * strlen(filepath) + 1);

    strcpy(fw->filePathPtr, filepath);

    struct kevent event;
    fw->fd = open(filepath, O_RDONLY);
    // todo: add error handling in case fd does not exist

    fw->kq = kqueue();
    EV_SET(&event, fw->fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE,
           0, fw->filePathPtr);
    kevent(fw->kq, &event, 1, NULL, 0, NULL);

    return (uintptr_t)fw;
}

static int changed(uintptr_t fwPtr)
{
    struct file_watcher *fw = (struct file_watcher *)fwPtr;
    if (fw->changed)
    {
        fw->changed = false;
        return 1;
    }
    return 0;
}

static void teardown(uintptr_t fwPtr)
{
    struct file_watcher *fw = (struct file_watcher *)fwPtr;

    /* kqueues are destroyed upon close() */
    (void)close(fw->kq);
    (void)close(fw->fd);

    free(fw->filePathPtr);
    free(fw);
}

int main(int argc, char **argv)
{
    struct kevent event;  /* Event we want to monitor */
    struct kevent tevent; /* Event triggered */
    int kq, fd, ret;

    if (argc != 2)
        err(EXIT_FAILURE, "Usage: %s path\n", argv[0]);
    fd = open(argv[1], O_RDONLY);
    if (fd == -1)
        err(EXIT_FAILURE, "Failed to open '%s'", argv[1]);

    char *filePath = (char *)malloc(sizeof(char) * strlen(argv[1]) + 1);
    strcpy(filePath, argv[1]);

    /* Create kqueue. */
    kq = kqueue();
    if (kq == -1)
        err(EXIT_FAILURE, "kqueue() failed");

    /* Initialize kevent structure. */
    EV_SET(&event, fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE,
           0, filePath);
    /* Attach event to the kqueue. */
    ret = kevent(kq, &event, 1, NULL, 0, NULL);
    if (ret == -1)
        err(EXIT_FAILURE, "kevent register");

    for (;;)
    {
        /* Sleep until something happens. */
        ret = kevent(kq, NULL, 0, &tevent, 1, NULL);
        if (ret == -1)
        {
            err(EXIT_FAILURE, "kevent wait");
        }
        else if (ret > 0)
        {
            if (tevent.flags & EV_ERROR)
                errx(EXIT_FAILURE, "Event error: %s", strerror(event.data));
            else
            {
                printf("Something was written in '%s'\n", argv[1]);
                printf("ident %ld\n", tevent.ident);
                printf("filter %d\n", tevent.filter);
                printf("data %d\n", tevent.data);

                char *p = (char *)tevent.udata;
                printf("udata %s\n", p);
            }
        }
    }

    /* kqueues are destroyed upon close() */
    (void)close(kq);
    (void)close(fd);
}