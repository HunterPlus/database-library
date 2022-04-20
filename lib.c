#include "lib.h"

/* error ***********************************************************************************************
 * <stdarg.h> <errorno.h> <string.h>
 */
static void 
err_doit(int errnoflag, const char *fmt, va_list ap)
{
        int     errno_save, n;
        char    buf[MAXLINE];

        errno_save = errno;     /* value caller might want printed */
        vsnprintf(buf, sizeof(buf), fmt, ap);
        n = strlen(buf);
        if (errnoflag)
                snprintf(buf + n, sizeof(buf) - n, ": %s", strerror(errno_save));
        strcat(buf, "\n");

        fflush(stdout);
        fputs(buf, stderr);
        fflush(stderr);
        
        return ;
}
void 
err_ret(const char *fmt, ...)
{
        va_list ap;

        va_start(ap, fmt);
        err_doit(1, fmt, ap);
        va_end(ap);
        return ;
}
void 
err_sys(const char *fmt, ...)
{
        va_list ap;

        va_start(ap, fmt);
        err_doit(1, fmt, ap);
        va_end(ap);
        exit(1);
}
void 
err_dump(const char *fmt, ...)
{
        va_list ap;

        va_start(ap, fmt);
        err_doit(1, fmt, ap);
        va_end(ap);
        abort();        /* dump core and terminate */
        exit(1);        /* shouldn't get here */
}
void 
err_msg(const char *fmt, ...)
{
        va_list ap;

        va_start(ap, fmt);
        err_doit(0, fmt, ap);
        va_end(ap);
        return ;
}
void 
err_quit(const char *fmt, ...)
{
        va_list ap;

        va_start(ap, fmt);
        err_doit(0, fmt, ap);
        va_end(ap);
        exit(1);
}

/* wrap socket ******************************************************************************************** 
 * <sys/socket.h>
*/
int 
Socket(int family, int type, int protocol)
{
        int     sockfd;
        if ((sockfd = socket(family, type, protocol)) < 0)
                err_sys("socket error");
        return (sockfd);
}
void 
Bind(int fd, const struct sockaddr *sa, socklen_t salen)
{
        if (bind(fd, sa, salen) < 0)
                err_sys("bind error");
}
void 
Listen(int fd, int backlog)
{
        char    *ptr;

        if ((ptr = getenv("LISTENQ")) != NULL)
                backlog = atoi(ptr);
        if (listen(fd, backlog) < 0)
                err_sys("listen error");
}
int 
Accept(int fd, struct sockaddr *sa, socklen_t *salenptr)
{
        int     n;
again:
        if ((n = accept(fd, sa, salenptr)) < 0) {
                if (errno == ECONNABORTED)
                        goto again;
                else
                        err_sys("accept error");
        }
        return (n);             /* accepted connected fd */
}
void 
Connect(int fd, const struct sockaddr *sa, socklen_t salen)
{
        if (connect(fd, sa, salen) < 0)
                err_sys("connect error");
}

/* wrap unix/linux *********************************************************************************************
 * <stdlib.h> <fcntl.h> <signal.h> <unistd.h>
*/
void *
Calloc(size_t n, size_t size)
{
        void    *ptr;

        if ((ptr = calloc(n, size)) == NULL)
                err_sys("calloc error");
        return (ptr);
}
void *
Malloc(size_t size)
{
        void    *ptr;

        if ((ptr = malloc(size)) == NULL)
                err_sys("malloc error");
        return (ptr);
}

int 
Open(const char *pathname, int oflag, mode_t mode)
{
        int     fd;

        if ((fd = open(pathname, oflag, mode)) == -1)
                err_sys("open error for %s", pathname);
        return fd;
}
void 
Close(int fd)
{
        if (close(fd) == -1)
                err_sys("close error");
}
pid_t 
Fork(void)
{
        pid_t   pid;

        if ((pid = fork()) == -1)
                err_sys("fork error");
        return (pid);
}
pid_t 
Wait(int *iptr)
{
        pid_t   pid;

        if ((pid = wait(iptr)) == -1)
                err_sys("wait error");
        return (pid);
}
pid_t 
Waitpid(pid_t pid, int *iptr, int options)
{
        pid_t   retpid;

        if ((retpid = waitpid(pid, iptr, options)) == -1)
                err_sys("waitpid error");
        return (retpid);
}
sighandler_t 
Signal(int signum, sighandler_t handler)
{
        sighandler_t old;

        if ((old = signal(signum, handler)) == SIG_ERR) 
                err_sys("signal error");
        return old;
}
ssize_t 
Read(int fd, void *ptr, size_t nbytes)
{
        ssize_t n;

        if ((n = read(fd, ptr, nbytes)) == -1)
                err_sys("read error");
        return (n);
}
ssize_t
readn(int fd, void *vptr, size_t n)
{
        size_t  nleft;
        ssize_t nread;
        char    *ptr;

        ptr = vptr;
        nleft = n;
        while (nleft > 0) {
                if ((nread = read(fd, ptr, nleft)) < 0) {
                        if (errno == EINTR)
                                nread = 0;      /* and call read() again */
                        else   
                                return (-1);
                } else if (nread == 0)
                        break;          /* EOF */
                nleft -= nread;
                ptr += nread;
        }
        return (n - nleft);
}
ssize_t
Readn(int fd, void *ptr, size_t nbytes)
{
        ssize_t n;

        if ((n = readn(fd, ptr, nbytes)) < 0)
                err_sys("readn error");
        return (n);
}
static ssize_t 
my_read(int fd, char *ptr)
{
        static int      read_cnt = 0;
        static char     *read_ptr;
        static char     read_buf[MAXLINE];

        if (read_cnt <= 0) {
again:
                if ((read_cnt = read(fd, read_buf, sizeof(read_buf))) < 0) {
                        if (errno == EINTR)
                                goto again;
                        return (-1);
                } else if (read_cnt == 0)
                        return (0);
                read_ptr = read_buf;
        }
        read_cnt--;
        *ptr = *read_ptr++;
        return (1);
}
ssize_t 
readline(int fd, void *vptr, size_t maxlen)
{
        int     n, rc;
        char    c, *ptr;

        ptr = vptr;
        for (n = 1; n < maxlen; n++) {
                if ((rc = my_read(fd, &c)) == 1) {
                        *ptr++ = c;
                        if (c == '\n')
                                break;  /* newline is stored, like fgets() */
                } else if (rc == 0) {
                        *ptr = 0;
                        return (n - 1); /* EOF, n - 1 bytes were read */
                } else
                        return (-1);    /* error, errno set by read() */
        }
        *ptr = 0;       /* null terminate like fgets */
        return (n);
}
ssize_t 
Readline(int fd, void *ptr, size_t maxlen)
{
        ssize_t n;

        if ((n = readline(fd, ptr, maxlen)) < 0)
                err_sys("readline error");
        return n;
}

void 
Write(int fd, void *ptr, size_t nbytes)
{
        if (write(fd, ptr, nbytes) != nbytes)
                err_sys("write error");
}
ssize_t 
writen(int fd, const void *vptr, size_t n)
{
        size_t  nleft;
        ssize_t nwritten;
        const char *ptr;

        ptr = vptr;
        nleft = n;
        while (nleft > 0) {
                if ((nwritten = write(fd, ptr, nleft)) <= 0) {
                        if (errno == EINTR)
                                nwritten = 0;   /* and call write() again */
                        else
                                return (-1);    /* error */
                }
                nleft -= nwritten;
                ptr += nwritten;
        }
        return (n);
}
void 
Writen(int fd, void *ptr, size_t nbytes)
{
        if (writen(fd, ptr, nbytes) != nbytes)
                err_sys("written error");
}

/* inet_ntop ***************************************************************
 * <arpa/inet.h>
 */
const char *
Inet_ntop(int family, const void *addrptr, char *strptr, size_t len)
{
        const char      *ptr;

        if (strptr == NULL)     /* check for old code */
                err_quit("NULL 3rd argument to inet_ntop");
        if ((ptr = inet_ntop(family, addrptr, strptr, len)) == NULL)
                err_sys("inet_ntop error");
        return (ptr);
}
void 
Inet_pton(int family, const char *strptr, void *addrptr)
{
        int     n;

        if ((n = inet_pton(family, strptr, addrptr)) < 0)
                err_sys("inet_pton error for %s", strptr);      /* errno set */
        else if (n == 0)
                err_quit("inet_pton error for %s", strptr);     /* errno not set */
}

/* wrap standard I/O *************************************************************
 * <stdio.h>
 */
FILE *
Fopen(const char *filename, const char *mode)
{
        FILE    *fp;

        if ((fp = fopen(filename, mode)) == NULL)
                err_sys("fopen error");
        return (fp);
}
FILE *
Fdopen(int fd, const char *type)
{
        FILE    *fp;

        if ((fp = fdopen(fd, type)) == NULL)
                err_sys("fdopen error");
        return fp;
}
void 
Fclose(FILE *fp)
{
        if (fclose(fp) != 0)
                err_sys("fclose error");
}
char * 
Fgets(char *ptr, int n, FILE *stream)
{
        char    *rptr;

        if ((rptr = fgets(ptr, n, stream)) == NULL && ferror(stream))
                err_sys("fgets error");
        return (rptr);
}
void 
Fputs(const char *ptr, FILE *stream)
{
        if (fputs(ptr, stream) == EOF)
                err_sys("fputs error");
}
