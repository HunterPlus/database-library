#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>

#define MAXLINE         1024

/* error ****************************************************************
 * <stdarg.h> <errorno.h> <string.h>
 */
void err_ret(const char *fmt, ...);
void err_sys(const char *fmt, ...);
void err_dump(const char *fmt, ...);
void err_msg(const char *fmt, ...);
void err_quit(const char *fmt, ...);

/* wrap unix/linux ************************************************************
 * <stdlib.h> <fcntl.h> <signal.h> <unistd.h>
*/
void *Calloc(size_t n, size_t size);
void *Malloc(size_t size);
int Open(const char *pathname, int oflag, mode_t mode);
void Close(int fd);

pid_t Fork(void);
pid_t Wait(int *iptr);
pid_t Waitpid(pid_t pid, int *iptr, int options);

typedef void (*sighandler_t)(int);
sighandler_t Signal(int signum, sighandler_t handler);

ssize_t Read(int fd, void *ptr, size_t nbytes);
ssize_t readn(int fd, void *vptr, size_t n);
ssize_t Readn(int fd, void *ptr, size_t nbytes);
ssize_t readline(int, void *vptr, size_t maxlen);
ssize_t Readline(int fd, void *ptr, size_t maxlen);

void Write(int fd, void *ptr, size_t nbytes);
ssize_t writen(int fd, const void *vptr, size_t n);
void Writen(int fd, void *ptr, size_t nbytes);

/* wrap inet_ntop ***************************************************************
 * <arpa/inet.h>
 */
const char *Inet_ntop(int family, const void *addrptr, char *strptr, size_t len);
void Inet_pton(int family, const char *strptr, void *addrptr);

/* wrap standard I/O *************************************************************
 * <stdio.h>
 */
FILE *Fopen(const char *filename, const char *mode);
FILE *Fdopen(int fd, const char *type);
void Fclose(FILE *fp);
char * Fgets(char *ptr, int n, FILE *stream);
void Fputs(const char *ptr, FILE *stream);
