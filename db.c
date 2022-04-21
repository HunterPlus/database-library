#include "lib.h"
#include "db.h"

#include <sys/uio.h>		/* struct iovec */

/*
 * internale index file constants.
 * These are used to construct record in the index file and data file. 
 */
#define IDXLEN_SIZE	4	/* index record length (ASCII chars) */
#define SEP		':'	/* separator char in index record */
#define SPACE		' '	/* space charactor */
#define NEWLINE		'\n'	/* newline charactor */

/* 
 * the following definitions are for hash chains and
 * free list chain in the index file.
 */
#define PTR_SZ		7	/* size of ptr field in hash chain */
#define PTR_MAX		9999999	/* max file offset 10 ^ PTRSZ - 1 */
#define NHASH_DEF	137	/* default hash table size */
#define FREE_OFF	0	/* free list offset in index file */
#define HASH_OFF	PTR_SZ	/* hash table offset in index file */

typedef unsigned long DBHASH;	/* hash value */
typedef unsigned long COUNT;	/* unsigned counter */

/*
 * library's private representation of the database.
 */
typedef struct {
	int	idxfd;		/* fd for index file */
	int	datfd;		/* fd for data file */
	char	*idxbuf;	/* malloc'ed buffer for index record */
	char	*datbuf;	/* malloc'ed buffer for data record */
	char	*name;		/* name db was opened under */
	off_t	idxoff;		/* offset in idx file of index record */
				/* key is at (idxoff + PTR_SZ + IDXLEN_SZ) */
	size_t	idxlen;		/* length of index record 	*/
				/* excludes IDXLEN_SZ bytes at front of record	*/
				/* incudes newline at end of index record	*/
	off_t	datoff;		/* offset in data file of data record */
	size_t	datlen;		/* length of data record include newline at end */
	off_t	ptrval;		/* contents of chain ptr in index record */
	off_t	ptroff;		/* chain ptr offset pointing to this index record */
	off_t	chainoff;	/* offset of hash chain for this index record */
	off_t	hashoff;	/* offset in index file of hash table */
	DBHASH	nhash;		/* current hash table size */
	
	COUNT	cnt_delok;	/* delete OK */
	COUNT	cnt_delerr;	/* delete error */
	COUNT	cnt_fetchok;	/* fetch OK */
	COUNT 	cnt_fetcherr;	/* fetch error */
	COUNT	cnt_nextrec;	/* next record */
	COUNT	cnt_stor1;	/* store: DB_INSERT, no empty, appended */
	COUNT	cnt_stor2;	/* store: DB_INSERT, found empty, reused */
	COUNT	cnt_stor3;	/* store: DB_REPLACE, diff len; appended */
	COUNT	cnt_stor4;	/* store: DB_REPLACE, same len; overwrote */
	COUNT	cnt_storerr;	/* store error */
} DB;

/* internal functions */
static DB	*_db_alloc(int);
static void	_db_dodelete(DB *);
static int	_db_find_and_lock(DB *, const char *, int);
static int 	_db_findfree(DB *, int, int);
static void	_db_free(DB *);
static DBHASH	_db_hash(DB *, const char *);
static char	*_db_readdat(DB *);
static off_t	_db_readidx(DB *, off_t);
static off_t	_db_readptr(DB *, off_t);
static void 	_db_writedat(DB *, const char *, off_t, int);
static void	_db_writeidx(DB *, const char *, off_t, int, off_t);
static void 	_db_writeptr(DB *, off_t, off_t);


/* 
 * open or create a database, same arguments as open(2)	
 */
DBHANDLE
db_open(const char *pathname, int oflag, ...)
{
	DB	*db;
	int	len, mode;
	size_t	i;
	char	asciiptr[PTR_SZ + 1],
		hash[(NHASH_DEF + 1) * PTR_SZ + 2];	/* +2 for newline and null */
	struct stat statbuff;
	
	/* allocate a DB structure, and the buffer it needs */
	len = strlen(pathname);
	if ((db = _db_alloc(len)) == NULL)
		err_dump("dp_open: _db_alloc error for DB");
	db->nhash = NHASH_DEF;		/* hash table size */
	db->hashoff = HASH_OFF;		/* offset in index file of hash table */
	strcpy(db->name, pathname);
	strcat(db->name, ".idx");
	
	if (oflag & O_CREAT) {
		va_list	ap;
		
		va_start(ap, oflag);
		mode = va_arg(ap, int);
		va_end(ap);
		
		/* open index file and data file */
		db->idxfd = open(db->name, oflag, mode);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag, mode);
	} else {	/* open index file and data file */
		db->idxfd = open(db->name, oflag);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag);
	}
	if (db->idxfd < 0 || db->datfd < 0) {
		_db_free(db);
		return NULL;
	}
}
/* 
 * allocate & initialize a DB structure and its buffers 
 */
static DB *
_db_alloc(int namelen)
{
	DB	*db;
	
	/* use calloc(), to initialize the structure to zero */
	if ((db = calloc(1, sizeof(DB))) == NULL) 
		err_dump("_db_alloc: calloc error for DB");
	db->idxfd = db->datfd = -1;	/* descriptors */
	/* alloc room for the name. +5 for ".idx" or ".dat" plus '\0' at end. */
	if ((db->name = malloc(namelen + 5)) == NULL)
		err_dump("_db_alloc: malloc error for name");
	/* allocate an index buffer and a data buffer. +2 for '\n' and '\0' at end. */
	if ((db->idxbuf = malloc(IDXLEN_MAX + 2)) == NULL)
		err_dump("_db_alloc: malloc error for index buffer");
	if ((db->datbuf = malloc(DATLEN_MAX + 2)) == NULL)
		err_dump("_db_alloc: malloc error for data buffer");
	
	return (db);	
}
/* 
 * relinquish access to the database. 
 */
void 
db_close(DBHANDLE h)
{
	_db_free((DB *) h);	/* close fds, free buffers & struct */
}
/* 
 * free up a DB structure, and all the malloc'ed buffers it may point to.
 * also close the descriptors it still open.
 */
static void
_db_free(DB *db)
{
	if (db->idxfd >= 0)
		close(db->idxfd);
	if (db->datfd >= 0)
		close(db->datfd);
	if (db->idxbuf != NULL)
		free(db->idxbuf);
	if (db->datbuf != NULL)
		free(db->datbuf);
	if (db->name != NULL)
		free(db->name);
	
	free(db);
}
/*
 * fetch a record. return a pointer to the null-terminated data.
 */
char *
db_fetch(DBHANDLE h, const char *key)
{
}
/*
 * find the specified record. call by db_delete, db_fetch, and db_store.
 * return with the hash chain locked.
 */
static int
_db_find_and_lock(DB *db, const char *key, int writelock)
{
}
/*
 * calculate the hash value for a key.
 */
static DBHASH
_db_hash(DB *db, const char *key)
{
	DBHASH	hval = 0;
	char	c;
	int	i;
	
	for (i = 1; (c = *key++) != 0; i++)
		hval += c * i;		/* ascii char times its 1-based index */
	return (hval % db->nhash);
}
