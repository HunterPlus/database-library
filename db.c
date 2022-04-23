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
	if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREATE | O_TRUNC)) {
		/* if the database was created, we have to initialize it.
		   write lock the entire file so that we can stat it,
		   check its size, and initialize it, automically.	*/
		if (writew_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: writew_lock error");
		if (fstat(db->idxfd, db->statbuff) < 0)
			err_sys("db_open: fstat error");
		if (statbuff.st_size == 0) {
			/* we have to build a list of (NHASH_DEF + 1) chain
			   ptrs with a value of 0. the +1 is for the free
			   list pointer that precedes the hash table.	*/
			sprintf(asciiptr, "%*d", PTR_SZ, 0);
			hash[0] = 0;
			for (i = 0; i < NHASH_DEF + 1; i++)
				strcat(hash, asciiptr);
			strcat(hash, "\n");
			i = strlen(hash);
			if (write(db->idxfd, hash, i) != i)
				err_dump("db_open: index file init write error");			
		}
		if (un_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: un_lock error");
	}
	db_rewind(db);
	return (db);
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
	DB	*db = h;
	char	*ptr;
	
	if (_db_find_and_lock(db, key, 0) < 0) {
		ptr = NULL;		/* error, record not found */
		db->cnt_fetcherr++;
	} else {
		ptr = _db_readdat(db);	/* return pointer to data */
		db->cnt_fetchok++;
	}
	
	/* unlock the hash chain that _db_find_and_lock locked. */
	if (un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("db_fetch: un_lock error");
	return (ptr);
}
/*
 * find the specified record. call by db_delete, db_fetch, and db_store.
 * return with the hash chain locked.
 */
static int
_db_find_and_lock(DB *db, const char *key, int writelock)
{
	off_t	offset, nextoffset;
	
	/* calculate the hash value for this key, then calculate the byte offset 
	   of corresponding chain ptr in hash table.
	   this is where our search starts. first we calculate the offset in the 
	   hash table for this key.	*/
	db->chainoff = (_db_hash(db, key) * PTR_SZ) + db->hashoff;
	db->ptroff = db->chainoff;
	
	/* we lock the hash chain here. the caller must un_lock it when done.
	   note we lock and unlock only the first byte.		*/
	if (writelock) {
		if (writew_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("_db_find_and_lock: writew_lock error");
	} else {
		if (readw_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("_db_find_and_lock: readw_lock error");
	}
	
	/* get the offset in the index file of first record on 
	   the hash chain (can be 0).		*/
	offset = _db_readptr(db->idxfd, db->ptroff);
	while (offset != 0) {
		nextoffset = _db_readidx(db->idxfd, offset);
		if (strcmp(db->idxbuf, key) == 0)
			break;		/* found a match */
		db->ptroff = offset;	/* offset of this (unequal) record */
		offset = nextoffset;	/* next one to compare */
	}
	
	/* offset == 0 on error (record not found) */
	return (offset == 0 ? -1 : 0);
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
/*
 * read a chain ptr field frome anywhere in the index file:
 * the free list pointer, a hash table chain ptr, or an index record chain ptr.
 */
static off_t
_db_readptr(DB *db, off_t offset)
{
	char	asciiptr[PTR_SZ + 1];
	
	if (lseek(db->idxfd, 0, SEEK_SET) == -1)
		err_dump("_db_readptr: lseek error to ptr field");
	if (read(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_readptr: read error of ptr field");
	asciiptr[PTR_SZ] = 0;
	return atol(asciiptr);
}

/*
 * read the next index record.
 * we start the specified offset in the index file. we read the index record into db->idxbuf
 * and replace the separators with null('\0') bytes. if all is OK we set db->datoff and db->datlen
 * to the offset and the length of the corresponding data record in the data file.
 */
static off_t
_db_readidx(DB *db, off_t offset)
{
	
}

/*
 * read the current data record into the data buffer.
 * returns a pointer to the null terminated data buffer.
 */
static char *
_db_readdat(DB *db)
{
	if (lseek(db->datfd, db->datoff, SEEK_SET) == -1)
		err_dump("_db_readdat: lseek error");
	if (read(db->datfd, db->datbuf, db->datlen) != db->datlen)
		err_dump("_db_readdat: read error");
	if (db->datbuf[db->datlen - 1] != NEWLINE)	/* sanity check */
		err_dump("_db_readdat: missing newline");
	db->datbuf[db->datlen - 1] = 0;		/* replace newline with null */
	
	return db->datbuf;
}

/*
 * delete the specified record.
 */
int db_delete(DBHANDLE h, const char *key)
{
	
}

/*
 * delete the current record specified by the DB structure.
 * this function is called by db_delete() and db_store(),
 * after the record has been located by _db_find_and_lock().
 */
static void
_db_dodelete(DB *db)
{
	
}

/*
 * write a data record.
 * called by _db_dodelete() (to write the record with blanks) and db_store().
 */
static void
_db_writedat(DB *db, const char *data, off_t offset, int whence)
{
	struct iovec	iov[2];
	static char	newline = NEWLINE;
	
	/* if we are appending, we have to lock before doing the lseek and
	   write to make the two an atomic operation. if we are overwriting
	   an existing record, we don't have to lock.		*/
	if (whence == SEEK_END)		/* we are appending, lock the entire file. */
		if (writew_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: writew_lock error");
	if ((db->dataoff = lseek(db->datfd, offset, whence)) == -1)
		err_dump("_db_writedat: lseek error");
	db->datlen = strlen(data) + 1;		/* +1 for newline */
	
	iov[0].iov_base = (char *) data;
	iov[0].iov_len = db->datlen - 1;
	iov[1].iov_base = &newline;
	iov[1].iov_len = 1;
	if (writev(db->datfd, &iov[0], 2) != db->datlen)
		err_dump("_db_writedat: writev error of data record");
	
	if (whence == SEEK_END)
		if (un_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: un_lock error");
}

/*
 * write an index record.
 * _db_wrtedat() is called before this function to set the datoff and datlen 
 * in the DB structure, which we need to write the index record.
 */
static void
_db_writeidx(DB *db, const char *key, off_t offset, int whence, off_t ptrval)
{
	
}

/*
 * write a chain ptr field somewhere in the index file:
 * the free list, the hash table, or in an index record.
 */
static void
_db_writeptr(DB *db, off_t offset, off_t ptrval)
{
	char	asciiptr[PTR_SZ + 1];
	
	if (ptrval < 0 || ptrval > PTR_MAX)
		err_quit("_db_writeptr: invalid ptr: %ld", ptrval);
	sprintf(asciiptr, "%*ld", PTR_SZ, ptrval);
	
	if (lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("_db_writeptr: lseek error to ptr field");
	if (write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_writeptr: write error of ptr field");
}

/*
 * store a record in the database.
 * return 0 if OK, 1 if record exists and DB_INSERT specified, -1 on error.
 */
int
db_store(DBHANDLE h, const char *key, const char *data, int flag)
{
	
}

/*
 * try to find a free index record and accompanying data record
 * of the correct sizes. We'are only called by db_store().
 */
static int
_db_findfree(DB *db, int keylen, int datlen)
{
	
}

/*
 * rewind the index file for db_nextrec.
 * automatically called by db_open().
 * must be called before first db_nextrec().
 */
void
db_rewind(DBHANDLE h)
{
	DB	*db = h;
	off_t	offset;
	
	offset = (db->nhash + 1) * PTR_SZ;	/* +1 for free list ptr */
	
	/* we are just setting the file offset for this process 
	   to the start of the index records; no need to lock.
	   +1 below for newline at end of the hash table.	*/
	if ((db->idxoff = lseek(db->idxfd, offset + 1, SEEK_SET)) == -1)
		err_dump("db_rewind: lseek error");	
}

/*
 * return the next sequential record.
 * we just step our way through the index file, ignoring deleted records.
 * db_rewind() must be called before this function is called the first time.
 */
char *
db_nextrec(DBHANDLE h, char *key)
{
	
}

