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
