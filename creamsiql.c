/*
 * CReaMSiQL — a minimal append-only SQL engine in one C file
 *
 * Now a small TCP server instead of a stdin/stdout REPL. Same grammar:
 *   CREATE TABLE t (id INDEX, name, email INDEX)
 *   INSERT INTO t VALUES ('1', 'Alice', 'alice@example.com')
 *   SELECT * FROM t [WHERE col = val [AND col = val ...]]
 *   SELECT COUNT FROM t [WHERE ...]
 *
 * Storage (flat files in current directory):
 *   TABLENAME.schema     — column names + index flags: "NAME" or "NAME:INDEX"
 *   TABLENAME.data       — pipe-delimited rows, newline-terminated, append-only
 *   TABLENAME.COL.idx    — fixed-size open-addressing hash table
 *
 * Threading model:
 *   - N worker threads block in accept() on one shared listening socket.
 *     The kernel wakes exactly one waiter per incoming connection — no
 *     application-level dispatch queue needed for connection fan-out.
 *   - A worker owns a connection start-to-finish: read a line, act on it,
 *     write the reply, repeat until the client closes. SELECT is executed
 *     directly on the worker — every query already opens its own fd and
 *     uses pread, so concurrent reads need no locking at all.
 *   - CREATE and INSERT are forwarded to one dedicated writer thread and
 *     the worker blocks until it replies. That thread is the only caller
 *     of write_schema/idx_create/append_row/idx_insert, so the existing
 *     multi-step, non-atomic mutation code is correct under concurrency
 *     without changing a single line of it — there is never more than one
 *     writer to race against. It also gives CREATE TABLE an ordering
 *     guarantee for free: a CREATE is fully finished (schema file *and*
 *     every index file) before the writer even looks at the next queued
 *     job, so a client that creates a table and immediately inserts into
 *     it can never observe it half-built.
 *   - The lexer's token buffer is the same "one static array" as before,
 *     just declared _Thread_local: each pool thread gets its own private
 *     copy, allocated once when the thread is created — not malloc'd per
 *     request. do_create/do_insert/do_select/do_select_scan/append_row
 *     are completely unmodified; only the dispatch layer changed.
 *
 * Design:
 *   - Zero deps beyond POSIX: socket/accept/pthread + open/read/write/
 *     pread/pwrite/lseek/ftruncate
 *   - Compile: cc creamsiql.c -o creamsiql -pthread
 *   - No malloc anywhere, including in the threading layer: the write
 *     queue is a fixed-size static array; each blocked worker's
 *     completion lock/cond live on its own call stack, not heap.
 *   - No AST. Parse → act in one pass; memory touched in one place —
 *     now read per-thread, and write in exactly one thread.
 *   - Append-only data: byte offsets in .data are stable forever.
 *   - WHERE: index used for first condition; remaining AND clauses filter
 *     the matched rows in a streaming pass. No index → full linear scan.
 *   - Unbounded file size: data scanned line-by-line, never slurped whole.
 *   - Case rules: keywords and column names uppercased; quoted string values
 *     are case-sensitive; unquoted values are uppercased.
 *
 * Index file layout (.COL.idx):
 *   Header (16 bytes):
 *     [0..7]   bucket_count  u64  — always IDX_TOTAL
 *     [8..15]  overflow_next u64  — index of next free overflow bucket
 *   Per bucket (24 bytes, starting at byte 16):
 *     [0..7]   key_hash  u64  — FNV-1a of value; 0 = empty slot
 *     [8..15]  data_off  u64  — byte offset of row in .data
 *    [16..23]  chain     u64  — next overflow bucket index (0 = end)
 *
 * Compile: cc creamsiql.c -o creamsiql
 * Run: ./creamsiql [port] [workers]   (defaults: 7878, one per core)
 * Talk to it with: nc 127.0.0.1 7878
 */

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <stdarg.h>
#include <time.h>
#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ─── tuneable limits ───────────────────────────────────────────── */
#define MAX_TOKENS 128
#define MAX_COLS 32
#define MAX_CONDS 8
#define MAX_VALS 32
#define COL_LEN 64
#define VAL_LEN 256
#define LINE_LEN 512
#define IDX_BUCKETS 4096  /* primary hash slots                  */
#define IDX_OVERFLOW 1024 /* overflow pool                       */
#define IDX_TOTAL (IDX_BUCKETS + IDX_OVERFLOW)
#define IDX_HDR_SIZE 16
#define IDX_BUCKET_SZ 24
#define DEFAULT_PORT 7878
#define MAX_WORKERS 256    /* hard cap on pool size                */
#define WRITE_QUEUE_CAP 64 /* fixed-size, no malloc                */

/* ─── token types ───────────────────────────────────────────────── */
typedef enum
{
    TOK_CREATE,
    TOK_INSERT,
    TOK_SELECT,
    TOK_WHERE,
    TOK_AND,
    TOK_INTO,
    TOK_FROM,
    TOK_TABLE,
    TOK_VALUES,
    TOK_INDEX,
    TOK_COUNT,
    TOK_STAR,
    TOK_EQUALS,
    TOK_COMMA,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_IDENT,
    TOK_STRING,
    TOK_NUMBER,
    TOK_EOF,
    TOK_ERROR
} TokType;

typedef struct
{
    TokType type;
    char value[VAL_LEN];
} Token;

/* ─── globals ───────────────────────────────────────────────────── */
/* _Thread_local: each pool thread gets its own private copy of these,
 * allocated once when the thread is created — same "no malloc, one
 * static array" property as before, just sliced per worker so two
 * threads parsing concurrently never clobber each other's tokens. */
static _Thread_local Token tokens[MAX_TOKENS];
static _Thread_local int ntok = 0;
static _Thread_local int pos = 0;

/* ─── buffered I/O ───────────────────────────────────────────────────
 * One ReadBuf + one WriteBuf per worker thread (and one WriteBuf for
 * the writer thread), stack-local, reused across every connection /
 * job that thread ever handles — never malloc'd, never per-connection.
 * Replaces byte-at-a-time read() and per-field write() with chunked
 * syscalls; same "no malloc anywhere" property as the rest of the file. */
#define IOBUF_SZ 4096

typedef struct
{
    int fd;
    char data[IOBUF_SZ];
    int start, end; /* [start,end) is unread; end == -1 means EOF seen */
} ReadBuf;

typedef struct
{
    int fd;
    char data[IOBUF_SZ];
    int len;
} WriteBuf;

static void rb_reset(ReadBuf *rb, int fd)
{
    rb->fd = fd;
    rb->start = 0;
    rb->end = 0;
}

static void rb_fill(ReadBuf *rb)
{
    if (rb->start < rb->end)
        return; /* still unread bytes, nothing to do */
    int n = (int)read(rb->fd, rb->data, IOBUF_SZ);
    if (n <= 0)
    {
        rb->end = -1;
        return;
    }
    rb->start = 0;
    rb->end = n;
}

/* same contract as the old fd_readline: strips trailing '\n', returns
 * chars in the line (0 for blank), -1 at EOF with nothing left to give. */
static int rb_readline(ReadBuf *rb, char *buf, int bufsz)
{
    int i = 0;
    while (1)
    {
        if (rb->start >= rb->end)
        {
            rb_fill(rb);
            if (rb->end == -1)
            {
                buf[i] = '\0';
                return (i > 0) ? i : -1;
            }
        }
        while (rb->start < rb->end)
        {
            char c = rb->data[rb->start++];
            if (c == '\n')
            {
                buf[i] = '\0';
                return i;
            }
            if (i < bufsz - 1)
                buf[i++] = c;
        }
    }
}

static void wb_reset(WriteBuf *wb, int fd)
{
    wb->fd = fd;
    wb->len = 0;
}

static void wb_flush(WriteBuf *wb)
{
    if (wb->len == 0)
        return;
    write(wb->fd, wb->data, wb->len);
    wb->len = 0;
}

static void wb_put(WriteBuf *wb, const char *s, int slen)
{
    int pos = 0;
    while (pos < slen)
    {
        int space = IOBUF_SZ - wb->len;
        int chunk = (slen - pos < space) ? (slen - pos) : space;
        memcpy(wb->data + wb->len, s + pos, chunk);
        wb->len += chunk;
        pos += chunk;
        if (wb->len == IOBUF_SZ)
            wb_flush(wb);
    }
}

/* ─── logging ────────────────────────────────────────────────────────
 * One mutex-guarded log_line() used by every thread: connections,
 * queries, and errors all funnel through it. Always writes to the
 * console (fd 1); also writes to a user-specified file if one was
 * given on the command line. No malloc — fixed stack buffers, like
 * everything else in this file. The mutex means logging is globally
 * serialized, which is fine: it's diagnostic output, not the hot path
 * (that's still the per-thread ReadBuf/WriteBuf above). */
static int log_fd = -1; /* -1 = no log file configured, console-only */
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_init(const char *path)
{
    if (!path)
        return;
    log_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0)
        write(1, "warning: could not open log file, console-only\n", 48);
}

static void log_line(const char *fmt, ...)
{
    char msg[768];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    char ts[32];
    time_t t = time(NULL);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    char line[896];
    int len = snprintf(line, sizeof(line), "[%s] %s\n", ts, msg);
    if (len < 0)
        return;
    if (len >= (int)sizeof(line))
        len = (int)sizeof(line) - 1;

    pthread_mutex_lock(&log_lock);
    write(1, line, (size_t)len); /* console, always */
    if (log_fd >= 0)
        write(log_fd, line, (size_t)len); /* file, if configured */
    pthread_mutex_unlock(&log_lock);
}
/* each pool/writer thread points this at its own stack-local WriteBuf
 * for the lifetime of the connection/job it's currently handling. NULL
 * on the main thread, where out()/oerr() fall back to raw fd 1 so the
 * startup banner still works unchanged. */
static _Thread_local WriteBuf *out_wb = NULL;

/* ─── output helpers ────────────────────────────────────────────── */
static void out(const char *s)
{
    if (out_wb)
        wb_put(out_wb, s, (int)strlen(s));
    else
        write(1, s, strlen(s));
}
static void oerr(const char *s)
{
    out("error: ");
    out(s);
    out("\n");
    log_line("ERROR fd=%d: %s", out_wb ? out_wb->fd : -1, s);
}

/* ─── integer to decimal string ─────────────────────────────────── */
static void out_int(int v)
{
    char s[32];
    int i = sizeof(s) - 1;
    s[i] = '\0';
    if (v == 0)
    {
        s[--i] = '0';
    }
    else
    {
        while (v > 0)
        {
            s[--i] = '0' + v % 10;
            v /= 10;
        }
    }
    out(s + i);
}

/* ─── FNV-1a hash ───────────────────────────────────────────────── */
static uint64_t fnv1a(const char *s)
{
    uint64_t h = 14695981039346656037ULL;
    while (*s)
    {
        h ^= (uint8_t)*s++;
        h *= 1099511628211ULL;
    }
    return h ? h : 1; /* 0 is reserved as empty-slot sentinel */
}

/* ─── lexer ─────────────────────────────────────────────────────── */
static void add_tok(TokType t, const char *v)
{
    if (ntok >= MAX_TOKENS)
        return;
    tokens[ntok].type = t;
    strncpy(tokens[ntok].value, v, VAL_LEN - 1);
    tokens[ntok].value[VAL_LEN - 1] = '\0';
    ntok++;
}

static TokType keyword(const char *s)
{
    if (!strcmp(s, "CREATE"))
        return TOK_CREATE;
    if (!strcmp(s, "INSERT"))
        return TOK_INSERT;
    if (!strcmp(s, "SELECT"))
        return TOK_SELECT;
    if (!strcmp(s, "WHERE"))
        return TOK_WHERE;
    if (!strcmp(s, "AND"))
        return TOK_AND;
    if (!strcmp(s, "INTO"))
        return TOK_INTO;
    if (!strcmp(s, "FROM"))
        return TOK_FROM;
    if (!strcmp(s, "TABLE"))
        return TOK_TABLE;
    if (!strcmp(s, "VALUES"))
        return TOK_VALUES;
    if (!strcmp(s, "INDEX"))
        return TOK_INDEX;
    if (!strcmp(s, "COUNT"))
        return TOK_COUNT;
    return TOK_IDENT;
}

static void lex(const char *input)
{
    ntok = 0;
    pos = 0;
    const char *p = input;
    char buf[VAL_LEN];

    while (*p)
    {
        while (*p && isspace((unsigned char)*p))
            p++;
        if (!*p)
            break;

        if (isalpha((unsigned char)*p) || *p == '_')
        {
            int i = 0;
            while (*p && (isalnum((unsigned char)*p) || *p == '_') && i < VAL_LEN - 1)
                buf[i++] = (char)toupper((unsigned char)*p++);
            buf[i] = '\0';
            add_tok(keyword(buf), buf);
        }
        else if (isdigit((unsigned char)*p))
        {
            int i = 0;
            while (*p && isdigit((unsigned char)*p) && i < VAL_LEN - 1)
                buf[i++] = *p++;
            buf[i] = '\0';
            add_tok(TOK_NUMBER, buf);
        }
        else if (*p == '\'')
        {
            p++;
            int i = 0;
            while (*p && *p != '\'' && i < VAL_LEN - 1)
                buf[i++] = *p++;
            if (*p == '\'')
                p++;
            buf[i] = '\0';
            add_tok(TOK_STRING, buf);
        }
        else
        {
            char sym[2] = {*p++, '\0'};
            switch (sym[0])
            {
            case '*':
                add_tok(TOK_STAR, sym);
                break;
            case '=':
                add_tok(TOK_EQUALS, sym);
                break;
            case ',':
                add_tok(TOK_COMMA, sym);
                break;
            case '(':
                add_tok(TOK_LPAREN, sym);
                break;
            case ')':
                add_tok(TOK_RPAREN, sym);
                break;
            default:
                add_tok(TOK_ERROR, sym);
                break;
            }
        }
    }
    add_tok(TOK_EOF, "");
}

/* ─── parser helpers ────────────────────────────────────────────── */
static Token peek(void) { return tokens[pos]; }
static Token advance(void) { return tokens[pos++]; }
static int expect(TokType t)
{
    if (tokens[pos].type == t)
    {
        pos++;
        return 1;
    }
    oerr("unexpected token");
    return 0;
}

/* ─── path builders ─────────────────────────────────────────────── */
static void make_path(const char *name, const char *ext, char *buf, int sz)
{
    int i = 0;
    while (*name && i < sz - 16)
        buf[i++] = *name++;
    buf[i++] = '.';
    while (*ext && i < sz - 1)
        buf[i++] = *ext++;
    buf[i] = '\0';
}

/* "TNAME.COLNAME.idx" */
static void make_idx_path(const char *t, const char *col, char *buf, int sz)
{
    int i = 0;
    while (*t && i < sz - 72)
        buf[i++] = *t++;
    buf[i++] = '.';
    while (*col && i < sz - 8)
        buf[i++] = *col++;
    const char *e = ".idx";
    while (*e && i < sz - 1)
        buf[i++] = *e++;
    buf[i] = '\0';
}

/* ─── streaming line reader (no full-file slurp) ─────────────────── */
/*
 * Reads one line from fd at its current position into buf (up to bufsz-1).
 * Strips the trailing newline. Returns chars read (0 for blank), -1 at EOF.
 */
static int fd_readline(int fd, char *buf, int bufsz)
{
    int i = 0;
    char c;
    while (i < bufsz - 1)
    {
        if (read(fd, &c, 1) <= 0)
        {
            buf[i] = '\0';
            return (i > 0) ? i : -1;
        }
        if (c == '\n')
            break;
        buf[i++] = c;
    }
    buf[i] = '\0';
    return i;
}

static void fd_puts(int fd, const char *s) { write(fd, s, strlen(s)); }

/* ─── schema ─────────────────────────────────────────────────────── */
typedef struct
{
    char name[COL_LEN];
    int indexed;
} ColDef;

static void write_schema(const char *tname, ColDef *cols, int ncols)
{
    char path[LINE_LEN];
    make_path(tname, "schema", path, LINE_LEN);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        oerr("cannot create schema file");
        return;
    }
    for (int i = 0; i < ncols; i++)
    {
        fd_puts(fd, cols[i].name);
        if (cols[i].indexed)
            fd_puts(fd, ":INDEX");
        fd_puts(fd, "\n");
    }
    close(fd);
}

static int read_schema(const char *tname, ColDef *cols, int *ncols)
{
    char path[LINE_LEN];
    make_path(tname, "schema", path, LINE_LEN);
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return 0;
    *ncols = 0;
    char line[COL_LEN + 8];
    int n;
    while (*ncols < MAX_COLS && (n = fd_readline(fd, line, sizeof(line))) >= 0)
    {
        if (n == 0)
            continue;
        char *colon = strchr(line, ':');
        if (colon)
        {
            *colon = '\0';
            cols[*ncols].indexed = !strcmp(colon + 1, "INDEX");
        }
        else
        {
            cols[*ncols].indexed = 0;
        }
        strncpy(cols[*ncols].name, line, COL_LEN - 1);
        cols[*ncols].name[COL_LEN - 1] = '\0';
        (*ncols)++;
    }
    close(fd);
    return (*ncols > 0);
}

/* ─── index: fixed-size open-addressing hash table on disk ──────── */

static void u64le_pack(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
    {
        b[i] = (uint8_t)(v & 0xff);
        v >>= 8;
    }
}
static uint64_t u64le_unpack(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = (v << 8) | b[i];
    return v;
}

static off_t bucket_off(uint64_t idx)
{
    return (off_t)(IDX_HDR_SIZE + idx * IDX_BUCKET_SZ);
}

static void idx_read_bucket(int fd, uint64_t idx,
                            uint64_t *h, uint64_t *d, uint64_t *chain)
{
    uint8_t b[IDX_BUCKET_SZ];
    if (pread(fd, b, IDX_BUCKET_SZ, bucket_off(idx)) != IDX_BUCKET_SZ)
    {
        *h = *d = *chain = 0;
        return;
    }
    *h = u64le_unpack(b);
    *d = u64le_unpack(b + 8);
    *chain = u64le_unpack(b + 16);
}

static void idx_write_bucket(int fd, uint64_t idx,
                             uint64_t h, uint64_t d, uint64_t chain)
{
    uint8_t b[IDX_BUCKET_SZ];
    u64le_pack(b, h);
    u64le_pack(b + 8, d);
    u64le_pack(b + 16, chain);
    pwrite(fd, b, IDX_BUCKET_SZ, bucket_off(idx));
}

/* allocate one overflow bucket; returns its index, or 0 on exhaustion */
static uint64_t idx_alloc_overflow(int fd)
{
    uint8_t hdr[IDX_HDR_SIZE];
    if (pread(fd, hdr, IDX_HDR_SIZE, 0) != IDX_HDR_SIZE)
        return 0;
    uint64_t next = u64le_unpack(hdr + 8);
    if (next >= IDX_TOTAL)
    {
        oerr("index overflow pool exhausted");
        return 0;
    }
    u64le_pack(hdr + 8, next + 1);
    pwrite(fd, hdr, IDX_HDR_SIZE, 0);
    return next;
}

static void idx_create(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return;
    uint8_t hdr[IDX_HDR_SIZE];
    u64le_pack(hdr, (uint64_t)IDX_TOTAL);
    u64le_pack(hdr + 8, (uint64_t)IDX_BUCKETS); /* first free overflow slot */
    write(fd, hdr, IDX_HDR_SIZE);
    uint8_t zero[IDX_BUCKET_SZ];
    memset(zero, 0, sizeof(zero));
    for (int i = 0; i < IDX_TOTAL; i++)
        write(fd, zero, IDX_BUCKET_SZ);
    close(fd);
}

static void idx_insert(int fd, uint64_t hash, uint64_t data_off)
{
    uint64_t slot = hash % IDX_BUCKETS;
    uint64_t h, d, chain;
    idx_read_bucket(fd, slot, &h, &d, &chain);

    if (h == 0)
    {
        /* empty primary slot — write directly */
        idx_write_bucket(fd, slot, hash, data_off, 0);
        return;
    }
    /* walk to end of chain, then link a new overflow bucket */
    uint64_t cur = slot;
    while (chain != 0)
    {
        cur = chain;
        idx_read_bucket(fd, cur, &h, &d, &chain);
    }
    uint64_t ov = idx_alloc_overflow(fd);
    if (!ov)
        return;
    idx_write_bucket(fd, ov, hash, data_off, 0);
    /* link cur → ov */
    idx_read_bucket(fd, cur, &h, &d, &chain);
    idx_write_bucket(fd, cur, h, d, ov);
}

/*
 * Walk every bucket in the chain for hash(val) and call
 * cb(data_fd, data_off, ud) for each hash hit.
 */
typedef void (*hit_cb)(int data_fd, uint64_t data_off, void *ud);

static void idx_lookup(int ifd, int dfd, const char *val, hit_cb cb, void *ud)
{
    uint64_t hash = fnv1a(val);
    uint64_t slot = hash % IDX_BUCKETS;
    uint64_t h, d, chain;
    idx_read_bucket(ifd, slot, &h, &d, &chain);
    if (h == 0)
        return;

    uint64_t cur = slot;
    do
    {
        idx_read_bucket(ifd, cur, &h, &d, &chain);
        if (h == hash)
            cb(dfd, d, ud);
        cur = chain;
    } while (cur != 0);
}

/* ─── row helpers ───────────────────────────────────────────────── */

/* split pipe-delimited line in-place; returns field count */
static int split_row(char *line, char *fields[], int max)
{
    int n = 0;
    fields[n++] = line;
    while (*line && n < max)
    {
        if (*line == '|')
        {
            *line = '\0';
            fields[n++] = line + 1;
        }
        line++;
    }
    /* strip trailing newline */
    char *last = fields[n - 1];
    int len = (int)strlen(last);
    if (len > 0 && last[len - 1] == '\n')
        last[len - 1] = '\0';
    return n;
}

/*
 * Condition list — used by both index callbacks and full-scan
 * skip_first: when arriving via index we already know conds[0] matched
 */
typedef struct
{
    char col[COL_LEN];
    char val[VAL_LEN];
} Cond;

typedef struct
{
    ColDef *cols;
    int ncols;
    Cond *conds;
    int nconds;
    int skip_first; /* index path: first cond already satisfied */
    int count_only;
    int matched;
} MatchCtx;

/* read the row at data_off from dfd, check conds, print or count */
static void try_row(int dfd, uint64_t data_off, MatchCtx *ctx)
{
    char line[LINE_LEN];
    ssize_t n = pread(dfd, line, LINE_LEN - 1, (off_t)data_off);
    if (n <= 0)
        return;
    /* null-terminate at first newline */
    int len = 0;
    while (len < n && line[len] != '\n')
        len++;
    line[len] = '\0';

    char copy[LINE_LEN];
    char *fields[MAX_COLS];
    strncpy(copy, line, LINE_LEN - 1);
    copy[LINE_LEN - 1] = '\0';
    int nf = split_row(copy, fields, MAX_COLS);

    int start = ctx->skip_first ? 1 : 0;
    for (int c = start; c < ctx->nconds; c++)
    {
        int ci = -1;
        for (int j = 0; j < ctx->ncols; j++)
            if (!strcmp(ctx->cols[j].name, ctx->conds[c].col))
            {
                ci = j;
                break;
            }
        if (ci < 0 || ci >= nf || strcmp(fields[ci], ctx->conds[c].val) != 0)
            return;
    }

    ctx->matched++;
    if (ctx->count_only)
        return;
    for (int j = 0; j < nf; j++)
    {
        out(fields[j]);
        out(j < nf - 1 ? " | " : "\n");
    }
}

/* callback shim for idx_lookup */
static void on_idx_hit(int dfd, uint64_t data_off, void *ud)
{
    try_row(dfd, data_off, (MatchCtx *)ud);
}

/* ─── data: append row + update indexes ─────────────────────────── */
static void append_row(const char *tname, ColDef *cols, int ncols,
                       char vals[][VAL_LEN], int nvals)
{
    char path[LINE_LEN];
    make_path(tname, "data", path, LINE_LEN);
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
    {
        oerr("cannot open data file");
        return;
    }

    /* record offset before writing — stable forever (append-only).
     * NOTE: O_APPEND guarantees the upcoming write() lands at EOF, but
     * it does not move the fd's own seek pointer there on open(), so
     * SEEK_CUR here would just read back the fd's initial position (0).
     * SEEK_END gives the real offset the row is about to be written at.
     * Safe with exactly one writer (true for both the original REPL and
     * the single dedicated writer thread below — never two appenders). */
    off_t row_off = lseek(fd, 0, SEEK_END);
    for (int i = 0; i < nvals; i++)
    {
        fd_puts(fd, vals[i]);
        fd_puts(fd, i < nvals - 1 ? "|" : "\n");
    }
    close(fd);

    /* update indexes */
    char idxpath[LINE_LEN];
    for (int i = 0; i < ncols && i < nvals; i++)
    {
        if (!cols[i].indexed)
            continue;
        make_idx_path(tname, cols[i].name, idxpath, LINE_LEN);
        int ifd = open(idxpath, O_RDWR);
        if (ifd < 0)
        {
            oerr("cannot open index file");
            continue;
        }
        idx_insert(ifd, fnv1a(vals[i]), (uint64_t)row_off);
        close(ifd);
    }
    out("row inserted\n");
}

/* ─── select core ───────────────────────────────────────────────── */
static void do_select_scan(const char *tname, ColDef *cols, int ncols,
                           Cond *conds, int nconds, int count_only)
{
    /* header */
    if (!count_only)
    {
        for (int i = 0; i < ncols; i++)
        {
            out(cols[i].name);
            out(i < ncols - 1 ? " | " : "\n");
        }
        for (int i = 0; i < ncols; i++)
        {
            out("--------");
            out(i < ncols - 1 ? "-+-" : "---\n");
        }
    }

    char data_path[LINE_LEN];
    make_path(tname, "data", data_path, LINE_LEN);
    int dfd = open(data_path, O_RDONLY);
    if (dfd < 0)
    {
        if (count_only)
        {
            out("0\n");
        }
        else
        {
            out("(no rows)\n");
        }
        return;
    }

    MatchCtx ctx = {cols, ncols, conds, nconds, 0, count_only, 0};

    /* try index on first WHERE condition */
    int used_index = 0;
    if (nconds > 0)
    {
        int ci = -1;
        for (int j = 0; j < ncols; j++)
            if (!strcmp(cols[j].name, conds[0].col) && cols[j].indexed)
            {
                ci = j;
                break;
            }
        if (ci >= 0)
        {
            char idxpath[LINE_LEN];
            make_idx_path(tname, cols[ci].name, idxpath, LINE_LEN);
            int ifd = open(idxpath, O_RDONLY);
            if (ifd >= 0)
            {
                ctx.skip_first = 1;
                idx_lookup(ifd, dfd, conds[0].val, on_idx_hit, &ctx);
                close(ifd);
                used_index = 1;
            }
        }
    }

    /* full linear scan (no index available or no WHERE clause) */
    if (!used_index)
    {
        ctx.skip_first = 0;
        lseek(dfd, 0, SEEK_SET);
        char line[LINE_LEN];
        int n;
        while ((n = fd_readline(dfd, line, LINE_LEN)) >= 0)
        {
            if (n == 0)
                continue;
            /* reconstruct data_off: current position minus the line just read
               (fd_readline consumed n chars + 1 newline)                      */
            off_t cur = lseek(dfd, 0, SEEK_CUR);
            uint64_t row_off = (uint64_t)(cur - n - 1);

            /* filter via try_row (which re-reads via pread — cheap, consistent) */
            try_row(dfd, row_off, &ctx);
        }
    }

    close(dfd);

    if (count_only)
    {
        out_int(ctx.matched);
        out("\n");
        return;
    }
    if (!ctx.matched)
        out("(no rows matched)\n");
}

/* ─── command handlers ──────────────────────────────────────────── */
static void do_create(void)
{
    if (!expect(TOK_CREATE) || !expect(TOK_TABLE))
        return;
    if (peek().type != TOK_IDENT)
    {
        oerr("expected table name");
        return;
    }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN - 1);
    tname[COL_LEN - 1] = '\0';

    if (!expect(TOK_LPAREN))
        return;
    ColDef cols[MAX_COLS];
    int ncols = 0;
    while (peek().type == TOK_IDENT && ncols < MAX_COLS)
    {
        strncpy(cols[ncols].name, advance().value, COL_LEN - 1);
        cols[ncols].name[COL_LEN - 1] = '\0';
        cols[ncols].indexed = 0;
        if (peek().type == TOK_INDEX)
        {
            advance();
            cols[ncols].indexed = 1;
        }
        ncols++;
        if (peek().type == TOK_COMMA)
            advance();
    }
    if (!expect(TOK_RPAREN))
        return;

    write_schema(tname, cols, ncols);

    char idxpath[LINE_LEN];
    for (int i = 0; i < ncols; i++)
    {
        if (!cols[i].indexed)
            continue;
        make_idx_path(tname, cols[i].name, idxpath, LINE_LEN);
        idx_create(idxpath);
    }
    out("table created\n");
}

static void do_insert(void)
{
    if (!expect(TOK_INSERT) || !expect(TOK_INTO))
        return;
    if (peek().type != TOK_IDENT)
    {
        oerr("expected table name");
        return;
    }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN - 1);
    tname[COL_LEN - 1] = '\0';

    ColDef cols[MAX_COLS];
    int ncols = 0;
    if (!read_schema(tname, cols, &ncols))
    {
        oerr("table not found");
        return;
    }

    if (!expect(TOK_VALUES) || !expect(TOK_LPAREN))
        return;
    char vals[MAX_VALS][VAL_LEN];
    int nvals = 0;
    while (peek().type != TOK_RPAREN && peek().type != TOK_EOF && nvals < MAX_VALS)
    {
        Token t = advance();
        if (t.type == TOK_COMMA)
            continue;
        strncpy(vals[nvals++], t.value, VAL_LEN - 1);
    }
    if (!expect(TOK_RPAREN))
        return;
    if (nvals != ncols)
    {
        oerr("column count mismatch");
        return;
    }
    append_row(tname, cols, ncols, vals, nvals);
}

static void do_select(void)
{
    if (!expect(TOK_SELECT))
        return;
    int count_only = 0;
    if (peek().type == TOK_COUNT)
    {
        advance();
        count_only = 1;
    }
    else if (!expect(TOK_STAR))
        return;

    if (!expect(TOK_FROM))
        return;
    if (peek().type != TOK_IDENT)
    {
        oerr("expected table name");
        return;
    }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN - 1);
    tname[COL_LEN - 1] = '\0';

    ColDef cols[MAX_COLS];
    int ncols = 0;
    if (!read_schema(tname, cols, &ncols))
    {
        oerr("table not found");
        return;
    }

    Cond conds[MAX_CONDS];
    int nconds = 0;
    if (peek().type == TOK_WHERE)
    {
        advance();
        while (nconds < MAX_CONDS)
        {
            if (peek().type != TOK_IDENT)
            {
                oerr("expected column name");
                return;
            }
            strncpy(conds[nconds].col, advance().value, COL_LEN - 1);
            conds[nconds].col[COL_LEN - 1] = '\0';
            if (!expect(TOK_EQUALS))
                return;
            strncpy(conds[nconds].val, advance().value, VAL_LEN - 1);
            conds[nconds].val[VAL_LEN - 1] = '\0';
            nconds++;
            if (peek().type != TOK_AND)
                break;
            advance();
        }
    }

    /* validate condition column names */
    for (int c = 0; c < nconds; c++)
    {
        int found = 0;
        for (int j = 0; j < ncols; j++)
            if (!strcmp(cols[j].name, conds[c].col))
            {
                found = 1;
                break;
            }
        if (!found)
        {
            oerr("unknown column in WHERE");
            return;
        }
    }

    do_select_scan(tname, cols, ncols, conds, nconds, count_only);
}

/* ─── single dedicated writer thread ───────────────────────────────
 * Only this thread ever calls do_create/do_insert, so it's the only
 * thread that ever calls write_schema/idx_create/append_row/idx_insert.
 * That's what makes those functions' multi-step, non-atomic writes
 * correct under concurrency without changing a line of them.
 *
 * Each job's completion lock/cond/flag live on the CALLING WORKER'S
 * OWN STACK (set up in submit_write_job), not in the queue slot. The
 * writer copies everything it needs out of the slot — the line (via
 * lex, into its own thread-local tokens) and the three pointers — while
 * still holding qlock, then immediately frees the slot. A new push can
 * safely reuse that slot from that point on, because nothing will ever
 * read it again for the old request; the writer finishes the slow part
 * (do_create/do_insert) using only its local copies, and finally
 * signals completion through the pointers it saved, not the slot. */
typedef struct
{
    char line[LINE_LEN];
    int client_fd;
    pthread_mutex_t *done_lock;
    pthread_cond_t *done_cond;
    int *done_flag;
} WriteJob;

static WriteJob wq[WRITE_QUEUE_CAP];
static int qhead = 0, qtail = 0, qcount = 0;
static pthread_mutex_t qlock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_has_work = PTHREAD_COND_INITIALIZER;
static pthread_cond_t q_has_room = PTHREAD_COND_INITIALIZER;
static volatile sig_atomic_t running = 1;

/* called by a worker thread for CREATE/INSERT; blocks until the writer
 * has fully handled it and replied to client_fd directly */
static void submit_write_job(const char *line, int client_fd)
{
    pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
    int done = 0;

    pthread_mutex_lock(&qlock);
    while (qcount == WRITE_QUEUE_CAP)
        pthread_cond_wait(&q_has_room, &qlock);
    WriteJob *j = &wq[qtail];
    qtail = (qtail + 1) % WRITE_QUEUE_CAP;
    qcount++;
    strncpy(j->line, line, LINE_LEN - 1);
    j->line[LINE_LEN - 1] = '\0';
    j->client_fd = client_fd;
    j->done_lock = &lk;
    j->done_cond = &cv;
    j->done_flag = &done;
    pthread_cond_signal(&q_has_work);
    pthread_mutex_unlock(&qlock);

    pthread_mutex_lock(&lk);
    while (!done)
        pthread_cond_wait(&cv, &lk);
    pthread_mutex_unlock(&lk);
}

static void *writer_main(void *unused)
{
    (void)unused;
    WriteBuf wwb; /* one per thread, reused across every job, reset per-job below */
    while (1)
    {
        pthread_mutex_lock(&qlock);
        while (qcount == 0 && running)
            pthread_cond_wait(&q_has_work, &qlock);
        if (qcount == 0 && !running)
        {
            pthread_mutex_unlock(&qlock);
            break;
        }

        WriteJob *j = &wq[qhead];
        qhead = (qhead + 1) % WRITE_QUEUE_CAP;

        char line[LINE_LEN];
        strncpy(line, j->line, LINE_LEN - 1);
        line[LINE_LEN - 1] = '\0';
        int cfd = j->client_fd;
        pthread_mutex_t *dl = j->done_lock;
        pthread_cond_t *dc = j->done_cond;
        int *df = j->done_flag;

        qcount--;
        pthread_cond_signal(&q_has_room); /* slot is safe to reuse now */
        pthread_mutex_unlock(&qlock);

        wb_reset(&wwb, cfd);
        out_wb = &wwb;
        log_line("WRITE fd=%d: %s", cfd, line);
        lex(line);
        switch (peek().type)
        {
        case TOK_CREATE:
            do_create();
            break;
        case TOK_INSERT:
            do_insert();
            break;
        default:
            oerr("internal: bad write job");
            break;
        }
        wb_flush(&wwb); /* one job, one fd — flush its reply before signaling done */

        pthread_mutex_lock(dl);
        *df = 1;
        pthread_cond_signal(dc);
        pthread_mutex_unlock(dl);
    }
    return NULL;
}

/* ─── worker threads ────────────────────────────────────────────────
 * All N workers block in accept() on the same listening socket; the
 * kernel hands each incoming connection to exactly one of them. A
 * worker owns its connection until the client closes it. SELECT runs
 * right here with zero coordination; CREATE/INSERT are hand off to
 * the writer thread above. */
static int listen_fd = -1;

static void *worker_main(void *unused)
{
    (void)unused;
    char line[LINE_LEN];
    ReadBuf rb; /* one per thread, reused across every connection */
    WriteBuf wb;
    while (running)
    {
        int cfd = accept(listen_fd, NULL, NULL);
        if (cfd < 0)
        {
            if (!running)
                break;
            continue;
        }
        log_line("CONNECT fd=%d", cfd);
        rb_reset(&rb, cfd);
        wb_reset(&wb, cfd);
        out_wb = &wb;
        while (running)
        {
            int n = rb_readline(&rb, line, LINE_LEN);
            if (n < 0)
                break; /* client closed or read error */
            if (n == 0)
                continue; /* blank line */
            log_line("QUERY fd=%d: %s", cfd, line);
            lex(line);
            switch (peek().type)
            {
            case TOK_CREATE:
            case TOK_INSERT:
                submit_write_job(line, cfd);
                break;
            case TOK_SELECT:
                do_select();
                break;
            case TOK_EOF:
                break;
            default:
                oerr("unknown command (try CREATE, INSERT, SELECT)");
                break;
            }
            wb_flush(&wb); /* one command, one reply — flush before reading the next line */
        }
        wb_flush(&wb); /* safety net for anything left buffered */
        log_line("DISCONNECT fd=%d", cfd);
        close(cfd);
    }
    return NULL;
}

static void on_shutdown_signal(int sig)
{
    (void)sig;
    running = 0;
    if (listen_fd >= 0)
        close(listen_fd); /* unblocks every accept() */
}

int main(int argc, char **argv)
{
    int port = DEFAULT_PORT;
    long ncores = sysconf(_SC_NPROCESSORS_ONLN);
    int nworkers = (ncores > 0 && ncores <= MAX_WORKERS) ? (int)ncores : 4;
    if (argc > 1)
        port = atoi(argv[1]);
    if (argc > 2)
    {
        int w = atoi(argv[2]);
        if (w > 0 && w <= MAX_WORKERS)
            nworkers = w;
    }
    if (argc > 3)
        log_init(argv[3]); /* optional: ./creamsiql [port] [workers] [logfile] */

    signal(SIGPIPE, SIG_IGN); /* a dead client socket must not kill the server */
    signal(SIGINT, on_shutdown_signal);
    signal(SIGTERM, on_shutdown_signal);

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        oerr("cannot create socket");
        return 1;
    }
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        oerr("cannot bind");
        return 1;
    }
    if (listen(listen_fd, 128) < 0)
    {
        oerr("cannot listen");
        return 1;
    }

    out("CReaMSiQL server\n");
    out("  listening on port ");
    out_int(port);
    out("\n");
    out("  workers: ");
    out_int(nworkers);
    out(" + 1 writer\n");
    out("  CREATE TABLE t (id INDEX, name, email INDEX)\n");
    out("  INSERT INTO t VALUES ('1', 'Alice', 'alice@x.com')\n");
    out("  SELECT * FROM t [WHERE col = val [AND col = val ...]]\n");
    out("  SELECT COUNT FROM t [WHERE ...]\n\n");
    log_line("STARTUP port=%d workers=%d log=%s", port, nworkers, argc > 3 ? argv[3] : "(console only)");

    pthread_t writer_tid;
    pthread_create(&writer_tid, NULL, writer_main, NULL);

    pthread_t workers[MAX_WORKERS];
    for (int i = 0; i < nworkers; i++)
        pthread_create(&workers[i], NULL, worker_main, NULL);

    for (int i = 0; i < nworkers; i++)
        pthread_join(workers[i], NULL);

    /* every worker has stopped accepting; wake the writer so it can
     * drain whatever's left in the queue and exit cleanly */
    pthread_mutex_lock(&qlock);
    pthread_cond_broadcast(&q_has_work);
    pthread_mutex_unlock(&qlock);
    pthread_join(writer_tid, NULL);

    log_line("SHUTDOWN");
    if (log_fd >= 0)
        close(log_fd);
    return 0;
}