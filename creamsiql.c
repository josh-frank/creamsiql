/*
 * CReaMSiQL — a minimal append-only SQL engine in one C file
 *
 * Supports:
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
 * Design:
 *   - Zero deps beyond POSIX: open/read/write/pread/pwrite/lseek/ftruncate
 *   - Compile: cc creamsiql.c -o creamsiql
 *   - No malloc. One static token array; all I/O via fd + pread/pwrite.
 *   - No AST. Parse → act in one pass; memory touched in one place.
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
 */

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

/* ─── tuneable limits ───────────────────────────────────────────── */
#define MAX_TOKENS      128
#define MAX_COLS          8
#define MAX_CONDS         8
#define MAX_VALS          8
#define COL_LEN          64
#define VAL_LEN         256
#define LINE_LEN        512
#define IDX_BUCKETS    4096   /* primary hash slots                  */
#define IDX_OVERFLOW   1024   /* overflow pool                       */
#define IDX_TOTAL      (IDX_BUCKETS + IDX_OVERFLOW)
#define IDX_HDR_SIZE     16
#define IDX_BUCKET_SZ    24

/* ─── token types ───────────────────────────────────────────────── */
typedef enum {
    TOK_CREATE, TOK_INSERT, TOK_SELECT, TOK_WHERE, TOK_AND,
    TOK_INTO,   TOK_FROM,   TOK_TABLE,  TOK_VALUES, TOK_INDEX,
    TOK_COUNT,
    TOK_STAR, TOK_EQUALS, TOK_COMMA, TOK_LPAREN, TOK_RPAREN,
    TOK_IDENT, TOK_STRING, TOK_NUMBER,
    TOK_EOF, TOK_ERROR
} TokType;

typedef struct { TokType type; char value[VAL_LEN]; } Token;

/* ─── globals ───────────────────────────────────────────────────── */
static Token tokens[MAX_TOKENS];
static int   ntok = 0;
static int   pos  = 0;

/* ─── output helpers ────────────────────────────────────────────── */
static void out(const char *s) { write(1, s, strlen(s)); }
static void oerr(const char *s) { out("error: "); out(s); out("\n"); }

/* ─── integer to decimal string ─────────────────────────────────── */
static void out_int(int v) {
    char s[32]; int i = sizeof(s) - 1; s[i] = '\0';
    if (v == 0) { s[--i] = '0'; }
    else { while (v > 0) { s[--i] = '0' + v % 10; v /= 10; } }
    out(s + i);
}

/* ─── FNV-1a hash ───────────────────────────────────────────────── */
static uint64_t fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h ? h : 1;   /* 0 is reserved as empty-slot sentinel */
}

/* ─── lexer ─────────────────────────────────────────────────────── */
static void add_tok(TokType t, const char *v) {
    if (ntok >= MAX_TOKENS) return;
    tokens[ntok].type = t;
    strncpy(tokens[ntok].value, v, VAL_LEN - 1);
    tokens[ntok].value[VAL_LEN - 1] = '\0';
    ntok++;
}

static TokType keyword(const char *s) {
    if (!strcmp(s,"CREATE")) return TOK_CREATE;
    if (!strcmp(s,"INSERT")) return TOK_INSERT;
    if (!strcmp(s,"SELECT")) return TOK_SELECT;
    if (!strcmp(s,"WHERE"))  return TOK_WHERE;
    if (!strcmp(s,"AND"))    return TOK_AND;
    if (!strcmp(s,"INTO"))   return TOK_INTO;
    if (!strcmp(s,"FROM"))   return TOK_FROM;
    if (!strcmp(s,"TABLE"))  return TOK_TABLE;
    if (!strcmp(s,"VALUES")) return TOK_VALUES;
    if (!strcmp(s,"INDEX"))  return TOK_INDEX;
    if (!strcmp(s,"COUNT"))  return TOK_COUNT;
    return TOK_IDENT;
}

static void lex(const char *input) {
    ntok = 0; pos = 0;
    const char *p = input;
    char buf[VAL_LEN];

    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        if (isalpha((unsigned char)*p) || *p == '_') {
            int i = 0;
            while (*p && (isalnum((unsigned char)*p) || *p == '_') && i < VAL_LEN-1)
                buf[i++] = (char)toupper((unsigned char)*p++);
            buf[i] = '\0';
            add_tok(keyword(buf), buf);

        } else if (isdigit((unsigned char)*p)) {
            int i = 0;
            while (*p && isdigit((unsigned char)*p) && i < VAL_LEN-1) buf[i++] = *p++;
            buf[i] = '\0';
            add_tok(TOK_NUMBER, buf);

        } else if (*p == '\'') {
            p++; int i = 0;
            while (*p && *p != '\'' && i < VAL_LEN-1) buf[i++] = *p++;
            if (*p == '\'') p++;
            buf[i] = '\0';
            add_tok(TOK_STRING, buf);

        } else {
            char sym[2] = { *p++, '\0' };
            switch (sym[0]) {
                case '*': add_tok(TOK_STAR,   sym); break;
                case '=': add_tok(TOK_EQUALS, sym); break;
                case ',': add_tok(TOK_COMMA,  sym); break;
                case '(': add_tok(TOK_LPAREN, sym); break;
                case ')': add_tok(TOK_RPAREN, sym); break;
                default:  add_tok(TOK_ERROR,  sym); break;
            }
        }
    }
    add_tok(TOK_EOF, "");
}

/* ─── parser helpers ────────────────────────────────────────────── */
static Token peek(void)    { return tokens[pos]; }
static Token advance(void) { return tokens[pos++]; }
static int expect(TokType t) {
    if (tokens[pos].type == t) { pos++; return 1; }
    oerr("unexpected token"); return 0;
}

/* ─── path builders ─────────────────────────────────────────────── */
static void make_path(const char *name, const char *ext, char *buf, int sz) {
    int i = 0;
    while (*name && i < sz-16) buf[i++] = *name++;
    buf[i++] = '.';
    while (*ext  && i < sz-1)  buf[i++] = *ext++;
    buf[i] = '\0';
}

/* "TNAME.COLNAME.idx" */
static void make_idx_path(const char *t, const char *col, char *buf, int sz) {
    int i = 0;
    while (*t   && i < sz-72) buf[i++] = *t++;
    buf[i++] = '.';
    while (*col && i < sz-8)  buf[i++] = *col++;
    const char *e = ".idx";
    while (*e   && i < sz-1)  buf[i++] = *e++;
    buf[i] = '\0';
}

/* ─── streaming line reader (no full-file slurp) ─────────────────── */
/*
 * Reads one line from fd at its current position into buf (up to bufsz-1).
 * Strips the trailing newline. Returns chars read (0 for blank), -1 at EOF.
 */
static int fd_readline(int fd, char *buf, int bufsz) {
    int i = 0; char c;
    while (i < bufsz - 1) {
        if (read(fd, &c, 1) <= 0) { buf[i] = '\0'; return (i > 0) ? i : -1; }
        if (c == '\n') break;
        buf[i++] = c;
    }
    buf[i] = '\0';
    return i;
}

static void fd_puts(int fd, const char *s) { write(fd, s, strlen(s)); }

/* ─── schema ─────────────────────────────────────────────────────── */
typedef struct { char name[COL_LEN]; int indexed; } ColDef;

static void write_schema(const char *tname, ColDef *cols, int ncols) {
    char path[LINE_LEN];
    make_path(tname, "schema", path, LINE_LEN);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { oerr("cannot create schema file"); return; }
    for (int i = 0; i < ncols; i++) {
        fd_puts(fd, cols[i].name);
        if (cols[i].indexed) fd_puts(fd, ":INDEX");
        fd_puts(fd, "\n");
    }
    close(fd);
}

static int read_schema(const char *tname, ColDef *cols, int *ncols) {
    char path[LINE_LEN];
    make_path(tname, "schema", path, LINE_LEN);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    *ncols = 0;
    char line[COL_LEN + 8];
    int n;
    while (*ncols < MAX_COLS && (n = fd_readline(fd, line, sizeof(line))) >= 0) {
        if (n == 0) continue;
        char *colon = strchr(line, ':');
        if (colon) { *colon = '\0'; cols[*ncols].indexed = !strcmp(colon+1,"INDEX"); }
        else        { cols[*ncols].indexed = 0; }
        strncpy(cols[*ncols].name, line, COL_LEN-1);
        cols[*ncols].name[COL_LEN-1] = '\0';
        (*ncols)++;
    }
    close(fd);
    return (*ncols > 0);
}

/* ─── index: fixed-size open-addressing hash table on disk ──────── */

static void u64le_pack(uint8_t *b, uint64_t v) {
    for (int i = 0; i < 8; i++) { b[i] = (uint8_t)(v & 0xff); v >>= 8; }
}
static uint64_t u64le_unpack(const uint8_t *b) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | b[i];
    return v;
}

static off_t bucket_off(uint64_t idx) {
    return (off_t)(IDX_HDR_SIZE + idx * IDX_BUCKET_SZ);
}

static void idx_read_bucket(int fd, uint64_t idx,
                            uint64_t *h, uint64_t *d, uint64_t *chain) {
    uint8_t b[IDX_BUCKET_SZ];
    if (pread(fd, b, IDX_BUCKET_SZ, bucket_off(idx)) != IDX_BUCKET_SZ)
        { *h = *d = *chain = 0; return; }
    *h     = u64le_unpack(b);
    *d     = u64le_unpack(b + 8);
    *chain = u64le_unpack(b + 16);
}

static void idx_write_bucket(int fd, uint64_t idx,
                             uint64_t h, uint64_t d, uint64_t chain) {
    uint8_t b[IDX_BUCKET_SZ];
    u64le_pack(b,      h);
    u64le_pack(b + 8,  d);
    u64le_pack(b + 16, chain);
    pwrite(fd, b, IDX_BUCKET_SZ, bucket_off(idx));
}

/* allocate one overflow bucket; returns its index, or 0 on exhaustion */
static uint64_t idx_alloc_overflow(int fd) {
    uint8_t hdr[IDX_HDR_SIZE];
    if (pread(fd, hdr, IDX_HDR_SIZE, 0) != IDX_HDR_SIZE) return 0;
    uint64_t next = u64le_unpack(hdr + 8);
    if (next >= IDX_TOTAL) { oerr("index overflow pool exhausted"); return 0; }
    u64le_pack(hdr + 8, next + 1);
    pwrite(fd, hdr, IDX_HDR_SIZE, 0);
    return next;
}

static void idx_create(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    uint8_t hdr[IDX_HDR_SIZE];
    u64le_pack(hdr,     (uint64_t)IDX_TOTAL);
    u64le_pack(hdr + 8, (uint64_t)IDX_BUCKETS); /* first free overflow slot */
    write(fd, hdr, IDX_HDR_SIZE);
    uint8_t zero[IDX_BUCKET_SZ]; memset(zero, 0, sizeof(zero));
    for (int i = 0; i < IDX_TOTAL; i++) write(fd, zero, IDX_BUCKET_SZ);
    close(fd);
}

static void idx_insert(int fd, uint64_t hash, uint64_t data_off) {
    uint64_t slot = hash % IDX_BUCKETS;
    uint64_t h, d, chain;
    idx_read_bucket(fd, slot, &h, &d, &chain);

    if (h == 0) {
        /* empty primary slot — write directly */
        idx_write_bucket(fd, slot, hash, data_off, 0);
        return;
    }
    /* walk to end of chain, then link a new overflow bucket */
    uint64_t cur = slot;
    while (chain != 0) { cur = chain; idx_read_bucket(fd, cur, &h, &d, &chain); }
    uint64_t ov = idx_alloc_overflow(fd);
    if (!ov) return;
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

static void idx_lookup(int ifd, int dfd, const char *val, hit_cb cb, void *ud) {
    uint64_t hash = fnv1a(val);
    uint64_t slot = hash % IDX_BUCKETS;
    uint64_t h, d, chain;
    idx_read_bucket(ifd, slot, &h, &d, &chain);
    if (h == 0) return;

    uint64_t cur = slot;
    do {
        idx_read_bucket(ifd, cur, &h, &d, &chain);
        if (h == hash) cb(dfd, d, ud);
        cur = chain;
    } while (cur != 0);
}

/* ─── row helpers ───────────────────────────────────────────────── */

/* split pipe-delimited line in-place; returns field count */
static int split_row(char *line, char *fields[], int max) {
    int n = 0;
    fields[n++] = line;
    while (*line && n < max) {
        if (*line == '|') { *line = '\0'; fields[n++] = line + 1; }
        line++;
    }
    /* strip trailing newline */
    char *last = fields[n-1]; int len = (int)strlen(last);
    if (len > 0 && last[len-1] == '\n') last[len-1] = '\0';
    return n;
}

/*
 * Condition list — used by both index callbacks and full-scan
 * skip_first: when arriving via index we already know conds[0] matched
 */
typedef struct { char col[COL_LEN]; char val[VAL_LEN]; } Cond;

typedef struct {
    ColDef *cols;  int ncols;
    Cond   *conds; int nconds;
    int     skip_first;   /* index path: first cond already satisfied */
    int     count_only;
    int     matched;
} MatchCtx;

/* read the row at data_off from dfd, check conds, print or count */
static void try_row(int dfd, uint64_t data_off, MatchCtx *ctx) {
    char line[LINE_LEN];
    ssize_t n = pread(dfd, line, LINE_LEN-1, (off_t)data_off);
    if (n <= 0) return;
    /* null-terminate at first newline */
    int len = 0;
    while (len < n && line[len] != '\n') len++;
    line[len] = '\0';

    char  copy[LINE_LEN];
    char *fields[MAX_COLS];
    strncpy(copy, line, LINE_LEN-1); copy[LINE_LEN-1] = '\0';
    int nf = split_row(copy, fields, MAX_COLS);

    int start = ctx->skip_first ? 1 : 0;
    for (int c = start; c < ctx->nconds; c++) {
        int ci = -1;
        for (int j = 0; j < ctx->ncols; j++)
            if (!strcmp(ctx->cols[j].name, ctx->conds[c].col)) { ci = j; break; }
        if (ci < 0 || ci >= nf || strcmp(fields[ci], ctx->conds[c].val) != 0) return;
    }

    ctx->matched++;
    if (ctx->count_only) return;
    for (int j = 0; j < nf; j++) { out(fields[j]); out(j < nf-1 ? " | " : "\n"); }
}

/* callback shim for idx_lookup */
static void on_idx_hit(int dfd, uint64_t data_off, void *ud) {
    try_row(dfd, data_off, (MatchCtx *)ud);
}

/* ─── data: append row + update indexes ─────────────────────────── */
static void append_row(const char *tname, ColDef *cols, int ncols,
                       char vals[][VAL_LEN], int nvals) {
    char path[LINE_LEN];
    make_path(tname, "data", path, LINE_LEN);
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) { oerr("cannot open data file"); return; }

    /* record offset before writing — stable forever (append-only) */
    off_t row_off = lseek(fd, 0, SEEK_CUR);
    for (int i = 0; i < nvals; i++) {
        fd_puts(fd, vals[i]);
        fd_puts(fd, i < nvals-1 ? "|" : "\n");
    }
    close(fd);

    /* update indexes */
    char idxpath[LINE_LEN];
    for (int i = 0; i < ncols && i < nvals; i++) {
        if (!cols[i].indexed) continue;
        make_idx_path(tname, cols[i].name, idxpath, LINE_LEN);
        int ifd = open(idxpath, O_RDWR);
        if (ifd < 0) { oerr("cannot open index file"); continue; }
        idx_insert(ifd, fnv1a(vals[i]), (uint64_t)row_off);
        close(ifd);
    }
    out("row inserted\n");
}

/* ─── select core ───────────────────────────────────────────────── */
static void do_select_scan(const char *tname, ColDef *cols, int ncols,
                           Cond *conds, int nconds, int count_only) {
    /* header */
    if (!count_only) {
        for (int i = 0; i < ncols; i++) { out(cols[i].name); out(i<ncols-1?" | ":"\n"); }
        for (int i = 0; i < ncols; i++) { out("--------");    out(i<ncols-1?"-+-":"---\n"); }
    }

    char data_path[LINE_LEN];
    make_path(tname, "data", data_path, LINE_LEN);
    int dfd = open(data_path, O_RDONLY);
    if (dfd < 0) {
        if (count_only) { out("0\n"); } else { out("(no rows)\n"); }
        return;
    }

    MatchCtx ctx = { cols, ncols, conds, nconds, 0, count_only, 0 };

    /* try index on first WHERE condition */
    int used_index = 0;
    if (nconds > 0) {
        int ci = -1;
        for (int j = 0; j < ncols; j++)
            if (!strcmp(cols[j].name, conds[0].col) && cols[j].indexed) { ci=j; break; }
        if (ci >= 0) {
            char idxpath[LINE_LEN];
            make_idx_path(tname, cols[ci].name, idxpath, LINE_LEN);
            int ifd = open(idxpath, O_RDONLY);
            if (ifd >= 0) {
                ctx.skip_first = 1;
                idx_lookup(ifd, dfd, conds[0].val, on_idx_hit, &ctx);
                close(ifd);
                used_index = 1;
            }
        }
    }

    /* full linear scan (no index available or no WHERE clause) */
    if (!used_index) {
        ctx.skip_first = 0;
        lseek(dfd, 0, SEEK_SET);
        char line[LINE_LEN];
        int  n;
        while ((n = fd_readline(dfd, line, LINE_LEN)) >= 0) {
            if (n == 0) continue;
            /* reconstruct data_off: current position minus the line just read
               (fd_readline consumed n chars + 1 newline)                      */
            off_t cur = lseek(dfd, 0, SEEK_CUR);
            uint64_t row_off = (uint64_t)(cur - n - 1);

            /* filter via try_row (which re-reads via pread — cheap, consistent) */
            try_row(dfd, row_off, &ctx);
        }
    }

    close(dfd);

    if (count_only) { out_int(ctx.matched); out("\n"); return; }
    if (!ctx.matched) out("(no rows matched)\n");
}

/* ─── command handlers ──────────────────────────────────────────── */
static void do_create(void) {
    if (!expect(TOK_CREATE) || !expect(TOK_TABLE)) return;
    if (peek().type != TOK_IDENT) { oerr("expected table name"); return; }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN-1); tname[COL_LEN-1]='\0';

    if (!expect(TOK_LPAREN)) return;
    ColDef cols[MAX_COLS]; int ncols = 0;
    while (peek().type == TOK_IDENT && ncols < MAX_COLS) {
        strncpy(cols[ncols].name, advance().value, COL_LEN-1);
        cols[ncols].name[COL_LEN-1] = '\0';
        cols[ncols].indexed = 0;
        if (peek().type == TOK_INDEX) { advance(); cols[ncols].indexed = 1; }
        ncols++;
        if (peek().type == TOK_COMMA) advance();
    }
    if (!expect(TOK_RPAREN)) return;

    write_schema(tname, cols, ncols);

    char idxpath[LINE_LEN];
    for (int i = 0; i < ncols; i++) {
        if (!cols[i].indexed) continue;
        make_idx_path(tname, cols[i].name, idxpath, LINE_LEN);
        idx_create(idxpath);
    }
    out("table created\n");
}

static void do_insert(void) {
    if (!expect(TOK_INSERT) || !expect(TOK_INTO)) return;
    if (peek().type != TOK_IDENT) { oerr("expected table name"); return; }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN-1); tname[COL_LEN-1]='\0';

    ColDef cols[MAX_COLS]; int ncols = 0;
    if (!read_schema(tname, cols, &ncols)) { oerr("table not found"); return; }

    if (!expect(TOK_VALUES) || !expect(TOK_LPAREN)) return;
    char vals[MAX_VALS][VAL_LEN]; int nvals = 0;
    while (peek().type != TOK_RPAREN && peek().type != TOK_EOF && nvals < MAX_VALS) {
        Token t = advance();
        if (t.type == TOK_COMMA) continue;
        strncpy(vals[nvals++], t.value, VAL_LEN-1);
    }
    if (!expect(TOK_RPAREN)) return;
    if (nvals != ncols) { oerr("column count mismatch"); return; }
    append_row(tname, cols, ncols, vals, nvals);
}

static void do_select(void) {
    if (!expect(TOK_SELECT)) return;
    int count_only = 0;
    if      (peek().type == TOK_COUNT) { advance(); count_only = 1; }
    else if (!expect(TOK_STAR)) return;

    if (!expect(TOK_FROM)) return;
    if (peek().type != TOK_IDENT) { oerr("expected table name"); return; }
    char tname[COL_LEN];
    strncpy(tname, advance().value, COL_LEN-1); tname[COL_LEN-1]='\0';

    ColDef cols[MAX_COLS]; int ncols = 0;
    if (!read_schema(tname, cols, &ncols)) { oerr("table not found"); return; }

    Cond conds[MAX_CONDS]; int nconds = 0;
    if (peek().type == TOK_WHERE) {
        advance();
        while (nconds < MAX_CONDS) {
            if (peek().type != TOK_IDENT) { oerr("expected column name"); return; }
            strncpy(conds[nconds].col, advance().value, COL_LEN-1);
            conds[nconds].col[COL_LEN-1] = '\0';
            if (!expect(TOK_EQUALS)) return;
            strncpy(conds[nconds].val, advance().value, VAL_LEN-1);
            conds[nconds].val[VAL_LEN-1] = '\0';
            nconds++;
            if (peek().type != TOK_AND) break;
            advance();
        }
    }

    /* validate condition column names */
    for (int c = 0; c < nconds; c++) {
        int found = 0;
        for (int j = 0; j < ncols; j++)
            if (!strcmp(cols[j].name, conds[c].col)) { found=1; break; }
        if (!found) { oerr("unknown column in WHERE"); return; }
    }

    do_select_scan(tname, cols, ncols, conds, nconds, count_only);
}

/* ─── REPL ──────────────────────────────────────────────────────── */
static int read_line(char *buf, int sz) {
    int i = 0; char c;
    while (i < sz - 1) {
        ssize_t n = read(0, &c, 1);
        if (n <= 0) { buf[i]='\0'; return (i > 0) ? i : -1; }
        if (c == '\n') break;
        buf[i++] = c;
    }
    buf[i] = '\0';
    return i;
}

int main(void) {
    char line[LINE_LEN];
    out("CReaMSiQL\n");
    out("  CREATE TABLE t (id INDEX, name, email INDEX)\n");
    out("  INSERT INTO t VALUES ('1', 'Alice', 'alice@x.com')\n");
    out("  SELECT * FROM t [WHERE col = val [AND col = val ...]]\n");
    out("  SELECT COUNT FROM t [WHERE ...]\n");
    out("  Ctrl-D to quit.\n\n");

    while (1) {
        out("creamsiql> ");
        if (read_line(line, LINE_LEN) < 0) { out("\nbye\n"); break; }
        if (!line[0]) continue;
        lex(line);
        switch (peek().type) {
            case TOK_CREATE: do_create(); break;
            case TOK_INSERT: do_insert(); break;
            case TOK_SELECT: do_select(); break;
            case TOK_EOF:    break;
            default: oerr("unknown command (try CREATE, INSERT, SELECT)"); break;
        }
    }
    return 0;
}
