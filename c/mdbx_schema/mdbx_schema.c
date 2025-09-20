//go@ plink -batch root@m1 sdk/c/mdbx_schema/build
/*

	Schema encoding and decoding for LMDB/LibMDBX.
	Written by Cosmin Apreutesei. Public Domain.

	Schema means partitioning lmdb keys and values into predefined columns,
	which allows composite keys and efficient storage of structured values.

	Data types:
		- ints: 8, 16, 32, 64 bit, signed/unsigned
		- floats: 32 and 64 bit
		- arrays: fixed-size and variable-size
		- nullable values

	Keys:
		- composite keys with per-field ascending/descending order.

	Limitations:
		- multi-value keys are \0-terminated so they are not 8-bit clean!

*/

#include <inttypes.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  bool8;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef float    f32;
typedef double   f64;

// API -----------------------------------------------------------------------

typedef enum schema_col_type {
	schema_col_type_i8,
	schema_col_type_i16,
	schema_col_type_i32,
	schema_col_type_i64,
	schema_col_type_u8,
	schema_col_type_u16,
	schema_col_type_u32,
	schema_col_type_u64,
	schema_col_type_f32,
	schema_col_type_f64,
} schema_col_type;

/*

In-memory layout:
 - key records: fixsize_cols, first_varsize_col, varoffset_cols (varsize or not).
 - val records: null_bits, fixsize_cols, offsets, first_varsize_col, varsize_cols.

Fixsize means scalar (len=1) or fixed-size array (zero-padded). The opposite
is varsize for which len in the definition means max len. Varsize values are
zero-terminated inside key records, so they are not 8-bit clean except the
last column. In value records an offset table is used instead so all columns
are 8-bit clean. The zero terminator is skipped for values with len = max len.
The offset table contains offsets of offset_size bytes.

Key records are encoded differently than val records because keys are encoded
for lexicographic binary ordering, which means: no nulls, no offset table for
varsize fields, instead we use \0 as separator, so no 8-bit clean varsize
keys either, value bits are negated for descending order, ints and floats are
encoded so that bit order matches numeric order.

*/
typedef struct schema_col {
	// definition
	schema_col_type type;
	u32      len; // for varsize cols it means max len.
	u16      index; // index in key_cols or in val_cols depending on is_key flag.
	bool8    fixsize; // fixed size array (padded) or varsize.
	bool8    descending; // for key cols
	// computed layout
	u32 offset; // -1 for dyn. offset cols
	u32 offset_offset; // offset in the buffer where the dyn. offset for this col is.
	u8  elem_size; // in bytes
} schema_col;

typedef struct schema_table {
	schema_col* key_cols;
	schema_col* val_cols;
	u16  n_key_cols;
	u16  n_val_cols;
	u8   offset_size; // 1,2,4
} schema_table;

int  schema_get(schema_table* tbl, int is_key, int col_i, void* buf, u64 rec_size, void* out, u64 out_len);
int  schema_set(schema_table* tbl, int is_key, int col_i, void* buf, u64 rec_size, void* in , u64 in_len);

// implementation ------------------------------------------------------------

static int scan_end(schema_col* col, void* p, int len) { // scan for \0 up-to len
	if (col->elem_size == 1) {
		uint8_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (col->elem_size == 2) {
		uint16_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (col->elem_size == 4) {
		uint32_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (col->elem_size == 8) {
		uint64_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else {
		assert(0);
	}
	return len;
}

static int get_offset(schema_table* tbl, schema_col* col, void* buf) {
	if (tbl->offset_size == 1) return *(u8* )(buf + col->offset_offset);
	if (tbl->offset_size == 2) return *(u16*)(buf + col->offset_offset);
	if (tbl->offset_size == 4) return *(u32*)(buf + col->offset_offset);
	assert(0);
}

static void* get_ptr(schema_table* tbl, int is_key, schema_col* col, void* buf) {
	if (col->offset != -1) // col at fixed offset
		return buf + col->offset;
	if (is_key) { // key col at dyn. offset
		void* p = buf;
		for (int col_i = 0; col_i < col->index-1; col_i++) {
			schema_col* col1 = &tbl->key_cols[col_i];
			if (col1->fixsize) {
				p += col1->len * col1->elem_size;
			} else { // varsize col
				p += scan_end(col1, p, col1->len);
			}
		}
		return p;
	} else { // val col at dyn. offset
		return buf + get_offset(tbl, col, buf);
	}
}

int get_len(schema_table* tbl, int is_key, schema_col* col, void* buf, void* p, int rec_size) {
	if (col->fixsize)
		return col->len;
	if (is_key) { // varsize key col
		if (!p) p = get_ptr(tbl, is_key, col, buf);
		return scan_end(col, p, col->len);
	} else { // varsize val col
		int offset = get_offset(tbl, col, buf);
		if (col->index == tbl->n_val_cols-1) { // last col
			return (rec_size - offset) / col->elem_size;
		} else { // non-last col
			schema_col* next_col = &tbl->val_cols[col->index + 1];
			int next_offset = get_offset(tbl, next_col, buf);
			return (next_offset - offset) / col->elem_size;
		}
	}
}

static void decode_u8(u8* s, u8* d) {
	*d = *s;
}
static void encode_u8(u8* s, u8* d) {
	*d = *s;
}
static void decode_u16(u16* s, u16* d) {
	*d = __builtin_bswap16(*s);
}
static void encode_u16(u16* s, u16* d) {
	*d = __builtin_bswap16(*s);
}
static void decode_u32(u32* s, u32* d) {
	*d = __builtin_bswap32(*s);
}
static void encode_u32(u32* s, u32* d) {
	*d = __builtin_bswap32(*s);
}
static void decode_u64(u64* s, u64* d) {
	*d = __builtin_bswap64(*s);
}
static void encode_u64(u64* s, u64* d) {
	*d = __builtin_bswap64(*s);
}
static void decode_i8(i8* s, i8 * d) {
	*d = *s ^ 0x80;
}
static void encode_i8(i8* s, i8 * d) {
	*d = *s ^ 0x80;
}
static void decode_i16(u16* s, i16* d) {
	*d = __builtin_bswap16(*s) ^ 0x8000;
}
static void encode_i16(i16* s, i16* d) {
	*d = __builtin_bswap16(*s) ^ 0x8000;
}
static void decode_i32(i32* s, i32* d) {
	*d = __builtin_bswap32(*s ^ 0x80000000);
}
static void encode_i32(i32* s, i32* d) {
	*d = __builtin_bswap32(*s ^ 0x80000000);
}
static void decode_i64(i64* s, i64* d) {
	*d = __builtin_bswap64(*s) ^ 0x8000000000000000ULL;
}
static void encode_i64(i64* s, i64* d) {
	*d = __builtin_bswap64(*s ^ 0x8000000000000000ULL);
}
static void decode_f32(u32* s, u32* d) {
	u32 v = __builtin_bswap32(*s);
	*d = v & 0x80000000 ? v ^ 0x80000000 : ~v;
}
static void encode_f32(u32* s, u32* d) {
	u32 v = *s; v = v & 0x80000000 ? ~v : v ^ 0x80000000;
	*d = __builtin_bswap32(v);
}
static void decode_f64(u64* s, u64* d) {
	u64 v = __builtin_bswap64(*s);
	*d = v & 0x8000000000000000ULL ? v ^ 0x8000000000000000ULL : ~v;
}
static void encode_f64(u64* s, u64* d) {
	u64 v = *s; v = v & 0x8000000000000000ULL ? ~v : v ^ 0x8000000000000000ULL;
	*d = __builtin_bswap64(v);
}

typedef void (*encdec_t)(void* s, void* d);

/* NOTE: must match schema_col_type enum order! */
static encdec_t decoders[] = {
	(encdec_t)&decode_i8,
	(encdec_t)&decode_i16,
	(encdec_t)&decode_i32,
	(encdec_t)&decode_i64,
	(encdec_t)&decode_u8,
	(encdec_t)&decode_u16,
	(encdec_t)&decode_u32,
	(encdec_t)&decode_u64,
	(encdec_t)&decode_f32,
	(encdec_t)&decode_f64,
};

/* NOTE: must match schema_col_type enum order! */
static encdec_t encoders[] = {
	(encdec_t)&encode_i8,
	(encdec_t)&encode_i16,
	(encdec_t)&encode_i32,
	(encdec_t)&encode_i64,
	(encdec_t)&encode_u8,
	(encdec_t)&encode_u16,
	(encdec_t)&encode_u32,
	(encdec_t)&encode_u64,
	(encdec_t)&encode_f32,
	(encdec_t)&encode_f64,
};

static inline schema_col* get_col(schema_table* tbl, int is_key, int col_i) {
	int n_cols = is_key ? tbl->n_key_cols : tbl->n_val_cols;
	schema_col* cols = is_key ? tbl->key_cols : tbl->val_cols;
	assert(col_i >= 0 && col_i < n_cols);
	return &cols[col_i];
}

static void invert_bits(void* d, void *s, int len) {
	u8 *s8 = s;
	u8 *d8 = d;
	u8 *d64 = s;
	int i = 0;
	for (i = 0; i + 8 <= len; i += 8) {
		u64 *s64 = (u64*)(s8 + i);
		*d64 = ~(*s64);
	}
	for (; i < len; i++) {
		d8[i] = ~(s8[i]);
	}
}

static int is_null(void* buf, int col_i) {
	int byte_i = col_i >> 3;
	int bit_i  = col_i & 7;
	int mask   = 1 << bit_i;
	u8* p = buf;
	return (p[byte_i] & mask) != 0;
}

static void set_null(void* buf, int col_i, int is_null) {
	int byte_i = col_i >> 3;
	int bit_i  = col_i & 7;
	int mask   = 1 << bit_i;
	u8* p = buf;
	if (is_null)
		p[byte_i] = p[byte_i] | mask;
	else
		p[byte_i] = p[byte_i] & ~mask;
}

int schema_get(schema_table* tbl, int is_key, int col_i,
	void* buf, u64 rec_size,
	void* out, u64 out_len
) {
	int ret = 0;
	schema_col* col = get_col(tbl, is_key, col_i);
	if (!is_key) {
		if (is_null(buf, col_i))
			return 1; // signal null
	}
	void* p = get_ptr(tbl, is_key, col, buf);
	int in_len = get_len(tbl, is_key, col, buf, p, rec_size);
	int copy_len = out_len;
	if (in_len < out_len) {
		out_len = in_len;
		ret = -1; // signal truncation
	}
	int elem_size = col->elem_size;
	if (!is_key) { // val col, copy
		memcpy(out, p, copy_len * elem_size);
		return ret;
	}
	// key col, decode
	if (col->descending) { // invert bits
		invert_bits(out, p, copy_len * elem_size);
		p = out; // will decode in-place then.
	}
	encdec_t decode = decoders[col->type];
	for (int i = 0; i < copy_len; i++) {
		decode(p, out);
		p   += elem_size;
		out += elem_size;
	}
	return ret;
}

int schema_set(schema_table* tbl, int is_key, int col_i,
	void* buf, u64 rec_size,
	void* in, u64 in_len
) {
	int ret = 0;
	schema_col* col = get_col(tbl, is_key, col_i);
	if (!is_key)
		set_null(buf, col_i, !in);
	else
		assert(in);
	void* p = get_ptr(tbl, is_key, col, buf);
	int max_len = col->len;
	int copy_len = in_len;
	if (max_len < in_len) {
		copy_len = max_len;
		ret = -1; // signal truncation
	}
	int elem_size = col->elem_size;
	if (col->fixsize) { // fixsize: pad
		int pad_len = max_len - copy_len;
		if (pad_len > 0)
			memset(p + copy_len, 0, pad_len * elem_size);
	}
	if (!is_key) { // val col, copy
		memcpy(p, in, copy_len * elem_size);
		return ret;
	}
	// key col, encode
	if (!col->fixsize && col_i < tbl->n_key_cols-1) {
		// varsize cols can't have embedded zeroes except the last one.
		// if \0 is detected, stop there.
		int data_len = scan_end(col, in, copy_len);
		if (data_len < copy_len) {
			copy_len = data_len;
			ret = -2; // signal truncation
		}
	}
	encdec_t encode = encoders[col->type];
	void* d = p;
	void* s = in;
	for (int i = 0; i < copy_len; i++) {
		encode(s, d);
		s += elem_size;
		d += elem_size;
	}
	if (!col->fixsize && col->index < tbl->n_key_cols-1) {
		// non-last varsize key col: null-terminate
		memset(d, 0, elem_size);
		copy_len++; // must invert the \0 too on descending
	}
	if (col->descending)
		invert_bits(p, p, copy_len * elem_size);
	return ret;
}

int schema_resize(schema_table* tbl, int is_key, int col_i,
	u64 rec_size, u64 len
) {
	int ret = 0;

	return ret;
};
