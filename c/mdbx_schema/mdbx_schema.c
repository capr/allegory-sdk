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
		- multi-value keys are 0-terminated so they are not 8-bit clean!

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
The offset table contains offsets of dyn_offset_size bytes.

Key records are encoded differently than val records because keys are encoded
for lexicographic binary ordering, which means: no nulls, no offset table for
varsize fields, instead we use \0 as separator, so no 8-bit clean varsize
keys either, value bits are negated for descending order, ints and floats are
encoded so that bit order matches numeric order.

*/
typedef struct schema_col {
	int   len; // for varsize cols it means max len.
	bool8 fixsize; // fixed size array (padded) or varsize.
	bool8 descending; // for key cols
	u8    type; // schema_col_type
	u8    elem_size_shift; // computed
	bool8 static_offset; // computed: decides what the .offset field means
	int   offset; // computed: a static offset or the offset where the dyn. offset is.
} schema_col;

typedef struct schema_table {
	schema_col* key_cols;
	schema_col* val_cols;
	u16  n_key_cols;
	u16  n_val_cols;
	u8   dyn_offset_size; // 1,2,4
} schema_table;

int  schema_get(schema_table* tbl, int is_key, int col_i, void* rec, int rec_size, void* out, int out_len);
void schema_set(schema_table* tbl, int is_key, int col_i, void* rec, int rec_size, void* in , int in_len, void **pp, int add);
int  schema_is_null(int col_i, void* rec);

// implementation ------------------------------------------------------------

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

static int is_null(void* rec, int col_i) {
	int byte_i = col_i >> 3;
	int bit_i  = col_i & 7;
	int mask   = 1 << bit_i;
	u8* p = rec;
	return (p[byte_i] & mask) != 0;
}

static void set_null(void* rec, int col_i, int is_null) {
	int byte_i = col_i >> 3;
	int bit_i  = col_i & 7;
	int mask   = 1 << bit_i;
	u8* p = rec;
	if (is_null)
		p[byte_i] = p[byte_i] | mask;
	else
		p[byte_i] = p[byte_i] & ~mask;
}

static int scan_end(schema_col* col, void* p, int len) { // scan for 0 up-to size
	int ss = col->elem_size_shift;
	if (ss == 0) {
		uint8_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (ss == 1) {
		uint16_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (ss == 2) {
		uint32_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else if (ss == 3) {
		uint64_t* q = p;
		for (int i = 0; i < len; i++)
			if (*q++ == 0)
				return i;
	} else {
		assert(0);
	}
	return len;
}

static int get_dyn_offset(schema_table* tbl, schema_col* col, void* rec) {
	if (tbl->dyn_offset_size == 1) return *(u8* )(rec + col->offset);
	if (tbl->dyn_offset_size == 2) return *(u16*)(rec + col->offset);
	if (tbl->dyn_offset_size == 4) return *(int*)(rec + col->offset);
	assert(0);
}

static void set_dyn_offset(schema_table* tbl, schema_col* col, void* rec, int offset) {
	if (tbl->dyn_offset_size == 1) *(u8* )(rec + col->offset) = offset;
	if (tbl->dyn_offset_size == 2) *(u16*)(rec + col->offset) = offset;
	if (tbl->dyn_offset_size == 4) *(int*)(rec + col->offset) = offset;
	assert(0);
}

static inline int get_key_mem_size(schema_table* tbl, schema_col* col,
	void *p
) {
	int max_len = col->len;
	if (col->fixsize)
		return max_len;
	int len = scan_end(col, p, max_len);
	if (len < max_len) // 0-terminated.
		len++;
	return len << col->elem_size_shift;
}

static inline void* get_next_ptr(schema_table* tbl, int is_key, schema_col* col,
	schema_col* next_col,
	void* rec,
	void* p
) {
	if (next_col->static_offset) {
		return rec + next_col->offset;
	} else if (is_key) { // key col at dyn. offset
		return p + get_key_mem_size(tbl, col, p);
	} else { // val col at dyn. offset
		return rec + get_dyn_offset(tbl, next_col, rec);
	}
}

static void* get_ptr(schema_table* tbl, int is_key, int col_i, schema_col* col,
	void* rec
) {
	if (col->static_offset) {
		return rec + col->offset;
	} else if (is_key) { // key col at dyn. offset
		void* p = rec;
		for (int col_i = 0; col_i < col_i-1; col_i++)
			p += get_key_mem_size(tbl, &tbl->key_cols[col_i], p);
		return p;
	} else { // val col at dyn. offset
		return rec + get_dyn_offset(tbl, col, rec);
	}
}

int get_len(schema_table* tbl, int is_key, int col_i, schema_col* col, void* rec, void* p, int rec_size) {
	if (col->fixsize)
		return col->len;
	if (is_key) { // varsize key col
		if (!p)
			p = get_ptr(tbl, is_key, col_i, col, rec);
		return scan_end(col, p, col->len);
	} else { // varsize val col
		int offset = get_dyn_offset(tbl, col, rec);
		if (col_i == tbl->n_val_cols-1) { // last col
			return (rec_size - offset) >> col->elem_size_shift;
		} else { // non-last col
			schema_col* next_col = &tbl->val_cols[col_i+1];
			int next_offset = get_dyn_offset(tbl, next_col, rec);
			return (next_offset - offset) >> col->elem_size_shift;
		}
	}
}

static inline schema_col* get_col(schema_table* tbl, int is_key, int col_i) {
	int n_cols = is_key ? tbl->n_key_cols : tbl->n_val_cols;
	schema_col* cols = is_key ? tbl->key_cols : tbl->val_cols;
	return col_i >= 0 && col_i < n_cols ? &cols[col_i] : 0;
}

int schema_get(schema_table* tbl, int is_key, int col_i,
	void* rec, int rec_size,
	void* out, int out_len
) {
	int ret = 0;
	schema_col* col = get_col(tbl, is_key, col_i);
	assert(col);
	if (!is_key) {
		if (is_null(rec, col_i))
			return 1; // signal null
	}
	void* p = get_ptr(tbl, is_key, col_i, col, rec);
	int in_len = get_len(tbl, is_key, col_i, col, rec, p, rec_size);
	int copy_len = out_len;
	if (in_len < out_len) {
		out_len = in_len;
		ret = -1; // signal truncation
	}
	int ss = col->elem_size_shift;
	if (!is_key) { // val col, copy
		memmove(out, p, copy_len << ss);
		return ret;
	}
	// key col, decode
	if (col->descending) { // invert bits
		invert_bits(out, p, copy_len << ss);
		p = out; // will decode in-place then.
	}
	encdec_t decode = decoders[col->type];
	for (int i = 0; i < copy_len; i++) {
		decode(p, out);
		p   += (1 << ss);
		out += (1 << ss);
	}
	return ret;
}

int schema_is_null(int col_i, void* rec) {
	return is_null(rec, col_i);
}

// update the dyn. offset of the next col if there is one.
static inline void set_next_dyn_offset(schema_table* tbl, int col_i,
	void* rec, void* p, int mem_size
) {
	if (col_i == tbl->n_val_cols-1)
		return;
	schema_col* next_col = &tbl->val_cols[col_i+1];
	if (next_col->static_offset)
		return;
	set_dyn_offset(tbl, next_col, rec, p + mem_size - rec);
}

static inline void resize_varsize(
	schema_table* tbl, int is_key, int col_i, schema_col* col,
	void* rec, int cur_rec_size,
	void* p,
	int mem_size
) {
	schema_col* next_col = get_col(tbl, is_key, col_i+1);
	if (!next_col)
		return;
	void* next_p = get_next_ptr(tbl, is_key, col, next_col, rec, p);
	void* new_next_p = p + mem_size;
	int next_mem_size = rec + cur_rec_size - next_p;
	memmove(new_next_p, next_p, next_mem_size);
	int shift_size = new_next_p - next_p; // positive means grow.
	if (!is_key) {
		// shift all dyn. offsets from next_col on.
		for (int i = col_i+1; i < tbl->n_val_cols; i++) {
			schema_col* col = &tbl->val_cols[i];
			int offset = get_dyn_offset(tbl, col, rec);
			set_dyn_offset(tbl, col, rec, offset + shift_size);
		}
	}
}

void schema_set(schema_table* tbl, int is_key, int col_i,
	void* rec, int cur_rec_size,
	void* in, int in_len,
	void** pp, int add
) {
	schema_col* col = get_col(tbl, is_key, col_i);
	assert(col);
	int ss = col->elem_size_shift;

	if (!in)
		assert(!in_len);

	if (!is_key)
		set_null(rec, col_i, !in);

	void* p = pp && *pp ? *pp : get_ptr(tbl, is_key, col_i, col, rec);

	int copy_len = (col->len < in_len ? col->len : in_len); // truncate input
	int copy_size = copy_len << ss;

	// non-last varsize key col: can't contain 0, so stop at first 0 if found.
	if (is_key && !col->fixsize && col_i < tbl->n_key_cols-1) {
		copy_len = scan_end(col, in, copy_len);
		copy_size = copy_len << ss;
	}

	// figure out mem_size and adjust rec before copying the data.
	int mem_size; // size of this value in memory.
	if (col->fixsize) {
		mem_size = col->len << ss;
	} else {
		mem_size = copy_size;
		// non-last varsize key col with len < max len: 0-terminate.
		if (is_key && col_i < tbl->n_key_cols-1 && copy_len < col->len)
			mem_size += (1 << ss);
		if (!add) {
			// varsize key or val set: resize (shrink or lengthen).
			resize_varsize(tbl, is_key, col_i, col, rec, cur_rec_size, p, mem_size);
		} else if (!is_key) {
			// varsize val add: set offset of next col for next add.
			set_next_dyn_offset(tbl, col_i, rec, p, mem_size);
		} else {
			// varsize key add: nothing to do.
		}
	}
	// zero-pad (whether fixsize or 0-terminated).
	memset(p + copy_size, 0, mem_size - copy_size);

	if (!is_key) {

		// val col: just copy
		memmove(p, in, copy_size);

	} else {

		// key col: encode for lexicographic binary ordering
		encdec_t encode = encoders[col->type];
		for (int o = 0; o < copy_size; o += (1 << ss))
			encode(in + o, p + o);

		// descending key col: invert bits (including padding and terminator).
		if (col->descending)
			invert_bits(p, p, mem_size);

	}

	if (pp)
		*pp = p + mem_size;

}
