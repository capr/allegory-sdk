--go@ plink -t root@m1 sdk/bin/debian12/luajit sp2/sp.lua -v install forealz
--go @ c:/tools/plink -batch -i c:/users/woods/.ssh/id_ed25519.ppk root@172.20.10.3 sdk/bin/debian12/luajit sdk/lua/mdbx_schema.lua
--[[

	mdbx schema: structured data and multi-key indexing for mdbx.
	Written by Cosmin Apreutsei. Public Domain.

	Data types:
		- ints: 8, 16, 32, 64 bit, signed/unsigned
		- floats: 32 and 64 bit
		- arrays: fixed-size and variable-size
		- nullable values

	Keys:
		- composite keys with per-field ascending/descending order
		- utf-8 ai_ci collation

	Limitations:
		- keys are not nullable
		- varsize keys are 0-terminated so they are not 8-bit clean!

API, extends mdbx.lua API

	tx:put             (table_name|dbi, k1,..., v1,...)
	tx:put_records     (table_name|dbi, {{k1,...,v1,...},...})
	tx:is_null         (table_name|dbi, col, k1,...) -> is_null, [reason]
	tx:exists          (table_name|dbi, k1,...) -> record_exists, table_exists
	tx:get             (table_name|dbi, [cols], k1,...) -> vals...
	tx:try_get         (table_name|dbi, [cols], k1,...) -> true, vals... | false, err
	tx:[try_]del       (table_name|dbi, k1,...) -> true | nil,err
	tx:[try_]del_exact (table_name|dbi, k1,...,v1,...) -> true | nil,err

	cur:current ([cols]) -> keysvals...
	cur:next    ([cols]) -> keysvals...
	tx:each     (tbl_name, [cols]) -> cur, keysvals...

		cols format   |  vals...              | keyvals...
		--------------+-----------------------+------------
		'col1 ...'    |   col1_val,...        |  keycol1_val,..., col1_val,...
		'[col1 ...]'  |  {col1_val,...}       | {keycol1_val,..., col1_val,...}
		'{col1 ...}'  |  {col1=col1_val,...}  | {keycol1=keycol1_val,col1=col1_val}


TODO:
	- no-gc rec update: copy-rec, resize, set, put
	- update and upsert rec with single lookup
	- delete rec
	- autoincrement
	- reverse cursor
	- mdbx_env_chk()
	- range lookup cursor
	- DDL:
		- rename table, incl. layout
		- delete table, incl. layout
	- table migration:
		- copy all records to tmp table, delete old table, rename tmp table.
			- copy existing fields by name, use aka mapping to find old field.
				- copy using raw pointers into decoded values and encoded values.
	-

]]

require'mdbx'
require'utf8proc'
require'cjson' -- for null
local C = ffi.load'mdbx_schema' --see src/c/mdbx_schema/mdbx_schema.c

local
	typeof, num, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast =
	typeof, num, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast

assert(ffi.abi'le')

local Db = mdbx_db
local Tx = mdbx_tx
local Cur = mdbx_cur

cdef[[
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

typedef enum schema_col_type {
	schema_col_type_i8,
	schema_col_type_i16,
	schema_col_type_i32,
	schema_col_type_i64,
	schema_col_type_u8,
	schema_col_type_u16,
	schema_col_type_u32,
	schema_col_type_u64,
	schema_col_type_u32_le,
	schema_col_type_u64_le,
	schema_col_type_f32,
	schema_col_type_f64,
} schema_col_type;

typedef struct schema_col {
	int   len; // for varsize cols it means max len.
	bool8 fixed_size; // fixed size array (padded) or varsize.
	bool8 descending; // for key cols
	u8    type; // schema_col_type
	u8    elem_size_shift; // computed
	bool8 fixed_offset; // computed: decides what the .offset field means
	int   offset; // computed: a static offset or the offset where the dyn. offset is.
} schema_col;

typedef struct schema_table {
	schema_col* key_cols;
	schema_col* val_cols;
	u16  n_key_cols;
	u16  n_val_cols;
	u8   dyn_offset_size; // 1,2,4
} schema_table;

int schema_val_is_null(schema_table* tbl, int col_i,
	void* rec, int rec_size
);

int schema_get_key(schema_table* tbl, int col_i,
	void* rec, int rec_size,
	u8* out, int out_size,
	u8** pout,
	u8** pp
);

int schema_get_val(schema_table* tbl, int col_i,
	void* rec, int rec_size,
	u8** pout
);

void schema_key_add(schema_table* tbl, int col_i,
	void* rec, int rec_buf_size, int val_len,
	u8** pp
);

void schema_val_add_start(schema_table* tbl,
	void* rec, int rec_buf_size,
	u8** pp
);

void schema_val_add(schema_table* tbl, int col_i,
	void* rec, int rec_buf_size, int val_len,
	u8** pp
);
]]

local col_ct = {
	utf8 = 'u8',
}

local schema_col_types = {
	i8     = C.schema_col_type_i8,
	i16    = C.schema_col_type_i16,
	i32    = C.schema_col_type_i32,
	i64    = C.schema_col_type_i64,
	u8     = C.schema_col_type_u8,
	u16    = C.schema_col_type_u16,
	u32    = C.schema_col_type_u32,
	u64    = C.schema_col_type_u64,
	f32    = C.schema_col_type_f32,
	f64    = C.schema_col_type_f64,
	utf8   = C.schema_col_type_u8,
}

local buf = buffer(); buf(256)
local map_opt = bor(UTF8_DECOMPOSE, UTF8_CASEFOLD, UTF8_STRIPMARK)
local function encode_ai_ci(s, len)
	local out, out_sz = buf()
	local out, sz = utf8_map(s, len, map_opt, out, out_sz)
	if not out and sz > 0 then --buffer to small, reallocate and retry
		out, out_sz = buf(sz)
		out, sz = utf8_map(s, len, map_opt, out, out_sz)
	end
	return out, sz
end

--schema processing ----------------------------------------------------------

--create an optimal physical column layout based on a table schema.
local function compile_table_schema(schema, table_name, db_max_key_size)

	schema.name = table_name

	--index fields by name, typecheck, check for inconsistencies.
	for i,f in ipairs(schema.fields) do
		assertf(isstr(f.col) and #f.col > 0,
			'invalid field name: %s.%s', table_name, f.col)
		schema.fields[f.col] = f
		f.col_pos = i
		local elem_ct = col_ct[f.mdbx_type] or f.mdbx_type
		local ok, elem_ct = pcall(ctype, elem_ct)
		assertf(ok, 'unknown type %s for field: %s.%s', f.mdbx_type, table_name, f.col)
		f.elem_size = sizeof(elem_ct)
		assertf(f.elem_size < 2^8) --must fit 8 bit (see sort below)
		assertf(not (f.maxlen and f.len),
			'both maxlen and len specified for field: %s.%s', table_name, f.col)
	end

	--split fields into key_fields and val_fields.
	local key_fields = {}
	local val_fields = {}

	--parse pk and set f.descending.
	for i,col in ipairs(schema.pk) do
		local f = assertf(schema.fields[col],
			'pk col unknown: %s for table: %s', col, table_name)
		add(key_fields, f)
		f.key_index = #key_fields
		if schema.pk.desc then
			f.descending = schema.pk.desc[i]
		end
	end
	assert(#key_fields > 0, 'table missing pk: %s', table_name)

	--build val fields array with all fields that are not in pk.
	for i,f in ipairs(schema.fields) do
		if not f.key_index then --not a key field
			add(val_fields, f)
			f.val_index = #val_fields
		end
	end

	assert(#key_fields < 2^16)
	assert(#val_fields < 2^16)

	--move varsize fields at the end to minimize the size of the dyn offset table.
	--order fields by inverse elem_size to maintain alignment.
	--order by field index to get stable sorting.
	sort(val_fields, function(f1, f2)
		--elem_size fits in 8 bit; field index fits in 16 bit; 8+16 = 24 bits,
		--so any bit from bit 25+ can be used for extra conditions.
		local i1 = (f1.maxlen and 2^26 or 0) + (2^8-1 - f1.elem_size) * 2^16 + f1.col_pos
		local i2 = (f2.maxlen and 2^26 or 0) + (2^8-1 - f2.elem_size) * 2^16 + f2.col_pos
		return i1 < i2
	end)

	schema.key_fields = key_fields
	schema.val_fields = val_fields

	--store u32 and u64 simple keys in little-endian and use fast comparator.
	if #key_fields == 1 then
		local f = key_fields[1]
		if not f.descending and (f.mdbx_type == 'u32' or f.mdbx_type == 'u64') then
			schema.int_key = f.mdbx_type
		end
	end

	--compute key and val column layout.
	for _,fields in ipairs{key_fields, val_fields} do

		local is_val = fields == val_fields
		local is_key = fields == key_fields

		--find the number of fixsize fields.
		local fixsize_n = #fields
		for i,f in ipairs(fields) do
			if f.maxlen then --first varsize field
				fixsize_n = i-1
				break
			end
		end

		--compute max row size, just the data (which is what it is for keys).
		local max_rec_size = 0
		for _,f in ipairs(fields) do
			local maxlen = f.maxlen and f.maxlen + 1 or f.len or 1
			max_rec_size = max_rec_size + maxlen * f.elem_size
		end

		if is_key then
			assertf(max_rec_size <= db_max_key_size,
				'pk too big: %d bytes (max is %d bytes)',
					max_rec_size, db_max_key_size)
		end

		--compute dynamic offset table (d.o.t.) length for val records.
		--all val fields after the first varsize field are at a dyn offset.
		--key fields can't have an offset table instead we use \0 separator.
		local dot_len = is_val and max(0, #fields - fixsize_n - 1) or 0

		--compute the number of bytes needed to hold all the null bits.
		local nulls_size = is_val and ceil(#fields / 8) or 0

		--also compute d.o.t. size and update max_rec_size to include nulls and d.o.t.
		local dyn_offset_size = 0
		if is_val then
			max_rec_size = max_rec_size + nulls_size
			if max_rec_size + dot_len < 2^8 then
				dyn_offset_size = 1
			elseif max_rec_size + dot_len * 2 < 2^16 then
				dyn_offset_size = 2
			elseif max_rec_size + dot_len * 4 < 2^31 then
				dyn_offset_size = 4
			else
				assertf(false, 'value record too big for table: %s', table_name)
			end
			schema.dyn_offset_size = dyn_offset_size
			max_rec_size = max_rec_size + dot_len * dyn_offset_size
		end

		assertf(max_rec_size < 2^31,
			'record too big: %.0f bytes (max is 2GB-1) for table: %s',
			max_rec_size, table_name)

		fields.max_rec_size = max_rec_size

		local cur_offset = nulls_size + dot_len * dyn_offset_size
		for kv_index,f in ipairs(fields) do
			--compute and set fixed offsets and dyn. offset offsets.
			f.fixed_offset = kv_index <= fixsize_n+1 or nil
			if f.fixed_offset then
				f.offset = cur_offset
			end
			if kv_index <= fixsize_n then --advance current offset while size is known.
				cur_offset = cur_offset + f.elem_size * (f.len or 1)
			end
			if is_val and not f.fixed_offset then
				local dot_index = kv_index - fixsize_n - 2 --field's index in d.o.t.
				assertf(dot_index >= 0 and dot_index < dot_len)
				f.offset = nulls_size + dot_index * dyn_offset_size
			end
		end

	end

end

--create encoders and decoders for a layouted schema.
local function prepare_table_schema(schema)

	local key_fields = schema.key_fields
	local val_fields = schema.val_fields

	--generate direct key record decoders and encoders for u32/u64 keys
	--stored in little endian.
	if schema.int_key then
		local f = key_fields[1]
		local elem_size = f.elem_size
		local elemp_ct = elem_size == 4 and u32p or u64p
		function schema.encode_int_key(rec, rec_buf_sz, val)
			assert(rec_buf_sz >= elem_size)
			cast(elemp_ct, rec)[0] = val
			return elem_size
		end
		function schema.decode_int_key(rec, rec_sz)
			assert(rec_sz == elem_size)
			return cast(elemp_ct, rec)[0]
		end
	end

	--allocate the C schema.
	local st = new'schema_table'
	local sc_key_cols = new('schema_col[?]', #key_fields)
	local sc_val_cols = new('schema_col[?]', #val_fields)
	st.key_cols = sc_key_cols
	st.val_cols = sc_val_cols
	st.n_key_cols = #key_fields
	st.n_val_cols = #val_fields
	st.dyn_offset_size = schema.dyn_offset_size
	--anchor these so they don't get collected
	key_fields._sc = sc_key_cols
	val_fields._sc = sc_val_cols
	schema._st = st

	local int_schema_col_type = schema.int_key and (
			schema.int_key == 'u32' and C.schema_col_type_u32_le or
			schema.int_key == 'u64' and C.schema_col_type_u64_le
		)

	--setup C schema and create field getters and setters.
	for _,fields in ipairs{key_fields, val_fields} do

		local is_key = fields == key_fields

		for kv_index,f in ipairs(fields) do

			--setup C schema.
			local sc = fields._sc[kv_index-1]
			sc.type = is_key and int_schema_col_type or schema_col_types[f.mdbx_type]
			sc.len = f.maxlen or f.len or 1
			sc.fixed_size = f.maxlen and 0 or 1
			sc.descending = f.descending and 1 or 0
			sc.elem_size_shift = log2(f.elem_size)
			sc.fixed_offset = f.fixed_offset and 1 or 0
			sc.offset = f.offset or 0

			--create field getters and setters.
			local elem_ct = col_ct[f.mdbx_type] or f.mdbx_type
			local elemp_ct = ctype(elem_ct..'*')
			local elem_size = f.elem_size
			if f.len or f.maxlen then --array
				local maxlen = f.len or f.maxlen
				function f.get_val_len(val) return #val end
				if f.mdbx_type == 'utf8' then --utf8 strings
					local ai_ci = f.mdbx_collation == 'utf8_ai_ci'
					if ai_ci then
						local desc = f.descending
						function f.encode(buf, val)
							local len = min(maxlen, len or #s) --truncate
							local sp
							if ai_ci then
								sp, len = encode_ai_ci(s, len)
							else
								sp = cast(u8p, s)
							end
							if desc then
								local dp = getp(buf)
								for i = 0, len-1 do
									dp[i] = enc(sp[i], true)
								end
							else
								rawset(buf, rec_sz, sp, len)
							end
						end
						function f.decode(p, len)
							local p, p_len = rawget(buf, rec_sz)
							local len = min(p_len, out_len or 1/0) --truncate
							local out = out and cast(u8p, out) or u8a(len)
							local dp = getp(buf)
							for i = 0, len-1 do
								out[i] = dec(dp[i])
							end
							return out, len
						end
					else --raw utf8
						function f.encode(buf, val, len)
							assertf(typeof(val) == 'string', 'invalid val type: %s', typeof(val))
							copy(buf, val, len)
						end
						function f.decode(p, len)
							return str(p, len)
						end
					end
				else --array
					function f.encode(buf, val, len)
						assertf(typeof(val) == 'table', 'invalid val type: %s for %s.%s',
							typeof(val), schema.name, f.col)
						local buf = cast(elemp_ct, buf)
						for i = 1, len do
							buf[i-1] = val[i]
						end
						return buf, len
					end
					function f.decode(p, len)
						local t = {}
						for i = 1, len do
							t[i] = p[i-1]
						end
						return t
					end
				end
			else --scalar
				function f.get_val_len() return 1 end
				function f.encode(buf, val)
					cast(elemp_ct, buf)[0] = val
				end
				function f.decode(p)
					return cast(elemp_ct, p)[0]
				end
			end

		end --for f in fields

	end --for fields in key_fields, val_fields

end

local try_raw_open_table = Tx.try_open_table

function Tx:save_table_schema(table_name, schema)
	--NOTE: only saving enough information to read the data back in absence of
	--a paper schema, and to validate a paper schema against the used layout.
	local t = {
		type = 1, --layout type (the only one we have, implemented here)
		dyn_offset_size = schema.dyn_offset_size,
		int_key = schema.int_key,
		key_fields = {max_rec_size = schema.key_fields.max_rec_size},
		val_fields = {max_rec_size = schema.val_fields.max_rec_size},
	}
	for i=1,2 do
		local F = i == 1 and 'key_fields' or 'val_fields'
		for i,f in ipairs(schema[F]) do
			t[F][i] = {
				col = f.col,
				col_pos = f.col_pos, --in original schema fields array
				mdbx_type = f.mdbx_type,
				maxlen = f.maxlen, --varsize
				len = f.len, --fixsize
				--computed attributes
				elem_size = f.elem_size, --for validating custom types in the future.
				descending = f.descending,
				mdbx_collation = f.mdbx_collation,
				fixed_offset = f.fixed_offset, --what offset means.
				offset = f.offset, --null for varsize keys
			}
		end
	end
	local k = table_name
	local v = pp(t, false)
	self:put_raw('$schema', k, #k, v, #v)
end

function Tx:load_table_schema(table_name)
	if table_name == '$schema' then return end
	if not self:table_exists'$schema' then return end
	local k = table_name
	local v, v_len = self:try_get_raw('$schema', k, #k)
	if not v then return end
	local schema = eval(str(v, v_len))
	--reconstruct schema from stored table schema.
	assertf(schema.type == 1,
		'unknown schema type for table %s: %s', table_name, schema.type)
	schema.name = table_name
	schema.fields = {}
	for i,f in ipairs(schema.key_fields) do
		schema.fields[f.col_pos] = f
	end
	for i,f in ipairs(schema.val_fields) do
		schema.fields[f.col_pos] = f
	end
	schema.pk = imap(schema.key_fields, 'col')
	schema.pk.desc = imap(schema.key_fields, 'descending')
	return schema --compiled but not prepared
end

function Tx:delete_table_schema(table_name)
	self:try_del_raw('$schema', table_name, #table_name)
end

function Tx:try_open_table(tab, mode, flags)
	if not tab then --opening the unnamed root table
		return try_raw_open_table(self, mode, flags)
	end
	local t = self.db.open_tables[tab]
	if t then return t end
	local table_name = tab
	local db_schema = self.db.schema
	local tables = db_schema and db_schema.tables
	local paper_schema = tables and tables[table_name]
	local stored_schema = self:load_table_schema(table_name)
	if stored_schema and not paper_schema then
		--old table for which paper schema was lost, use stored schema.
		paper_schema = stored_schema
		paper_schema.compiled = true
	end
	if paper_schema then
		if not paper_schema.compiled then
			local db_max_key_size = self.db:db_max_key_size()
			compile_table_schema(paper_schema, table_name, db_max_key_size)
			paper_schema.compiled = true
		end
		if not paper_schema.prepared then
			prepare_table_schema(paper_schema)
			paper_schema.prepared = true
		end
	end
	if stored_schema and paper_schema ~= stored_schema then
		--table has stored schema, schemas must match.
		local errs
		for i=1,2 do
			local F = i == 1 and 'key_fields' or 'val_fields'
			for i,pf in ipairs(paper_schema[F]) do
				local sf = stored_schema[F][i]
				for _,k in ipairs{
					'col', 'col_pos', 'mdbx_type', 'maxlen', 'len',
					'elem_size', 'descending', 'mdbx_collation',
					'fixed_offset', 'offset',
				} do
					local pv = pf[k]
					local sv = sf and sf[k]
					if pv ~= sv then
						errs = errs or {}
						add(errs, fmt(' %s[%d].%s expected: %s, got: %s, for table: %s',
							F, i, k, pv, sv, table_name))
					end
				end
			end
		end
		if errs then
			error(cat(errs, '\n'))
		end
	end
	flags = bor(flags or 0,
		paper_schema and paper_schema.int_key and C.MDBX_INTEGERKEY or 0)
	local t, created = try_raw_open_table(self, table_name, mode, flags)
	if not t then return nil, created end
	t.schema = paper_schema
	if not stored_schema and paper_schema and created then
		self:save_table_schema(table_name, paper_schema)
	end
	return t, created
end

function Tx:dbi(tab, mode)
	local t = self.db.open_tables[tab or false]
	if not t then
		if mode == 'w' then
			t = self:open_table(tab, 'w')
		else
			local err
			t, err = self:try_open_table(tab)
			if not t then return nil, err end
		end
	end
	return t.dbi, t.schema, t.name
end
function Tx:dbi_schema(tab, mode)
	local dbi, schema, name = self:dbi(tab, mode)
	if dbi then assertf(schema, 'no schema for table: %s', name) end
	return dbi, schema
end

local try_raw_drop_table = Tx.try_drop_table
function Tx:try_drop_table(tab)
	local dbi, schema, name = self:dbi(tab)
	if not dbi then return nil, schema end
	assert(name)
	assert(try_raw_drop_table(self, dbi))
	self:delete_table_schema(name)
	return true
end

function Tx:create_table(tbl_name)
	self:delete_table_schema(tbl_name)
	local t, created = self:open_table(tbl_name, 'w')
	if not created then
		self:clear_table(tbl_name)
	end
end

local function key_field(schema, col)
	local f = assertf(schema.fields[col], 'unknown field: %s', col)
	assertf(f.key_index, 'not a key field: %s', col)
	return f
end

local function val_field(schema, col)
	local f = assertf(schema.fields[col], 'unknown field: %s.%s', schema.name, col)
	local vi = f.val_index
	assertf(vi, 'not a value field: %s.%s', schema.name, col)
	return f, vi
end

local key_rec_buffer = buffer()
local val_rec_buffer = buffer()

--writing --------------------------------------------------------------------

local encode_key_record do
local pp = new'u8*[1]'
function encode_key_record(schema, rec, rec_buf_sz, ...)
	if #schema.key_fields == 0 then return 0 end
	local encode_int_key = schema.encode_int_key
	if encode_int_key then
		return encode_int_key(rec, rec_buf_sz, ...)
	else
		pp[0] = rec
		for ki,f in ipairs(schema.key_fields) do
			local val = select(f.col_pos, ...)
			assert(val ~= nil)
			local len = f.get_val_len(val)
			local len = min(len, f.maxlen or f.len or 1) --truncate
			f.encode(pp[0], val, len)
			C.schema_key_add(schema._st, ki-1, rec, rec_buf_sz, len, pp)
		end
		return pp[0] - rec
	end
end
end

local encode_val_record do
local pp = new'u8*[1]'
function encode_val_record(schema, rec, rec_buf_sz, ...)
	if #schema.val_fields == 0 then return 0 end
	C.schema_val_add_start(schema._st, rec, rec_buf_sz, pp)
	for vi,f in ipairs(schema.val_fields) do
		local val = select(f.col_pos, ...)
		local len
		if val == nil or val == null then
			len = -1
		else
			len = f.get_val_len(val)
			len = min(len, f.maxlen or f.len or 1) --truncate
			f.encode(pp[0], val, len)
		end
		C.schema_val_add(schema._st, vi-1, rec, rec_buf_sz, len, pp)
	end
	return pp[0] - rec
end
end

function Tx:put(tab, ...)
	local dbi, schema = self:dbi_schema(tab, 'w')
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local val_rec, val_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local key_sz = encode_key_record(schema, key_rec, key_buf_sz, ...)
	local val_sz = encode_val_record(schema, val_rec, val_buf_sz, ...)
	self:put_raw(dbi, key_rec, key_sz, val_rec, val_sz)
end

function Tx:try_del(tab, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return nil, schema end
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local key_sz = encode_key_record(schema, key_rec, key_buf_sz, ...)
	return self:try_del_raw(dbi, key_rec, key_sz)
end
function Tx:del(...)
	assert(self:try_del(...))
end

function Tx:try_del_exact(tab, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return nil, schema end
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local val_rec, val_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local key_sz = encode_key_record(schema, key_rec, key_buf_sz, ...)
	local val_sz = encode_val_record(schema, val_rec, val_buf_sz, ...)
	return self:try_del_raw(dbi, key_rec, key_sz, val_rec, val_sz)
end
function Tx:del_exact(...)
	assert(self:try_del_exact(...))
end

function Tx:put_records(table_name, records)
	local dbi, schema = self:create_table(table_name)
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local val_rec, val_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	for _,vals in ipairs(records) do
		local key_sz = encode_key_record(schema, key_rec, key_buf_sz, unpack(vals))
		local val_sz = encode_val_record(schema, val_rec, val_buf_sz, unpack(vals))
		self:put_raw(dbi, key_rec, key_sz, val_rec, val_sz)
	end
end

--reading --------------------------------------------------------------------

local function get_raw_by_pk(self, dbi, schema, ...)
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local key_sz = self.db:encode_key_record(schema, key_rec, key_buf_sz, ...)
	return self:get_raw(dbi, key_rec, key_sz)
end

local cols_list = memoize(function(cols)
	return collect(words(cols))
end)

local m_cols_list = memoize(function(cols)
	return collect(words(cols))
end)
local function cols_list(cols)
	if not cols then
		return nil, nil
	end
	local as
	if cols:starts'[' then
		assert(cols:ends']')
		cols = cols:sub(2, -2)
		as = '[]'
	elseif cols:starts'{' then
		assert(cols:ends'}')
		cols = cols:sub(2, -2)
		as = '{}'
	end
	return #cols > 0 and m_cols_list(cols) or nil, as
end

local decode_key do
local pout = new'u8*[1]'
local pp = new'u8*[1]'
function decode_key(schema, rec, rec_sz, t, as)
	local key_fields = schema.key_fields
	local decode_int_key = schema.decode_int_key
	if decode_int_key then
		local v = decode_int_key(rec, rec_sz)
		local k = as == '{}' and key_fields[1].col or 1
		t[k] = v
		return 1
	else
		local out, out_sz = key_rec_buffer(key_fields.max_rec_size)
		pp[0] = rec
		local kn = #key_fields
		for ki = 1, kn do
			local f = key_fields[ki]
			local len = C.schema_get_key(schema._st, ki-1,
				rec, rec_sz,
				out, out_sz,
				pout, pp)
			local k = as == '{}' and f.col or ki
			if len ~= -1 then
				t[k] = f.decode(pout[0], len)
			else
				t[k] = nil
			end
		end
		return kn
	end
end
end

function Tx:is_null(tab, col, ...) --returns is_null, [reason]
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return true, schema end
	local f, vi = val_field(schema, col)
	local rec, rec_sz, err = get_raw_by_pk(self, dbi, schema, ...)
	if not rec then return true, err end
	return C.schema_val_is_null(schema._st, vi-1, rec, rec_sz) ~= 0
end

function Tx:exists(tab, ...) --returns record_exists, table_exists
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return false, false end
	local rec = get_raw_by_pk(self, dbi, schema, ...)
	if not rec then return false, true end
	return true, true
end

local decode_val do
local pout = new'u8*[1]'
function decode_val(schema, rec, rec_sz, t, cols, as, i0)
	local n = cols and #cols or #schema.val_fields
	for i=1,n do
		local f, col, vi
		if cols then
			col = cols[i]
			f, vi = val_field(schema, col)
		else
			f = schema.val_fields[i]
			vi = i
			col = f.col
		end
		local len = C.schema_get_val(schema._st, vi-1, rec, rec_sz, pout)
		local k = as == '{}' and col or i0 + i - 1
		if len ~= -1 then
			t[k] = f.decode(pout[0], len)
		else
			t[k] = nil
		end
	end
	return n
end
end

function Tx:try_get(tab, cols, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return false, schema end
	local rec, rec_sz, err = get_raw_by_pk(self, dbi, schema, ...)
	if not rec then return false, err end
	local cols, as = cols_list(cols)
	local t = {}
	local n = decode_val(schema, rec, rec_sz, t, cols, as, 1)
	if as then
		return true, t, n
	else
		return true, unpack(t, 1, n)
	end
end
do
local function finish(ok, ...)
	if not ok then return end
	return ...
end
function Tx:get(...)
	return finish(self:get(...))
end
end
do
local function finish(ok, ...)
	assert(ok, ...)
	return ...
end
function Tx:must_get(...)
	return finish(self:get(...))
end
end

local raw_cursor = Tx.cursor
function Tx:cursor(tab, mode)
	local dbi, schema = self:dbi(tab)
	if not dbi then return nil, 'not_found' end
	local cur = raw_cursor(self, dbi)
	cur.schema = schema
	cur._cols = nil
	return cur
end

local function decode_kv(schema, k, v, t, val_cols, as)
	local kn = decode_key(schema, k.data, k.size, t, as)
	local vn = decode_val(schema, v.data, v.size, t, val_cols, as, kn + 1)
	local n = kn + vn
	if as then
		return t, n
	else
		return unpack(t, 1, n) --keys can't be nil so this can't stop too soon.
	end
end
function Cur:current(val_cols)
	local k,v = self:current_raw_kv()
	if not k then return end
	local t, val_cols, as = self._t, self._val_cols, self._as
	if not t then t, val_cols, as = {}, cols_list(val_cols) end
	return decode_kv(self.schema, k, v, t, val_cols, as)
end
function Cur:next(val_cols)
	local k,v = self:next_raw_kv()
	if not k then return end
	local t, val_cols, as = self._t, self._val_cols, self._as
	if not t then t, val_cols, as = {}, cols_list(val_cols) end
	return decode_kv(self.schema, k, v, t, val_cols, as)
end
local function cur_next(self, _, val_cols)
	return self, self:next(val_cols)
end
function Tx:each(tbl_name, val_cols, mode, t)
	local cur = self:cursor(tbl_name, mode)
	cur._val_cols, cur._as = cols_list(val_cols)
	cur._t = t or {}
	return cur_next, cur
end

--schema sync'ing ------------------------------------------------------------

function Db:extract_schema()
	require'schema'
	local schema = schema.new{engine = 'mdbx'}
	schema.relevant_field_attrs = {
		col=1,
		col_pos=1,
		mdbx_type=1,
		maxlen=1,
		len=1,
	}
	self:atomic(function(tx)
		for table_name in tx:each_table() do
			if not table_name:starts'$' and not table_name:has'-by-' then
				schema.tables[table_name] = tx:load_table_schema(table_name) or {raw = true}
			end
		end
		for table_name in tx:each_table() do
			if not table_name:starts'$' and table_name:has'-by-' then
				local table_name, cols = table_name:match'(.?)-by-(.*)'
				local x = cols:gmatch'[^%-]+'
				local table_schema = schema.tables[table_name]
				--add(attr(table_schema, 'uks'),
			end
		end
	end)
	return schema
end

function Db:schema_diff()
	local ss = self:extract_schema()
	return self.schema:diff(ss)
end

function Tx:create_index(tbl_name, cols)
	local dbi, schema = self:dbi_schema(tbl_name)

	--create index schema
	local ix_tbl_name = tbl_name..'-by-'..cat(cols, '-')
	local ix_fields = {}
	for _,col in ipairs(cols) do
		local f = update({}, schema.fields[col])
		add(ix_fields, f)
	end
	local ix_schema = {fields = ix_fields, pk = cols}
	local db_max_key_size = self.db:db_max_key_size()
	compile_table_schema(ix_schema, ix_tbl_name, db_max_key_size)
	prepare_table_schema(ix_schema)
	self.db.schema.tables[ix_tbl_name] = ix_schema

	--create index table and fill it up
	self:create_table(ix_tbl_name)
	local dt = {}
	local key_rec, key_buf_sz = key_rec_buffer(ix_schema.key_fields.max_rec_size)
	for cur, k, v in self:each_raw_kv(tbl_name, '[]') do
		local vn = decode_val(schema, v.data, v.size, cols, dt, '[]')
		local key_sz = encode_key_record(uk_schema, key_rec, key_buf_sz, unpack(dt, 1, kn))
		self:put_raw(dbi, key_rec, key_sz, k.data, k.size)
	end
end

function Db:sync_schema(src, opt)
	local tx = self:txw()
	for tbl in tx:each_table() do
		pr('DROP ', tbl)
		tx:drop_table(tbl)
	end
	tx:commit()
	opt = opt or empty
	require'schema'
	local src_sc =
		schema.isschema(src) and src
		or inherits(src, mdbx_db) and src:extract_schema()
		or assertf(false, 'schema or mdbx_db expected, got %s', type(src))
	local this_sc = self:extract_schema()
	local diff = schema.diff(this_sc, src_sc)
	diff:pp()
	local dry = opt.dry
	local function P(...)
		pr(_(...))
	end
	self:atomic(dry and 'r' or 'w', function(tx)
		if diff.tables then
			if diff.tables.add then
				for tbl_name, tbl in sortedpairs(diff.tables.add) do
					P('create table: %s', tbl_name)
					if not dry then
						tx:create_table(tbl_name)
					end
					if tbl.rows then
						for _,row in ipairs(tbl.rows) do
							tx:put(tbl_name, unpack(row))
						end
					end
				end
			end
			if diff.tables.update then
				for tbl_name, tbl in sortedpairs(diff.tables.update) do
					if tbl.uks then
						if tbl.uks.add then
							for _,uk in pairs(tbl.uks.add) do
								if not dry then
									tx:create_index(tbl_name, uk)
								end
								pr(tbl_name, uk_name, uk)
							end
						end
					end
				end
			end
		end
	end)
end

--test -----------------------------------------------------------------------

if not ... then

	pr('libmdbx.so vesion: ',
		mdbx_version.major..'.'..
		mdbx_version.minor..'.'..
		mdbx_version.patch,
		str(mdbx_version.git.commit, 6))
	pr()

	rm'mdbx_schema_test.mdb'
	rm'mdbx_schema_test.mdb-lck'
	local schema = {tables = {}}
	local db = mdbx_open('mdbx_schema_test.mdb')
	db.schema = schema
	local types = 'u8 u16 u32 u64 i8 i16 i32 i64 f32 f64'
	local num_tables = {}
	local varsize_key1_tables = {}
	local varsize_key2_tables = {}
	for order in words'asc desc' do
		--single numeric keys + vals at fixed offsets.
		for typ in words(types) do
			local name = typ..':'..order
			local tbl = {
				name = name,
				test_type = typ,
				fields = {
					{col = 'k' , mdbx_type = typ},
					{col = 'v1', mdbx_type = typ},
					{col = 'v2', mdbx_type = typ},
				},
				pk = {'k', desc = {order == 'desc'}},
			}
			add(num_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

		--varsize key and val at fixed offset with utf8 enc/dec.
		local tbl = {
			name = 'varsize_key1'..':'..order,
			fields = {
				{col = 's', mdbx_type = 'utf8', maxlen = 100},
				{col = 'v', mdbx_type = 'utf8', maxlen = 100},
			},
			pk = {'s', desc = {order == 'desc'}},
		}
		add(varsize_key1_tables, tbl)
		schema.tables[tbl.name] = tbl

		--varsize key and val at dyn offset.
		for order2 in words'asc desc' do
			local tbl = {
				name = 'varsize_key2'..':'..order..':'..order2,
				fields = {
					{col = 's1', mdbx_type = 'utf8', maxlen = 100},
					{col = 's2', mdbx_type = 'utf8', maxlen = 100},
					{col = 's3', mdbx_type = 'utf8', maxlen = 100},
					{col = 's4', mdbx_type = 'utf8', maxlen = 100},
				},
				pk = {'s1', 's2', desc = {order == 'desc', order2 == 'desc'}},
			}
			add(varsize_key2_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

	end

	--test int and float decoders and encoders
	for _,tbl in ipairs(num_tables) do
		local typ = tbl.test_type
		local tx = db:txw()
		local bits = num(typ:sub(2))
		local ntyp = typ:sub(1,1)
		local nums =
			ntyp == 'u'  and {0,1,2,2ULL^bits-1} or
			ntyp == 'i'  and {-2LL^(bits-1),-(2LL^(bits-1)-1),-2,-1,0,1,2,2LL^(bits-1)-2,2LL^(bits-1)-1} or
			typ == 'f64' and {-2^52,-2,-1,-0.1,-0,0,0.1,1,2^52} or
			typ == 'f32' and {-2^23,-2,-1,cast('float', -0.1),-0,0,cast('float', 0.1),1,2^23}
		assert(nums)
		local t = {}
		for _,i in ipairs(nums) do
			add(t, {i, i, i})
		end
		tx:put_records(tbl.name, t)
		tx:commit()

		if tbl.fields.k.descending then
			reverse(nums)
		end
		tx = db:tx()
		local i = 1
		for cur, k, v1, v2 in tx:each(tbl.name) do
			pr(tbl.name, k, v1, v2)
			assertf(k  == nums[i], '%q ~= %q', k , nums[i])
			assertf(v1 == nums[i], '%q ~= %q', v1, nums[i])
			assertf(v2 == nums[i], '%q ~= %q', v2, nums[i])
			i = i + 1
		end
		tx:commit()
	end

	--test varsize_key1
	for _,tbl in ipairs(varsize_key1_tables) do
		local t = {
			{'a' , 'b' },
			{'bb', nil },
			{'aa', 'bb'},
			{'b' , nil },
		}
		local tx = db:txw()
		tx:put_records(tbl.name, t)
		tx:commit()

		sort(t, function(r1, r2)
			if tbl.fields.s.descending then
				return r2[1] < r1[1]
			else
				return r1[1] < r2[1]
			end
		end)
		local tx = db:tx()
		local t1 = {}
		for cur, t in tx:each(tbl.name, '[]') do
			add(t1, t)
		end
		tx:commit()
		for i=1,#t do
			assert(t1[i][1] == t[i][1])
			assert(t1[i][2] == t[i][2])
		end
		pr()
	end
	pr()

	--test varsize_key2
	for _,tbl in ipairs(varsize_key2_tables) do
		local t = {
			{'a'  , 'b'  , 'a'  , nil  , },
			{'a'  , 'a'  , 'a'  , 'a'  , },
			{'a'  , 'aaa', 'a'  , 'aaa', },
			{'a'  , 'bbb', 'a'  , nil  , },
			{'aa' , 'a'  , 'aa' , 'a'  , },
			{'aa' , 'b'  , 'aa' , nil  , },
			{'bb' , 'a'  , nil  , 'a'  , },
			{'bb' , 'aa' , nil  , 'aa' , },
			{'bb' , 'bb' , nil  , nil  , },
			{'aa' , 'bb' , 'aa' , nil  , },
			{'b'  , 'a'  , nil  , 'a'  , },
			{''   , 'a'  , ''   , 'a'  , },
			{'a'  , ''   , 'a'  , ''   , },
			{'xx' , 'y'  , 'z'  , 'zz' , },
		}
		local tx = db:txw()
		tx:put_records(tbl.name, t)
		tx:commit()

		local s1_desc = tbl.fields.s1.descending
		local s2_desc = tbl.fields.s2.descending
		sort(t,
			function(r1, r2)
				local c1; if s1_desc then c1 = r2[1] < r1[1] else c1 = r1[1] < r2[1] end
				local c2; if s2_desc then c2 = r2[2] < r1[2] else c2 = r1[2] < r2[2] end
				if r2[1] == r1[1] then return c2 else return c1 end
			end)
		local tx = db:tx()
		pr('***', tbl.pk, '***')
		local t1 = {}
		for cur, s1, s2, s3, s4 in tx:each(tbl.name) do
			assert(tx:is_null(tbl.name, 's3', s1, s2) == (s3 == nil))
			assert(tx:is_null(tbl.name, 's4', s1, s2) == (s4 == nil))
			add(t1, {s1, s2, s3, s4})
		end
		local s3, s4 = tx:get(tbl.name, 's3 s4', 'xx', 'y')
		assert(s3 == 'z')
		assert(s4 == 'zz')
		tx:commit()
		for i=1,#t do
			pr(unpack(t1[i], 1, 4))
			assert(t[i][1] == t1[i][1])
			assert(t[i][2] == t1[i][2])
			assert(t[i][3] == t1[i][3])
			assert(t[i][4] == t1[i][4])
		end
		pr()
	end

	local tx = db:tx()
	pr(rpad('TABLE ('..tx:table_count()..')', 24),
		'ENTRIES', 'PSIZE', 'DEPTH',
		'BR_PG', 'LEAF_PG', 'OVER_PG',
		'TXNID')
	pr(rep('-', 90))
	for table_name in tx:each_table() do
		local s = tx:stat(table_name)
		pr(rpad(table_name, 24),
			num(s.entries), s.psize, s.depth,
			num(s.branch_pages), num(s.leaf_pages), num(s.overflow_pages),
			num(s.mod_txnid))
	end
	tx:abort()
	pr()

	local tx = db:tx()
	pr(rpad('TABLE ('..tx:table_count()..')', 24),
		'KCOLS', 'VCOLS', 'PK')
	pr(rep('-', 90))
	for table_name in tx:each_table() do
		local s = db.schema.tables[table_name]
		if s then
			pr(rpad(table_name, 24),
				cat(imap(s.key_fields, 'name'),','),
				cat(imap(s.val_fields, 'name'),','),
				s.pk)
		else
			pr(rpad(table_name, 24), '')
		end
	end
	tx:abort()
	pr()

	db:close()

	--reopen db to check that stored schema matches paper schema.
	local db = mdbx_open('mdbx_schema_test.mdb')
	db.schema = schema
	local tx = db:tx()
	for tab in tx:each_table() do
		tx:open_table(tab)
	end
	tx:abort()
	db:close()

--	major, C.mdbx_version.minor, C.mdbx_version.patch, C.mdbx_version.tweak)
--	const char *semver_prerelease;
--	struct {
--		const char *datetime;
--		const char *tree;
--		const char *commit;
--		const char *describe;
--	} git;
--	const char *sourcery;
--} mdbx_version;

	--[[
	local db = mdbx_open('mdbx_schema_test.mdb')
	db:load_schema()
	local u = {
		uid = 1234,
		active = 1,
		roles = {123, 321},
		email = 'admin@some.com',
		name = 'John Galt',
	}
	local key_max_sz = db:max_key_size'users'
	local val_max_sz = db:max_val_size'users'
	local buf_sz = key_max_sz + val_max_sz
	local buf = u8a(buf_sz)
	local key_sz = db:encode_key('users', u, buf, buf_sz)
	local val_sz = db:encode_val('users', u, buf, buf_sz, key_max_sz)
	db:encode_val_col('users', 'name', 'Dagny Taggart', buf, val_sz, key_max_sz)
	pr(db:val_tostring('users', 'email', buf, val_sz, key_max_sz))
	pr(db:val_tostring('users', 'name' , buf, val_sz, key_max_sz))
	--db:decode_val('users', u, buf, sz)
	db:close()
	]]

end
