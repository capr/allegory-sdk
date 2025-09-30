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

TODO:
	- tx:get()
	- cols list on: db:decode_record(), tx:get_record(), tx:each_record()
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
local function parse_table_schema(schema, table_name, db_max_key_size)

	--index fields by name, typecheck, check for inconsistencies.
	for i,f in ipairs(schema.fields) do
		assertf(isstr(f.name) and #f.name > 0, 'invalid field name: %s', f.name)
		assertf(not schema.fields[f.name],
			'duplicate field name: %s', f.name)
		schema.fields[f.name] = f
		f.index = i
		local elem_ct = col_ct[f.type] or f.type
		local elem_ct = assertf(elem_ct,
			'unknown type %s for field: %s', f.type, f.name)
		f.elem_size = sizeof(elem_ct)
		f.elemp_ct = ctype(elem_ct..'*')
		assertf(f.elem_size < 2^8) --must fit 8 bit (see sort below)
		assertf(not (f.maxlen and f.len),
			'both maxlen and len specified for field: %s', f.name)
	end

	--split fields into key_fields and val_fields.
	local key_fields = {}
	local val_fields = {}

	--parse pk and set f.descending, .type, .collation.
	for s in words(schema.pk) do
		local col, s1 = s:match'^(.-):(.*)'
		local order = 'asc'
		local collation
		if not col then
			col = s
		else
			local s2, s3 = s1:match'^(.-):(.*)'
			if s2 then
				if s2 == 'asc' or s2 == 'desc' then
					order, collation = s2, s3
				else
					collation, order = s2, s3
				end
			else
				if s1 == 'asc' or s1 == 'desc' then
					order = s1
				else
					collation = s1
				end
			end
			assertf(order == 'desc' or order == 'asc', 'invalid order: %s', order)
		end

		local f = assertf(schema.fields[col], 'pk field not found: %s', col)
		f.descending = order == 'desc' or nil
		if f.type == 'utf8' then
			collation = collation or 'utf8'
			assertf(collation == 'utf8_ai_ci' or collation == 'utf8',
				'invalid collation %s for field: %s', collation, f.name)
			f.collation = collation
			assert(f.maxlen, 'utf8 field not varsize: %s', col)
		else
			assertf(not collation, 'collation on %s field: %s', f.type, col)
		end
		add(key_fields, f)
		f.key_index = #key_fields
	end
	assert(#key_fields > 0, 'table missing pk: %s', table_name)

	--build val fields array with all fields that are not in pk.
	for i,f in ipairs(schema.fields) do
		if not f.key_index then --not a key field
			add(val_fields, f)
			f.key_index = #val_fields
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
		local i1 = (f1.maxlen and 2^26 or 0) + (2^8-1 - f1.elem_size) * 2^16 + f1.index
		local i2 = (f2.maxlen and 2^26 or 0) + (2^8-1 - f2.elem_size) * 2^16 + f2.index
		return i1 < i2
	end)

	schema.key_fields = key_fields
	schema.val_fields = val_fields

	--store u32 and u64 simple keys in little-endian and use fast comparator.
	if #key_fields == 1 then
		local f = key_fields[1]
		if not f.descending and (f.type == 'u32' or f.type == 'u64') then
			schema.int_key = f.type
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
				local dot_index = fixsize_n+2 - kv_index --field's index in d.o.t.
				assert(dot_index >= 0 and dot_index < dot_len)
				f.offset = nulls_size + dot_index * dyn_offset_size
			end
		end

	end

end

--create encoders and decoders for a layouted schema.
local function compile_table_schema(schema)

	local key_fields = schema.key_fields
	local val_fields = schema.val_fields

	--generate direct key record decoders and encoders for u32/u64 keys
	--stored in little endian.
	if schema.int_key then
		local f = key_fields[1]
		local elemp_ct = f.elemp_ct
		local elem_size = f.elem_size
		function schema.encode_key_record(rec, rec_buf_sz, val)
			assert(rec_buf_sz >= elem_size)
			cast(elemp_ct, rec)[0] = val
			return elem_size
		end
		function schema.decode_key_record(rec, rec_sz, t)
			assert(rec_sz == elem_size)
			t[1] = cast(elemp_ct, rec)[0]
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
			sc.type = is_key and int_schema_col_type or schema_col_types[f.type]
			sc.len = f.maxlen or f.len or 1
			sc.fixed_size = f.maxlen and 0 or 1
			sc.descending = f.descending and 1 or 0
			sc.elem_size_shift = log2(f.elem_size)
			sc.fixed_offset = f.fixed_offset and 1 or 0
			sc.offset = f.offset or 0

			--create field getters and setters.
			local elemp_ct = f.elemp_ct
			local elem_size = f.elem_size
			if f.len or f.maxlen then --array
				local maxlen = f.len or f.maxlen
				function f.val_len(val) return #val end
				if f.type == 'utf8' then --utf8 strings
					local ai_ci = f.collation == 'utf8_ai_ci'
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
						assertf(typeof(val) == 'table', 'invalid val type: %s', typeof(val))
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
				function f.val_len() return 1 end
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

local open_table_raw = Tx.open_table

function Tx:save_table_schema(table_name, schema)
	--NOTE: only saving enough information to read the data back in absence of
	--a paper schema, and to validate a paper schema against the used layout.
	local t = {
		type = 1, --layout type (the only one we have, implemented here)
		dyn_offset_size = schema.dyn_offset_size,
		int_key = schema.int_key,
		key_fields = {max_rec_size = max_key_size},
		val_fields = {max_rec_size = max_val_size},
	}
	for i,f in ipairs(schema.key_fields) do
		t.key_fields[i] = {
			name = f.name,
			index = f.index, --in original schema fields array
			type = f.type,
			maxlen = f.maxlen, --varsize
			len = f.len, --fixsize
			--computed attributes
			elem_size = f.elem_size, --for validating custom types in the future.
			descending = f.descending,
			collation = f.collation,
			fixed_offset = f.fixed_offset, --what offset means.
			offset = f.offset, --null for varsize keys
		}
	end
	for i,f in ipairs(schema.val_fields) do
		t.val_fields[i] = {
			name = f.name,
			index = f.index, --in original schema fields array
			type = f.type,
			maxlen = f.maxlen, --varsize
			len = f.len, --fixsize
			--computed attributes
			elem_size = f.elem_size, --for validating custom types in the future.
			fixed_offset = f.fixed_offset, --what offset means.
			offset = f.offset, --always present.
		}
	end
	local v = pp(t, false)
	local k = table_name
	local dbi = open_table_raw(self, '$schema', 'w')
	self:put_raw(dbi, k, #k, v, #v)
	self:close_table()
end

function Tx:load_table_schema(table_name)
	if not self:table_exists'$schema' then return end
	local k = table_name
	local dbi, schema = open_table_raw(self, '$schema')
	local v, v_len = self:get_raw(dbi, k, #k)
	if not v then return end
	local schema = eval(str(v, v_len))
	--reconstruct schema from stored table schema.
	assertf(schema.type == 1,
		'unknown schema type for table %s: %s', table_name, schema.type)
	schema.fields = {}
	for i,f in ipairs(schema.key_fields) do
		schema.fields[f.index] = f
	end
	for i,f in ipairs(schema.val_fields) do
		schema.fields[f.index] = f
	end
	schema.pk = cat(imap(schema.key_fields, function(f)
		return f.name .. (f.descending and ':desc' or '') ..
			(f.collation ~= ':utf8' and f.collation or '')
	end), ' ')
	return schema --not compiled
end

local function table_schema(self, table_name)
	return assertf(self.schema.tables[table_name],
		'no schema for table: %s', table_name)
end

function Tx:open_table(table_name, flags)
	if not table_name then --opening the unnamed root table
		return open_table_raw(self)
	end
	local paper_schema = self.db.schema.tables[table_name]
	if paper_schema and not paper_schema.compiled then
		local db_max_key_size = self.db:db_max_key_size()
		parse_table_schema(paper_schema, table_name, db_max_key_size)
		compile_table_schema(paper_schema)
		paper_schema.compiled = true
	end
	local dbi = self.db.dbis[table_name]
	if dbi then
		assertf(not paper_schema or paper_schema.opened,
			'table with schema already opened in raw mode: %s', table_name)
		return dbi, paper_schema
	end
	local stored_schema = self:load_table_schema(table_name)
	if not paper_schema then
		if not stored_schema then --no paper schema, no stored schema
			return open_table_raw(self, table_name, flags)
		else --old table for which paper schema was lost, use stored schema
			compile_table_schema(stored_schema)
			stored_schema.compiled = true
			self.db.schema.tables[table_name] = stored_schema
			paper_schema = stored_schema
		end
	else
		if stored_schema and self:entries() > 0 then
			--table already has records in it, schemas must match or we bail.
			local errs = {}
			for _,F in ipairs{'key_fields', 'val_fields'} do
				local pf = cat(imap( paper_schema[F], 'name'), ',')
				local sf = cat(imap(stored_schema[F], 'name'), ',')
				if #sf ~= #pf then
					add(errs, _(' %s expected: %s, got: %s, for table: %s',
						F, pf, sf, table_name))
				end
				for i,pf in ipairs(paper_schema[k]) do
					local sf = stored_schema[i]
					for _,k in ipairs{
						'name', 'index', 'type', 'maxlen', 'len',
						'elem_size', 'descending', 'collation',
						'fixed_offset', 'offset',
					} do
						if sf[k] ~= pf[k] then
							add(_(' %s.%s expected: %s, got: %s, for table: %s',
								F, k, pf[k], sf[k], table_name))
						end
					end
				end
			end
		end
		if flags == 'w' then flags = C.MDBX_CREATE end
		local dbi = open_table_raw(self, table_name, bor(flags,
			paper_schema.int_key and C.MDBX_INTEGERKEY or 0
		))
		paper_schema.opened = true
		return dbi, paper_schema
	end
end

local function key_field(schema, col)
	local f = assertf(schema.fields[col], 'unknown field: %s', col)
	assertf(f.key_index, 'not a key field: %s', col)
	return f
end

local function val_field(schema, col)
	local f = assertf(schema.fields[col], 'unknown field: %s', col)
	assertf(f.val_index, 'not a value field: %s', col)
	return f
end

Db.table_schema = table_schema
Db.key_field = key_field
Db.val_field = val_field

function Db:max_key_size(tbl_name)
	return table_schema(self, tbl_name).key_fields.max_rec_size
end

function Db:max_val_size(tbl_name)
	return table_schema(self, tbl_name).val_fields.max_rec_size
end

local key_rec_buffer = buffer()
local val_rec_buffer = buffer()

--writing --------------------------------------------------------------------

do
local pp = new'u8*[1]'
function Db:encode_key_record(schema, rec, rec_buf_sz, ...)
	if #schema.key_fields == 0 then return 0 end
	if schema.encode_key_record then
		return schema.encode_key_record(rec, rec_buf_sz, ...)
	else
		pp[0] = rec
		for ki,f in ipairs(schema.key_fields) do
			local val = select(f.index, ...)
			assert(val ~= nil)
			local len = f.val_len(val)
			local len = min(len, f.maxlen or f.len or 1) --truncate
			f.encode(pp[0], val, len)
			C.schema_key_add(schema._st, ki-1, rec, rec_buf_sz, len, pp)
		end
		return pp[0] - rec
	end
end
end

do
local pp = new'u8*[1]'
function Db:encode_val_record(schema, rec, rec_buf_sz, ...)
	if #schema.val_fields == 0 then return 0 end
	C.schema_val_add_start(schema._st, rec, rec_buf_sz, pp)
	for vi,f in ipairs(schema.val_fields) do
		local val = select(f.index, ...)
		local len
		if val == nil then
			len = -1
		else
			len = f.val_len(val)
			len = min(len, f.maxlen or f.len or 1) --truncate
			f.encode(pp[0], val, len)
		end
		C.schema_val_add(schema._st, vi-1, rec, rec_buf_sz, len, pp)
	end
	return pp[0] - rec
end
end

function Tx:put(table_name, ...)
	local dbi, schema = self:open_table(table_name, 'w')
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local val_rec, val_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local key_sz = self.db:encode_key_record(schema, key_rec, key_buf_sz, ...)
	local val_sz = self.db:encode_val_record(schema, val_rec, val_buf_sz, ...)
	self:put_raw(dbi, key_rec, key_sz, val_rec, val_sz)
end

function Tx:put_records(table_name, records)
	local dbi, schema = self:open_table(table_name, 'w')
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local val_rec, val_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	for _,vals in ipairs(records) do
		local key_sz = self.db:encode_key_record(schema, key_rec, key_buf_sz, unpack(vals))
		local val_sz = self.db:encode_val_record(schema, val_rec, val_buf_sz, unpack(vals))
		self:put_raw(dbi, key_rec, key_sz, val_rec, val_sz)
	end
end

--reading --------------------------------------------------------------------

function Db:decode_is_null(schema, col, rec, rec_sz)
	local f = val_field(schema, col)
	return C.schema_val_is_null(schema._st, f.val_index-1, rec, rec_sz) ~= 0
end

function Tx:get_raw_by_pk(table_name, ...)
	local dbi, schema = self:open_table(table_name)
	local key_rec, key_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local key_sz = self.db:encode_key_record(schema, key_rec, key_buf_sz, ...)
	return self:get_raw(dbi, key_rec, key_sz)
end

do
local pout = new'u8*[1]'
local pp = new'u8*[1]'
function Db:decode_key_record(schema, rec, rec_sz, t)
	t = t or {}
	if schema.decode_key_record then
		schema.decode_key_record(rec, rec_sz, t)
	else
		local out, out_sz = key_rec_buffer(schema.key_fields.max_rec_size)
		pp[0] = rec
		for ki,f in ipairs(schema.key_fields) do
			local len = C.schema_get_key(schema._st, ki-1,
				rec, rec_sz,
				out, out_sz,
				pout, pp)
			if len ~= -1 then
				t[f.index] = f.decode(pout[0], len)
			end
		end
	end
	return t
end
end

do
local pout = new'u8*[1]'
function Db:decode_val_record(schema, rec, rec_sz, t)
	t = t or {}
	for vi,f in ipairs(schema.val_fields) do
		local len = C.schema_get_val(schema._st, vi-1, rec, rec_sz, pout)
		if len ~= -1 then
			t[f.index] = f.decode(pout[0], len)
		end
	end
	return t
end
end

do
local pout = new'u8*[1]'
function Db:decode_key_col(schema, col, rec, rec_sz)
	local f = key_field(schema, col)
	local out, out_sz = key_rec_buffer(cols.max_rec_size)
	local len = C.schema_get_key(schema._st, col.key_index-1,
		rec, rec_sz,
		out, out_sz,
		pout)
	if len ~= -1 then
		return col.decode(pout[0], len)
	else
		return nil
	end
end
end

function Tx:get_record(table_name, ...)
	local dbi, schema = self:open_table(table_name)
	local rec, rec_sz = self:get_raw_by_pk(table_name, ...)
	return self.db:decode_val_record(schema, rec, rec_sz, {...})
end

function Tx:get(table_name, ...)
	local dbi, schema = self:open_table(table_name)
	local rec, rec_sz = self:get_raw_by_pk(table_name, ...)
	local t = self.db:decode_val_record(schema, rec, rec_sz, {...})
	if not t then return end
	return unpack(t, 1, #schema.fields)
end

function Tx:cursor(table_name)
	local dbi, schema = self:open_table(table_name)
	local cur = self:raw_cursor(dbi)
	cur._t = {}
	cur.schema = schema
	return cur
end

function Cur:next_record(_, t)
	t = t or {}
	local k,v = self:next_raw_kv()
	if not k then return end
	self.tx.db:decode_key_record(self.schema, k.data, k.size, t)
	self.tx.db:decode_val_record(self.schema, v.data, v.size, t)
	return t
end

function Cur:next()
	local n = #self.schema.fields
	for i=n,1,-1 do self._t[i] = nil end
	local t = self:next_record(nil, self._t)
	if not t then return end
	return unpack(t, 1, n)
end

function Tx:each_record(tbl_name)
	return Cur.next_record, self:cursor(tbl_name)
end

function Tx:each(tbl_name)
	local cur = self:cursor(tbl_name)
	cur._t = {}
	return Cur.next, cur
end

--test -----------------------------------------------------------------------

if not ... then

	pr('libmdbx.so vesion: ',
		mdbx_version.major..'.'..
		mdbx_version.minor..'.'..
		mdbx_version.patch,
		str(mdbx_version.git.commit, 6))
	pr()

	rm'mdbx_schema_test/mdbx.dat'
	rm'mdbx_schema_test/mdbx.lck'
	local schema = {tables = {}}
	local db = mdbx_open('mdbx_schema_test')
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
				test_order = order,
				fields = {
					{name = 'k' , type = typ},
					{name = 'v1', type = typ},
					{name = 'v2', type = typ},
				},
				pk = 'k:'..order,
			}
			add(num_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

		--varsize key and val at fixed offset with utf8 enc/dec.
		local tbl = {
			name = 'varsize_key1'..':'..order,
			fields = {
				{name = 's', type = 'utf8', maxlen = 100},
				{name = 'v', type = 'utf8', maxlen = 100},
			},
			pk = 's:'..order,
		}
		add(varsize_key1_tables, tbl)
		schema.tables[tbl.name] = tbl

		--varsize key and val at dyn offset.
		for order2 in words'asc desc' do
			local tbl = {
				name = 'varsize_key2'..':'..order..':'..order2,
				fields = {
					{name = 's1', type = 'utf8', maxlen = 100},
					{name = 's2', type = 'utf8', maxlen = 100},
					{name = 's3', type = 'utf8', maxlen = 100},
					{name = 's4', type = 'utf8', maxlen = 100},
				},
				pk = 's1:'..order..' s2:'..order2,
			}
			add(varsize_key2_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

	end

	--test int and float decoders and encoders
	for _,tbl in ipairs(num_tables) do
		local typ = tbl.test_type
		local tx = db:tx'w'
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
		tx = db:tx'r'
		local i = 1
		for k, v1, v2 in tx:each(tbl.name) do
			-- --assert(kv == db:decode_key_col(tbl.name, 'k', k.data, k.size))
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
		local tx = db:tx'w'
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
		pr('***', tbl.pk, '***')
		local t1 = {}
		for t in tx:each_record(tbl.name) do
			add(t1, t)
		end
		tx:commit()
		for i=1,#t do
			pr(t1[i][1], t1[i][2])
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
		local tx = db:tx'w'
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
		for t in tx:each_record(tbl.name) do
			--assert(db:decode_is_null(tbl.name, 's3', v.data, v.size) == (s3 == nil))
			--assert(db:decode_is_null(tbl.name, 's4', v.data, v.size) == (s4 == nil))
			add(t1, t)
		end
		local s1, s2, s3, s4 = tx:get(tbl.name, 'xx', 'y')
		assert(s1 == 'xx')
		assert(s2 == 'y')
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
	local db = mdbx_open('mdbx_schema_test')
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
