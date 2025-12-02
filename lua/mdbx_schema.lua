--go@ plink -t root@m1 sdk/bin/debian12/luajit sdk/tests/mdbx_schema_test.lua
--go@ plink -t root@m1 sdk/bin/debian12/luajit sp2/sp.lua -v install forealz
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

	tx:put             (table_name|dbi, [cols], keysvals...)
	tx:[try_]insert    (table_name|dbi, [cols], keysvals...) -> true | nil,'exists'
	tx:[try_]update    (table_name|dbi, [cols], keysvals...) -> true | nil,'not_found'
	tx:upsert          (table_name|dbi, [cols], keysvals...)
	tx:put_records     (table_name|dbi, [cols, ]{keysvals1,...})
	tx:is_null         (table_name|dbi, col, keys...) -> is_null, [reason]
	tx:exists          (table_name|dbi, keys...) -> record_exists, table_exists
	tx:[try_|must_]get (table_name|dbi, [val_cols], keys...) -> [ok, ]vals...
	tx:try_get         (table_name|dbi, [val_cols], keys...) -> true, vals... | false, err
	tx:[try_]del       (table_name|dbi, keys...) -> true | nil,err
	tx:[try_]del_exact (table_name|dbi, [cols], keysvals...) -> true | nil,err

	cur:current        ([cols]) -> keysvals...
	cur:next           ([cols]) -> keysvals...
	cur:update         ([val_cols], vals...)
	cur:[try_|must]get ([val_cols], keys...) -> vals...

	tx:each            (tbl_name|dbi, [cols]) -> cur, keysvals...

		cols format   |  vals...              | keyvals...
		--------------+-----------------------+------------
		nil           |   col1_val,...        |  keycol1_val,..., col1_val,...
		'col1 ...'    |   col1_val,...        |  keycol1_val,..., col1_val,...
		'[col1 ...]'  |  {col1_val,...}       | {keycol1_val,..., col1_val,...}
		'{col1 ...}'  |  {col1=col1_val,...}  | {keycol1=keycol1_val,col1=col1_val}


TODO:
	- reverse cursor
	- mdbx_env_chk()
	- range lookup cursor
	- table migration:
		- copy all records to tmp table, delete old table, rename tmp table.
			- copy existing fields by name, use aka mapping to find old field.
				- copy using raw pointers into decoded values and encoded values.
	-

]]

require'mdbx'
require'utf8proc'
require'cjson' -- for null
require'schema'
local C = ffi.load'mdbx_schema' --see src/c/mdbx_schema/mdbx_schema.c

local
	typeof, num, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast, memcmp =
	typeof, num, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast, memcmp

assert(ffi.abi'le')

local mdbx = mdbx
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

--fw. decl.
local cols_list
local encode_key
local encode_val
local decode_key
local decode_val

--schema processing ----------------------------------------------------------

--create an optimal physical column layout based on a table schema.
function Tx:layout_table_schema(schema)

	if schema.layouted then return end
	schema.layouted = true

	local table_name = assert(schema.name)

	--index fields by name, typecheck, check for inconsistencies.
	for i,f in ipairs(schema.fields) do
		assertf(isstr(f.col) and #f.col > 0 and not f.col:find'%s',
			'invalid field name: %s.%s', table_name, f.col)
		schema.fields[f.col] = f
		f.col_pos = i
		local elem_ct = col_ct[f.mdbx_type] or f.mdbx_type
		local ok, elem_ct = pcall(ctype, elem_ct)
		assertf(ok, 'unknown type: %s for field: %s.%s', f.mdbx_type, table_name, f.col)
		f.elem_size = sizeof(elem_ct)
		assertf(f.elem_size < 2^8) --must fit 8 bit (see sort below)
	end

	--split fields into key_fields and val_fields.
	local key_fields = {}
	local val_fields = {}

	--parse pk and set f.descending.
	assertf(schema.pk, 'pk missing for table: %s', table_name)
	for i,col in ipairs(schema.pk) do
		local f = assertf(schema.fields[col],
			'pk col unknown: `%s` for table: %s', col, table_name)
		add(key_fields, f)
		f.key_index = #key_fields
		if schema.pk.desc then
			f.descending = schema.pk.desc[i]
		end
		if f.auto_increment then
			local f0 = schema.autoinc_field
			if f0 then
				assertf(false,
				'auto_increment on a second key field: %s (already on: %s)',
				f.col, f0.col)
			end
			schema.autoinc_field = f
		end
	end
	assert(#key_fields > 0, 'table has no pk: %s', table_name)

	--build val fields array with all fields that are not in pk.
	for i,f in ipairs(schema.fields) do
		if not f.key_index then --not a key field
			add(val_fields, f)
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
	for i,f in ipairs(val_fields) do
		f.val_index = i
	end

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
			if f.maxlen and not f.padded then --first varsize field
				fixsize_n = i-1
				break
			end
		end

		--compute max row size, just the data (which is what it is for keys).
		local max_rec_size = 0
		for _,f in ipairs(fields) do
			local maxlen = f.maxlen and f.maxlen + (f.padded and 0 or 1) or 1
			max_rec_size = max_rec_size + maxlen * f.elem_size
		end

		if is_key then
			local db_max_key_size = self.db:db_max_key_size()
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
				cur_offset = cur_offset + f.elem_size * (f.maxlen or 1)
			end
			if is_val and not f.fixed_offset then
				local dot_index = kv_index - fixsize_n - 2 --field's index in d.o.t.
				assertf(dot_index >= 0 and dot_index < dot_len)
				f.offset = nulls_size + dot_index * dyn_offset_size
			end
		end

	end

end

local S = function() end

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

--create encoders and decoders for a layouted schema.
function Tx:compile_table_schema(schema)

	if schema.compiled then return end
	schema.compiled = true

	self:layout_table_schema(schema)

	local key_fields = schema.key_fields
	local val_fields = schema.val_fields

	--default col lists for get, put, etc.
	schema.    cols = imap(schema.    fields, 'col')
	schema.key_cols = imap(schema.key_fields, 'col')
	schema.val_cols = imap(schema.val_fields, 'col')
	for i,col in ipairs(schema.    cols) do schema.    cols[col] = i end
	for i,col in ipairs(schema.key_cols) do schema.key_cols[col] = i end
	for i,col in ipairs(schema.val_cols) do schema.val_cols[col] = i end

	schema.    cols[S] = cat(schema.    cols, ',')
	schema.key_cols[S] = cat(schema.key_cols, ',')
	schema.val_cols[S] = cat(schema.val_cols, ',')

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
			sc.len = f.maxlen or 1
			sc.fixed_size = f.maxlen and not f.padded and 0 or 1
			sc.descending = f.descending and 1 or 0
			sc.elem_size_shift = log2(f.elem_size)
			sc.fixed_offset = f.fixed_offset and 1 or 0
			sc.offset = f.offset or 0

			--create field getters and setters.
			local elem_ct = col_ct[f.mdbx_type] or f.mdbx_type
			local elemp_ct = ctype(elem_ct..'*')
			local elem_size = f.elem_size
			if f.maxlen then --array
				function f.get_val_len(val) return #val end
				if f.mdbx_type == 'utf8' then --utf8 strings
					local ai_ci = f.mdbx_collation == 'utf8_ai_ci'
					if ai_ci then
						local desc = f.descending
						local maxlen = f.maxlen
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

	if schema.is_index then
		self:compile_index_schema(schema)
	end

end

local try_raw_open_table = Tx.try_open_table

local function save_index_def(ix)
	local t = extend({}, ix)
	if ix.desc then
		t.desc = extend({}, ix.desc)
	end
	t.type = ix.type
	return t
end
function Tx:save_table_schema(schema)
	--NOTE: only saving enough information to read the data back in absence of
	--a paper schema, and to validate a paper schema against the used layout.
	local t = {
		format = 1, --layout format (the only one we have, implemented here)
		dyn_offset_size = schema.dyn_offset_size,
		int_key = schema.int_key,
		key_fields = {max_rec_size = schema.key_fields.max_rec_size},
		val_fields = {max_rec_size = schema.val_fields.max_rec_size},
		is_index = schema.is_index,
		val_table = schema.val_table,
	}
	for i=1,2 do
		local is_key = i == 1
		local is_val = i == 2
		local F = is_key and 'key_fields' or 'val_fields'
		for i,f in ipairs(schema[F]) do
			t[F][i] = {
				col = f.col,
				col_pos = f.col_pos, --in original schema fields array
				mdbx_type = f.mdbx_type,
				maxlen = f.maxlen,
				padded = f.padded,
				not_null = is_val and f.not_null or nil,
				--computed attributes
				elem_size = f.elem_size, --for validating custom types in the future.
				descending = f.descending,
				mdbx_collation = f.mdbx_collation,
				fixed_offset = f.fixed_offset, --what offset means.
				offset = f.offset, --null for varsize keys
			}
		end
	end
	if schema.indexes then
		t.indexes = {}
		for _,ix in ipairs(schema.indexes) do
			add(t.indexes, save_index_def(ix))
		end
	end
	local k = schema.name
	local v = pp(t, false)
	self:put_raw('$schema',
		cast(u8p, k), #k,
		cast(u8p, v), #v
	)
end

function Tx:load_table_schema(table_name)
	if table_name == '$schema' then return end
	if not self:table_exists'$schema' then return end
	local k = table_name
	local v, v_len = self:get_raw('$schema', cast(u8p, k), #k)
	if not v then return end
	local schema = eval(str(v, v_len))
	--reconstruct schema from stored table schema.
	assertf(schema.format == 1,
		'unknown schema format for table %s: %s', table_name, schema.format)
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
	schema.layouted = true --layouted but not compiled
	return schema
end

function Tx:try_drop_table_schema(table_name)
	return self:try_del_raw('$schema', cast(u8p, table_name), #table_name)
end

local function sort_indexes(indexes)
	sort(indexes, function(ix1, ix2)
		return ix1.name < ix2.name
	end)
end

function Tx:try_open_table(tab, mode, flags, schema)

	if not tab then --opening the unnamed root table
		return try_raw_open_table(self, mode, flags)
	end

	local t = self.db.open_tables[tab]
	if t then return t end

	local table_name = tab
	if not schema then
		local db_schema = self.db.schema
		local tables = db_schema and db_schema.tables
		schema = tables and tables[table_name]
	end

	local stored_schema = self:load_table_schema(table_name)
	if stored_schema and not schema then
		if mode == 'c' then
			--old table with stored schema, but now we're creating one without.
			self:try_drop_table_schema(table_name)
			stored_schema = nil
		else
			--old table for which paper schema was lost, use stored schema.
			schema = stored_schema
		end
	end

	if schema then
		schema.name = table_name
		self:compile_table_schema(schema)
	end

	if stored_schema and schema and schema ~= stored_schema then
		--table has stored schema, schemas must match.
		local errs = {}
		for _,k in ipairs{
			'dyn_offset_size', 'int_key', 'is_index', 'val_table',
		} do
			local pv =  schema[k]
			local sv = stored_schema[k]
			if pv ~= sv then
				add(errs, fmt(' %s mismatch: expected: %s, got: %s', k, pv, sv))
			end
		end
		for i=1,2 do
			local F = i == 1 and 'key_fields' or 'val_fields'
			local pfields =  schema[F]
			local sfields = stored_schema[F]
			if #pfields ~= #sfields then
				add(errs, fmt(' %s count differs: expected: %d, got: %d',
					F, #pfields, #sfields))
			end
			for i = 1, min(#pfields, #sfields) do
				local pf = pfields[i]
				local sf = sfields[i]
				for _,k in ipairs{
					'col', 'col_pos', 'mdbx_type', 'maxlen', 'padded', 'not_null',
					'elem_size', 'descending', 'mdbx_collation',
					'fixed_offset', 'offset',
				} do
					local pv = pf[k]
					local sv = sf[k]
					if pv ~= sv then
						add(errs, fmt(' %s[%d].%s mismatch: expected: %s, got: %s',
							F, i, k, pv, sv))
					end
				end
			end
		end
		if #errs > 0 then
			error(fmt('schema mismatch for table: `%s`:\n%s',
				table_name, cat(errs, '\n')))
		end
	end

	local in_sub = (mode == 'w' or mode == 'c') and schema
		and schema and (schema.is_index or schema.uks or schema.ixs)
	if in_sub then
		self = self:txw()
	end

	flags = bor(flags or 0,
		schema and schema.int_key and mdbx.MDBX_INTEGERKEY or 0)
	local t, created = try_raw_open_table(self, table_name, mode, flags)
	if not t then
		if in_sub then self:abort() end
		return nil, created
	end

	if schema then

		if created and not stored_schema then
			self:save_table_schema(schema)
		end

		if schema.uks or schema.ixs then
			schema.indexes = {}
			if schema.uks then
				for uk_name, uk in pairs(schema.uks) do
					add(schema.indexes, uk)
				end
			end
			if schema.ixs then
				for ix_name, ix in pairs(schema.ixs) do
					add(schema.indexes, ix)
				end
			end
			sort_indexes(schema.indexes)
			for _,ix in ipairs(schema.indexes) do
				local ix_t, err = self:try_open_index(table_name, ix.name, mode)
				if not ix_t then
					if in_sub then self:abort() end
					return nil, err
				end
			end
		end

		if created and schema.is_index then
			local ok, err = schema:try_create(self)
			if not ok then
				if in_sub then self:abort() end
				return nil, err
			end
		end

	end

	if in_sub then
		self:commit()
	end

	t.schema = schema
	return t, created
end

function Tx:dbi(tab, mode)
	local t = self.db.open_tables[tab or false]
	if not t then
		if mode == 'w' or mode == 'c' then
			t = self:open_table(tab, mode)
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

local raw_try_drop_table = Tx.try_drop_table
function Tx:try_drop_table(tab)
	local dbi, schema, name = self:dbi(tab)
	if not dbi then return nil, schema end
	assert(name)
	assert(raw_try_drop_table(self, dbi))
	self:try_drop_table_schema(name)
	if schema.indexes then
		while #schema.indexes > 0 do
			last(schema.indexes):drop()
		end
	end
	if schema.fks then
		while #schema.fks > 0 do
			last(schema.fks):drop()
		end
	end
	return true
end

local try_rename_table = Tx.try_rename_table
function Tx:try_rename_table(tab, new_table_name)
	local dbi, schema = self:dbi(tab)
	if not dbi then return nil, schema end
	self = self:txw()
	local ok, err = try_rename_table(self, dbi, new_table_name)
	if not ok then
		self:abort()
		return nil, err
	end
	if schema then
		local k1 = schema.name
		local k2 = new_table_name
		local ok, err = self:try_move_key_raw('$schema',
			cast(u8p, k1), #k1,
			cast(u8p, k2), #k2
		)
		if not ok then
			self:abort()
			return nil, err
		end
	end
	self:commit()
	return true
end

--indexes --------------------------------------------------------------------

local ix1_key_rec_buffer = buffer()
local ix2_key_rec_buffer = buffer()
local ix_val_rec_buffer = buffer()

function Tx:index_schema(val_table, cols)
	local val_schema = assert(self.db.schema.tables[val_table])
	local ix_tbl_name = val_table..'/'..cat(cols, '-')
	local ix_fields = {}
	for _,col in ipairs(cols) do
		local f = update({}, val_schema.fields[col])
		add(ix_fields, f)
	end
	local ix_schema = {
		name = ix_tbl_name,
		fields = ix_fields,
		pk = cols,
		is_index = true,
		val_table = val_table,
	}
	return ix_schema
end

function Tx:compile_index_schema(ix_schema)

	assert(ix_schema.is_index)

	local val_table = assert(ix_schema.val_table)
	local val_schema = assert(self.db.schema.tables[val_table])

	local cols = cols_list(cat(ix_schema.pk, ' '))
	local dt = {}

	--create index methods

	function ix_schema:try_create(self)
		local self = self:txw()
		local ix_dbi = self:dbi(ix_tbl_name, 'c')
		local xk, xk_buf_sz = ix1_key_rec_buffer(ix_schema.key_fields.max_rec_size)
		local xv, xv_buf_sz = ix_val_rec_buffer(val_schema.key_fields.max_rec_size)
		for cur, k, k_sz, v, v_sz in self:each_raw(val_table) do
			local vn = decode_val(val_schema, v, v_sz, dt, cols, '[]')
			local xk_sz = encode_key(self, ix_schema, nil, xk, xk_buf_sz, cols, '[]', dt)
			assert(k_sz <= xv_buf_sz, k_sz)
			copy(xv, k, k_sz)
			local ok, err = self:try_insert_raw(ix_dbi, xk, xk_sz, xv, k_sz)
			if not ok then
				self:abort()
				return nil, err
			end
		end
		self:commit()
		return true
	end

	local dt0 = {}
	function ix_schema:update(self, k, k_sz, v, v_sz, v0, v0_sz)

		local ix_dbi = self:dbi(ix_tbl_name, 'w')

		--[[ cases to cover:
		      record       index
         ----------------------
				A -> X       X -> A  existing record and associated index key
			----------------------
			~  A -> X       X -> A  record updated but index key didn't change (do nothing)
			~  A -> Y    -  X -> A  record updated: remove old index
			             +  Y -> A  and add new index
			+  B -> X    x  X -> B  record inserted: unique key violation
			+  B -> Y    +  Y -> B  record inserted: add index
		]]

		--derive index key from v
		local xk, xk_buf_sz = ix1_key_rec_buffer(ix_schema.key_fields.max_rec_size)
		local vn = decode_val(val_schema, v, v_sz, dt, cols, '[]')
		local xk_sz = encode_key(self, ix_schema, nil, xk, xk_buf_sz, cols, '[]', dt)
		clear(dt)

		if v0 then --record updated: remove the old index record

			--derive old index key from v0 to compare with the new one.
			local xk0, xk0_buf_sz = ix2_key_rec_buffer(ix_schema.key_fields.max_rec_size)
			local vn = decode_val(val_schema, v0, v0_sz, dt0, cols, '[]')
			local xk0_sz = encode_key(self, ix_schema, nil, xk0, xk0_buf_sz, cols, '[]', dt0)
			clear(dt0)

			--abort if index key didn't change
			if xk_sz == xk0_sz and memcmp(xk, xk0, xk_sz) == 0 then
				return
			end

			self:must_del_raw(ix_dbi, xk0, xk0_sz)
		end

		return self:try_insert_raw(ix_dbi, xk, xk_sz, k, k_sz)
	end

	function ix_schema:del(self, k, k_sz)
		--
	end

	function ix_schema:drop(self)
		self:drop_table(ix_dbi)
		ix_dbi = nil
		self.db.schema.tables[ix_tbl_name] = nil
		assert(remove_value(val_schema.indexes, ix_schema))
	end

	--add index to table schema to be auto-updated on put and del.
	local indexes = attr(val_schema, 'indexes')
	add(indexes, ix_schema)
	indexes[ix_tbl_name] = ix_schema

end

function Tx:try_open_index(tbl_name, cols, mode)
	local ix_schema = self:index_schema(tbl_name, cols)
	local schema_tables = self.db.schema.tables
	assert(not schema_tables[ix_schema.name])
	local t, created = self:try_open_table(ix_schema.name, mode, nil, ix_schema)
	if t then
		schema_tables[ix_schema.name] = ix_schema
	end
	return t, created
end

function Tx:try_create_index(tbl_name, cols)
	return self:try_open_index(tbl_name, cols, 'c')
end

function Tx:create_index(tbl_name, cols)
	assert(self:try_create_index(tbl_name, cols))
end

function Tx:try_drop_index(ix_tbl_name)
	local ix_schema = self.db.schema.tables[ix_tbl_name]
	if not ix_schema then return nil, 'not_found' end
	if not ix_schema.is_index then return nil, 'not_index' end
	return ix_schema:drop(self)
end

------------------------------------------------------------------------------

local function key_field(schema, col)
	local f = schema.fields[col]
	local ki = f.key_index
	if f and ki then return f end
	assertf(f, 'unknown field: %s.%s', schema.name, col)
	assertf(ki, 'not a key field: %s.%s', schema.name, col)
end

local function val_field(schema, col)
	local f = schema.fields[col]
	local vi = f and f.val_index
	if f and vi then return f, vi end
	assertf(f, 'unknown field: %s.%s', schema.name, col)
	assertf(vi, 'not a value field: %s.%s', schema.name, col)
end

--encoding and decoding ------------------------------------------------------

local key_rec_buffer = buffer()
local val_rec_buffer = buffer()

local m_cols_list = memoize(function(cols)
	if cols:starts'[' then
		assert(cols:ends']')
		cols = cols:sub(2, -2)
	elseif cols:starts'{' then
		assert(cols:ends'}')
		cols = cols:sub(2, -2)
	end
	local t = collect(words(cols))
	assert(#t > 0)
	for i,col in ipairs(t) do t[col] = i end
	t[S] = cat(t, ',')
	return t
end)
function cols_list(cols)
	if not cols then return nil, nil end
	if cols == '[]' then return nil, '[]' end
	if cols == '{}' then return nil, '{}' end
	local as = cols:starts'[' and '[]' or cols:starts '{' and '{}' or nil
	return m_cols_list(cols), as
end

local function select_col(cols, as, col, ...)
	if as == '{}' then
		local t = ...
		return t[col]
	else
		local i = assert(cols[col])
		if as == '[]' then
			local t = ...
			return t[i]
		else
			return (select(i, ...))
		end
	end
end

local function resolve_null_val(self, schema, f)
	local default = f.mdbx_default
	if isfunc(default) then
		default = default(schema, f)
	end
	return default
end

do
local pp = new'u8*[1]'
function encode_key(self, schema, autoinc_f, rec, rec_buf_sz, cols, as, ...)
	if #schema.key_fields == 0 then return 0 end
	local encode_int_key = schema.encode_int_key
	pp[0] = rec
	for ki,f in ipairs(schema.key_fields) do
		local val = select_col(cols, as, f.col, ...)
		if val == nil or val == null then
			val = resolve_null_val(self, schema, f)
		end
		if val == nil and f == autoinc_f then
			val = self:gen_id(schema.name)
		end
		if val == nil then
			error(fmt('null key: %s.%s', schema.name, f.col), 2)
		end
		if encode_int_key then
			return encode_int_key(rec, rec_buf_sz, val)
		else
			local len = f.get_val_len(val)
			len = min(len, f.maxlen or 1) --truncate
			f.encode(pp[0], val, len)
			C.schema_key_add(schema._st, ki-1, rec, rec_buf_sz, len, pp)
		end
	end
	return pp[0] - rec
end
end

do
local pp = new'u8*[1]'
function encode_val(self, schema, rec, rec_buf_sz, cols, as, ...)
	if #schema.val_fields == 0 then return 0 end
	C.schema_val_add_start(schema._st, rec, rec_buf_sz, pp)
	for vi,f in ipairs(schema.val_fields) do
		local val = select_col(cols, as, f.col, ...)
		if val == nil or val == null then
			val = resolve_null_val(self, schema, f)
		end
		if val == nil and f.not_null then
			error(fmt('not_null column is null: %s.%s', schema.name, f.col), 2)
		end
		local len
		if val ~= nil then
			len = f.get_val_len(val)
			len = min(len, f.maxlen or 1) --truncate
			f.encode(pp[0], val, len)
		else
			len = -1
		end
		C.schema_val_add(schema._st, vi-1, rec, rec_buf_sz, len, pp)
	end
	return pp[0] - rec
end
end

local function get_raw_by_pk(self, dbi, schema, ...)
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local k_sz = encode_key(self, schema, nil, k, k_buf_sz, schema.key_cols, nil, ...)
	return self:get_raw(dbi, k, k_sz)
end

do
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

do
local pout = new'u8*[1]'
function decode_val(schema, rec, rec_sz, t, cols, as, i0)
	i0 = i0 or 1
	local n = cols and #cols or #schema.val_fields
	for i=1,n do
		local col = cols[i]
		local f, vi = val_field(schema, col)
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

--CRUD -----------------------------------------------------------------------

function Tx:is_null(tab, col, ...) --returns is_null, [reason]
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return true, schema end
	local f, vi = val_field(schema, col)
	local v, v_sz = get_raw_by_pk(self, dbi, schema, ...)
	if not v then return true, v_sz end
	return C.schema_val_is_null(schema._st, vi-1, v, v_sz) ~= 0
end

function Tx:exists(tab, ...) --returns record_exists, table_exists
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return false, false end
	local v = get_raw_by_pk(self, dbi, schema, ...)
	if not v then return false, true end
	return true, true
end

function Tx:try_get(tab, cols, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return false, schema end
	local v, v_sz = get_raw_by_pk(self, dbi, schema, ...)
	if not v then return false, v_sz end
	local cols, as = cols_list(cols)
	local t = {}
	local i0 = 1
	if schema.is_index then
		local val_table = assert(schema.val_table)
		local t_dbi, t_schema = self:dbi_schema(val_table)
		if not t_dbi then return false, t_schema end
		cols = cols or t_schema.val_cols
		local k, k_sz = v, v_sz
		v, v_sz = self:must_get_raw(t_dbi, k, k_sz)
		i0 = decode_key(t_schema, k, k_sz, t, as) + 1
		schema = t_schema
	else
		cols = cols or schema.val_cols
	end
	local n = decode_val(schema, v, v_sz, t, cols, as, i0)
	if as then
		return true, t, n
	else
		return true, unpack(t, 1, n)
	end
end

function Db:on(table_name, ...)
	local schema = self.schema.tables[table_name]
	events.on(schema, ...)
end
function Db:off(table_name, ...)
	local schema = self.schema.tables[table_name]
	events.off(schema, ...)
end

local put_v0_buffer = buffer()
local function try_put(self, flags, op, tab, cols, ...)
	local dbi, schema = self:dbi_schema(tab, 'w')
	local cols, as = cols_list(cols)
	cols = cols or schema.cols
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local v, v_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local autoinc_f = op == 'insert' and schema.autoinc_field
	local k_sz = encode_key(self, schema, autoinc_f, k, k_buf_sz, cols, as, ...)
	local ret, err
	if op == 'update' or op == 'upsert' or schema.fks or schema.indexes then
		local cur = self:cursor(dbi, 'w')
		local v0, v0_sz = cur:get_raw(k, k_sz)
		local v_sz
		if v0 then
			if op == 'insert' then
				cur:close()
				return nil, 'exists'
			end
			--next mdbx put command will invalidate v0 so we need to save it.
			local v0_unstable = v0
			v0, v0_sz = put_v0_buffer(v0_sz)
			copy(v0, v0_unstable, v0_sz)
			if op == 'update' or op == 'upsert' then --decode v0 and override it.
				local all_cols = schema.cols
				local t = {}
				decode_val(schema, v0, v0_sz, t, all_cols, '[]')
				for i=1,#cols do
					local v = select_col(cols, as, cols[i], ...)
					if v ~= nil then --when updating, nil means skip, null means null.
						t[i] = v
					end
				end
				v_sz = encode_val(self, schema, v, v_buf_sz, all_cols, '[]', t)
			else --update all cols so no need to decode v0
				v_sz = encode_val(self, schema, v, v_buf_sz, cols, as, ...)
			end
			if schema.fks then
				for _,fk in ipairs(schema.fks) do
					local ok, err = fk:check(self, k, k_sz, v, v_sz)
					if not ok then
						cur:close()
						return nil, err
					end
				end
			end
			cur:set_raw(v, v_sz)
			cur:close()
		elseif op == 'update' then --update but existing row not found
			cur:close()
			return nil, v0_sz
		else --put, insert, or upsert new record
			cur:close()
			v_sz = encode_val(self, schema, v, v_buf_sz, cols, as, ...)
			if schema.fks then
				for _,fk in ipairs(schema.fks) do
					local ok, err = fk:check(self, k, k_sz, v, v_sz)
					if not ok then
						return nil, err
					end
				end
			end
			local ret, err = self:try_put_raw(dbi, k, k_sz, v, v_sz, flags)
			if not ret then return nil, err end
		end
		if schema.indexes then
			local self = self:txw()
			for _,ix in ipairs(schema.indexes) do
				local ok, err = ix:update(self, k, k_sz, v, v_sz, v0, v0_sz)
				if not ok then
					self:abort()
					return nil, err
				end
			end
			self:commit()
		end
	else --put or insert with no indexes to update or fks to check.
		local v_sz = encode_val(self, schema, v, v_buf_sz, cols, as, ...)
		local ret, err = self:try_put_raw(dbi, k, k_sz, v, v_sz, flags)
		if not ret then return nil, err end
	end
	log('note', 'db', op, '%s %s', schema.name, cols[S])
	return true, autoinc_v
end
function Tx:try_put(tab, ...)
	return try_put(self, nil, 'put', tab, ...)
end
function Tx:put(tab, ...)
	local ret, err = try_put(self, nil, 'put', tab, ...)
	if ret then return ret end
	check('db', 'put', ret, '%s: %s', self:table_name(tab), err)
end
function Tx:try_insert(tab, ...)
	return try_put(self, mdbx.MDBX_NOOVERWRITE, 'insert', tab, ...)
end
function Tx:insert(tab, ...)
	local ret, err = try_put(self, mdbx.MDBX_NOOVERWRITE, 'insert', tab, ...)
	if ret then return ret end
	check('db', 'insert', false, '%s: %s', self:table_name(tab), err)
end
function Tx:try_update(tab, ...)
	return try_put(self, mdbx.MDBX_CURRENT, 'update', tab, ...)
end
function Tx:update(tab, ...)
	local ret, err = try_put(self, mdbx.MDBX_CURRENT, 'update', tab, ...)
	if ret then return ret end
	check('db', 'update', false, '%s: %s', self:table_name(tab), err)
end
function Tx:upsert(tab, ...)
	local ret, err = try_put(self, nil, 'upsert', tab, ...)
	if ret then return ret end
	check('db', 'upsert', false, '%s: %s', self:table_name(tab), err)
end

function Tx:try_del(tab, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return nil, schema end
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local k_sz = encode_key(self, schema, nil, k, k_buf_sz, schema.key_cols, nil, ...)
	local ok, err = self:try_del_raw(dbi, k, k_sz)
	if not ok then return nil, err end
	if schema.indexes then
		for _,ix in ipairs(schema.indexes) do
			ix:del(k, k_sz)
		end
	end
	return true
end
function Tx:del(tab, ...)
	local ok, err = self:try_del(tab, ...)
	if ok then return end
	check('db', 'del', false, '%s: %s', self:table_name(tab), err)
end

function Tx:del_exact(tab, cols, ...)
	local dbi, schema = self:dbi_schema(tab)
	if not dbi then return nil, schema end
	local cols, as = cols_list(cols)
	cols = cols or schema.cols
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local v, v_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local k_sz = encode_key(self, schema, nil, k, k_buf_sz, cols, as, ...)
	local v_sz = encode_val(self, schema     , v, v_buf_sz, cols, as, ...)
	local ok, err = self:try_del_raw(dbi, k, k_sz, v, v_sz)
	if not ok then return nil, err end
	if schema.indexes then
		for _,ix in ipairs(schema.index) do
			ix:del(k, k_sz, v, v_sz)
		end
	end
	return true
end
function Tx:must_del_exact(tab, ...)
	local ok, err = self:del_exact(tab, ...)
	if ok then return end
	check('db', 'del_exact', false, '%s: %s', self:table_name(tab), err)
end

function Tx:put_records(tab, cols, records)
	if istab(cols) then
		cols, records = '[]', cols
	end
	local dbi, schema = self:dbi_schema(tab, 'w')
	local cols, as = cols_list(cols)
	cols = cols or schema.cols
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local v, v_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	for _,vals in ipairs(records) do
		local k_sz = encode_key(self, schema, nil, k, k_buf_sz, cols, as, vals)
		local v_sz = encode_val(self, schema     , v, v_buf_sz, cols, as, vals)
		self:put_raw(dbi, k, k_sz, v, v_sz)
	end
end

--cursors --------------------------------------------------------------------

local function skip_ok(ok, ...)
	if not ok then return end
	return ...
end
local function must_ok(ok, ...)
	assert(ok, ...)
	return ...
end

function Tx:get(...)
	return skip_ok(self:try_get(...))
end
function Tx:must_get(...)
	return must_ok(self:try_get(...))
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

local function decode_kv(schema, k, k_sz, v, v_sz, t, val_cols, as)
	local kn = decode_key(schema, k, k_sz, t, as)
	local vn = decode_val(schema, v, v_sz, t, val_cols, as, kn + 1)
	local n = kn + vn
	if as then
		return true, t, n
	else
		return true, unpack(t, 1, n) --keys can't be nil so this can't stop too soon.
	end
end
function Cur:_try_get(val_cols, flags)
	local k, k_sz, v, v_sz = self:_get_raw(flags)
	if not k then return end
	local val_cols, as = cols_list(val_cols)
	val_cols = val_cols or self.schema.val_cols
	return decode_kv(self.schema, k, k_sz, v, v_sz, t or {}, val_cols, as)
end
function Cur:try_current(val_cols)
	return self:_try_get(val_cols, mdbx.MDBX_GET_CURRENT)
end
function Cur:current(val_cols)
	return skip_ok(self:_try_get(val_cols, mdbx.MDBX_GET_CURRENT))
end
function Cur:try_next(val_cols)
	return self:_try_get(val_cols, mdbx.MDBX_NEXT)
end
function Cur:next(val_cols)
	return skip_ok(self:_try_get(val_cols, mdbx.MDBX_NEXT))
end
function Cur:try_get(val_cols, ...)
	local k, k_buf_sz = key_rec_buffer(schema.key_fields.max_rec_size)
	local k_sz = encode_key(self, schema, nil, k, k_buf_sz, self.schema.key_cols, nil, ...)
	local v, v_sz = self:get_raw(k, k_sz)
	if not v then return end
	local t, val_cols, as = self._t, self._val_cols, self._as
	if not t then t, val_cols, as = {}, cols_list(val_cols) end
	val_cols = val_cols or self.schema.val_cols
	return decode_kv(self.schema, k, k_sz, v, v_sz, t, val_cols, as)
end
function Cur:get(...)
	return skip_ok(self:try_get(...))
end
function Cur:must_get(...)
	return must_ok(self:try_get(...))
end

function Cur:update(val_cols, ...)
	local schema = self.schema
	local val_cols, as = cols_list(val_cols)
	val_cols = val_cols or schema.val_cols
	local v, v_buf_sz = val_rec_buffer(schema.val_fields.max_rec_size)
	local k, k_sz, v0, v0_sz = self:current_raw()
	if not k then return nil, 'not_found' end
	assert(v_buf_sz >= v0_sz)
	copy(v, v0, v0_sz)
	local v_sz = encode_val(self, schema, v, v_buf_sz, val_cols, as, ...)
	self:set_raw(v, v_sz)
end

local function each_next_skip_ok(self, ok, ...)
	if not ok then return end
	return self, ...
end
local function each_next(self)
	local k, k_sz, v, v_sz = self:_get_raw(mdbx.MDBX_NEXT)
	if not k then return end
	local t, val_cols, as = self._t, self._val_cols, self._as
	return each_next_skip_ok(self,
		decode_kv(self.schema, k, k_sz, v, v_sz, t or {}, val_cols, as))
end
function Tx:each(tbl_name, val_cols, mode, t)
	local cur = self:cursor(tbl_name, mode)
	cur._each = true
	local val_cols, as = cols_list(val_cols)
	val_cols = val_cols or cur.schema.val_cols
	cur._val_cols, cur._as = val_cols, as
	cur._t = t or (not cur._as and {} or nil)
	return each_next, cur
end

--schema sync'ing ------------------------------------------------------------

function Db:extract_schema()
	local schema = schema.new{engine = 'mdbx'}
	schema.relevant_field_attrs = {
		col=1,
		col_pos=1,
		mdbx_type=1,
		maxlen=1,
		padded=1,
	}
	self:atomic(function(tx)
		for table_name in tx:each_table() do
			if not table_name:starts'$' and not table_name:has'/' then
				schema.tables[table_name] = tx:load_table_schema(table_name) or {raw = true}
			end
		end
	end)
	return schema
end

function Db:schema_diff()
	local ss = self:extract_schema()
	return self.schema:diff(ss)
end

function Tx:create_fk(fk)
	local dbi, schema = self:dbi_schema(fk.ref_table)
	add(attr(schema, 'fks'), fk)
	--self.db:on(fk.table, 'put', function()
	--	--
	--end)
	local fk_del
	if fk.onupdate == 'cascade' then
		function fk_del()

		end
	elseif fk.onupdate == 'set null' then
		function fk_del()

		end
	else --assume restrict
		function fk_del()

		end
	end
	self.db:on(fk.ref_table, 'del', fk_del)
end

function Tx:drop_fk(fk)

end

function Db:sync_schema(src, opt)
	local tx = self:txw()
	for tbl in tx:each_table() do
		tx:drop_table(tbl)
	end
	tx:commit()
	opt = opt or empty
	local src_sc =
		schema.isschema(src) and src
		or inherits(src, mdbx_db) and src:extract_schema()
		or assertf(false, 'schema or mdbx_db expected, got %s', type(src))
	local this_sc = self:extract_schema()
	local diff = schema.diff(this_sc, src_sc)
	diff:pp()
	local dry = opt.dry
	local function P(...)
		pr(fmt(...))
	end
	self:atomic(dry and 'r' or 'w', function(tx)
		if diff.tables then
			if diff.tables.add then
				for tbl_name, tbl in sortedpairs(diff.tables.add) do
					P('create table: %s', tbl_name)
					tx:create_table(tbl_name)
					if tbl.rows then
						for _,row in ipairs(tbl.rows) do
							assert(not tx.readonly)
							tx:insert(tbl_name, '[]', row)
						end
					end
					if tbl.indexes then
						for ix_name, ix in pairs(tbl.indexes) do
							P('create index: %s', ix_name)
							tx:create_index(tbl_name, ix)
						end
					end
					if tbl.fks then
						for fk_name, fk in pairs(tbl.fks) do
							P('create fk: %s', fk_name)
							tx:create_fk(fk)
						end
					end
				end
			end
			if diff.tables.update then
				for tbl_name, tbl in sortedpairs(diff.tables.update) do
					if tbl.indexes then
						if tbl.indexes.del then
							for ix_name, ix in pairs(tbl.indexes.remove) do
								--
							end
						end
						if tbl.indexes.add then
							for ix_name, ix in pairs(tbl.indexes.add) do
								P('create ix: %s', ix_name)
								tx:create_index(tbl_name, ix)
							end
						end
					end
				end
			end
		end
	end)
end
