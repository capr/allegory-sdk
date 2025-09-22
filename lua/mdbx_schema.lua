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
		- multi-value keys are \0-terminated so they are not 8-bit clean!

]]

require'mdbx'
require'utf8proc'
local C = ffi.load'mdbx_schema'

local
	typeof, tonumber, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast =
	typeof, tonumber, shl, shr, band, bor, xor, bnot, bswap, u8p, copy, cast

assert(ffi.abi'le')

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
	schema_col_type_f32,
	schema_col_type_f64,
} schema_col_type;

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
void schema_set(schema_table* tbl, int is_key, int col_i, void* rec, int rec_size, void* in , int in_len, u8** pp, int add);
int  schema_is_null(int col_i, void* rec);
]]

local col_ct = {
	utf8 = 'u8',
}

local schema_col_types = {
	i8   = C.schema_col_type_i8,
	i16  = C.schema_col_type_i16,
	i32  = C.schema_col_type_i32,
	i64  = C.schema_col_type_i64,
	u8   = C.schema_col_type_u8,
	u16  = C.schema_col_type_u16,
	u32  = C.schema_col_type_u32,
	u64  = C.schema_col_type_u64,
	f32  = C.schema_col_type_f32,
	f64  = C.schema_col_type_f64,
	utf8 = C.schema_col_type_u8,
}

--types than are stored in little-endian with MDBX_REVERSEKEY.
local le_col_type = {
	u64 = true,
	u32 = true,
	u16 = true,
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

local Db = mdbx_db

local key_rec_buf = buffer()
local val_rec_buf = buffer()

function Db:load_schema(schema)

	if schema then
		self.schema = schema
	else
		local schema_file = self.dir..'/schema.lua'
		if not self.readonly then
			mkdir(self.dir)
			if not exists(schema_file) then
				save(schema_file, 'return {}')
			end
		end
		self.schema = eval_file(schema_file)
	end

	self.schema.max_key_rec_size = 0
	self.schema.max_val_rec_size = 0
	for table_name, table_schema in pairs(self.schema.tables or {}) do
		self:load_table(table_name, table_schema)
		self.schema.max_key_rec_size = max(
			self.schema.max_key_rec_size,
			table_schema.key_cols.max_rec_size
		)
		self.schema.max_val_rec_size = max(
			self.schema.max_val_rec_size,
			table_schema.val_cols.max_rec_size
		)
	end

end

function Db:load_table(table_name, table_schema)

	--compute field layout and encoding parameters based on schema.
	--NOTE: changing this algorithm or making it non-deterministic in any way
	--will trash your existing databses, so better version it or something!

	--index fields by name and check for duplicate names.
	for col_i, col in ipairs(table_schema.fields) do
		assertf(not table_schema.fields[col.name], 'duplicate field name: %s', col.name)
		table_schema.fields[col.name] = col
	end

	local key_cols = {is_key = true}
	local val_cols = {is_key = false}

	--parse pk and set col.order, .type, .collation.
	for s in words(table_schema.pk) do
		local pk, s1 = s:match'^(.-):(.*)'
		local order = 'asc'
		local collation
		if not pk then
			pk = s
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
			assertf(not collation or collation == 'utf8_ai_ci' or collation == 'utf8',
				'invalid collation: %s', collation)
			assert(#pk > 0, 'pk is empty')
		end

		local col = assertf(table_schema.fields[pk], 'pk field not found: %s', pk)
		col.order = order
		if col.type == 'utf8' then
			col.collation = collation or 'utf8'
		end
		add(key_cols, col)
		col.key_index = #key_cols
	end

	--build val cols array with all cols that are not in pk.
	for col_i, col in ipairs(table_schema.fields) do
		col.index = col_i
		if not col.order then
			add(val_cols, col)
			col.val_index = #val_cols
		end
		--typecheck the field while we're at it.
		local elem_ct = col_ct[col.type] or col.type
		local elem_ct = assertf(elem_ct, 'unknown col type %s', col.type)
		col.elem_size = sizeof(elem_ct)
		col.elemp_ct = ctype(elem_ct..'*')
		assert(col.elem_size < 2^4) --must fit 4bit (see sort below)
	end

	assert(#key_cols < 2^16)
	assert(#val_cols < 2^16)

	--move varsize cols at the end to minimize the size of the dyn offset table.
	--order cols by elem_size to maintain alignment.
	--finally, order by index to get stable sorting.
	sort(val_cols, function(col1, col2)
		--elem_size fits in 4bit; col index fits in 16bit; 4+16 = 20 bits,
		--so any bit from bit 21+ can be used for extra conditions.
		local i1 = (col1.maxlen and 2^22 or 0) + (2^4-1 - col1.elem_size) * 2^16 + col1.index
		local i2 = (col2.maxlen and 2^22 or 0) + (2^4-1 - col2.elem_size) * 2^16 + col2.index
		return i1 < i2
	end)

	--MDBX_REVERSEKEY allows us to store uints in little endian,
	--exploit that in the simple case of a single uint key.
	local le_key = #key_cols == 1
		and key_cols[1].order == 'asc'
		and le_col_type[key_cols[1].type]
		and true or nil

	table_schema.key_cols = key_cols
	table_schema.val_cols = val_cols
	table_schema.reverse_keys = le_key

	--allocate the C schema
	local st = new'schema_table'
	local sc_key_cols = new('schema_col[?]', #key_cols)
	local sc_val_cols = new('schema_col[?]', #val_cols)
	st.key_cols = sc_key_cols
	st.val_cols = sc_val_cols
	st.n_key_cols = #key_cols
	st.n_val_cols = #val_cols
	--anchor these so they don't get collected
	key_cols._sc = sc_key_cols
	val_cols._sc = sc_val_cols
	table_schema._st = st

	--compute key and val layout and create field getters and setters.
	for _,cols in ipairs{key_cols, val_cols} do

		local is_val = cols == val_cols
		local is_key = cols == key_cols

		--find the number of fixsize cols.
		local fixsize_n = #cols
		for col_i, col in ipairs(cols) do
			if col.maxlen then --first varsize col
				fixsize_n = col_i - 1
				break
			end
		end
		cols.fixsize_n = fixsize_n

		--compute max row size, just the data (which is what it is for keys).
		local max_rec_size = 0
		for col_i,col in ipairs(cols) do
			local maxlen = col.maxlen or col.len or 1
			max_rec_size = max_rec_size + maxlen * col.elem_size
		end

		if is_key then
			assertf(max_rec_size <= self:db_max_key_size(),
				'pk too big: %d bytes (max is %d bytes)',
					max_rec_size, self:db_max_key_size())
		end

		--compute dynamic offset table (d.o.t.) length for val records.
		--all val cols after the first varsize col are at a dyn offset.
		--key cols can't have an offset table instead we use \0 separator.
		local dot_len = is_val and max(0, #cols - fixsize_n - 1) or 0

		--compute the number of bytes needed to hold all the null bits.
		local nulls_size = is_val and ceil(#cols / 8) or 0

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
				assert(false, 'value record too big')
			end
			st.dyn_offset_size = dyn_offset_size
			max_rec_size = max_rec_size + dot_len * dyn_offset_size
		end

		assertf(max_rec_size < 2^31,
			'record too big: %.0f bytes (max is 2GB-1)', max_rec_size)

		cols.max_rec_size = max_rec_size

		local fixsize_offset = nulls_size
		for col_i,col in ipairs(cols) do

			local sc = is_key and sc_key_cols or sc_val_cols
			sc[col_i-1].type = schema_col_types[col.type]
			sc[col_i-1].len = col.maxlen or col.len or 1
			sc[col_i-1].fixsize = col.maxlen and 0 or 1
			sc[col_i-1].descending = col.order == 'desc'
			sc[col_i-1].elem_size_shift = log2(col.elem_size)

			--compute and set fixed offset.
			local at_fixed_offset = col_i <= fixsize_n+1
			sc[col_i-1].static_offset = at_fixed_offset
			if at_fixed_offset then
				sc[col_i-1].offset = fixsize_offset
			end

			--move current fixed offset past this fixsize col.
			if col_i <= fixsize_n then
				fixsize_offset = fixsize_offset + col.elem_size * (col.len or 1)
			end

			--compute and set the offset where the dyn. offset is for this col.
			if is_val and not at_fixed_offset then
				local dot_index = fixsize_n+2 - col_i --col's index in d.o.t.
				assert(dot_index >= 0 and dot_index < dot_len)
				st_cols[col_i-1].offset_offset = fixsize_offset + dot_index * dyn_offset_size
			end

			--create col getters and setters.
			if col.len or col.maxlen then --array

			else --scalar
				function col.encode(buf, val)
					local buf, sz = buf(col.elem_size)
					cast(col.elemp_ct, buf)[0] = val
					return buf, 1
				end
				function col.decode(p)
					return cast(col.elemp_ct, p)[0]
				end
			end

			--[[
			if col.len or col.maxlen then --varsize and fixsize arrays
				local maxlen = col.len or col.maxlen
				if col.type == 'utf8' then --utf8 strings
					local ai_ci = col.collation == 'utf8_ai_ci'
					if desc or ai_ci then
						function set(buf, rec_sz, s, len)
							clear(buf)
							if is_val and set_null(buf, col_i, s == nil) then
								return
							end
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
						function get(buf, rec_sz, out, out_len)
							if is_val and is_null(buf, col_i) then
								return nil
							end
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
						function get(buf, rec_sz, out, out_len)
							if is_val and is_null(buf, col_i) then
								return nil
							end
							local p, p_len = rawget(buf, rec_sz)
							local len = min(p_len, out_len or 1/0) --truncate
							local out = out and cast(u8p, out) or u8a(len)
							copy(out, p, len)
							return out, len
						end
						function set(buf, rec_sz, val, len)
							clear(buf)
							if is_val and set_null(buf, col_i, val == nil) then
								return
							end
							rawset(buf, rec_sz, val, len)
						end
					end
				else
					function set(buf, rec_sz, val, len)
						clear(buf)
						if is_val and set_null(buf, col_i, val == nil) then
							return
						end
						assertf(typeof(val) == 'table', 'invalid val type %s', typeof(val))
						len = min(maxlen, len or #val) --truncate
						for i = 1, len do
							rawseti(buf, i-1, val[i])
						end
					end
				end
			else
				function get(buf, rec_sz)
					if is_val and is_null(buf, col_i) then
						return nil
					end
					return dec(getp(buf)[0], desc)
				end
				function set(buf, rec_sz, val)
					if is_val and set_null(buf, col_i, val == nil) then
						return
					end
					getp(buf)[0] = enc(val, desc)
				end
			end
			]]

		end --for col in cols

	end --for cols in key_cols, val_cols

	--pr(table_schema)

	local tx = self:tx'w'
	tx:open_table(table_name, bor(
		C.MDBX_CREATE,
		table_schema.reverse_keys and C.MDBX_REVERSEKEY or 0
	))
	tx:commit()

end

local function val_len(col, val) --truncates the value if needed.
	if val == nil then return 0 end
	local max_len = col.maxlen or col.len
	return max_len and min(#val, max_len) or 1
end

function key_cols(self, tbl)
	local s = self.schema.tables[tbl]
	return s, s.key_cols
end
function val_cols(self, tbl)
	local s = self.schema.tables[tbl]
	return s, s.val_cols
end

function key_col(self, tbl, col)
	local s = self.schema.tables[tbl]
	assert(s.key_cols[col.key_index], 'key col')
	return s, s.key_cols, col.key_index
end
function val_cols(self, tbl, col)
	local s = self.schema.tables[tbl]
	return s, s.val_cols
end

function Db:max_key_size(tbl)
	return key_cols(self, tbl).max_rec_size
end

function Db:max_val_size(tbl)
	return val_cols(self, tbl).max_rec_size
end

local encode_rec do
local pp = new'u8*[1]'
local val_buf = buffer()
function encode_rec(schema, is_key, cols, rec, rec_sz, ...)
	if #cols == 0 then return 0 end
	pp[0] = nil
	for col_i, col in ipairs(cols) do
		local val = select(col.index, ...)
		local buf, len = col.encode(val_buf, val)
		C.schema_set(schema._st, is_key, col_i-1, rec, rec_sz, buf, len, pp, true)
		pr(is_key, col_i, pp[0] - rec)
	end
	return pp[0] - rec
end
end

function Db:encode_key(tbl, buf, buf_sz, ...)
	local schema, cols = key_cols(self, tbl)
	return encode_rec(schema, true, cols, buf, buf_sz, ...)
end

function Db:encode_val(tbl, buf, buf_sz, ...)
	local schema, cols = val_cols(self, tbl)
	return encode_rec(schema, false, cols, buf, buf_sz, ...)
end

--[[
function Db:encode_key_col(tbl, col, val, buf, rec_sz, offset)
	return encode_rec_col(self, key_cols(self, tbl), col, val, buf, offset, rec_sz)
end

function Db:encode_val_col(tbl, col, val, buf, rec_sz, offset)
	return encode_rec_col(self, val_cols(self, tbl), col, val, buf, offset, rec_sz)
end
]]

local function rec_col_tostring(self, cols, col, buf, offset, rec_sz)
	local col = assertf(cols[col], 'unknown field: %s', col)
	local buf = cast(cols.pct, buf + (offset or 0))
	return col.tostring(buf, rec_sz)
end

function Db:is_null(tbl, col, buf, offset)
	local cols = val_cols(self, tbl)
	local col = assertf(cols[col], 'unknown field: %s', col)
	local buf = cast(cols.pct, buf + (offset or 0))
	return col.is_null(buf)
end

function Db:key_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, key_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:val_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, val_cols(self, tbl), col, buf, offset, rec_sz)
end

local function rec_col_decode(schema, is_key, cols, col, rec, rec_sz, out, out_len)
	rec_sz = tonumber(rec_sz)
	C.schema_get(schema._st, is_key, col.key_index-1, rec, rec_sz, out, out_len)
	return col.decode(p)
end

function Db:decode_key(tbl, col, buf, rec_sz, out, out_len)
	local schema, cols = key_col(self, tbl, col)
	return rec_col_decode(schema, true, cols, col, buf, rec_sz, out, out_len)
end

function Db:decode_val(tbl, col, buf, rec_sz, out, out_len)
	local schema, cols = val_col(self, tbl, col)
	return rec_col_decode(schema, false, cols, col, buf, rec_sz, out, out_len)
end

function Db:decode(tbl, col, buf, rec_sz, out, out_len)
	local schema, cols = val_col(self, tbl, col)
	return rec_col_decode(schema, cols, col, buf, rec_sz, out, out_len)
end

do
local key_buf = buffer()
local val_buf = buffer()
function mdbx_tx:put(tbl_name, ...)
	local key_rec, key_max_sz = key_buf(self.db.schema.max_key_rec_size)
	local val_rec, val_max_sz = val_buf(self.db.schema.max_val_rec_size)
	local key_sz = self.db:encode_key(tbl_name, key_rec, key_max_sz, ...)
	local val_sz = self.db:encode_val(tbl_name, val_rec, val_max_sz, ...)
	pr(key_rec, key_sz, val_rec, val_sz)
	self:put(tbl_name, key_rec, key_sz, val_rec, val_sz)
end
function mdbx_tx:put_records(tbl_name, records)
	local key_rec, key_max_sz = key_buf(self.db.schema.max_key_rec_size)
	local val_rec, val_max_sz = val_buf(self.db.schema.max_val_rec_size)
	for _,vals in ipairs(records) do
		local key_sz = self.db:encode_key(tbl_name, key_rec, key_max_sz, unpack(vals))
		local val_sz = self.db:encode_val(tbl_name, val_rec, val_max_sz, unpack(vals))
		pr(key_rec, key_sz, val_rec, val_sz)
		self:put(tbl_name, key_rec, key_sz, val_rec, val_sz)
	end
end
function mdbx_tx:get(tbl_name, ...)
		self:get(tbl_name, key_rec, key_sz, val_rec, val_sz)
end
end


--test -----------------------------------------------------------------------

if not ... then

	rm'mdbx_schema_test/mdbx.dat'
	rm'mdbx_schema_test/mdbx.lck'
	local db = mdbx_open('mdbx_schema_test')
	local schema = {tables = {}}
	local types = 'u8 u16 u32 u64 i8 i16 i32 i64 f32 f64'
	local num_tables = {}
	local varsize1_tables = {}
	local varsize2_tables = {}
	for order in words'asc desc' do

		--numeric at fixed offset.
		for typ in words(types) do
			local name = typ..':'..order
			local tbl = {
				name = name,
				test_type = typ,
				test_order = order,
				fields = {{name = 'id', type = typ}},
				pk = 'id:'..order,
			}
			add(num_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

		--varsize at fixed offset and utf8 enc/dec.
		local tbl = {
			name = 'varsize1'..':'..order,
			fields = {
				{name = 's', type = 'utf8', maxlen = 100},
			},
			pk = 's:'..order,
		}
		add(varsize1_tables, tbl)
		schema.tables[tbl.name] = tbl

		--varsize at dyn offset.
		local tbl = {
			name = 'varsize2'..':'..order,
			fields = {
				{name = 's1', type = 'utf8', maxlen = 100},
				{name = 's2', type = 'utf8', maxlen = 100},
			},
			pk = 's1 s2:'..order,
		}
		add(varsize2_tables, tbl)
		schema.tables[tbl.name] = tbl

	end
	db:load_schema(schema)

	--test int and float decoders and encoders
	for _,tbl in ipairs(num_tables) do
		local typ = tbl.test_type
		local tx = db:tx'w'
		local bits = tonumber(typ:sub(2))
		local ntyp = typ:sub(1,1)
		local nums =
			ntyp == 'u'  and {0,1,2,2ULL^bits-1} or
			ntyp == 'i'  and {-2LL^(bits-1),-(2LL^(bits-1)-1),-2,-1,0,1,2,2LL^(bits-1)-2,2LL^(bits-1)-1} or
			typ == 'f64' and {-2^52,-2,-1,-0.1,-0,0,0.1,1,2^52} or
			typ == 'f32' and {-2^23,-2,-1,cast('float', -0.1),-0,0,cast('float', 0.1),1,2^23}
		assert(nums)
		local t = {}
		for _,i in ipairs(nums) do
			add(t, {i})
		end
		tx:put_records(tbl.name, t)
		tx:commit()
		if tbl.fields.id.order == 'desc' then
			reverse(nums)
		end
		tx = db:tx'r'
		local i = 1
		for k,v in tx:each(tbl.name) do
			local id = db:decode_key(tbl.name, 'id', k.data, k.size)
			--local k = str(k.data, k.size)
			--local v = str(v.data, v.size)
			pr(tbl.name, id)
			assertf(id == nums[i], '%q ~= %q', id, nums[i])
			i = i + 1
		end
		tx:commit()
	end

	--test varsize1
	for _,tbl in ipairs(varsize1_tables) do
		local t = {
			{'a' },
			{'bb'},
			{'aa'},
			{'b' },
		}
		local tx = db:tx'w'
		tx:put_records(tbl.name, t)
		tx:commit()

		local tx = db:tx()
		pr('***', tbl.pk, '***')
		for k,v in tx:each(tbl.name) do
			local s, len = db:decode_key(tbl.name, 's', k.data, tonumber(k.size))
			pr(str(s, len))
		end
		tx:commit()
		pr()
	end
	pr()

	--test varsize2
	for _,tbl in ipairs(varsize2_tables) do
		local t = {
			{'a'  , 'b' , },
			{'a'  , 'a' , },
			{'a'  , 'aa', },
			{'a'  , 'bb', },
			{'aa' , 'a' , },
			{'aa' , 'b' , },
			{'bb' , 'a' , },
			{'bb' , 'aa', },
			{'bb' , 'bb', },
			{'aa' , 'bb', },
			{'b'  , 'a' , },
			{nil  , 'a' , },
			{'a'  , nil , },
		}
		local tx = db:tx'w'
		tx:put_records(tbl.name, t)
		tx:commit()

		local tx = db:tx()
		pr('***', tbl.pk, '***')
		for k,v in tx:each(tbl.name) do
			local s1, len1 = db:decode_key(tbl.name, 's1', k.data, tonumber(k.size))
			local s2, len2 = db:decode_key(tbl.name, 's2', k.data, tonumber(k.size))
			pr(str(s1, len1), str(s2, len2))
		end
		tx:commit()
		pr()
	end

	db:close()

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
