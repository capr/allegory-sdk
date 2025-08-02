--TODO: descending keys.

require'mdbx'

local
	typeof, tonumber, shl, shr, xor, bnot, bswap, u8p, copy, cast =
	typeof, tonumber, shl, shr, xor, bnot, bswap, u8p, copy, cast

local col_ct = {
	double = 'double',
	float  = 'float',
	int    = 'int32_t',
	string = 'char',
	f64    = 'double',
	f32    = 'float',
	u64    = 'uint64_t',
	i64    = 'int64_t',
	u32    = 'uint32_t',
	i32    = 'int32_t',
	u16    = 'uint16_t',
	i16    = 'int16_t',
	u8     = 'uint8_t',
	i8     = 'int8_t',
}

local key_col_ct = {
	double   = 'uint64_t',
	float    = 'uint32_t',
}

local u = new'union { uint64_t u; double f; struct { int32_t s1; int32_t s2; }; }'
local function decode_f64(v)
	u.u = v
	if shr(u.u, 63) ~= 0 then
		u.u = xor(u.u, 0x8000000000000000ULL)
	else
		u.u = bnot(u.u)
	end
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	return tonumber(u.f)
end
local function encode_f64(v)
	u.f = v
	if shr(u.u, 63) ~= 0 then
		u.u = bnot(u.u)
	else
		u.u = xor(u.u, 0x8000000000000000ULL)
	end
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	return u.u
end

local u = new'union { uint32_t u; float f; int32_t s; }'
local function decode_f32(v)
	u.u = v
	if shr(u.u, 31) ~= 0 then
		u.u = xor(u.u, 0x80000000)
	else
		u.u = bnot(u.u)
	end
	u.s = bswap(u.s)
	return tonumber(u.f)
end
local function encode_f32(v)
	u.f = v
	if shr(u.u, 31) ~= 0 then
		u.u = bnot(u.u)
	else
		u.u = xor(u.u, 0x80000000)
	end
	u.s = bswap(u.s)
	return u.u
end

local u = new'union { uint64_t u; int64_t i; struct { int32_t s1; int32_t s2; }; }'
local function decode_u64(v)
	u.u = v
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	return u.u
end
local encode_u64 = decode_u64
local function decode_i64(v)
	u.i = v
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	u.u = xor(u.u, 0x8000000000000000ULL)
	return u.i
end
local function encode_i64(v)
	u.i = v
	u.u = xor(u.u, 0x8000000000000000ULL)
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	return u.i
end

local Db = mdbx.Db

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

	self:open_tables(self.schema.tables)

	--compute field layout and encoding parameters based on schema.
	--NOTE: changing this algorithm or making it non-deterministic in any way
	--will trash your existing databses, so better version it or something!
	for table_name, table_schema in pairs(self.schema.tables or {}) do

		--index fields by name and check for duplicate names.
		for i,col in ipairs(table_schema.fields) do
			assertf(not table_schema.fields[col.name], 'duplicate field name: %s', col.name)
			table_schema.fields[col.name] = col
		end

		--split cols into key cols and val cols based on pk.
		local i = 1
		for s in words(table_schema.pk) do
			local pk, order = s:match'^(.-):(.*)'
			if not pk then
				pk, order = s, 'asc'
			else
				assert(order == 'desc' or order == 'asc')
				assert(#pk > 0)
			end
			local col = assertf(table_schema.fields[pk], 'pk field not found: %s', pk)
			col.order = order
			col.index = i
			i = i + 1
		end

		local key_cols = {}
		local val_cols = {}
		for _,col in ipairs(table_schema.fields) do
			local elem_ct = col_ct[col.type]
			if col.index then --part of pk
				key_cols[col.index] = col
				key_cols[col.name] = col
				elem_ct = key_col_ct[col.name] or elem_ct
			else
				add(val_cols, col)
				val_cols[col.name] = col
				col.index = #val_cols
			end
			--typecheck the field while we're at it.
			col.elem_ct = assertf(elem_ct, 'unknown col type %s', col.type)
			col.elem_size = sizeof(col.elem_ct)
			assert(col.elem_size < 2^4) --must fit 4bit (see sort below)
		end

		assert(#key_cols < 2^16)
		assert(#val_cols < 2^16)

		--move varsize cols at the end to minimize the size of the dyn offset table.
		--order cols by elem_size to maintain alignment.
		sort(val_cols, function(col1, col2)
			--elem_size fits in 4bit; col_index fits in 16bit; 4+16 = 20 bits,
			--so any bit from bit 21+ can be used for extra conditions.
			local i1 = (col1.maxlen and 2^22 or 0) + (2^4-1 - col1.elem_size) * 2^16 + col1.index
			local i2 = (col2.maxlen and 2^22 or 0) + (2^4-1 - col2.elem_size) * 2^16 + col2.index
			return i1 < i2
		end)

		table_schema.key_cols = key_cols
		table_schema.val_cols = val_cols

		for _,cols in ipairs{key_cols, val_cols} do

			--compute dynamic offset table (d.o.t.) length.
			--all cols after the first varsize col need a dyn offset.
			local dot_len = 0
			local fixsize_n = #cols
			for i,col in ipairs(cols) do
				if col.maxlen then --first varsize col
					dot_len = #cols - i
					fixsize_n = i - 1
					break
				end
			end
			cols.fixsize_n = fixsize_n

			--compute max row size, excluding d.o.t.
			local max_rec_size = 0
			for _,col in ipairs(cols) do
				max_rec_size = max_rec_size + (col.maxlen or col.len or 1) * col.elem_size
			end

			--compute offset C type based on how large the offsets can be.
			local offset_ct = 'uint64_t'
			if max_rec_size - dot_len < 2^8 then
				offset_ct = 'uint8_t'
			elseif max_rec_size - dot_len * 2 < 2^16 then
				offset_ct = 'uint16_t'
			elseif max_rec_size - dot_len * 4 < 2^32 then
				offset_ct = 'uint32_t'
			end

			--compute max row size, including d.o.t.
			local max_rec_size = max_rec_size + dot_len * sizeof(offset_ct)
			if cols == key_cols then
				assert(max_rec_size <= self:db_max_key_size(),
					'pk too big: %d bytes (max is %d bytes)', max_rec_size, self:db_max_key_size())
			end
			cols.max_rec_size = max_rec_size

			--compute row layout: fixsize cols, d.o.t., varsize cols.
			local ct = 'struct __attribute__((__packed__)) {\n'
			--cols at a fixed offset: direct access.
			for i=1,fixsize_n do
				local col = cols[i]
				ct = ct .. '\t' .. col.elem_ct .. ' ' .. col.name
					.. (col.len and '[' .. col.len .. ']' or '') .. ';\n'
			end
			--cols at a dynamic offset: d.o.t.
			if dot_len > 0 then
				ct = ct .. '\t' .. offset_ct .. ' _offsets_['..dot_len..'];\n'
			end
			--first col after d.o.t. at fixed offset.
			if fixsize_n < #cols then
				local col = cols[fixsize_n+1]
				ct = ct .. '\t' .. col.elem_ct .. ' ' .. col.name .. (
						col.maxlen and '[?]' or
						col.len and '[' .. col.len ..']' or ''
					) .. ';\n'
			end
			--all other cols are at dyn. offsets so can't be struct fields.
			ct = ct .. '}'
			--pr(ct)
			ct = ctype(ct)
			cols.ct = ct
			cols.pct = ctype('$*', ct)

			--generate value encoders and decoders
			local dot_index = -1
			local function pass1(v) return v end
			for i,col in ipairs(cols) do

				local elem_size = col.elem_size
				local elem_ct = col.elem_ct
				local elem_p_ct = ctype(elem_ct..'*')
				local COL = col.name

				local geti, seti, getp, getsize, getlen, resize

				local isarray = col.len or col.maxlen

				if i <= fixsize_n+1 then --value at fixed offset
					if isarray then --fixsize or varsize at fixed offset
						function geti(buf, i)
							return buf[COL][i]
						end
						function seti(buf, i, val)
							buf[COL][i] = val
						end
						function getp(buf)
							return cast(elem_p_ct, buf[COL])
						end
						if col.maxlen then --varsize at fixed offset (first col after d.o.t.)
							local col_offset = offsetof(cols.ct, COL)
							if dot_len == 0 then --last col, no d.o.t.
								function getsize(buf, rec_sz)
									local next_offset = rec_sz
									return next_offset - col_offset
								end
								resize = noop
							else --d.o.t. follows
								function getsize(buf, rec_sz)
									local next_offset = buf._offsets_[0]
									return next_offset - col_offset
								end
								resize = true
							end
						end
					else --single value at fixed offset
						function geti(buf)
							return buf[COL]
						end
						function seti(buf, i, val)
							buf[COL] = val
						end
						local offset = offsetof(ct, COL)
						function getp(buf)
							return cast(elem_p_ct, cast(u8p, buf) + offset)
						end
					end
				else --value at dynamic offset
					dot_index = dot_index + 1
					local OFFSET = dot_index
					function getp(buf)
						local offset = buf._offsets_[OFFSET]
						return cast(elem_p_ct, cast(u8p, buf) + offset)
					end
					function geti(buf, i)
						return getp(buf)[i]
					end
					function seti(buf, i, val)
						getp(buf)[i] = val
					end
					if isarray then --fixsize or varsize at dyn offset
						if col.maxlen then --varsize at dyn offset
							if OFFSET == dot_len-1 then --last col, sz based on rec_sz
								function getsize(buf, rec_sz)
									local next_offset = rec_sz
									return next_offset - buf._offsets_[OFFSET]
								end
								resize = noop
							else
								function getsize(buf, rec_sz)
									local next_offset = buf._offsets_[OFFSET+1]
									return next_offset - buf._offsets_[OFFSET]
								end
								resize = true
							end
						end
					else --single value at dyn offset
						--nothing to do.
					end
				end

				if cols == key_cols then
					local rawgeti = geti
					local rawseti = seti
					if elem_ct == 'uint32_t' or elem_ct == 'int32_t' then
						function geti(buf, i)
							return bswap(rawgeti(buf, i))
						end
						function seti(buf, i, val)
							rawseti(buf, i, bswap(val))
						end
					elseif elem_ct == 'uint16_t' then
						function geti(buf, i)
							return bswap(shl(rawgeti(buf, i), 16))
						end
						function seti(buf, i, val)
							rawseti(buf, i, bswap(shl(val, 16)))
						end
					elseif elem_ct == 'int8_t' then
						function geti(buf, i)
							return band(tonumber(rawgeti(buf, i)), 0xff) - 0x80 --flip sign
						end
						function seti(buf, i, val)
							rawseti(buf, i, xor(band(val, 0xff), 0x80)) --flip sign
						end
					elseif elem_ct == 'int16_t' then
						function geti(buf, i)
							local x = bswap(shl(tonumber(rawgeti(buf, i)), 16)) --swap lo/hi
							local y = x - 0x8000 --flip sign
							return y
						end
						function seti(buf, i, val)
							local x = xor(band(val, 0xffff), 0x8000) --flip sign
							local y = bswap(shl(x, 16)) --swap lo/hi
							rawseti(buf, i, y)
						end
					elseif elem_ct == 'int32_t' then
						function geti(buf, i)
							local x = bswap(tonumber(rawgeti(buf, i))) --swap bytes
							local y = x - 0x80000000 --flip sign
							return y
						end
						function seti(buf, i, val)
							local x = xor(band(val, 0xffffffff), 0x80000000) --flip sign
							local x = bswap(x) --swap bytes
							rawseti(buf, i, x)
						end
					elseif elem_ct == 'double' then
						function geti(buf, i)
							return decode_f64(rawgeti(buf, i))
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_f64(val))
						end
					elseif elem_ct == 'float' then
						function geti(buf, i)
							return decode_f32(rawgeti(buf, i))
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_f32(val))
						end
					elseif elem_ct == 'uint64_t' then
						function geti(buf, i)
							return decode_u64(rawgeti(buf, i))
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_u64(val))
						end
					elseif elem_ct == 'int64_t' then
						function geti(buf, i)
							return decode_i64(rawgeti(buf, i))
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_i64(val))
						end
					end
				end

				if col.len then
					local len = col.len
					function getlen()
						return len
					end
					assert(not getsize)
					function getsize()
						return len * elem_size
					end
					resize = noop
				else
					local elem_size_bits = ln(elem_size) / ln(2)
					if elem_size_bits == floor(elem_size_bits) then --power-of-two
						function getlen(buf, rec_sz)
							return shr(getsize(buf, rec_sz), elem_size_bits)
						end
					else
						function getlen(buf, rec_sz)
							return getsize(buf, rec_sz) / elem_size
						end
					end
				end

				local rawgeti = geti
				local rawseti = seti

				function geti(buf, rec_sz, i)
					assert(i >= 0 and i <= getlen(buf, rec_sz)-1, 'index out of range')
					return rawgeti(buf, i)
				end
				function seti(buf, rec_sz, i, val)
					assert(i >= 0 and i <= getlen(buf, rec_sz)-1, 'index out of range')
					rawseti(buf, i, val)
				end

				local maxsize = (col.len or col.maxlen or 1) * elem_size
				function col.rawset(buf, rec_sz, val, sz)
					sz = min(maxsize, sz or #val) --truncate
					copy(getp(buf), val, sz)
				end

				function col.rawget(buf, rec_sz)
					local sz = getsize(buf, rec_sz)
					return cast(voidp, getp(buf)), sz
				end

				if isarray then
					local maxlen = col.len or col.maxlen
					function set(buf, rec_sz, val, len)
						assertf(typeof(val) == 'table', 'invalid val type %s', typeof(val))
						len = min(maxlen, len or #val) --truncate
						for i = 1, len do
							rawseti(buf, val[i], i-1)
						end
					end
				else
					function get(buf, rec_sz)
						return rawgeti(buf, 0)
					end
					function set(buf, rec_sz, val)
						rawseti(buf, 0, val)
					end
				end

				function col.rawstr(buf, rec_sz)
					local sz = getsize(buf, rec_sz)
					return str(getp(buf), sz)
				end

				if resize == true then --varsize field with fields following it.
					local pct = cols.pct
					local OFFSET = dot_index
					function resize(buf, offset, rec_sz, len)
						local set_buf = cast(pct, buf + offset)
						local sz0 = getsize(set_buf, rec_sz)
						local shift_sz = len * elem_size - sz0
						local next_offset = set_buf._offsets_[OFFSET+1]
						if shift_sz > 0 then --grow: shift right
							copy(
								buf + offset + next_offset + shift_sz,
								buf + offset + next_offset,
								rec_sz - next_offset)
						else --shrink: shift left
							copy(
								buf + offset + next_offset,
								buf + offset + next_offset - shift_sz,
								rec_sz - next_offset + shift_sz)
						end
						for i = OFFSET+1, dot_len-1 do
							set_buf._offsets_[i] = set_buf._offsets_[i] + shift_sz
						end
						return rec_sz + shift_sz
					end
				end

				col.geti = geti
				col.seti = seti
				col.get = get
				col.set = set
				col.getsize = getsize
				col.getlen = getlen
				col.getp = getp
				col.resize = resize

			end --for col in cols

		end --for cols in key_cols, val_cols

		--pr(table_schema)

	end --for table in schema_tables

end

local function val_len(col, val) --truncates the value if needed.
	local max_len = col.maxlen or col.len
	return max_len and min(#val, max_len) or 1
end

local function rec_size(self, cols, vals)
	local sz = sizeof(cols.ct, 0) --fixsize cols + d.o.t.
	for i = cols.fixsize_n+1, #cols do
		local col = cols[i]
		local padded_val_len = col.len
			or col.maxlen and min(#vals[col.name], col.maxlen)
			or 1
		sz = sz + padded_val_len * col.elem_size
	end
	return sz
end

function key_cols(self, tbl)
	return self.schema.tables[tbl].key_cols
end
function val_cols(self, tbl)
	return self.schema.tables[tbl].val_cols
end

function Db:encoded_key_size(tbl, vals)
	return rec_size(self, key_cols(self, tbl), vals)
end

function Db:encoded_val_size(tbl, vals)
	return rec_size(self, val_cols(self, tbl), vals)
end

function Db:max_key_size(tbl)
	return key_cols(self, tbl).max_rec_size
end

function Db:max_val_size(tbl)
	return val_cols(self, tbl).max_rec_size
end

local function encode_rec(self, cols, vals, buf, offset, buf_sz)
	offset = offset or 0
	local rec_sz = rec_size(self, cols, vals)
	assert(buf_sz - offset >= rec_sz, 'buffer too short')
	local set_buf = cast(cols.pct, buf + offset)
	local fixsize_n = cols.fixsize_n
	for i=1,fixsize_n do
		local col = cols[i]
		local val = vals[col.name]
		local len = val_len(col, val)
		col.set(set_buf, rec_sz, val, len)
	end
	local offset = sizeof(cols.ct, 0)
	local NEXT_OFFSET = 0
	for i = fixsize_n+1, #cols do
		local col = cols[i]
		local val = vals[col.name]
		local len = val_len(col, val)
		col.set(set_buf, rec_sz, val, len)
		offset = offset + (col.len or len) * col.elem_size
		if i < #cols then
			set_buf._offsets_[NEXT_OFFSET] = offset
			NEXT_OFFSET = NEXT_OFFSET + 1
		end
	end
	return rec_sz
end

local function encode_rec_col(self, cols, col, val, buf, offset, rec_sz)
	local col = assertf(cols[col], 'unknown field: %s', col)
	local val_len = val_len(col, val)
	rec_sz = col.resize(buf, offset, rec_sz, val_len) or rec_sz
	local set_buf = cast(cols.pct, buf + offset)
	col.set(set_buf, rec_sz, val, val_len)
	return rec_sz
end

function Db:encode_key(tbl, vals, buf, buf_sz, offset)
	return encode_rec(self, key_cols(self, tbl), vals, buf, offset, buf_sz)
end

function Db:encode_val(tbl, vals, buf, buf_sz, offset)
	return encode_rec(self, val_cols(self, tbl), vals, buf, offset, buf_sz)
end

function Db:encode_key_col(tbl, col, val, buf, rec_sz, offset)
	return encode_rec_col(self, key_cols(self, tbl), col, val, buf, offset, rec_sz)
end

function Db:encode_val_col(tbl, col, val, buf, rec_sz, offset)
	return encode_rec_col(self, val_cols(self, tbl), col, val, buf, offset, rec_sz)
end

local function rec_col_tostring(self, cols, col, buf, offset, rec_sz)
	local buf = cast(cols.pct, buf + (offset or 0))
	local col = assertf(cols[col], 'unknown field: %s', col)
	return col.tostring(buf, rec_sz)
end

local function rec_col_decode(self, cols, col, buf, offset, rec_sz)
	local buf = cast(cols.pct, offset and buf + offset or buf)
	local col = assertf(cols[col], 'unknown field: %s', col)
	return col.get(buf, rec_sz)
end

function Db:key_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, key_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:val_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, val_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:decode_key(tbl, col, buf, rec_sz, offset)
	return rec_col_decode(self, key_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:decode_val(tbl, col, buf, rec_sz, offset)
	return rec_col_decode(self, val_cols(self, tbl), col, buf, offset, rec_sz)
end

--test -----------------------------------------------------------------------

if not ... then

	rm'mdbx_schema_test/mdbx.dat'
	rm'mdbx_schema_test/mdbx.lck'
	local db = mdbx.open('mdbx_schema_test')
	local schema = {tables = {}}
	local types = 'i32' -- 'u8 u16 u32 u64 i8 i16 i32 i64 f32 f64'
	for t in words(types) do
		schema.tables[t] = {fields = {{name = 'id', type = t}}, pk = 'id'}
	end
	db:load_schema(schema)
	for typ in words(types) do
		local key_max_sz = db:max_key_size(typ)
		local val_max_sz = db:max_val_size(typ)
		local buf_sz = key_max_sz + val_max_sz
		local buf = u8a(buf_sz)
		local tx = db:tx'w'
		local bits = tonumber(typ:sub(2))
		local ntyp = typ:sub(1,1)
		local nums =
			ntyp == 'u' and {0,1,2,2^bits-1} or
			ntyp == 'i' and {-2^(bits-1),-(2^(bits-1)-1),-2,-1,0,1,2,2^(bits-1)-2,2^(bits-1)-1} or
			ntyp == 'f' and {-2^52,-2,-1,-0.1,-0,0,0.1,1,2^52}
		assert(nums)
		for _,i in ipairs(nums) do
			local r = {id = i}
			local key_sz = db:encode_key(typ, r, buf, buf_sz)
			local val_sz = db:encode_val(typ, r, buf, buf_sz, key_max_sz)
			tx:put(typ, buf, key_sz, buf + key_max_sz, val_sz)
		end
		tx:commit()
		tx = db:tx'r'
		for k,v in tx:each(typ) do
			local id = db:decode_key(typ, 'id', k.data, k.size)
			--local k = str(k.data, k.size)
			--local v = str(v.data, v.size)
			pr(typ, id)
		end
		tx:commit()
	end
	db:close()

	--[[
	local db = mdbx.open('mdbx_schema_test')
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
