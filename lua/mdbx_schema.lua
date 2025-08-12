--[[

	mdbx schema: structured data and multi-key indexing for mdbx.
	Written by Cosmin Apreutsei. Public Domain.

	Data types:
		- ints: 8, 16, 32, 64 bit, signed/unsigned
		- floats: 32 and 64 bit
		- arrays: fixed-size and variable-size
		- nullable values

	Keys:
		- composite keys with per-field ordering
		- utf-8 ai_ci collation

]]

require'mdbx'
require'utf8proc'

local
	typeof, tonumber, shl, shr, xor, bnot, bswap, u8p, copy, cast =
	typeof, tonumber, shl, shr, xor, bnot, bswap, u8p, copy, cast

local col_ct = {
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
	utf8   = 'uint8_t',
}

local key_col_ct = {
	f64 = 'uint64_t',
	f32 = 'uint32_t',
	i8  = 'uint8_t',
}

local function decode_u8(x, desc)
	if desc then x = band(bnot(x), 0xff) end
	return x
end
local function encode_u8(x, desc)
	local x = tonumber(x) --(u)int64 -> number (truncate)
	if desc then x = bnot(x) end
	return x --uint32 -> uint8 (truncate)
end

function decode_u16(x, desc)
	if desc then x = bnot(x) end
	return bswap(shl(x, 16)) --BE->LE & truncate
end
function encode_u16(x, desc)
	local x = tonumber(x) --(u)int64 -> number (truncate)
	local x = bswap(shl(x, 16)) --LE->BE
	if desc then x = bnot(x) end
	return x
end

function decode_u32(x, desc)
	if desc then x = bnot(x) end
	local x = bswap(x) --BE->LE
	return x + shr(x, 31) * 0x100000000 --because bitops are signed
end
function encode_u32(x, desc)
	local x = tonumber(x) --(u)int64 -> number (truncate)
	local x = bswap(x) --LE->BE
	if desc then x = bnot(x) end
	return x
end

local u = new'union { uint64_t u; double f; struct { int32_t s1; int32_t s2; }; }'
local function decode_f64(v, desc)
	u.u = v
	if desc then
		u.s1 = bnot(u.s1)
		u.s2 = bnot(u.s2)
	end
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	if shr(u.u, 63) ~= 0 then
		u.u = xor(u.u, 0x8000000000000000ULL)
	else
		u.u = bnot(u.u)
	end
	return tonumber(u.f)
end
local function encode_f64(v, desc)
	u.f = v
	if shr(u.u, 63) ~= 0 then
		u.u = bnot(u.u)
	else
		u.u = xor(u.u, 0x8000000000000000ULL)
	end
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	if desc then
		u.s1 = bnot(u.s1)
		u.s2 = bnot(u.s2)
	end
	return u.u
end

local u = new'union { uint32_t u; float f; int32_t s; }'
local function decode_f32(v, desc)
	u.u = v
	if desc then u.s = bnot(u.s) end
	u.s = bswap(u.s)
	if shr(u.u, 31) ~= 0 then
		u.u = xor(u.u, 0x80000000)
	else
		u.u = bnot(u.u)
	end
	return tonumber(u.f)
end
local function encode_f32(v, desc)
	u.f = v
	if shr(u.u, 31) ~= 0 then
		u.u = bnot(u.u)
	else
		u.u = xor(u.u, 0x80000000)
	end
	u.s = bswap(u.s)
	if desc then u.s = bnot(u.s) end
	return u.u
end

local u = new'union { uint64_t u; int64_t i; struct { int32_t s1; int32_t s2; }; }'
local function decode_u64(v, desc)
	u.u = v
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	if desc then
		u.s1 = bnot(u.s1)
		u.s2 = bnot(u.s2)
	end
	return u.u
end
local encode_u64 = decode_u64
local function decode_i64(v, desc)
	u.i = v
	if desc then
		u.s1 = bnot(u.s1)
		u.s2 = bnot(u.s2)
	end
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	u.u = xor(u.u, 0x8000000000000000ULL)
	return u.i
end
local function encode_i64(v, desc)
	u.i = v
	u.u = xor(u.u, 0x8000000000000000ULL)
	u.s1, u.s2 = bswap(u.s2), bswap(u.s1)
	if desc then
		u.s1 = bnot(u.s1)
		u.s2 = bnot(u.s2)
	end
	return u.i
end

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

local is_null_set_null_funcs = memoize_multiret(function(i)
	local byte_i = shr(i-1, 3)
	local bit_i = band(i-1, 7)
	local mask = shl(1, bit_i)
	local function is_null(buf)
		return band(buf._nulls_[byte_i], mask) ~= 0
	end
	local function set_null(buf, is_null)
		local b = buf._nulls_[byte_i]
		if is_null then
			buf._nulls_[byte_i] = bor(buf._nulls_[byte_i], mask)
		else
			buf._nulls_[byte_i] = band(buf._nulls_[byte_i], bnot(mask))
		end
	end
	return is_null, set_null
end)

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

	local tx = self:tx'w'
	for table_name in pairs(schema.tables) do
		tx:open_table(table_name, true)
	end
	tx:commit()

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
				elem_ct = key_col_ct[col.type] or elem_ct
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
			local set_offset, get_offset
			if max_rec_size - dot_len < 2^8 then
				offset_ct = 'uint8_t'
				function get_offset(buf, i)
					return buf._offsets_[i]
				end
				function set_offset(buf, i, offset)
					buf._offsets_[i] = offset
				end
			elseif max_rec_size - dot_len * 2 < 2^16 then
				offset_ct = 'uint16_t'
				function get_offset(buf, i)
					return decode_u16(buf._offsets_[i], desc)
				end
				function set_offset(buf, i, offset)
					buf._offsets_[i] = offset
				end
			elseif max_rec_size - dot_len * 4 < 2^32 then
				offset_ct = 'uint32_t'
				function get_offset(buf, i)
					return buf._offsets_[i]
				end
				function set_offset(buf, i, offset)
					buf._offsets_[i] = offset
				end
			end

			--compute max row size, including d.o.t.
			local max_rec_size = max_rec_size + dot_len * sizeof(offset_ct)
			if cols == key_cols then
				assert(max_rec_size <= self:db_max_key_size(),
					'pk too big: %d bytes (max is %d bytes)', max_rec_size, self:db_max_key_size())
			end
			cols.max_rec_size = max_rec_size

			--compute row layout: fixsize cols, d.o.t., varsize cols.
			local ct = {'struct __attribute__((__packed__)) {\n'}
			--add the nulls bit array
			if cols == val_cols then
				local n = ceil(#cols / 8) --number of byes needed to hold all the bits.
				append(ct, '\t', 'uint8_t _nulls_[', n, '];\n')
			end
			--cols at a fixed offset: direct access.
			for i=1,fixsize_n do
				local col = cols[i]
				append(ct, '\t', col.elem_ct, ' ', col.name)
				if col.len then append(ct, '[', col.len, ']') end
				append(ct, ';\n')
			end
			--cols at a dynamic offset: d.o.t.
			if dot_len > 0 then
				append(ct, '\t', offset_ct, ' _offsets_[', dot_len, '];\n')
			end
			--first col after d.o.t. at fixed offset.
			if fixsize_n < #cols then
				local col = cols[fixsize_n+1]
				append(ct, '\t', col.elem_ct, ' ', col.name)
				if col.maxlen then append(ct, '[?]')
				elseif col.len then append(ct, '[', col.len, ']')
				end
				append(ct, ';\n')
			end
			--all other cols are at dyn. offsets so can't be struct fields.
			append(ct, '}')
			local ct = cat(ct)
			--pr(ct)
			ct = ctype(ct)
			cols.ct = ct
			cols.pct = ctype('$*', ct)

			--generate value encoders and decoders
			local dot_index = -1
			local function pass1(v) return v end
			for i,col in ipairs(cols) do

				local elem_size = col.elem_size
				local elem_p_ct = ctype(col.elem_ct..'*')
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
									local next_offset = get_offset(buf, 0)
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
						local offset = get_offset(buf, OFFSET)
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
									return next_offset - get_offset(buf, OFFSET)
								end
								resize = noop
							else
								function getsize(buf, rec_sz)
									local next_offset = get_offset(buf, OFFSET+1)
									return next_offset - get_offset(buf, OFFSET)
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
					local desc = col.order == 'desc'
					local t = col.type
					if t == 'u32' then
						function geti(buf, i)
							return decode_u32(rawgeti(buf, i), desc)
						end
						function seti(buf, i, x)
							rawseti(buf, i, encode_u32(x, desc))
						end
					elseif t == 'i32' then
						function geti(buf, i)
							local x = rawgeti(buf, i)
							if desc then x = bnot(x) end
							local x = bswap(x) --BE->LE
							return xor(x, 0x80000000) --flip sign
						end
						function seti(buf, i, x)
							local x = tonumber(x) --(u)int64 -> number (truncate)
							local x = xor(x, 0x80000000) --flip sign
							local x = bswap(x) --LE->BE
							if desc then x = bnot(x) end
							rawseti(buf, i, x)
						end
					elseif t == 'u16' then
						function geti(buf, i)
							return decode_u16(rawgeti(buf, i), desc)
						end
						function seti(buf, i, x)
							rawseti(buf, i, encode_u16(x, desc))
						end
					elseif t == 'i16' then
						function geti(buf, i)
							local x = rawgeti(buf, i)
							if desc then x = bnot(x) end
							local x = bswap(shl(x, 16)) --BE->LE & truncate
							return x - 0x8000 --flip sign
						end
						function seti(buf, i, x)
							local x = tonumber(x) --(u)int64 -> number (truncate)
							local x = xor(x, 0x8000) --flip 16bit sign bit
							local x = bswap(shl(x, 16)) --LE->BE
							if desc then x = bnot(x) end
							rawseti(buf, i, x) --int32 -> int16 (truncate)
						end
					elseif t == 'i8' then
						function geti(buf, i)
							local x = rawgeti(buf, i)
							if desc then x = band(bnot(x), 0xff) end
							return x - 0x80 --flip sign
						end
						function seti(buf, i, x)
							local x = tonumber(x) --(u)int64 -> number (truncate)
							local x = xor(x, 0x80) --flip int8 sign bit
							if desc then x = bnot(x) end
							rawseti(buf, i, x) --int32 -> uint8 (truncate)
						end
					elseif t == 'u8' or t == 'utf8' then
						function geti(buf, i)
							return decode_u8(rawgeti(buf, i), desc)
						end
						function seti(buf, i, x)
							rawseti(buf, i, encode_u8(x, desc)) --uint32 -> uint8 (truncate)
						end
					elseif t == 'u64' then
						function geti(buf, i)
							return decode_u64(rawgeti(buf, i), desc)
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_u64(val, desc))
						end
					elseif t == 'i64' then
						function geti(buf, i)
							return decode_i64(rawgeti(buf, i), desc)
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_i64(val, desc))
						end
					elseif t == 'f64' then
						function geti(buf, i)
							return decode_f64(rawgeti(buf, i), desc)
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_f64(val, desc))
						end
					elseif t == 'f32' then
						function geti(buf, i)
							return decode_f32(rawgeti(buf, i), desc)
						end
						function seti(buf, i, val)
							rawseti(buf, i, encode_f32(val, desc))
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
					local elem_size_bits = log2(elem_size)
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

				local maxsize = (col.len or col.maxlen or 1) * elem_size
				local function rawset(buf, rec_sz, val, sz)
					sz = min(maxsize, sz or #val) --truncate
					copy(getp(buf), val, sz)
				end

				local function rawget(buf, rec_sz)
					local sz = getsize(buf, rec_sz)
					return cast(u8p, getp(buf)), sz
				end

				local function rawstr(buf, rec_sz)
					return str(rawget(buf, rec_sz))
				end

				if isarray then
					local maxlen = col.len or col.maxlen
					if col.type == 'utf8' then
						local desc = col.order == 'desc'
						local ai_ci = col.collation == 'utf8_ai_ci'
						if desc or ai_ci then
							local rawgeti = geti
							local rawseti = seti
							function set(buf, rec_sz, s, len)
								local len = min(maxlen, len or #s) --truncate
								local p
								if ai_ci then
									p, len = encode_ai_ci(s, len)
								else
									p = cast(u8p, s)
								end
								if desc then
									for i = 0, len-1 do
										rawseti(buf, i, p[i])
									end
								else
									rawset(buf, rec_sz, p, len)
								end
							end
							function get(buf, rec_sz, out, out_len)
								local p, p_len = rawget(buf, rec_sz)
								local len = min(p_len, out_len or 1/0) --truncate
								local out = out and cast(u8p, out) or u8a(len)
								for i = 0, len-1 do
									out[i] = rawgeti(buf, i)
								end
								return out, len
							end
						else
							function get(buf, rec_sz, out, out_len)
								local p, p_len = rawget(buf, rec_sz)
								local len = min(p_len, out_len or 1/0) --truncate
								local out = out and cast(u8p, out) or u8a(len)
								copy(out, p, len)
								return out, len
							end
							set = rawset
						end
					else
						function set(buf, rec_sz, val, len)
							assertf(typeof(val) == 'table', 'invalid val type %s', typeof(val))
							len = min(maxlen, len or #val) --truncate
							for i = 1, len do
								rawseti(buf, i-1, val[i])
							end
						end
					end
				else
					function get(buf, rec_sz)
						return geti(buf, 0)
					end
					function set(buf, rec_sz, val)
						seti(buf, 0, val)
					end
				end

				--varsize field with more fields following it. resizing it
				--requires shifting all those fields left or right.
				if resize == true then
					local pct = cols.pct
					local OFFSET = dot_index
					function resize(buf, offset, rec_sz, len)
						local set_buf = cast(pct, buf + offset)
						local sz0 = getsize(set_buf, rec_sz)
						local sz1 = len * elem_size
						local shift_sz = sz1 - sz0 --positive for growth
						local next_offset = get_offset(set_buf, OFFSET+1)
						copy(
							buf + offset + next_offset + shift_sz,
							buf + offset + next_offset,
							rec_sz - next_offset)
						for i = OFFSET+1, dot_len-1 do
							set_offset(set_buf, i, get_offset(set_buf, i) + shift_sz)
						end
						return rec_sz + shift_sz
					end
				end

				if cols == val_cols then
					col.is_null, col.set_null = is_null_set_null_funcs(i)
				end

				function col.geti(buf, rec_sz, i)
					assert(i >= 0 and i <= getlen(buf, rec_sz)-1, 'index out of range')
					return geti(buf, i)
				end
				function col.seti(buf, rec_sz, i, val)
					assert(i >= 0 and i <= getlen(buf, rec_sz)-1, 'index out of range')
					rawseti(buf, i, val)
				end

				col.get = get
				col.set = set
				col.rawset = rawset
				col.rawget = rawget
				col.getp = getp
				col.rawstr = rawstr
				col.getsize = getsize
				col.getlen = getlen
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
			set_offset(set_buf, NEXT_OFFSET, offset)
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
	local col = assertf(cols[col], 'unknown field: %s', col)
	local buf = cast(cols.pct, buf + (offset or 0))
	return col.tostring(buf, rec_sz)
end

function Db:set_null(tbl, col, is_null, buf, offset)
	local cols = val_cols(self, tbl)
	local col = assertf(cols[col], 'unknown field: %s', col)
	local buf = cast(cols.pct, buf + (offset or 0))
	col.set_null(buf, is_null)
end

function Db:is_null(tbl, col, buf, offset)
	local cols = val_cols(self, tbl)
	local col = assertf(cols[col], 'unknown field: %s', col)
	local buf = cast(cols.pct, buf + (offset or 0))
	return col.is_null(buf)
end

local function rec_col_decode(self, cols, col, buf, offset, rec_sz, out, out_len)
	rec_sz = tonumber(rec_sz)
	local buf = cast(cols.pct, offset and buf + offset or buf)
	local col = assertf(cols[col], 'unknown field: %s', col)
	return col.get(buf, rec_sz, out, out_len)
end

function Db:key_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, key_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:val_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, val_cols(self, tbl), col, buf, offset, rec_sz)
end

function Db:decode_key(tbl, col, buf, rec_sz, offset, out, out_len)
	return rec_col_decode(self, key_cols(self, tbl), col, buf, offset, rec_sz, out, out_len)
end

function Db:decode_val(tbl, col, buf, rec_sz, offset, out, out_len)
	return rec_col_decode(self, val_cols(self, tbl), col, buf, offset, rec_sz, out, out_len)
end

function mdbx_tx:put_records(tbl_name, records)
	local key_max_sz = self.db:max_key_size(tbl_name)
	local val_max_sz = self.db:max_val_size(tbl_name)
	local buf_sz = key_max_sz + val_max_sz
	local buf = u8a(buf_sz)
	for _,rec in ipairs(records) do
		local key_sz = self.db:encode_key(tbl_name, rec, buf, buf_sz)
		local val_sz = self.db:encode_val(tbl_name, rec, buf, buf_sz, key_max_sz)
		self:put(tbl_name, buf, key_sz, buf + key_max_sz, val_sz)
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
			add(t, {id = i})
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
			{s = 'a' },
			{s = 'bb'},
			{s = 'aa'},
			{s = 'b' },
		}
		local tx = db:tx'w'
		tx:put_records(tbl.name, t)
		tx:commit()

		local tx = db:tx()
		for k,v in tx:each(tbl.name) do
			local s, len = db:decode_key(tbl.name, 's', k.data, tonumber(k.size))
			pr(str(s, len))
		end
		tx:commit()
	end

	--test varsize2
	for _,tbl in ipairs(varsize2_tables) do
		local t = {
			{s1 = 'a'    , s2 = 'b' , },
			{s1 = 'bb'   , s2 = 'aa', },
			{s1 = 'aa'   , s2 = 'bb', },
			{s1 = 'b'    , s2 = 'a' , },
		}
		local tx = db:tx'w'
		tx:put_records(tbl.name, t)
		tx:commit()
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
