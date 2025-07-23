--go@ c:\tools\plink.exe -i c:\users\woods\.ssh\id_ed25519.ppk root@172.20.10.9 ~/sdk/bin/debian12/luajit sdk/lua/*
--go@ ssh -ic:\users\cosmin\.ssh\id_ed25519 root@10.0.0.8 ~/sdk/bin/linux/luajit ~/sdk/lua/*

--TODO: big-endian ints.
--TODO: signed ints.
--TODO: floats & doubles.
--TODO: descending keys.
--TODO: .

require'mdbx'

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

local Db = mdbx.Db

function Db:load_schema()

	local schema_file = self.dir..'/schema.lua'
	if not self.readonly then
		mkdir(self.dir)
		if not exists(schema_file) then
			save(schema_file, 'return {}')
		end
	end
	self.schema = eval_file(schema_file)

	self:open_tables(self.schema.tables)

	--compute column layout and encoding parameters based on schema.
	--NOTE: changing this algorithm or making it non-deterministic in any way
	--will trash your existing databses, so better version it or something!
	for table_name, table_schema in pairs(self.schema.tables or {}) do

		--split cols into key cols and val cols based on pk.
		local pk_cols = {}
		local pk_order = {}
		for s in words(table_schema.pk) do
			local pk, order = s:match'^(.-):(.*)'
			if not pk then
				pk, order = s, 'asc'
			else
				assert(order == 'desc' or order == 'asc')
				assert(#pk > 0)
			end
			add(pk_cols, pk)
			add(pk_order, order)
		end
		local key_cols = {}
		local val_cols = {}
		for i,col in ipairs(table_schema.fields) do
			if indexof(col.name, pk_cols) then
				add(key_cols, col)
				key_cols[col.name] = col
				col.index = #key_cols
			else
				add(val_cols, col)
				val_cols[col.name] = col
				col.index = #val_cols
			end
			--typecheck the column while we're at it.
			col.elem_ct = assertf(col_ct[col.type], 'unknown col type %s', col.type)
			col.elem_size = sizeof(col.elem_ct)
			assert(col.elem_size < 2^4) --must fit 4bit (see sort below)
			--index fields by name and check for duplicate names.
			assertf(not table_schema.fields[col.name], 'duplicate column name: %s', col.name)
			table_schema.fields[col.name] = col
		end
		assert(#key_cols < 2^16)
		assert(#val_cols < 2^16)

		table_schema.key_cols = key_cols
		table_schema.val_cols = val_cols

		for _,cols in ipairs{key_cols, val_cols} do

			if cols == val_cols then --we can layout val_cols freely.
				--move varsize cols at the end to minimize the size of the dyn offset table.
				--order cols by elem_size to maintain alignment.
				sort(cols, function(col1, col2)
					--elem_size fits in 4bit; col_index fits in 16bit; 4+16 = 20 bits,
					--so any bit from bit 21+ can be used for extra conditions.
					local i1 = (col1.maxlen and 2^22 or 0) + (2^4-1 - col1.elem_size) * 2^16 + col1.index
					local i2 = (col2.maxlen and 2^22 or 0) + (2^4-1 - col2.elem_size) * 2^16 + col2.index
					return i1 < i2
				end)
			end

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
			local offset = 0
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
						col.len and '[' .. col.len ..']' or
						''
					) .. ';\n'
			end
			--all other cols are at dyn. offsets so can't be struct fields.
			ct = ct .. '}'
			pr(ct)
			local ct = ctype(ct)
			cols.ct = ct
			cols.pct = ctype('$*', ct)

			--generate value encoders and decoders
			for i,col in ipairs(cols) do
				if col.len then
					col.size = function()
						return col.len * elem_size
					end
					col.resize = noop
				end
				local elem_p_ct = ctype(col.elem_ct..'*')
				local COL = col.name
				if i <= fixsize_n+1 then
					if col.len or col.maxlen then --fixsize or varsize at fixed offset
						col.seti = function(buf, rec_sz, val, i)
							buf[COL][i] = val
						end
						local elem_size = col.elem_size
						local typeof = typeof
						col.set = function(buf, val, len)
							local tval = typeof(val)
							if tval == 'string' or tval == 'cdata' then
								copy(buf[COL], val, len * elem_size)
							elseif tval == 'table' then
								for i = 1, len do
									buf[COL][i-1] = val[i]
								end
							else
								assertf(false, 'invalid val type %s', tval)
							end
						end
						col.get = function(buf, rec_sz)
							return cast(elem_p_ct, buf[COL])
						end
						col.tostring = function(buf, rec_sz)
							local sz = col.size(buf, rec_sz)
							return str(buf[COL], sz)
						end
						if col.maxlen then --varsize at fixed offset (first col after d.o.t.)
							local col_offset = offsetof(ct, COL)
							if dot_len > 0 then --d.o.t. follows
								col.size = function(buf, rec_sz)
									return buf._offsets_[0] - col_offset
								end
								col.resize = function(buf, offset, rec_sz, len)
									local sz0 = col.size(buf, rec_sz)
									local shift_sz = len * elem_size - sz0
									local next_offset = buf._offsets_[0]
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
									for i = 0, dot_len-1 do
										buf._offsets_[i] = buf._offsets_[i] + shift_sz
									end
									return rec_sz + shift_sz
								end
							end
						end
					else --value at fixed offset
						col.set = function(buf, val)
							buf[COL] = val
						end
						col.get = function(buf, rec_sz)
							return buf[COL]
						end
					end
				else
					local OFFSET = i - (fixsize_n+2)
					col.get = function(buf, rec_sz)
						local offset = buf._offsets_[OFFSET]
						return cast(elem_p_ct, cast(u8p, buf) + offset)
					end
					local elem_size = col.elem_size
					col.tostring = function(buf, rec_sz)
						local sz = col.size(buf, rec_sz)
						local offset = buf._offsets_[OFFSET]
						return str(cast(u8p, buf) + offset, sz)
					end
					if col.len or col.maxlen then --fixsize or varsize at dyn offset
						col.seti = function(buf, rec_sz, val, i)
							local offset = buf._offsets_[OFFSET]
							cast(elem_p_ct, cast(u8p, buf) + offset)[i] = val
						end
						local elem_size = col.elem_size
						local typeof = typeof
						col.set = function(buf, val, len)
							local tval = typeof(val)
							local offset = buf._offsets_[OFFSET]
							if tval == 'string' or tval == 'cdata' then
								copy(cast(u8p, buf) + offset, val, len * elem_size)
							elseif tval == 'table' then
								for i = 1, len do
									cast(elem_p_ct, cast(u8p, buf) + offset)[i-1] = val[i]
								end
							else
								assertf(false, 'invalid val type %s', tval)
							end
						end
						if col.maxlen then --varsize at dyn offset
							if OFFSET == dot_len-1 then --last col, sz based on rec_sz
								col.size = function(buf, rec_sz)
									local next_offset = rec_sz
									return next_offset - buf._offsets_[OFFSET]
								end
								col.resize = noop
							else
								col.size = function(buf, rec_sz)
									local next_offset = buf._offsets_[OFFSET+1]
									return next_offset - buf._offsets_[OFFSET]
								end
								--make room for n more elements.
								col.resize = function(buf, offset, rec_sz, len)
									local sz0 = col.size(buf, rec_sz)
									local shift_sz = len * elem_size - sz0
									local next_offset = buf._offsets_[OFFSET+1]
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
										buf._offsets_[i] = buf._offsets_[i] + shift_sz
									end
									return rec_sz + shift_sz
								end
							end
						end
					else --value at dyn offset
						col.set = function(buf, val)
							local offset = buf._offsets_[OFFSET]
							cast(elem_p_ct, cast(u8p, buf) + offset)[0] = val
						end
					end
				end
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
		local padded_val_len = col.len or col.maxlen and min(#vals[col.name], col.maxlen) or 1
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

local function encode_rec(self, cols, vals, buf, buf_sz, offset)
	offset = offset or 0
	local rec_sz = rec_size(self, cols, vals)
	assert(buf_sz - offset >= rec_sz, 'buffer too short')
	local set_buf = cast(cols.pct, buf + offset)
	local fixsize_n = cols.fixsize_n
	for i=1,fixsize_n do
		local col = cols[i]
		local val = vals[col.name]
		local len = val_len(col, val)
		col.set(set_buf, val, len)
	end
	local offset = sizeof(cols.ct, 0)
	local NEXT_OFFSET = 0
	for i = fixsize_n+1, #cols do
		local col = cols[i]
		local val = vals[col.name]
		local len = val_len(col, val)
		col.set(set_buf, val, len)
		offset = offset + (col.len or len) * col.elem_size
		if i < #cols then
			set_buf._offsets_[NEXT_OFFSET] = offset
			NEXT_OFFSET = NEXT_OFFSET + 1
		end
	end
end

local function encode_rec_col(self, cols, col, val, buf, offset, rec_sz)
	local col = assertf(cols[col], 'unknown column: %s', col)
	local val_len = val_len(col, val)
	rec_sz = col.resize(buf, offset, rec_sz, val_len) or rec_sz
	local set_buf = cast(cols.pct, buf + offset)
	col.set(set_buf, val, val_len)
	return rec_sz
end

function Db:encode_key(tbl, vals, buf, buf_sz, offset)
	return encode_rec(self, key_cols(self, tbl), vals, buf, buf_sz, offset)
end

function Db:encode_val(tbl, vals, buf, buf_sz, offset)
	return encode_rec(self, val_cols(self, tbl), vals, buf, buf_sz, offset)
end

function Db:encode_key_col(tbl, col, val, buf, offset, rec_sz)
	return encode_rec_col(self, key_cols(self, tbl), col, val, buf, offset, rec_sz)
end

function Db:encode_val_col(tbl, col, val, buf, offset, rec_sz)
	return encode_rec_col(self, val_cols(self, tbl), col, val, buf, offset, rec_sz)
end

local function rec_col_tostring(self, cols, col, buf, offset, rec_sz)
	local buf = cast(cols.pct, buf + offset)
	local col = assertf(cols[col], 'unknown column: %s', col)
	return col.tostring(buf, rec_sz)
end

function Db:key_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, key_cols(self, tbl), col, buf, rec_sz, offset)
end

function Db:val_tostring(tbl, col, buf, rec_sz, offset)
	return rec_col_tostring(self, val_cols(self, tbl), col, buf, rec_sz, offset)
end

--test -----------------------------------------------------------------------

if not ... then

	local db = mdbx.open('mdbx_schema_test')
	db:load_schema()
	local u = {
		uid = 1234,
		active = 1,
		roles = {123, 321},
		email = 'admin@some.com',
		name = 'John Galt',
	}
	local key_sz = db:max_key_size'users'
	local val_sz = db:max_val_size'users'
	local buf_sz = key_sz + val_sz
	local buf = u8a(buf_sz)
	db:encode_key('users', u, buf, buf_sz)
	db:encode_val('users', u, buf, buf_sz, key_sz)
	db:encode_val_col('users', 'name', 'Dagny Taggart', buf, 0, sz)
	pr(db:val_tostring('users', 'email', buf, buf_sz, 0))
	pr(db:val_tostring('users', 'name' , buf, buf_sz, key_sz))
	--db:decode_val('users', u, buf, sz)
	db:close()

end
